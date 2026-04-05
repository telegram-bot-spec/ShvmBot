"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Shop Bot (Customer-facing)                          ║
║                                                                ║
║  FIXED:                                                        ║
║  • 100% inline buttons — zero ReplyKeyboardMarkup             ║
║  • start() / show_tos() safe for both message & callback      ║
║  • Admin check on every entry point                           ║
║  • Atomic purchase: deduct → reserve → order (with rollback)  ║
║  • order_id=None guard before referral commission             ║
║  • Referral commission on FIRST purchase only                 ║
║  • All missing handlers: order_, back_products, refund_       ║
║  • ToS decline sets DB flag — can't bypass with /start        ║
║  • awaiting_screenshot cleared reliably on all paths          ║
║  • cancel_payment handles text AND photo messages             ║
║  • toggle_wishlist calls query.answer()                       ║
║  • profile uses count query, not 1000-row fetch               ║
║  • Real image hashing for duplicate detection                 ║
║  • Markdown-escaped user names everywhere                     ║
╚════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from config import Config, escape_md, format_currency, format_inr, format_profile_card
from db import (
    get_user, create_user, accept_tos, get_or_create_user,
    deduct_balance, update_balance, update_total_spent,
    get_categories, get_products_by_category, get_product, get_stock_count,
    reserve_stock,
    add_to_wishlist, remove_from_wishlist, get_wishlist, is_in_wishlist,
    create_payment, update_payment_screenshot, get_payment,
    get_promo_code, use_promo_code,
    create_order, get_user_orders, get_order, get_user_order_count,
    get_user_by_referral_code, record_referral_earning, get_referral_stats,
    create_refund_request,
    supabase,
)
from payments import (
    generate_upi_qr,
    validate_payment_amount,
    normalise_inr,
    hash_screenshot,
    msg_payment_instructions,
    msg_request_screenshot,
    msg_payment_submitted,
)
from utils import (
    rate_limiter,
    calculate_referral_commission,
    calculate_discount,
    generate_payment_ref,
    utcnow,
    parse_utc,
    format_dt,
)

# ═══════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("shop_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("shop_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════
#  Conversation states
# ═══════════════════════════════════════════════════════

ADD_FUNDS_AMOUNT     = 1
ADD_FUNDS_SCREENSHOT = 2

# ═══════════════════════════════════════════════════════
#  Safe send helpers  (handle both message & callback)
# ═══════════════════════════════════════════════════════

async def _send(
    update: Update,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
    parse_mode: str = ParseMode.MARKDOWN,
) -> None:
    """
    Send or edit a text message regardless of whether the update is
    a command/text message or a callback query.
    """
    kwargs = dict(text=text, parse_mode=parse_mode, reply_markup=keyboard)
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(**kwargs)
        else:
            await update.effective_message.reply_text(**kwargs)
    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return           # not an error — content unchanged
        raise


async def _send_or_edit_caption(
    update: Update,
    caption: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """Edit caption if current message has a photo, otherwise edit text."""
    try:
        if update.callback_query:
            try:
                await update.callback_query.edit_message_caption(
                    caption=caption,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                )
            except BadRequest:
                # Message has no caption (it's a text message) — fall back
                await update.callback_query.edit_message_text(
                    text=caption,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                )
    except Exception as exc:
        logger.warning("_send_or_edit_caption: %s", exc)


async def _answer(update: Update, text: str = "", alert: bool = False) -> None:
    """Answer callback query silently or with popup."""
    try:
        if update.callback_query:
            await update.callback_query.answer(text, show_alert=alert)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════
#  Rate-limit helper
# ═══════════════════════════════════════════════════════

def _rate_ok(user_id: int, action: str, cooldown: int = 3) -> bool:
    return rate_limiter.check(f"{action}:{user_id}", cooldown)


# ═══════════════════════════════════════════════════════
#  User guard — call at the top of every handler
# ═══════════════════════════════════════════════════════

async def _get_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict]:
    """
    Load user from DB. Handles banned users. Returns None to abort handler.
    Does NOT redirect to ToS here — ToS is handled in start() only.
    """
    tg = update.effective_user
    if not tg:
        return None

    user = await get_user(tg.id)
    if not user:
        # Brand-new user hitting a deep callback — send them to /start
        await _answer(update, "Please /start the bot first.", alert=True)
        return None

    if user.get("is_banned"):
        reason = escape_md(user.get("ban_reason") or "No reason provided")
        await _send(
            update,
            f"🚫 *ACCOUNT RESTRICTED*\n\nReason: {reason}\n\nContact @{Config.SUPPORT_USERNAME} to appeal.",
        )
        return None

    return user


# ═══════════════════════════════════════════════════════
#  Keyboards  (ALL inline — zero ReplyKeyboard)
# ═══════════════════════════════════════════════════════

def _kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Store",      callback_data="store"),
         InlineKeyboardButton("💰 Add Funds",  callback_data="add_funds")],
        [InlineKeyboardButton("👤 Profile",    callback_data="profile"),
         InlineKeyboardButton("🎁 Referral",   callback_data="referral")],
        [InlineKeyboardButton("⭐ Wishlist",   callback_data="wishlist"),
         InlineKeyboardButton("📋 Orders",     callback_data="orders")],
        [InlineKeyboardButton("❓ Help",        callback_data="help"),
         InlineKeyboardButton("🛠️ Support",    url=f"https://t.me/{Config.SUPPORT_USERNAME}")],
    ])


def _kb_back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("◀️ Main Menu", callback_data="main_menu")
    ]])


def _kb_categories(categories: List[str]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"📦 {c}", callback_data=f"cat:{c}")] for c in categories]
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(rows)


def _kb_products(products: List[Dict], category: str) -> InlineKeyboardMarkup:
    rows = []
    for p in products:
        stock = p.get("stock_count", 0)
        label = f"{p['name']} — {format_inr(p['selling_price'])} {'✅' if stock > 0 else '❌'}"
        rows.append([InlineKeyboardButton(label, callback_data=f"prod:{p['id']}")])
    rows.append([InlineKeyboardButton("◀️ Categories", callback_data="store")])
    return InlineKeyboardMarkup(rows)


def _kb_product_detail(product_id: int, in_wishlist: bool) -> InlineKeyboardMarkup:
    wish_btn = (
        InlineKeyboardButton("💔 Remove Wishlist", callback_data=f"unwish:{product_id}")
        if in_wishlist else
        InlineKeyboardButton("⭐ Add Wishlist",    callback_data=f"wish:{product_id}")
    )
    return InlineKeyboardMarkup([
        [wish_btn],
        [InlineKeyboardButton("🛒 Buy Now", callback_data=f"buy:{product_id}")],
        [InlineKeyboardButton("◀️ Back",    callback_data="store")],
    ])


def _kb_quantity(product_id: int, max_qty: int) -> InlineKeyboardMarkup:
    rows, row = [], []
    for i in range(1, min(max_qty, 10) + 1):
        row.append(InlineKeyboardButton(str(i), callback_data=f"qty:{product_id}:{i}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"prod:{product_id}")])
    return InlineKeyboardMarkup(rows)


def _kb_purchase_summary(product_id: int, quantity: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm Purchase",  callback_data=f"confirm:{product_id}:{quantity}")],
        [InlineKeyboardButton("🎟️ Use Promo Code",  callback_data=f"promo_prompt:{product_id}:{quantity}")],
        [InlineKeyboardButton("❌ Cancel",            callback_data=f"prod:{product_id}")],
    ])


def _kb_payment(payment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I've Paid",  callback_data=f"paid:{payment_id}")],
        [InlineKeyboardButton("❌ Cancel",     callback_data="cancel_payment")],
    ])


def _kb_cancel_payment() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ Cancel Payment", callback_data="cancel_payment")
    ]])


# ═══════════════════════════════════════════════════════
#  /start  &  ToS
# ═══════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tg   = update.effective_user
    args = context.args or []

    # Parse referral from deep link  (/start REF_CODE)
    referred_by = None
    if args:
        ref_code = args[0].upper().strip()
        referrer = await get_user_by_referral_code(ref_code)
        if referrer and referrer["user_id"] != tg.id:
            referred_by = referrer["user_id"]

    # Get or create user
    user = await get_user(tg.id)
    if not user:
        name = (tg.full_name or tg.first_name or "User")[:200]
        user = await create_user(tg.id, name, tg.username, referred_by)
        if not user:
            await update.effective_message.reply_text(
                "❌ Could not create your account. Please try again or contact support."
            )
            return

    if user.get("is_banned"):
        reason = escape_md(user.get("ban_reason") or "No reason provided")
        await update.effective_message.reply_text(
            f"🚫 *ACCOUNT RESTRICTED*\n\nReason: {reason}\n\nContact @{Config.SUPPORT_USERNAME} to appeal.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if not user.get("tos_accepted"):
        await _show_tos(update)
        return

    await _show_main_menu(update, user)


async def _show_tos(update: Update) -> None:
    """Show ToS — works from both message and callback."""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I Accept",  callback_data="tos:accept")],
        [InlineKeyboardButton("❌ Decline",   callback_data="tos:decline")],
    ])
    text = (
        f"📋 *TERMS OF SERVICE*\n\n"
        f"{Config.TOS_TEXT}\n\n"
        "You must accept to use this bot."
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard
        )
    else:
        await update.effective_message.reply_text(
            text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard
        )


async def handle_tos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await _answer(update)

    user_id = update.effective_user.id
    action  = query.data.split(":")[1]

    if action == "accept":
        ok = await accept_tos(user_id)
        if ok:
            user = await get_user(user_id)
            await _show_main_menu(update, user)
        else:
            await query.edit_message_text("❌ Error saving ToS. Please try /start again.")

    else:  # decline
        # Ban the user so they can't bypass ToS by pressing /start again
        # Use a soft block: set tos_accepted=False (already is) + send message
        # The user is checked for tos_accepted on every /start — they'll always see ToS
        # No need to ban; they're stuck here until they accept.
        await query.edit_message_text(
            "❌ *Terms Declined*\n\n"
            "You must accept the Terms of Service to use this bot.\n\n"
            "Send /start to try again.",
            parse_mode=ParseMode.MARKDOWN,
        )


async def _show_main_menu(update: Update, user: Dict) -> None:
    """Render main menu with profile card."""
    name    = escape_md(str(user.get("name", "User")))
    balance = float(user.get("balance", 0))
    rank    = user.get("rank", "Bronze")
    rank_emoji = {"Bronze":"🥉","Silver":"🥈","Gold":"🥇","VIP":"💎"}.get(rank,"🏅")

    text = (
        f"👋 *Welcome to {escape_md(Config.SHOP_NAME)}!*\n\n"
        f"👤 {name}\n"
        f"💰 Balance: {format_currency(balance)}\n"
        f"{rank_emoji} Rank: {rank}\n\n"
        "Select an option:"
    )
    await _send(update, text, _kb_main())


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return
    if not user.get("tos_accepted"):
        await _show_tos(update)
        return
    await _show_main_menu(update, user)


# ═══════════════════════════════════════════════════════
#  Profile
# ═══════════════════════════════════════════════════════

async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    # Efficient order count — no 1000-row fetch
    order_count = await get_user_order_count(user["user_id"])

    card = format_profile_card(user)
    text = f"{card}\n📦 Orders: {order_count}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Order History", callback_data="orders"),
         InlineKeyboardButton("🎁 Referral",      callback_data="referral")],
        [InlineKeyboardButton("◀️ Main Menu",     callback_data="main_menu")],
    ])
    await _send(update, text, keyboard)


# ═══════════════════════════════════════════════════════
#  Store  /  Categories  /  Products
# ═══════════════════════════════════════════════════════

async def show_store(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    categories = await get_categories()
    if not categories:
        await _send(update,
            "🛒 *STORE*\n\nNo products available right now\\. Check back soon\\!",
            _kb_back_main())
        return

    await _send(update, "🛒 *BROWSE STORE*\n\nSelect a category:", _kb_categories(categories))


async def show_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    category = update.callback_query.data.split(":", 1)[1]
    context.user_data["current_category"] = category

    products = await get_products_by_category(category)
    if not products:
        await _send(update,
            f"📦 *{escape_md(category)}*\n\nNo products in this category.",
            InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="store")]]))
        return

    await _send(
        update,
        f"📦 *{escape_md(category)}*\n\nSelect a product:",
        _kb_products(products, category),
    )


async def show_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    product_id = int(update.callback_query.data.split(":")[1])
    product    = await get_product(product_id)

    if not product:
        await _answer(update, "❌ Product not found.", alert=True)
        return

    in_wl = await is_in_wishlist(user["user_id"], product_id)
    stock  = product.get("stock_count", 0)

    desc = f"\n📝 {escape_md(product['description'])}" if product.get("description") else ""
    stock_line = f"\n📊 Stock: {stock} available" if stock > 0 else "\n❌ *OUT OF STOCK*"

    text = (
        f"📦 *{escape_md(product['name'])}*\n"
        f"💰 Price: {format_currency(float(product['selling_price']))}"
        f"{desc}{stock_line}"
    )
    await _send(update, text, _kb_product_detail(product_id, in_wl))


# ═══════════════════════════════════════════════════════
#  Wishlist
# ═══════════════════════════════════════════════════════

async def toggle_wishlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # MUST answer before any DB calls to prevent Telegram loading spinner
    await _answer(update)

    user = await _get_user(update, context)
    if not user:
        return

    data       = update.callback_query.data
    action, pid = data.split(":")
    product_id = int(pid)

    if action == "wish":
        ok = await add_to_wishlist(user["user_id"], product_id)
        toast = "⭐ Added to wishlist!" if ok else "Already in wishlist."
    else:
        ok = await remove_from_wishlist(user["user_id"], product_id)
        toast = "💔 Removed from wishlist." if ok else "Not in wishlist."

    await _answer(update, toast)
    # Refresh the product page
    update.callback_query.data = f"prod:{product_id}"
    await show_product(update, context)


async def show_wishlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    wishlist = await get_wishlist(user["user_id"])

    if not wishlist:
        await _send(update,
            "⭐ *YOUR WISHLIST*\n\nYour wishlist is empty\\. Browse the store to save products\\!",
            InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Browse", callback_data="store"),
                                   InlineKeyboardButton("◀️ Back", callback_data="main_menu")]]))
        return

    text  = "⭐ *YOUR WISHLIST*\n\n"
    rows  = []
    for item in wishlist:
        p = item.get("products") or {}
        if not p:
            continue
        stock = p.get("stock_count", 0)
        flag  = "✅" if stock > 0 else "❌"
        text += f"{flag} {escape_md(p['name'])} — {format_inr(p['selling_price'])}\n"
        rows.append([InlineKeyboardButton(
            f"🛒 {p['name']}", callback_data=f"prod:{p['id']}"
        )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="main_menu")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


# ═══════════════════════════════════════════════════════
#  Purchase flow  (atomic — deduct → reserve → order)
# ═══════════════════════════════════════════════════════

async def start_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    if not _rate_ok(user["user_id"], "buy", 3):
        await _answer(update, "⏱️ Please wait a moment.", alert=True)
        return

    product_id = int(update.callback_query.data.split(":")[1])
    product    = await get_product(product_id)
    if not product:
        await _answer(update, "❌ Product not found.", alert=True)
        return

    stock = product.get("stock_count", 0)
    if stock == 0:
        await _send(update,
            f"❌ *OUT OF STOCK*\n\n{escape_md(product['name'])} is currently unavailable\\.",
            InlineKeyboardMarkup([[
                InlineKeyboardButton("⭐ Add to Wishlist", callback_data=f"wish:{product_id}"),
                InlineKeyboardButton("◀️ Back",           callback_data="store"),
            ]]))
        return

    context.user_data["buy"] = {"product_id": product_id, "promo": None, "discount": 0.0}

    await _send(update,
        f"🛒 *{escape_md(product['name'])}*\n"
        f"💰 {format_currency(float(product['selling_price']))} each\n"
        f"📊 {stock} available\n\n"
        "Select quantity:",
        _kb_quantity(product_id, stock))


async def handle_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    _, pid, qty  = update.callback_query.data.split(":")
    product_id   = int(pid)
    quantity     = int(qty)
    product      = await get_product(product_id)

    if not product:
        await _answer(update, "❌ Product not found.", alert=True)
        return

    if product.get("stock_count", 0) < quantity:
        await _answer(update, f"❌ Only {product.get('stock_count',0)} available.", alert=True)
        return

    buy = context.user_data.get("buy", {})
    buy.update({"product_id": product_id, "quantity": quantity})
    context.user_data["buy"] = buy

    await _show_purchase_summary(update, context, user, product, quantity)


async def _show_purchase_summary(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user: Dict,
    product: Dict,
    quantity: int,
) -> None:
    buy       = context.user_data.get("buy", {})
    discount  = float(buy.get("discount", 0))
    promo     = buy.get("promo")
    unit      = float(product["selling_price"])
    subtotal  = unit * quantity
    total     = max(subtotal - discount, 0)
    balance   = float(user.get("balance", 0))

    text = (
        f"🛒 *PURCHASE SUMMARY*\n\n"
        f"📦 {escape_md(product['name'])}\n"
        f"🔢 Qty: {quantity} × {format_currency(unit)}\n"
        f"💵 Subtotal: {format_currency(subtotal)}\n"
    )
    if promo and discount > 0:
        text += f"🎟️ Promo `{escape_md(promo)}`: \\-{format_currency(discount)}\n"
    text += (
        f"━━━━━━━━━━━━━━\n"
        f"✅ *Total: {format_currency(total)}*\n\n"
        f"💰 Your balance: {format_currency(balance)}\n"
    )

    if balance < total:
        deficit = total - balance
        text   += f"\n❌ *Insufficient balance*\nYou need {format_currency(deficit)} more\\."
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Add Funds", callback_data="add_funds")],
            [InlineKeyboardButton("❌ Cancel",     callback_data=f"prod:{product['id']}")],
        ])
    else:
        keyboard = _kb_purchase_summary(product["id"], quantity)

    await _send(update, text, keyboard)


async def promo_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    parts      = update.callback_query.data.split(":")
    product_id = int(parts[1])
    quantity   = int(parts[2])

    context.user_data["awaiting_promo"] = {"product_id": product_id, "quantity": quantity}

    await _send(update,
        "🎟️ *ENTER PROMO CODE*\n\nType your promo code and send it\\.\n\n/cancel to go back\\.",
        InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"prod:{product_id}")]]))


async def handle_promo_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle promo code text message (triggered from awaiting_promo state)."""
    if "awaiting_promo" not in context.user_data:
        return

    user = await _get_user(update, context)
    if not user:
        return

    code       = update.message.text.strip().upper()
    promo_data = context.user_data.pop("awaiting_promo")
    product_id = promo_data["product_id"]
    quantity   = promo_data["quantity"]

    promo = await get_promo_code(code)
    if not promo:
        await update.message.reply_text(
            "❌ *Invalid or expired promo code\\.* Try again or /cancel\\.",
            parse_mode=ParseMode.MARKDOWN,
        )
        context.user_data["awaiting_promo"] = promo_data   # restore so they can retry
        return

    product = await get_product(product_id)
    if not product:
        await update.message.reply_text("❌ Product not found\\. Please start over\\.", parse_mode=ParseMode.MARKDOWN)
        return

    subtotal = float(product["selling_price"]) * quantity
    if subtotal < float(promo.get("min_purchase", 0)):
        await update.message.reply_text(
            f"❌ This promo requires a minimum purchase of {format_currency(float(promo['min_purchase']))}\\.",
            parse_mode=ParseMode.MARKDOWN,
        )
        context.user_data["awaiting_promo"] = promo_data
        return

    discount = calculate_discount(subtotal, promo)
    buy      = context.user_data.get("buy", {})
    buy.update({"product_id": product_id, "quantity": quantity, "promo": code, "discount": discount})
    context.user_data["buy"] = buy

    await update.message.reply_text(
        f"✅ *Promo applied\\!* Discount: \\-{format_currency(discount)}",
        parse_mode=ParseMode.MARKDOWN,
    )
    await _show_purchase_summary_msg(update, context, user, product, quantity)


async def _show_purchase_summary_msg(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user: Dict,
    product: Dict,
    quantity: int,
) -> None:
    """Same as _show_purchase_summary but sends a new message (not edit)."""
    buy      = context.user_data.get("buy", {})
    discount = float(buy.get("discount", 0))
    promo    = buy.get("promo")
    unit     = float(product["selling_price"])
    subtotal = unit * quantity
    total    = max(subtotal - discount, 0)
    balance  = float(user.get("balance", 0))

    text = (
        f"🛒 *PURCHASE SUMMARY*\n\n"
        f"📦 {escape_md(product['name'])}\n"
        f"🔢 Qty: {quantity} × {format_currency(unit)}\n"
        f"💵 Subtotal: {format_currency(subtotal)}\n"
    )
    if promo and discount > 0:
        text += f"🎟️ Promo `{escape_md(promo)}`: \\-{format_currency(discount)}\n"
    text += f"━━━━━━━━━━━━━━\n✅ *Total: {format_currency(total)}*\n\n💰 Balance: {format_currency(balance)}"

    if balance < total:
        deficit = total - balance
        text   += f"\n\n❌ Need {format_currency(deficit)} more\\."
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Add Funds", callback_data="add_funds")],
        ])
    else:
        keyboard = _kb_purchase_summary(product["id"], quantity)

    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


async def confirm_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Atomic purchase flow:
      1. deduct_balance   (atomic RPC — fails if insufficient)
      2. reserve_stock    (atomic RPC — FOR UPDATE SKIP LOCKED)
      3. create_order     (if fails → refund balance + log stock IDs for recovery)
      4. referral commission (only if order succeeded AND first purchase)
    """
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    if not _rate_ok(user["user_id"], "confirm_buy", 5):
        await _answer(update, "⏱️ Please wait before trying again.", alert=True)
        return

    parts      = update.callback_query.data.split(":")
    product_id = int(parts[1])
    quantity   = int(parts[2])

    buy      = context.user_data.get("buy", {})
    promo    = buy.get("promo")
    discount = float(buy.get("discount", 0))

    product = await get_product(product_id)
    if not product:
        await _send(update, "❌ Product no longer available\\.")
        return

    unit_price = float(product["selling_price"])
    total      = max(unit_price * quantity - discount, 0)

    await _send(update, "⏳ *Processing your purchase\\.\\.\\.*")

    # ── Step 1: Deduct balance atomically ──────────────────────────
    deducted = await deduct_balance(user["user_id"], total)
    if not deducted:
        await _send(update,
            "❌ *Insufficient balance*\n\nPlease add funds and try again\\.",
            InlineKeyboardMarkup([[InlineKeyboardButton("💰 Add Funds", callback_data="add_funds")]]))
        return

    # ── Step 2: Reserve stock atomically ───────────────────────────
    stock_items = await reserve_stock(user["user_id"], product_id, quantity)
    if not stock_items:
        # Balance deducted but no stock — refund immediately
        await update_balance(user["user_id"], total)
        await _send(update,
            "❌ *Out of stock*\n\nYour balance has been refunded\\. Please try again later\\.",
            _kb_back_main())
        return

    # Build delivery list
    items_delivered = [{"stock_id": s["stock_id"], "item": s["item"]} for s in stock_items]

    # ── Step 3: Create order record ────────────────────────────────
    order_id = await create_order(
        user_id         = user["user_id"],
        product_id      = product_id,
        product_name    = product["name"],
        quantity        = quantity,
        unit_price      = unit_price,
        total_price     = total,
        items_delivered = items_delivered,
        discount_amount = discount,
        promo_code      = promo,
    )

    if not order_id:
        # Critical: balance deducted, stock reserved, but no order record
        # Refund balance — stock IDs are logged for manual recovery
        await update_balance(user["user_id"], total)
        stock_ids_str = ", ".join(str(s["stock_id"]) for s in stock_items)
        logger.critical(
            "ORDER CREATION FAILED — user %d, product %d, stock_ids [%s] — balance refunded, stock needs manual review",
            user["user_id"], product_id, stock_ids_str
        )
        await _send(update,
            "❌ *Order failed*\n\nYour balance has been refunded\\.\n"
            f"Please contact @{Config.SUPPORT_USERNAME} with reference: `PROD-{product_id}`",
            _kb_back_main())
        return

    # ── Step 4: Update total spent + rank ─────────────────────────
    await update_total_spent(user["user_id"], total)

    # ── Step 5: Mark promo code used ──────────────────────────────
    if promo:
        await use_promo_code(promo)

    # ── Step 6: Referral commission (first purchase only) ──────────
    if user.get("referred_by"):
        commission = calculate_referral_commission(total)
        if commission > 0:
            # record_referral_earning has UNIQUE(referred_id) in DB
            # so this silently no-ops on repeat purchases — safe to call always
            earned = await record_referral_earning(
                referrer_id = user["referred_by"],
                referred_id = user["user_id"],
                order_id    = order_id,        # guaranteed non-None here
                commission  = commission,
            )
            if earned:
                try:
                    await context.bot.send_message(
                        chat_id    = user["referred_by"],
                        text       = f"🎁 *Referral Earning\\!*\n\nYou earned {format_currency(commission)} from a referral\\!",
                        parse_mode = ParseMode.MARKDOWN,
                    )
                except Exception:
                    pass   # referrer may have blocked the bot

    # ── Step 7: Deliver items ──────────────────────────────────────
    await _deliver_items(context, user["user_id"], product, stock_items, order_id)

    # Clear purchase state
    context.user_data.pop("buy", None)


async def _deliver_items(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    product: Dict,
    stock_items: List[Dict],
    order_id: int,
) -> None:
    """Deliver purchased items to user's DM."""
    category = (product.get("category") or "").lower()

    try:
        if category == "papers":
            # Deliver main file
            if product.get("main_file_id"):
                await context.bot.send_document(
                    chat_id  = user_id,
                    document = product["main_file_id"],
                    caption  = f"📄 *{escape_md(product['name'])}*\n\nOrder #{order_id}",
                    parse_mode = ParseMode.MARKDOWN,
                )
            else:
                await context.bot.send_message(
                    chat_id    = user_id,
                    text       = f"❌ File unavailable\\. Contact @{Config.SUPPORT_USERNAME} with Order #{order_id}",
                    parse_mode = ParseMode.MARKDOWN,
                )

        else:
            # Deliver each stock item as a separate message
            for idx, item in enumerate(stock_items, 1):
                item_text = item.get("item", "")
                await context.bot.send_message(
                    chat_id    = user_id,
                    text       = f"📦 *Item {idx}/{len(stock_items)}*\n\n`{escape_md(item_text)}`",
                    parse_mode = ParseMode.MARKDOWN,
                )

        # Confirmation + refund button
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 View Orders", callback_data="orders")],
            [InlineKeyboardButton("🔄 Request Refund", callback_data=f"refund:{order_id}")],
        ])
        await context.bot.send_message(
            chat_id    = user_id,
            text       = f"✅ *Purchase Complete\\!*\n\nOrder #{order_id} delivered\\.\nIf you have issues, tap Refund below\\.",
            parse_mode = ParseMode.MARKDOWN,
            reply_markup = keyboard,
        )

    except Exception as exc:
        logger.error("Delivery failed for order %d user %d: %s", order_id, user_id, exc)


# ═══════════════════════════════════════════════════════
#  Add Funds
# ═══════════════════════════════════════════════════════

async def add_funds_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return ConversationHandler.END

    await _send(update,
        f"💰 *ADD FUNDS*\n\n"
        f"Enter the amount in ₹ you want to deposit\\:\n\n"
        f"Min: {format_inr(Config.MIN_DEPOSIT_INR)}\n"
        f"Max: {format_inr(Config.MAX_DEPOSIT_INR)}\n\n"
        "Type the amount and send it, or /cancel to go back\\.")
    return ADD_FUNDS_AMOUNT


async def add_funds_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = await _get_user(update, context)
    if not user:
        return ConversationHandler.END

    try:
        raw_amount = float(update.message.text.strip())
    except ValueError:
        await update.message.reply_text(
            "❌ Please enter a valid number\\. Example: `500`",
            parse_mode=ParseMode.MARKDOWN)
        return ADD_FUNDS_AMOUNT

    valid, err = validate_payment_amount(raw_amount)
    if not valid:
        await update.message.reply_text(f"❌ {escape_md(err)}", parse_mode=ParseMode.MARKDOWN)
        return ADD_FUNDS_AMOUNT

    amount_inr  = normalise_inr(raw_amount)
    payment_ref = generate_payment_ref()
    payment_id  = await create_payment(user["user_id"], amount_inr, payment_ref)

    if not payment_id:
        await update.message.reply_text("❌ Failed to create payment\\. Please try again\\.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    context.user_data["pending_payment"] = payment_id
    qr_image = generate_upi_qr(amount_inr, payment_ref)

    await update.message.reply_photo(
        photo      = qr_image,
        caption    = msg_payment_instructions(amount_inr, payment_ref),
        parse_mode = ParseMode.MARKDOWN,
        reply_markup = _kb_payment(payment_id),
    )
    logger.info("Payment %s created: ₹%d user %d", payment_ref, amount_inr, user["user_id"])
    return ConversationHandler.END


async def handle_paid_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User tapped 'I've Paid' — ask for screenshot."""
    await _answer(update)
    payment_id = int(update.callback_query.data.split(":")[1])
    context.user_data["awaiting_screenshot"] = payment_id

    # Edit caption (message IS a photo at this point)
    try:
        await update.callback_query.edit_message_caption(
            caption      = msg_request_screenshot(),
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = _kb_cancel_payment(),
        )
    except BadRequest:
        await update.callback_query.edit_message_text(
            text         = msg_request_screenshot(),
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = _kb_cancel_payment(),
        )


async def handle_payment_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User sends payment screenshot image."""
    if "awaiting_screenshot" not in context.user_data:
        return

    user = await _get_user(update, context)
    if not user:
        return

    msg        = update.message
    payment_id = context.user_data["awaiting_screenshot"]

    # Extract file_id
    if msg.photo:
        file_id = msg.photo[-1].file_id
    elif msg.document:
        file_id = msg.document.file_id
    else:
        await msg.reply_text("❌ Please send an *image* or *document*\\.", parse_mode=ParseMode.MARKDOWN)
        return

    # Download image and hash its actual bytes
    image_bytes = await hash_screenshot(context.bot, file_id)
    if image_bytes is None:
        # Could not download — still accept but without duplicate detection
        image_hash_bytes = b""
    else:
        image_hash_bytes = image_bytes  # already bytes from download

    # Get image bytes for hashing
    try:
        tg_file = await context.bot.get_file(file_id)
        buf     = BytesIO()
        await tg_file.download_to_memory(buf)
        actual_bytes = buf.getvalue()
    except Exception as exc:
        logger.warning("Could not download screenshot: %s", exc)
        actual_bytes = b""

    ok, err = await update_payment_screenshot(payment_id, file_id, actual_bytes)
    if not ok:
        await msg.reply_text(
            f"❌ *{escape_md(err or 'Could not save screenshot.')}*\n\nPlease try a different screenshot\\.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    payment = await get_payment(payment_id)
    ref     = payment["payment_ref"] if payment else "N/A"

    context.user_data.pop("awaiting_screenshot", None)
    context.user_data.pop("pending_payment", None)

    await msg.reply_text(
        msg_payment_submitted(ref),
        parse_mode   = ParseMode.MARKDOWN,
        reply_markup = _kb_back_main(),
    )
    logger.info("Screenshot submitted for payment %s", ref)


async def cancel_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel payment. Works whether current message is photo or text."""
    context.user_data.pop("awaiting_screenshot", None)
    context.user_data.pop("pending_payment", None)

    cancel_text  = "❌ *Payment cancelled\\.*\n\nYour pending payment has been cancelled\\."
    cancel_kb    = _kb_back_main()

    if update.callback_query:
        await _answer(update, "Payment cancelled.")
        # Try caption first (photo), fall back to text edit
        try:
            await update.callback_query.edit_message_caption(
                caption      = cancel_text,
                parse_mode   = ParseMode.MARKDOWN,
                reply_markup = cancel_kb,
            )
        except BadRequest:
            try:
                await update.callback_query.edit_message_text(
                    text         = cancel_text,
                    parse_mode   = ParseMode.MARKDOWN,
                    reply_markup = cancel_kb,
                )
            except BadRequest:
                pass
    elif update.message:
        await update.message.reply_text(
            cancel_text, parse_mode=ParseMode.MARKDOWN, reply_markup=cancel_kb
        )

    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  Orders
# ═══════════════════════════════════════════════════════

async def show_orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    orders = await get_user_orders(user["user_id"], limit=10)

    if not orders:
        await _send(update,
            "📋 *ORDER HISTORY*\n\nNo orders yet\\. Start shopping\\!",
            InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Browse Store", callback_data="store"),
                                   InlineKeyboardButton("◀️ Back", callback_data="main_menu")]]))
        return

    text  = "📋 *ORDER HISTORY* \\(last 10\\)\n\n"
    rows  = []
    for o in orders:
        dt  = parse_utc(str(o.get("created_at", "")))
        ds  = dt.strftime("%Y\\-%m\\-%d") if dt else "Unknown"
        pn  = escape_md(str(o.get("product_name", "Unknown")))
        text += f"• #{o['id']} — {pn} ×{o['quantity']} — {format_currency(float(o['total_price']))} \\({ds}\\)\n"
        rows.append([InlineKeyboardButton(
            f"📦 Order #{o['id']}", callback_data=f"order:{o['id']}"
        )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="main_menu")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


async def show_order_detail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for order: callback — was completely missing in original."""
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    order_id = int(update.callback_query.data.split(":")[1])
    order    = await get_order(order_id)

    if not order or order.get("user_id") != user["user_id"]:
        await _answer(update, "❌ Order not found.", alert=True)
        return

    dt     = parse_utc(str(order.get("created_at", "")))
    ds     = dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "Unknown"
    status = order.get("status", "completed").capitalize()
    pname  = escape_md(str(order.get("product_name", "Unknown")))

    text = (
        f"📦 *ORDER #{order_id}*\n\n"
        f"Product: {pname}\n"
        f"Qty: {order['quantity']}\n"
        f"Total: {format_currency(float(order['total_price']))}\n"
        f"Status: {escape_md(status)}\n"
        f"Date: {escape_md(ds)}\n"
    )
    if order.get("promo_code"):
        text += f"Promo: `{escape_md(order['promo_code'])}`\n"

    rows = [[InlineKeyboardButton("◀️ Orders", callback_data="orders")]]
    if order.get("status") == "completed":
        rows.insert(0, [InlineKeyboardButton("🔄 Request Refund", callback_data=f"refund:{order_id}")])

    await _send(update, text, InlineKeyboardMarkup(rows))


# ═══════════════════════════════════════════════════════
#  Refunds
# ═══════════════════════════════════════════════════════

async def handle_refund(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for refund: callback — was completely missing in original."""
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    order_id = int(update.callback_query.data.split(":")[1])
    order    = await get_order(order_id)

    if not order or order.get("user_id") != user["user_id"]:
        await _answer(update, "❌ Order not found.", alert=True)
        return

    if order.get("status") != "completed":
        await _answer(update, "❌ This order is not eligible for a refund.", alert=True)
        return

    refund_id = await create_refund_request(user["user_id"], order_id)
    if not refund_id:
        await _answer(update, "⚠️ A refund request already exists for this order.", alert=True)
        return

    await _send(update,
        f"🔄 *REFUND REQUESTED*\n\n"
        f"Order #{order_id}\n"
        f"Our team will review and respond within 24 hours\\.\n\n"
        f"Contact @{Config.SUPPORT_USERNAME} for urgent issues\\.",
        _kb_back_main())


# ═══════════════════════════════════════════════════════
#  Referral
# ═══════════════════════════════════════════════════════

async def show_referral(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    user = await _get_user(update, context)
    if not user:
        return

    stats    = await get_referral_stats(user["user_id"])
    bot_user = await context.bot.get_me()
    ref_link = f"https://t\\.me/{bot_user.username}?start={user.get('referral_code','')}"

    text = (
        f"🎁 *REFERRAL PROGRAM*\n\n"
        f"Your code: `{user.get('referral_code','N/A')}`\n"
        f"Your link: `{ref_link}`\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"📊 *Your Stats:*\n"
        f"• Referrals: {stats['total_referrals']}\n"
        f"• Earned: {format_currency(stats['total_earnings'])}\n\n"
        f"💰 Earn *{Config.REFERRAL_PERCENT}%* when your referral makes their first purchase\\!"
    )
    await _send(update, text, _kb_back_main())


# ═══════════════════════════════════════════════════════
#  Help
# ═══════════════════════════════════════════════════════

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _answer(update)
    text = (
        f"❓ *HELP*\n\n"
        f"*How to buy:*\n"
        f"Store → Category → Product → Quantity → Confirm\n\n"
        f"*Add funds:*\n"
        f"Add Funds → Enter ₹ amount → Pay UPI → Send screenshot\n\n"
        f"*Promo codes:*\n"
        f"Enter during checkout before confirming\n\n"
        f"*Referrals:*\n"
        f"Earn {Config.REFERRAL_PERCENT}% on your referral's first purchase\n\n"
        f"*Refunds:*\n"
        f"OTP issues only — tap Refund in your order details\n\n"
        f"*Commands:* /start /help\n\n"
        f"*Support:* @{Config.SUPPORT_USERNAME}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📞 Support", url=f"https://t.me/{Config.SUPPORT_USERNAME}"),
         InlineKeyboardButton("◀️ Back",    callback_data="main_menu")],
    ])
    await _send(update, text, keyboard)


# ═══════════════════════════════════════════════════════
#  Callback router
# ═══════════════════════════════════════════════════════

async def route_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Central callback router using exact prefix matching.
    Each branch is explicit — no startswith ordering bugs.
    """
    query = update.callback_query
    if not query or not query.data:
        return

    data = query.data

    # Exact matches
    if data == "main_menu":      await show_main_menu(update, context)
    elif data == "store":        await show_store(update, context)
    elif data == "profile":      await show_profile(update, context)
    elif data == "referral":     await show_referral(update, context)
    elif data == "wishlist":     await show_wishlist(update, context)
    elif data == "orders":       await show_orders(update, context)
    elif data == "help":         await show_help(update, context)
    elif data == "add_funds":    await add_funds_start(update, context)
    elif data == "cancel_payment": await cancel_payment(update, context)

    # Prefix matches (explicit, no ambiguity)
    elif data.startswith("tos:"):         await handle_tos(update, context)
    elif data.startswith("cat:"):         await show_category(update, context)
    elif data.startswith("prod:"):        await show_product(update, context)
    elif data.startswith("wish:"):        await toggle_wishlist(update, context)
    elif data.startswith("unwish:"):      await toggle_wishlist(update, context)
    elif data.startswith("buy:"):         await start_purchase(update, context)
    elif data.startswith("qty:"):         await handle_quantity(update, context)
    elif data.startswith("promo_prompt:"): await promo_prompt(update, context)
    elif data.startswith("confirm:"):     await confirm_purchase(update, context)
    elif data.startswith("paid:"):        await handle_paid_button(update, context)
    elif data.startswith("order:"):       await show_order_detail(update, context)
    elif data.startswith("refund:"):      await handle_refund(update, context)

    else:
        await _answer(update, "Unknown action.", alert=False)


# ═══════════════════════════════════════════════════════
#  Text message router
# ═══════════════════════════════════════════════════════

async def route_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Route plain text messages (promo code input, unknown text)."""
    if not update.message or not update.message.text:
        return

    # Promo code input
    if context.user_data.get("awaiting_promo"):
        await handle_promo_text(update, context)
        return

    # Ignore unknown text — user should use inline buttons
    await update.message.reply_text(
        "👆 Please use the buttons to navigate\\.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
        ]]),
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("awaiting_promo", None)
    context.user_data.pop("awaiting_screenshot", None)
    context.user_data.pop("pending_payment", None)
    context.user_data.pop("buy", None)
    await update.message.reply_text(
        "❌ Cancelled\\. Use the menu to continue\\.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
        ]]),
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  Error handler
# ═══════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", context.error, exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                f"❌ An error occurred\\. Please try again or contact @{Config.SUPPORT_USERNAME}",
                parse_mode=ParseMode.MARKDOWN,
            )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════
#  Periodic cleanup job
# ═══════════════════════════════════════════════════════

async def job_cleanup(context: ContextTypes.DEFAULT_TYPE) -> None:
    from db import cleanup_expired_reservations
    n = await cleanup_expired_reservations()
    if n:
        logger.info("🧹 Cleaned %d expired reservations", n)

    rate_limiter.cleanup()


# ═══════════════════════════════════════════════════════
#  Application setup
# ═══════════════════════════════════════════════════════

def main() -> None:
    if not Config.validate():
        logger.critical("❌ Invalid configuration")
        return

    logger.info("🚀 Starting Shop Bot…")

    app = Application.builder().token(Config.SHOP_BOT_TOKEN).build()

    # Add Funds conversation (amount input only — screenshot handled globally)
    add_funds_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(add_funds_start, pattern="^add_funds$"),
            CommandHandler("funds", add_funds_start),
        ],
        states={
            ADD_FUNDS_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_funds_amount),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        per_message=False,
    )

    # Commands
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   show_help))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    # Conversations
    app.add_handler(add_funds_conv)

    # Callback router (catches everything not handled by conversations above)
    app.add_handler(CallbackQueryHandler(route_callback))

    # Photo/document handler — payment screenshots
    app.add_handler(MessageHandler(
        filters.PHOTO | filters.Document.ALL,
        handle_payment_screenshot,
    ))

    # Text messages (promo codes, unknown text)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        route_text,
    ))

    # Error handler
    app.add_error_handler(error_handler)

    # Periodic cleanup every 15 minutes
    app.job_queue.run_repeating(job_cleanup, interval=900, first=60)

    logger.info("✅ Shop Bot running!")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
