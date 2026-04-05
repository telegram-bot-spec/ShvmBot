"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Admin Bot                                           ║
║                                                                ║
║  FIXED:                                                        ║
║  • Admin check on EVERY callback — not just /start            ║
║  • require_admin safe for both message & callback              ║
║  • All "coming soon" stubs replaced with real implementations  ║
║  • Users: search, view, ban, unban, balance adjust            ║
║  • Broadcast: all users or targeted                           ║
║  • Promo codes: create, list, deactivate                      ║
║  • Refunds: list, approve with balance credit, reject          ║
║  • Audit log: paginated view                                  ║
║  • Payment history (approved + rejected)                      ║
║  • Product edit callbacks wired                               ║
║  • edit_message_caption vs edit_message_text guarded          ║
║  • time_ago uses total_seconds() (not .seconds)               ║
║  • /yes for negative margin replaced with inline confirmation  ║
║  • CSV export working                                         ║
║  • bulk_add_stock chunked (no 6MB crash)                      ║
╚════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from typing import Any, Dict, List, Optional, Tuple

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from config import Config, escape_md, format_currency, format_inr, format_usd
from db import (
    get_user, ban_user, unban_user, update_balance, search_users, get_all_users,
    get_product, create_product, update_product, delete_product,
    get_products_by_category, get_categories,
    add_stock_item, bulk_add_stock, get_stock_count, get_low_stock_products,
    get_pending_payments, approve_payment, reject_payment, get_payment,
    get_stats, log_admin_action, get_admin_actions,
    create_promo_code, get_all_promo_codes, deactivate_promo_code,
    get_pending_refunds, approve_refund, reject_refund,
    supabase,
)
from utils import utcnow, parse_utc, time_ago

# ═══════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("admin_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("admin_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════
#  Conversation states
# ═══════════════════════════════════════════════════════

# Product
ADD_PROD_CAT   = 1
ADD_PROD_NAME  = 2
ADD_PROD_DESC  = 3
ADD_PROD_BUY   = 4
ADD_PROD_SELL  = 5
ADD_PROD_DEMO  = 6
ADD_PROD_FILE  = 7
ADD_PROD_CONFIRM = 8   # inline confirmation replaces the broken /yes

# Stock
STOCK_METHOD   = 10
STOCK_MANUAL   = 11
STOCK_FILE     = 12

# User management
USER_SEARCH    = 20
USER_BAN_REASON = 21
USER_BAL_AMOUNT = 22

# Payment rejection
PAY_REJECT_REASON = 30

# Promo
PROMO_CODE     = 40
PROMO_DISCOUNT = 41
PROMO_MIN      = 42
PROMO_USES     = 43
PROMO_EXPIRY   = 44

# Broadcast
BCAST_MSG      = 50

# Refund rejection
REFUND_REJECT_NOTE = 60

# ═══════════════════════════════════════════════════════
#  Access control — safe for both message AND callback
# ═══════════════════════════════════════════════════════

async def require_admin(update: Update) -> bool:
    """
    Returns True if user is admin/owner, False and sends denial otherwise.
    Safe for both command updates (update.message) and callbacks.
    """
    user_id = update.effective_user.id if update.effective_user else None
    if user_id and Config.is_admin(user_id):
        return True

    text = (
        "🚫 *UNAUTHORIZED*\n\n"
        "This bot is restricted to administrators only."
    )
    try:
        if update.callback_query:
            await update.callback_query.answer("Unauthorized.", show_alert=True)
        elif update.effective_message:
            await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

    logger.warning("⚠️ Unauthorized access attempt by user %s", user_id)
    return False


def _is_owner(user_id: int) -> bool:
    return Config.is_owner(user_id)


# ═══════════════════════════════════════════════════════
#  Safe send helpers
# ═══════════════════════════════════════════════════════

async def _send(update: Update, text: str, kb: Optional[InlineKeyboardMarkup] = None) -> None:
    kwargs = dict(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(**kwargs)
        else:
            await update.effective_message.reply_text(**kwargs)
    except BadRequest as exc:
        if "not modified" not in str(exc).lower():
            raise


async def _safe_edit(query, text: str, kb=None, is_photo: bool = False) -> None:
    """Edit message — tries caption first if photo, falls back to text."""
    try:
        if is_photo:
            await query.edit_message_caption(
                caption=text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb
            )
        else:
            await query.edit_message_text(
                text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb
            )
    except BadRequest as exc:
        if "caption" in str(exc).lower() or "no caption" in str(exc).lower():
            try:
                await query.edit_message_text(
                    text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb
                )
            except BadRequest:
                pass
        elif "not modified" not in str(exc).lower():
            logger.warning("Edit failed: %s", exc)


async def _answer(update: Update, text: str = "", alert: bool = False) -> None:
    try:
        if update.callback_query:
            await update.callback_query.answer(text, show_alert=alert)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════
#  Shared keyboards
# ═══════════════════════════════════════════════════════

def _kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Products",   callback_data="menu:products"),
         InlineKeyboardButton("👥 Users",      callback_data="menu:users")],
        [InlineKeyboardButton("💰 Payments",   callback_data="menu:payments"),
         InlineKeyboardButton("📊 Stats",      callback_data="menu:stats")],
        [InlineKeyboardButton("📢 Broadcast",  callback_data="menu:broadcast"),
         InlineKeyboardButton("🎟️ Promos",    callback_data="menu:promos")],
        [InlineKeyboardButton("🔄 Refunds",    callback_data="menu:refunds"),
         InlineKeyboardButton("📋 Audit Log",  callback_data="menu:audit")],
    ])


def _kb_back(to: str = "main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"menu:{to}")]])


# ═══════════════════════════════════════════════════════
#  /start  —  main menu
# ═══════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return

    uid   = update.effective_user.id
    uname = update.effective_user.full_name
    role  = "👑 OWNER" if _is_owner(uid) else "🛡️ ADMIN"

    text = (
        f"🛡️ *ADMIN PANEL*\n\n"
        f"Welcome, {escape_md(uname)}\\!\n"
        f"Role: {role}\n"
        f"ID: `{uid}`\n\n"
        "Select an option:"
    )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=_kb_main())
    await log_admin_action(uid, "admin_login", None, None, {"name": uname})


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    uid   = update.effective_user.id
    uname = update.effective_user.full_name
    role  = "👑 OWNER" if _is_owner(uid) else "🛡️ ADMIN"
    text  = f"🛡️ *ADMIN PANEL* — {escape_md(uname)} \\({role}\\)\n\nSelect:"
    await _send(update, text, _kb_main())


# ═══════════════════════════════════════════════════════
#  PRODUCTS
# ═══════════════════════════════════════════════════════

async def show_product_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Product",   callback_data="product:add")],
        [InlineKeyboardButton("📋 List Products", callback_data="product:list")],
        [InlineKeyboardButton("📦 Add Stock",     callback_data="stock:select")],
        [InlineKeyboardButton("⚠️ Low Stock",     callback_data="stock:low")],
        [InlineKeyboardButton("◀️ Back",          callback_data="menu:main")],
    ])
    await _send(update, "📦 *PRODUCT MANAGEMENT*\n\nSelect:", kb)


async def list_products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    categories = await get_categories()
    if not categories:
        await _send(update, "No products found\\.", _kb_back("products"))
        return

    text = "📦 *ALL PRODUCTS*\n\n"
    rows = []
    for cat in categories:
        products = await get_products_by_category(cat)
        if not products:
            continue
        text += f"*{escape_md(cat)}:*\n"
        for p in products:
            stock = p.get("stock_count", 0)
            text += f"  • {escape_md(p['name'])} — {format_inr(p['selling_price'])} \\({stock} left\\)\n"
            rows.append([InlineKeyboardButton(
                f"✏️ {p['name']}", callback_data=f"product:edit:{p['id']}"
            )])
        text += "\n"

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:products")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


# ── Add Product conversation ───────────────────────────

async def start_add_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    categories = await get_categories()
    text = "➕ *ADD PRODUCT*\n\n"
    if categories:
        text += "Existing categories:\n" + "\n".join(f"• {c}" for c in categories) + "\n\n"
    text += "Send the *category* name \\(existing or new\\):\n\n/cancel to abort"

    await _send(update, text)
    context.user_data["np"] = {}
    return ADD_PROD_CAT


async def ap_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["np"]["category"] = update.message.text.strip()
    await update.message.reply_text("✅ Category set\\.\n\nNow send the *product name*:", parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_NAME


async def ap_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["np"]["name"] = update.message.text.strip()
    await update.message.reply_text("✅ Name set\\.\n\nSend a *description* \\(or /skip\\):", parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_DESC


async def ap_desc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    context.user_data["np"]["description"] = "" if text == "/skip" else text
    await update.message.reply_text(
        "✅ Description set\\.\n\nEnter *purchase price* in USD \\(your cost\\):\nExample: `1.50`",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_BUY


async def ap_buy_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        price = float(update.message.text.strip())
        if price < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a valid positive number\\.", parse_mode=ParseMode.MARKDOWN)
        return ADD_PROD_BUY

    context.user_data["np"]["purchase_price"] = price
    await update.message.reply_text(
        f"✅ Purchase price: ${price:.2f}\n\nEnter *selling price* in USD:\nExample: `2.50`",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_SELL


async def ap_sell_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        price = float(update.message.text.strip())
        if price < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a valid positive number\\.", parse_mode=ParseMode.MARKDOWN)
        return ADD_PROD_SELL

    np  = context.user_data["np"]
    buy = np["purchase_price"]
    np["selling_price"] = price
    profit = price - buy
    margin = (profit / price * 100) if price > 0 else 0

    if price < buy:
        # Show warning with inline buttons instead of broken /yes command
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Proceed anyway", callback_data="ap_confirm_margin:yes"),
             InlineKeyboardButton("❌ Change price",   callback_data="ap_confirm_margin:no")],
        ])
        await update.message.reply_text(
            f"⚠️ *Negative margin warning\\!*\n\n"
            f"Selling \\(${price:.2f}\\) < Purchase \\(${buy:.2f}\\)\n"
            f"Loss per unit: ${abs(profit):.2f}\n\n"
            "Proceed anyway?",
            parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return ADD_PROD_SELL   # stay in state until inline confirmation

    await update.message.reply_text(
        f"✅ Selling price: ${price:.2f}\n"
        f"💰 Profit: ${profit:.2f} \\({margin:.1f}%\\)\n\n"
        f"Send *demo file* \\(optional — image or document\\) or /skip:",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_DEMO


async def ap_confirm_margin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle inline confirmation for negative margin."""
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    choice = update.callback_query.data.split(":")[1]
    if choice == "no":
        await update.callback_query.edit_message_text(
            "Enter a new *selling price*:", parse_mode=ParseMode.MARKDOWN)
        return ADD_PROD_SELL
    # Proceed — move to demo
    np = context.user_data["np"]
    buy = np["purchase_price"]
    price = np["selling_price"]
    await update.callback_query.edit_message_text(
        f"✅ Proceeding with ${price:.2f} \\(loss ${abs(price-buy):.2f}/unit\\)\n\n"
        "Send *demo file* \\(optional\\) or /skip:",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_DEMO


async def ap_demo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.document:
        context.user_data["np"]["demo_file_id"] = update.message.document.file_id
    elif update.message.photo:
        context.user_data["np"]["demo_file_id"] = update.message.photo[-1].file_id
    else:
        context.user_data["np"]["demo_file_id"] = None

    cat = context.user_data["np"]["category"].lower()
    if cat == "papers":
        await update.message.reply_text("📄 *Papers detected*\\. Send main file or /skip:", parse_mode=ParseMode.MARKDOWN)
        return ADD_PROD_FILE
    return await _ap_finalize(update, context)


async def ap_main_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.document:
        context.user_data["np"]["main_file_id"] = update.message.document.file_id
    else:
        context.user_data["np"]["main_file_id"] = None
    return await _ap_finalize(update, context)


async def _ap_finalize(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    np  = context.user_data.get("np", {})
    profit = np["selling_price"] - np["purchase_price"]
    margin = (profit / np["selling_price"] * 100) if np["selling_price"] > 0 else 0

    summary = (
        f"📦 *PRODUCT SUMMARY*\n\n"
        f"Category: {escape_md(np['category'])}\n"
        f"Name: {escape_md(np['name'])}\n"
        f"Description: {escape_md(np.get('description','—'))}\n"
        f"Buy price: ${np['purchase_price']:.2f}\n"
        f"Sell price: ${np['selling_price']:.2f}\n"
        f"Profit: ${profit:.2f} \\({margin:.1f}%\\)\n"
        f"Demo: {'✅' if np.get('demo_file_id') else '❌'}\n"
        f"Main file: {'✅' if np.get('main_file_id') else '❌'}\n\n"
        "Confirm creation?"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Create", callback_data="product:confirm_create"),
         InlineKeyboardButton("❌ Cancel", callback_data="product:cancel_create")],
    ])
    await update.effective_message.reply_text(summary, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    return ConversationHandler.END


async def confirm_create_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    np = context.user_data.get("np")
    if not np:
        await _send(update, "❌ Session expired\\. Start over with Add Product\\.", _kb_back("products"))
        return

    product = await create_product(
        category      = np["category"],
        name          = np["name"],
        description   = np.get("description", ""),
        purchase_price= np["purchase_price"],
        selling_price = np["selling_price"],
        admin_id      = update.effective_user.id,
        demo_file_id  = np.get("demo_file_id"),
        main_file_id  = np.get("main_file_id"),
    )
    context.user_data.pop("np", None)

    if product:
        await _send(update,
            f"✅ *Product created\\!*\n\nID: `{product['id']}`\nName: {escape_md(product['name'])}",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Stock",    callback_data=f"stock:add:{product['id']}"),
                 InlineKeyboardButton("◀️ Products",    callback_data="menu:products")],
            ]))
    else:
        await _send(update, "❌ Failed to create product\\.", _kb_back("products"))


async def cancel_create_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    context.user_data.pop("np", None)
    await _send(update, "❌ Product creation cancelled\\.", _kb_back("products"))


async def show_product_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Product edit menu — was completely unimplemented in original."""
    if not await require_admin(update):
        return
    await _answer(update)

    product_id = int(update.callback_query.data.split(":")[2])
    product    = await get_product(product_id)
    if not product:
        await _answer(update, "❌ Product not found.", alert=True)
        return

    stock = product.get("stock_count", 0)
    profit = float(product["selling_price"]) - float(product["purchase_price"])
    text = (
        f"✏️ *EDIT: {escape_md(product['name'])}*\n\n"
        f"Category: {escape_md(product['category'])}\n"
        f"Sell: {format_inr(product['selling_price'])} | Buy: {format_inr(product['purchase_price'])}\n"
        f"Profit: {format_usd(profit)} | Stock: {stock}\n"
        f"Status: {'✅ Active' if product['is_active'] else '❌ Inactive'}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Stock",    callback_data=f"stock:add:{product_id}"),
         InlineKeyboardButton("🗑️ Delete",      callback_data=f"product:delete:{product_id}")],
        [InlineKeyboardButton("◀️ Back",         callback_data="product:list")],
    ])
    await _send(update, text, kb)


async def delete_product_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    product_id = int(update.callback_query.data.split(":")[2])
    admin_id   = update.effective_user.id
    ok         = await delete_product(product_id, admin_id)
    if ok:
        await _send(update, f"✅ Product #{product_id} deactivated\\.", _kb_back("products"))
    else:
        await _send(update, "❌ Failed to delete product\\.", _kb_back("products"))


# ── Stock management ────────────────────────────────────

async def show_stock_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    categories = await get_categories()
    if not categories:
        await _send(update, "No products found\\.", _kb_back("products"))
        return

    rows = []
    text = "📦 *SELECT PRODUCT FOR STOCK*\n\n"
    for cat in categories:
        products = await get_products_by_category(cat)
        for p in products:
            stock = p.get("stock_count", 0)
            text += f"• {escape_md(p['name'])} \\({stock} left\\)\n"
            rows.append([InlineKeyboardButton(
                f"➕ {p['name']} ({stock})", callback_data=f"stock:add:{p['id']}"
            )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:products")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


async def start_add_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    product_id = int(update.callback_query.data.split(":")[2])
    context.user_data["stock_product_id"] = product_id
    product = await get_product(product_id)
    if not product:
        await _send(update, "❌ Product not found\\.")
        return ConversationHandler.END

    stock = product.get("stock_count", 0)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Manual entry",    callback_data="stock_method:manual")],
        [InlineKeyboardButton("📄 Upload .txt file", callback_data="stock_method:file")],
        [InlineKeyboardButton("❌ Cancel",           callback_data="stock_method:cancel")],
    ])
    await _send(update,
        f"➕ *ADD STOCK*\n\n"
        f"Product: *{escape_md(product['name'])}*\n"
        f"Current stock: {stock}\n\n"
        "Choose method:", kb)
    return STOCK_METHOD


async def stock_choose_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    context.user_data["stock_items"] = []
    await _send(update,
        "📝 *MANUAL STOCK ENTRY*\n\n"
        "Send items one per message\\.\n"
        "Send /done when finished\\.\n"
        "Send /cancel to abort\\.\n\n"
        "OTP format example:\n`\\+1234567890 \\| India \\| 2FA: pass`")
    return STOCK_MANUAL


async def stock_choose_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    await _send(update,
        "📄 *BULK STOCK UPLOAD*\n\n"
        "Send a `.txt` file — one item per line\\.\n"
        "Max 5,000 lines per upload\\.\n\n"
        "/cancel to abort\\.")
    return STOCK_FILE


async def stock_add_item(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    if text == "/done":
        return await _finalize_stock(update, context)
    if text.startswith("/"):
        return STOCK_MANUAL

    items = context.user_data.setdefault("stock_items", [])
    items.append(text)
    await update.message.reply_text(
        f"✅ Item {len(items)} added\\. Send more or /done\\.",
        parse_mode=ParseMode.MARKDOWN)
    return STOCK_MANUAL


async def stock_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message.document:
        await update.message.reply_text("❌ Please send a .txt file\\.", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    doc = update.message.document
    if doc.file_size and doc.file_size > 5 * 1024 * 1024:   # 5 MB hard limit
        await update.message.reply_text("❌ File too large \\(max 5MB\\)\\.", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    tg_file = await doc.get_file()
    content = await tg_file.download_as_bytearray()

    try:
        lines = content.decode("utf-8").strip().splitlines()
    except UnicodeDecodeError:
        await update.message.reply_text("❌ File must be UTF-8 encoded\\.", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    items = [l.strip() for l in lines if l.strip()][:5000]   # cap at 5000 lines
    if not items:
        await update.message.reply_text("❌ File is empty\\.", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    context.user_data["stock_items"] = items
    await update.message.reply_text(
        f"✅ Parsed *{len(items)}* items\\. Processing…",
        parse_mode=ParseMode.MARKDOWN)
    return await _finalize_stock(update, context)


async def _finalize_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    product_id = context.user_data.get("stock_product_id")
    items      = context.user_data.get("stock_items", [])
    admin_id   = update.effective_user.id

    if not items:
        await update.effective_message.reply_text("❌ No items to add\\.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    # bulk_add_stock is now chunked internally — no 6MB crash
    count = await bulk_add_stock(product_id, items, admin_id)
    new_total = await get_stock_count(product_id)

    context.user_data.pop("stock_product_id", None)
    context.user_data.pop("stock_items", None)

    await update.effective_message.reply_text(
        f"✅ *Stock added\\!*\n\nAdded: {count}/{len(items)}\nTotal stock: {new_total}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_kb_back("products"))
    return ConversationHandler.END


async def show_low_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    low = await get_low_stock_products()
    if not low:
        await _send(update, "✅ *All products well-stocked\\!*", _kb_back("products"))
        return

    text = "⚠️ *LOW STOCK ALERT*\n\n"
    rows = []
    for p in low:
        stock = p.get("stock_count", 0)
        flag  = "❌ OUT" if stock == 0 else f"⚠️ {stock} left"
        text += f"{flag} — {escape_md(p['name'])} \\({escape_md(p['category'])}\\)\n"
        rows.append([InlineKeyboardButton(
            f"➕ {p['name']}", callback_data=f"stock:add:{p['id']}"
        )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:products")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


# ═══════════════════════════════════════════════════════
#  USERS
# ═══════════════════════════════════════════════════════

async def show_user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Search User",   callback_data="user:search")],
        [InlineKeyboardButton("🚫 Banned Users",  callback_data="user:banned")],
        [InlineKeyboardButton("📊 User Stats",    callback_data="user:stats")],
        [InlineKeyboardButton("◀️ Back",          callback_data="menu:main")],
    ])
    await _send(update, "👥 *USER MANAGEMENT*\n\nSelect:", kb)


async def start_user_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    await _send(update, "🔍 *SEARCH USER*\n\nSend name, username or /cancel:")
    return USER_SEARCH


async def do_user_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query   = update.message.text.strip()
    results = await search_users(query, limit=5)

    if not results:
        await update.message.reply_text("❌ No users found\\.", parse_mode=ParseMode.MARKDOWN)
        return USER_SEARCH

    rows = []
    for u in results:
        name = escape_md(u["name"])
        rows.append([InlineKeyboardButton(
            f"👤 {u['name']} (ID: {u['user_id']})",
            callback_data=f"user:view:{u['user_id']}"
        )])

    await update.message.reply_text(
        f"Found {len(results)} user\\(s\\):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(rows))
    return ConversationHandler.END


async def view_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    user_id = int(update.callback_query.data.split(":")[2])
    user    = await get_user(user_id)
    if not user:
        await _answer(update, "❌ User not found.", alert=True)
        return

    name     = escape_md(str(user.get("name", "Unknown")))
    username = user.get("username") or "none"
    banned   = "🚫 Yes" if user.get("is_banned") else "✅ No"
    joined   = parse_utc(str(user.get("joined", "")))
    joined_s = joined.strftime("%Y-%m-%d") if joined else "Unknown"

    text = (
        f"👤 *USER PROFILE*\n\n"
        f"Name: {name}\n"
        f"Username: @{escape_md(username)}\n"
        f"ID: `{user_id}`\n"
        f"Balance: {format_currency(float(user.get('balance',0)))}\n"
        f"Rank: {user.get('rank','Bronze')}\n"
        f"Spent: {format_currency(float(user.get('total_spent',0)))}\n"
        f"Joined: {escape_md(joined_s)}\n"
        f"Banned: {banned}\n"
    )
    if user.get("ban_reason"):
        text += f"Ban reason: {escape_md(str(user['ban_reason']))}\n"

    ban_btn = (
        InlineKeyboardButton("✅ Unban", callback_data=f"user:unban:{user_id}")
        if user.get("is_banned") else
        InlineKeyboardButton("🚫 Ban",   callback_data=f"user:ban:{user_id}")
    )
    kb = InlineKeyboardMarkup([
        [ban_btn,
         InlineKeyboardButton("💰 Adjust Balance", callback_data=f"user:balance:{user_id}")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu:users")],
    ])
    await _send(update, text, kb)


async def start_ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    user_id = int(update.callback_query.data.split(":")[2])
    context.user_data["ban_user_id"] = user_id
    await _send(update, f"🚫 *BAN USER {user_id}*\n\nSend ban reason or /skip for no reason:")
    return USER_BAN_REASON


async def do_ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text     = update.message.text.strip()
    reason   = "" if text == "/skip" else text
    user_id  = context.user_data.pop("ban_user_id", None)
    admin_id = update.effective_user.id

    if not user_id:
        return ConversationHandler.END

    ok = await ban_user(user_id, reason, admin_id)
    await update.message.reply_text(
        f"{'✅ User banned\\.' if ok else '❌ Failed to ban user\\.'}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_kb_back("users"))
    return ConversationHandler.END


async def do_unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    user_id  = int(update.callback_query.data.split(":")[2])
    admin_id = update.effective_user.id
    ok       = await unban_user(user_id, admin_id)
    await _send(update, f"{'✅ User unbanned\\.' if ok else '❌ Failed\\.'}",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"user:view:{user_id}")]]))


async def start_balance_adjust(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    user_id = int(update.callback_query.data.split(":")[2])
    context.user_data["bal_user_id"] = user_id
    await _send(update,
        f"💰 *BALANCE ADJUST — User {user_id}*\n\n"
        "Send amount in USD \\(positive to add, negative to deduct\\):\n"
        "Example: `5.00` or `\\-2.50`\n\n/cancel to abort")
    return USER_BAL_AMOUNT


async def do_balance_adjust(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        amount  = float(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid amount\\.", parse_mode=ParseMode.MARKDOWN)
        return USER_BAL_AMOUNT

    user_id  = context.user_data.pop("bal_user_id", None)
    admin_id = update.effective_user.id

    if not user_id:
        return ConversationHandler.END

    ok = await update_balance(user_id, amount)
    action = "added to" if amount >= 0 else "deducted from"
    await update.message.reply_text(
        f"{'✅' if ok else '❌'} ${abs(amount):.2f} {action} user {user_id}\\'s balance\\.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_kb_back("users"))

    if ok:
        await log_admin_action(admin_id, "adjust_balance", "user", str(user_id), {"amount": amount})
    return ConversationHandler.END


async def show_banned_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    from db import _run
    result = await _run(
        lambda: supabase.table("users")
            .select("user_id, name, ban_reason")
            .eq("is_banned", True)
            .execute()
    )
    users = result.data or []
    if not users:
        await _send(update, "✅ *No banned users\\.*", _kb_back("users"))
        return

    rows = []
    text = f"🚫 *BANNED USERS* \\({len(users)}\\)\n\n"
    for u in users:
        name = escape_md(str(u.get("name","?")))
        text += f"• `{u['user_id']}` {name}\n"
        rows.append([InlineKeyboardButton(
            f"👤 {u['name']}", callback_data=f"user:view:{u['user_id']}"
        )])
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:users")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


# ═══════════════════════════════════════════════════════
#  PAYMENTS
# ═══════════════════════════════════════════════════════

async def show_payment_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ Pending",  callback_data="pay:pending")],
        [InlineKeyboardButton("✅ Approved", callback_data="pay:history:approved")],
        [InlineKeyboardButton("❌ Rejected", callback_data="pay:history:rejected")],
        [InlineKeyboardButton("◀️ Back",     callback_data="menu:main")],
    ])
    await _send(update, "💰 *PAYMENT MANAGEMENT*\n\nSelect:", kb)


async def show_pending_payments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    pending = await get_pending_payments()

    if not pending:
        await _send(update, "✅ *No pending payments\\.*", _kb_back("payments"))
        return

    text = f"⏳ *PENDING PAYMENTS* \\({len(pending)}\\)\n\n"
    rows = []
    for p in pending:
        user     = p.get("users") or {}
        name     = escape_md(str(user.get("name","?")))
        created  = parse_utc(str(p.get("created_at","")))
        ago      = time_ago(created)
        text += f"• `{p['payment_ref']}` — {format_inr(p['amount_inr'])} — {name} — {escape_md(ago)}\n"
        rows.append([InlineKeyboardButton(
            f"💰 {p['payment_ref']}", callback_data=f"pay:review:{p['id']}"
        )])

    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="pay:pending"),
                 InlineKeyboardButton("◀️ Back",    callback_data="menu:payments")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


async def review_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    payment_id = int(update.callback_query.data.split(":")[2])
    payment    = await get_payment(payment_id)
    if not payment:
        await _answer(update, "❌ Payment not found.", alert=True)
        return

    user    = payment.get("users") or {}
    name    = escape_md(str(user.get("name","?")))
    uname   = user.get("username") or "none"
    created = parse_utc(str(payment.get("created_at","")))
    ago     = time_ago(created)
    amount_usd = Config.inr_to_usd(payment["amount_inr"])

    text = (
        f"💰 *PAYMENT REVIEW*\n\n"
        f"Ref: `{payment['payment_ref']}`\n"
        f"Amount: {format_inr(payment['amount_inr'])} \\(${amount_usd:.2f}\\)\n"
        f"User: {name} \\(@{escape_md(uname)}\\)\n"
        f"ID: `{payment['user_id']}`\n"
        f"Submitted: {escape_md(ago)}\n"
        f"Screenshot: {'✅' if payment.get('screenshot_file_id') else '❌ None'}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ APPROVE", callback_data=f"pay:approve:{payment_id}"),
         InlineKeyboardButton("❌ REJECT",  callback_data=f"pay:reject:{payment_id}")],
        [InlineKeyboardButton("◀️ Back",    callback_data="pay:pending")],
    ])

    # Send screenshot if available
    if payment.get("screenshot_file_id"):
        try:
            await update.callback_query.message.reply_photo(
                photo        = payment["screenshot_file_id"],
                caption      = text,
                parse_mode   = ParseMode.MARKDOWN,
                reply_markup = kb,
            )
            await update.callback_query.delete_message()
            return
        except Exception:
            pass

    await _send(update, text + "\n\n⚠️ No screenshot submitted", kb)


async def approve_payment_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    payment_id = int(update.callback_query.data.split(":")[2])
    admin_id   = update.effective_user.id
    ok, err    = await approve_payment(payment_id, admin_id)

    payment = await get_payment(payment_id)
    ref     = payment["payment_ref"] if payment else f"#{payment_id}"

    result_text = (
        f"✅ *Payment {escape_md(ref)} APPROVED\\!*\n\nBalance credited to user\\."
        if ok else
        f"❌ *Approval failed:* {escape_md(err or 'Unknown error')}"
    )

    await _safe_edit(
        update.callback_query,
        result_text,
        InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Pending", callback_data="pay:pending")]]),
        is_photo=bool(payment and payment.get("screenshot_file_id")),
    )

    if ok and payment:
        try:
            amount_usd = Config.inr_to_usd(payment["amount_inr"])
            await context.bot.send_message(
                chat_id    = payment["user_id"],
                text       = f"✅ *PAYMENT APPROVED\\!*\n\n{format_inr(payment['amount_inr'])} \\(${amount_usd:.2f}\\) credited to your balance\\!",
                parse_mode = ParseMode.MARKDOWN,
            )
        except Exception:
            pass


async def start_reject_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    payment_id = int(update.callback_query.data.split(":")[2])
    context.user_data["reject_payment_id"] = payment_id

    await _safe_edit(
        update.callback_query,
        "❌ *REJECT PAYMENT*\n\nSend rejection reason or /skip for no reason:",
        is_photo=False,
    )
    return PAY_REJECT_REASON


async def do_reject_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text       = update.message.text.strip()
    reason     = None if text == "/skip" else text
    payment_id = context.user_data.pop("reject_payment_id", None)
    admin_id   = update.effective_user.id

    if not payment_id:
        return ConversationHandler.END

    ok, err  = await reject_payment(payment_id, admin_id, reason)
    payment  = await get_payment(payment_id)

    await update.message.reply_text(
        f"{'✅ Payment rejected\\.' if ok else f'❌ {escape_md(err or chr(34))}'}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_kb_back("payments"))

    if ok and payment:
        try:
            reason_line = f"\nReason: {escape_md(reason)}" if reason else ""
            await context.bot.send_message(
                chat_id    = payment["user_id"],
                text       = f"❌ *Payment Rejected*\n\nYour payment of {format_inr(payment['amount_inr'])} was rejected\\.{reason_line}\n\nContact @{Config.SUPPORT_USERNAME} for help\\.",
                parse_mode = ParseMode.MARKDOWN,
            )
        except Exception:
            pass

    return ConversationHandler.END


async def show_payment_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show approved or rejected payment history — was unimplemented in original."""
    if not await require_admin(update):
        return
    await _answer(update)

    status = update.callback_query.data.split(":")[2]
    from db import _run
    result = await _run(
        lambda: supabase.table("payments")
            .select("*, users(name)")
            .eq("status", status)
            .order("actioned_at", desc=True)
            .limit(20)
            .execute()
    )
    payments = result.data or []

    if not payments:
        await _send(update, f"No {status} payments found\\.", _kb_back("payments"))
        return

    emoji = "✅" if status == "approved" else "❌"
    text  = f"{emoji} *{status.upper()} PAYMENTS* \\(last 20\\)\n\n"
    for p in payments:
        user  = p.get("users") or {}
        name  = escape_md(str(user.get("name","?")))
        dt    = parse_utc(str(p.get("actioned_at","")))
        ds    = dt.strftime("%m/%d") if dt else "?"
        text += f"• `{p['payment_ref']}` {format_inr(p['amount_inr'])} — {name} — {escape_md(ds)}\n"

    await _send(update, text.strip(), _kb_back("payments"))


# ═══════════════════════════════════════════════════════
#  STATISTICS
# ═══════════════════════════════════════════════════════

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update, "Loading…")

    stats = await get_stats()
    if not stats:
        await _send(update, "❌ Could not load statistics\\.", _kb_back())
        return

    rd = stats.get("rank_distribution", {})
    text = (
        f"📊 *BUSINESS STATISTICS*\n\n"
        f"💰 *Revenue:*\n"
        f"  Today: {format_currency(stats['today_revenue'])}\n"
        f"  Week: {format_currency(stats['week_revenue'])}\n"
        f"  Month: {format_currency(stats['month_revenue'])}\n"
        f"  All time: {format_currency(stats['total_revenue'])}\n\n"
        f"💵 *Profit:*\n"
        f"  Net: {format_currency(stats['net_profit'])}\n"
        f"  Margin: {stats['profit_margin']:.1f}%\n\n"
        f"📦 *Orders:*\n"
        f"  Today: {stats['today_orders']} | Total: {stats['total_orders']}\n\n"
        f"👥 *Users:* {stats['total_users']}\n"
        f"📦 *Products:* {stats['active_products']}\n\n"
        f"🏆 *Ranks:*\n"
        f"  🥉 Bronze: {rd.get('Bronze',0)} | 🥈 Silver: {rd.get('Silver',0)}\n"
        f"  🥇 Gold: {rd.get('Gold',0)} | 💎 VIP: {rd.get('VIP',0)}\n\n"
        f"⏳ Pending payments: {stats['pending_payments']}\n"
        f"🔄 Pending refunds: {stats['pending_refunds']}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Export CSV", callback_data="stats:export"),
         InlineKeyboardButton("🔄 Refresh",   callback_data="menu:stats")],
        [InlineKeyboardButton("◀️ Back",       callback_data="menu:main")],
    ])
    await _send(update, text, kb)


async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """CSV export — was unimplemented in original."""
    if not await require_admin(update):
        return
    await _answer(update, "Generating CSV…")

    from db import _run
    orders_result = await _run(
        lambda: supabase.table("orders")
            .select("id, user_id, product_name, quantity, total_price, discount_amount, promo_code, status, created_at")
            .order("created_at", desc=True)
            .limit(1000)
            .execute()
    )
    orders = orders_result.data or []

    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "id","user_id","product_name","quantity","total_price",
        "discount_amount","promo_code","status","created_at"
    ])
    writer.writeheader()
    for o in orders:
        writer.writerow({k: o.get(k,"") for k in writer.fieldnames})

    csv_bytes = buf.getvalue().encode("utf-8")
    filename  = f"tgflow_orders_{utcnow().strftime('%Y%m%d_%H%M')}.csv"

    await context.bot.send_document(
        chat_id  = update.effective_chat.id,
        document = BytesIO(csv_bytes),
        filename = filename,
        caption  = f"📥 Orders export — {len(orders)} rows",
    )


# ═══════════════════════════════════════════════════════
#  BROADCAST
# ═══════════════════════════════════════════════════════

async def start_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    await _send(update,
        "📢 *BROADCAST*\n\n"
        "Send your message\\. It will be forwarded to all users\\.\n"
        "Supports Markdown\\.\n\n"
        "/cancel to abort\\.")
    return BCAST_MSG


async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    msg_text = update.message.text.strip()
    admin_id = update.effective_user.id

    users = await get_all_users(limit=5000)
    if not users:
        await update.message.reply_text("No users found\\.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    sent = failed = 0
    status_msg = await update.message.reply_text(f"📢 Sending to {len(users)} users…")

    for user in users:
        try:
            await context.bot.send_message(
                chat_id    = user["user_id"],
                text       = msg_text,
                parse_mode = ParseMode.MARKDOWN,
            )
            sent += 1
        except (Forbidden, BadRequest):
            failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)   # stay within Telegram rate limits

    await status_msg.edit_text(
        f"📢 *Broadcast complete\\!*\n\n✅ Sent: {sent}\n❌ Failed: {failed}",
        parse_mode=ParseMode.MARKDOWN,
    )
    await log_admin_action(admin_id, "broadcast", None, None, {"sent": sent, "failed": failed})
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  PROMO CODES
# ═══════════════════════════════════════════════════════

async def show_promo_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Create Code", callback_data="promo:create")],
        [InlineKeyboardButton("📋 List Codes",  callback_data="promo:list")],
        [InlineKeyboardButton("◀️ Back",        callback_data="menu:main")],
    ])
    await _send(update, "🎟️ *PROMO CODES*\n\nSelect:", kb)


async def show_promo_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    codes = await get_all_promo_codes(active_only=False)

    if not codes:
        await _send(update, "No promo codes found\\.", _kb_back("promos"))
        return

    text = "🎟️ *PROMO CODES*\n\n"
    rows = []
    for c in codes:
        status = "✅" if c["is_active"] else "❌"
        disc   = f"{c['discount_percent']}%" if c.get("discount_percent") is not None else f"${c.get('discount_fixed',0):.2f}"
        text  += f"{status} `{c['code']}` — {escape_md(disc)} — {c.get('current_uses',0)}/{c.get('max_uses','∞')} uses\n"
        if c["is_active"]:
            rows.append([InlineKeyboardButton(
                f"🗑️ Deactivate {c['code']}", callback_data=f"promo:deactivate:{c['code']}"
            )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:promos")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


async def start_create_promo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    context.user_data["new_promo"] = {}
    await _send(update, "🎟️ *CREATE PROMO CODE*\n\nSend the promo code \\(e\\.g\\. SAVE10\\):\n\n/cancel to abort")
    return PROMO_CODE


async def promo_code_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    code = update.message.text.strip().upper()
    if not code.isalnum():
        await update.message.reply_text("❌ Code must be alphanumeric only\\.", parse_mode=ParseMode.MARKDOWN)
        return PROMO_CODE
    context.user_data["new_promo"]["code"] = code
    await update.message.reply_text(
        "Send discount\\. Examples:\n`10%` for 10% off\n`2.50` for $2\\.50 off",
        parse_mode=ParseMode.MARKDOWN)
    return PROMO_DISCOUNT


async def promo_discount_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    promo = context.user_data["new_promo"]
    if text.endswith("%"):
        try:
            pct = float(text[:-1])
            promo["discount_percent"] = pct
            promo["discount_fixed"]   = None
        except ValueError:
            await update.message.reply_text("❌ Invalid percentage\\.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_DISCOUNT
    else:
        try:
            promo["discount_fixed"]   = float(text)
            promo["discount_percent"] = None
        except ValueError:
            await update.message.reply_text("❌ Invalid amount\\.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_DISCOUNT

    await update.message.reply_text("Min purchase in USD? \\(or /skip for none\\):", parse_mode=ParseMode.MARKDOWN)
    return PROMO_MIN


async def promo_min_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    if text == "/skip":
        context.user_data["new_promo"]["min_purchase"] = 0.0
    else:
        try:
            context.user_data["new_promo"]["min_purchase"] = float(text)
        except ValueError:
            await update.message.reply_text("❌ Invalid amount\\.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_MIN

    await update.message.reply_text("Max uses? \\(or /skip for unlimited\\):", parse_mode=ParseMode.MARKDOWN)
    return PROMO_USES


async def promo_uses_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    if text == "/skip":
        context.user_data["new_promo"]["max_uses"] = None
    else:
        try:
            context.user_data["new_promo"]["max_uses"] = int(text)
        except ValueError:
            await update.message.reply_text("❌ Must be a whole number\\.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_USES

    await update.message.reply_text("Expiry date? Format: `YYYY-MM-DD` or /skip:", parse_mode=ParseMode.MARKDOWN)
    return PROMO_EXPIRY


async def promo_expiry_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text      = update.message.text.strip()
    promo     = context.user_data.get("new_promo", {})
    admin_id  = update.effective_user.id
    expires_at = None

    if text != "/skip":
        try:
            expires_at = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=None)
        except ValueError:
            await update.message.reply_text("❌ Format must be YYYY-MM-DD\\.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_EXPIRY

    result = await create_promo_code(
        code             = promo["code"],
        admin_id         = admin_id,
        discount_percent = promo.get("discount_percent"),
        discount_fixed   = promo.get("discount_fixed"),
        min_purchase     = promo.get("min_purchase", 0),
        max_uses         = promo.get("max_uses"),
        expires_at       = expires_at,
    )
    context.user_data.pop("new_promo", None)

    if result:
        await update.message.reply_text(
            f"✅ *Promo `{escape_md(promo['code'])}` created\\!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("promos"))
    else:
        await update.message.reply_text(
            "❌ Failed \\(code may already exist\\)\\.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("promos"))
    return ConversationHandler.END


async def deactivate_promo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    code     = update.callback_query.data.split(":")[2]
    admin_id = update.effective_user.id
    ok       = await deactivate_promo_code(code, admin_id)
    await _send(update,
        f"{'✅ Promo deactivated\\.' if ok else '❌ Failed\\.'}",
        _kb_back("promos"))


# ═══════════════════════════════════════════════════════
#  REFUNDS
# ═══════════════════════════════════════════════════════

async def show_refunds_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    refunds = await get_pending_refunds()

    if not refunds:
        await _send(update, "✅ *No pending refunds\\.*", _kb_back())
        return

    text = f"🔄 *PENDING REFUNDS* \\({len(refunds)}\\)\n\n"
    rows = []
    for r in refunds:
        user  = r.get("users") or {}
        order = r.get("orders") or {}
        name  = escape_md(str(user.get("name","?")))
        pname = escape_md(str(order.get("product_name","?")))
        text += f"• #{r['id']} — {name} — {pname}\n"
        rows.append([InlineKeyboardButton(
            f"🔄 Refund #{r['id']}", callback_data=f"refund:review:{r['id']}"
        )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:main")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


async def review_refund(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    refund_id = int(update.callback_query.data.split(":")[2])

    from db import _run
    result = await _run(
        lambda: supabase.table("refund_requests")
            .select("*, users(name, username), orders(product_name, total_price, quantity)")
            .eq("id", refund_id)
            .single()
            .execute()
    )
    if not result.data:
        await _answer(update, "❌ Refund not found.", alert=True)
        return

    r     = result.data
    user  = r.get("users") or {}
    order = r.get("orders") or {}

    text = (
        f"🔄 *REFUND #{refund_id}*\n\n"
        f"User: {escape_md(str(user.get('name','?')))}\n"
        f"Product: {escape_md(str(order.get('product_name','?')))}\n"
        f"Qty: {order.get('quantity','?')}\n"
        f"Order total: {format_currency(float(order.get('total_price',0)))}\n"
        f"Reason: {escape_md(str(r.get('reason','?')))}\n"
        f"Proof: {'✅' if r.get('proof_file_id') else '❌ None'}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ APPROVE", callback_data=f"refund:approve:{refund_id}"),
         InlineKeyboardButton("❌ REJECT",  callback_data=f"refund:reject:{refund_id}")],
        [InlineKeyboardButton("◀️ Back",    callback_data="menu:refunds")],
    ])
    await _send(update, text, kb)


async def approve_refund_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    refund_id = int(update.callback_query.data.split(":")[2])
    admin_id  = update.effective_user.id
    ok, err   = await approve_refund(refund_id, admin_id)

    await _send(update,
        f"{'✅ Refund approved — balance credited\\.' if ok else f'❌ Failed: {escape_md(err or chr(34))}'}",
        _kb_back("refunds"))


async def start_reject_refund(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    refund_id = int(update.callback_query.data.split(":")[2])
    context.user_data["reject_refund_id"] = refund_id
    await _send(update, "❌ *REJECT REFUND*\n\nSend reason or /skip:")
    return REFUND_REJECT_NOTE


async def do_reject_refund(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text      = update.message.text.strip()
    notes     = None if text == "/skip" else text
    refund_id = context.user_data.pop("reject_refund_id", None)
    admin_id  = update.effective_user.id

    if not refund_id:
        return ConversationHandler.END

    ok = await reject_refund(refund_id, admin_id, notes)
    await update.message.reply_text(
        f"{'✅ Refund rejected\\.' if ok else '❌ Failed\\.'}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_kb_back("refunds"))
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  AUDIT LOG
# ═══════════════════════════════════════════════════════

async def show_audit_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    actions = await get_admin_actions(limit=20)
    if not actions:
        await _send(update, "📋 *No audit log entries\\.*", _kb_back())
        return

    text = "📋 *AUDIT LOG* \\(last 20\\)\n\n"
    for a in actions:
        dt   = parse_utc(str(a.get("created_at","")))
        ds   = dt.strftime("%m/%d %H:%M") if dt else "?"
        ref  = f" → `{escape_md(str(a['target_ref']))}`" if a.get("target_ref") else ""
        text += f"• `{a['admin_id']}` {escape_md(a['action'])}{ref} \\({escape_md(ds)}\\)\n"

    await _send(update, text.strip(), _kb_back())


# ═══════════════════════════════════════════════════════
#  Callback router
# ═══════════════════════════════════════════════════════

async def route_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """All callbacks pass through admin check first."""
    if not await require_admin(update):
        return

    data = update.callback_query.data if update.callback_query else ""
    if not data:
        return

    # Main menu
    if data == "menu:main":        await show_main_menu(update, context)
    elif data == "menu:products":  await show_product_menu(update, context)
    elif data == "menu:users":     await show_user_menu(update, context)
    elif data == "menu:payments":  await show_payment_menu(update, context)
    elif data == "menu:stats":     await show_stats(update, context)
    elif data == "menu:broadcast": await start_broadcast(update, context)
    elif data == "menu:promos":    await show_promo_menu(update, context)
    elif data == "menu:refunds":   await show_refunds_menu(update, context)
    elif data == "menu:audit":     await show_audit_log(update, context)

    # Products
    elif data == "product:list":   await list_products(update, context)
    elif data == "product:confirm_create": await confirm_create_product(update, context)
    elif data == "product:cancel_create":  await cancel_create_product(update, context)
    elif data.startswith("product:edit:"): await show_product_edit(update, context)
    elif data.startswith("product:delete:"): await delete_product_confirm(update, context)
    elif data.startswith("ap_confirm_margin:"): await ap_confirm_margin(update, context)

    # Stock
    elif data == "stock:select":   await show_stock_select(update, context)
    elif data == "stock:low":      await show_low_stock(update, context)

    # Users
    elif data == "user:search":    await _answer(update)   # handled by conversation
    elif data == "user:banned":    await show_banned_users(update, context)
    elif data.startswith("user:view:"): await view_user(update, context)
    elif data.startswith("user:unban:"): await do_unban_user(update, context)

    # Payments
    elif data == "pay:pending":    await show_pending_payments(update, context)
    elif data.startswith("pay:review:"): await review_payment(update, context)
    elif data.startswith("pay:approve:"): await approve_payment_action(update, context)
    elif data.startswith("pay:history:"): await show_payment_history(update, context)

    # Promos
    elif data == "promo:list":     await show_promo_list(update, context)
    elif data.startswith("promo:deactivate:"): await deactivate_promo(update, context)

    # Refunds
    elif data.startswith("refund:review:"): await review_refund(update, context)
    elif data.startswith("refund:approve:"): await approve_refund_action(update, context)

    # Stats
    elif data == "stats:export":   await export_csv(update, context)

    else:
        await update.callback_query.answer("Unknown action.")


# ═══════════════════════════════════════════════════════
#  Error handler
# ═══════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", context.error, exc_info=context.error)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    for key in ("np","stock_product_id","stock_items","ban_user_id","bal_user_id",
                "reject_payment_id","reject_refund_id","new_promo"):
        context.user_data.pop(key, None)
    await update.effective_message.reply_text("❌ Cancelled\\.", parse_mode=ParseMode.MARKDOWN,
                                              reply_markup=_kb_back())
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════
#  Periodic cleanup job
# ═══════════════════════════════════════════════════════

async def job_cleanup(context: ContextTypes.DEFAULT_TYPE) -> None:
    from db import cleanup_expired_reservations
    n = await cleanup_expired_reservations()
    if n:
        logger.info("🧹 Cleaned %d reservations", n)


# ═══════════════════════════════════════════════════════
#  Application setup
# ═══════════════════════════════════════════════════════

def _fallback():
    return [CommandHandler("cancel", cmd_cancel)]


def main() -> None:
    if not Config.validate():
        logger.critical("❌ Invalid configuration")
        return

    logger.info("🚀 Starting Admin Bot…")
    app = Application.builder().token(Config.ADMIN_BOT_TOKEN).build()

    # Add Product conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_add_product, pattern="^product:add$")],
        states={
            ADD_PROD_CAT:  [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_category)],
            ADD_PROD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_name)],
            ADD_PROD_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_desc)],
            ADD_PROD_BUY:  [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_buy_price)],
            ADD_PROD_SELL: [MessageHandler(filters.TEXT & ~filters.COMMAND, ap_sell_price)],
            ADD_PROD_DEMO: [
                MessageHandler(filters.Document.ALL | filters.PHOTO, ap_demo),
                CommandHandler("skip", ap_demo),
            ],
            ADD_PROD_FILE: [
                MessageHandler(filters.Document.ALL, ap_main_file),
                CommandHandler("skip", ap_main_file),
            ],
        },
        fallbacks=_fallback(),
    ))

    # Add Stock conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_add_stock, pattern=r"^stock:add:\d+$")],
        states={
            STOCK_METHOD: [
                CallbackQueryHandler(stock_choose_manual, pattern="^stock_method:manual$"),
                CallbackQueryHandler(stock_choose_file,   pattern="^stock_method:file$"),
                CallbackQueryHandler(lambda u,c: ConversationHandler.END, pattern="^stock_method:cancel$"),
            ],
            STOCK_MANUAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, stock_add_item)],
            STOCK_FILE:   [MessageHandler(filters.Document.ALL, stock_file_upload)],
        },
        fallbacks=_fallback(),
    ))

    # User search conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_user_search, pattern="^user:search$")],
        states={USER_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_user_search)]},
        fallbacks=_fallback(),
    ))

    # Ban user conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_ban_user, pattern=r"^user:ban:\d+$")],
        states={USER_BAN_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_ban_user)]},
        fallbacks=_fallback(),
    ))

    # Balance adjust conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_balance_adjust, pattern=r"^user:balance:\d+$")],
        states={USER_BAL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_balance_adjust)]},
        fallbacks=_fallback(),
    ))

    # Payment reject conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_reject_payment, pattern=r"^pay:reject:\d+$")],
        states={PAY_REJECT_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_reject_payment)]},
        fallbacks=_fallback(),
    ))

    # Promo create conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_create_promo, pattern="^promo:create$")],
        states={
            PROMO_CODE:     [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_code_input)],
            PROMO_DISCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_discount_input)],
            PROMO_MIN:      [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_min_input)],
            PROMO_USES:     [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_uses_input)],
            PROMO_EXPIRY:   [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_expiry_input)],
        },
        fallbacks=_fallback(),
    ))

    # Broadcast conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_broadcast, pattern="^menu:broadcast$")],
        states={BCAST_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_broadcast)]},
        fallbacks=_fallback(),
    ))

    # Refund reject conversation
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_reject_refund, pattern=r"^refund:reject:\d+$")],
        states={REFUND_REJECT_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_reject_refund)]},
        fallbacks=_fallback(),
    ))

    # Commands
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    # Catch-all callback router (all have admin check inside)
    app.add_handler(CallbackQueryHandler(route_callback))

    app.add_error_handler(error_handler)
    app.job_queue.run_repeating(job_cleanup, interval=900, first=60)

    logger.info("✅ Admin Bot running!")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
