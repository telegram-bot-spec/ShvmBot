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
    # TG Accounts
    get_tg_account_by_id, link_tg_account_to_product,
    get_tg_accounts_for_product, get_tg_account_stock_count,
    supabase,
)
from utils import md_username, utcnow, parse_utc, time_ago

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

# Product — category selected via inline button, then name/desc/prices
ADD_PROD_CAT_CUSTOM = 0  # only used when admin types a brand new category
ADD_PROD_NAME  = 2
ADD_PROD_DESC  = 3
ADD_PROD_BUY   = 4
ADD_PROD_SELL  = 5
ADD_PROD_DEMO  = 6
ADD_PROD_FILE  = 7
ADD_PROD_CONFIRM = 8   # inline confirmation replaces the broken /yes

# Sub-product flow: when a known category is chosen, ask sub-type
ADD_PROD_SUBTYPE = 9   # e.g. OTP → Telegram / WP / IG / add new
ADD_PROD_SUBTYPE_CUSTOM = 95  # admin types a new sub-type name

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

# TG Account management (admin adds accounts via OTP)
TG_ACCT_PHONE = 71
TG_ACCT_CODE  = 72
TG_ACCT_2FA   = 73
TG_ACCT_LINK  = 74   # after successful add — optionally link to a product

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
        [InlineKeyboardButton("📦 Products",    callback_data="menu:products"),
         InlineKeyboardButton("👥 Users",       callback_data="menu:users")],
        [InlineKeyboardButton("💰 Payments",    callback_data="menu:payments"),
         InlineKeyboardButton("📊 Statistics",  callback_data="menu:stats")],
        [InlineKeyboardButton("📢 Broadcast",   callback_data="menu:broadcast"),
         InlineKeyboardButton("🎟️ Promo Codes", callback_data="menu:promos")],
        [InlineKeyboardButton("🔄 Refunds",     callback_data="menu:refunds"),
         InlineKeyboardButton("📋 Audit Log",   callback_data="menu:audit")],
        [InlineKeyboardButton("📱 TG Accounts", callback_data="menu:tgaccounts"),
         InlineKeyboardButton("⚠️ Low Stock",   callback_data="stock:low")],
        [InlineKeyboardButton("📤 Export CSV",  callback_data="stats:export")],
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

    from datetime import datetime, timezone
    now_str = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")

    # Quick pending counts — use parallel count queries, NOT full row fetches
    from db import _run, supabase as _supa
    pay_res, ref_res = await asyncio.gather(
        _run(lambda: _supa.table("payments").select("id", count="exact").eq("status", "pending").execute()),
        _run(lambda: _supa.table("refund_requests").select("id", count="exact").eq("status", "pending").execute()),
    )
    pay_count = pay_res.count or 0
    ref_count = ref_res.count or 0
    pay_alert   = f"⚠️ *{pay_count} payment{'s' if pay_count!=1 else ''} awaiting review*\n" if pay_count else "✅ No pending payments\n"
    ref_alert   = f"⚠️ *{ref_count} refund{'s' if ref_count!=1 else ''} awaiting review*\n" if ref_count else "✅ No pending refunds\n"

    text = (
        f"🛡️ *ADMIN CONTROL PANEL*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👋 Welcome back, *{escape_md(uname)}!*\n\n"
        f"🏷️ *Role:* {role}\n"
        f"🆔 *Admin ID:* `{uid}`\n"
        f"🕐 *Session:* {now_str}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *Dashboard Alerts:*\n"
        f"{pay_alert}"
        f"{ref_alert}\n"
        f"Select a section below to get started."
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
    text = (
        f"🛡️ *ADMIN CONTROL PANEL*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*{escape_md(uname)}* — {role}\n\n"
        f"What would you like to manage?"
    )
    await _send(update, text, _kb_main())


# ═══════════════════════════════════════════════════════
#  PRODUCTS
# ═══════════════════════════════════════════════════════

async def show_product_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    # Quick stock summary
    low = await get_low_stock_products()
    low_count = len(low)
    low_note = f"\n⚠️ *{low_count} product{'s' if low_count != 1 else ''} low/out of stock*" if low_count else "\n✅ All products well-stocked"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 OTP",            callback_data="ap_cat:OTP"),
         InlineKeyboardButton("📄 Papers",          callback_data="ap_cat:Papers")],
        [InlineKeyboardButton("🔐 Subscriptions",   callback_data="ap_cat:Subscriptions")],
        [InlineKeyboardButton("➕ Add New Product", callback_data="product:add")],
        [InlineKeyboardButton("📋 View All Products",    callback_data="product:list"),
         InlineKeyboardButton("📦 Add Stock",            callback_data="stock:select")],
        [InlineKeyboardButton("⚠️ Low Stock Alerts",     callback_data="stock:low")],
        [InlineKeyboardButton("◀️ Back to Main Panel",   callback_data="menu:main")],
    ])
    await _send(update,
        f"📦 *PRODUCT MANAGEMENT*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Quickly add a product by selecting its category below, "
        f"or use the management buttons to view/edit existing products.{low_note}", kb)


async def list_products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    categories = await get_categories()
    if not categories:
        await _send(update, "No products found.", _kb_back("products"))
        return

    text = "📦 *ALL PRODUCTS*\n\n"
    rows = []
    # Fetch all categories in parallel — one gather() instead of N sequential awaits
    all_products_list = await asyncio.gather(*[
        get_products_by_category(cat) for cat in categories
    ])
    for cat, products in zip(categories, all_products_list):
        if not products:
            continue
        text += f"*{escape_md(cat)}:*\n"
        for p in products:
            stock = p.get("stock_count", 0)
            text += f"  • {escape_md(p['name'])} — {format_inr(p['selling_price'])} ({stock} left)\n"
            rows.append([InlineKeyboardButton(
                f"✏️ {p['name']}", callback_data=f"product:edit:{p['id']}"
            )])
        text += "\n"

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:products")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


# ── Add Product — Step 0: category picker (inline buttons) ───────────

# Known categories with their sub-products
_KNOWN_CATEGORIES = {
    "OTP":           ["Telegram", "WhatsApp", "Instagram"],
    "Papers":        [],   # no sub-types for papers
    "Subscriptions": ["Netflix", "Spotify", "Amazon Prime", "Disney+", "YouTube Premium"],
}

def _kb_add_product_category(categories: list) -> InlineKeyboardMarkup:
    """4 preset category buttons + Add Product (custom) button."""
    rows = [
        [InlineKeyboardButton("📱 OTP",            callback_data="ap_cat:OTP"),
         InlineKeyboardButton("📄 Papers",          callback_data="ap_cat:Papers")],
        [InlineKeyboardButton("🔐 Subscriptions",   callback_data="ap_cat:Subscriptions")],
        [InlineKeyboardButton("➕ Add New Category", callback_data="ap_cat:__custom__")],
        [InlineKeyboardButton("◀️ Back",             callback_data="menu:products")],
    ]
    return InlineKeyboardMarkup(rows)


def _kb_otp_subtype() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✈️ Telegram",        callback_data="ap_sub:Telegram"),
         InlineKeyboardButton("💬 WhatsApp",        callback_data="ap_sub:WhatsApp")],
        [InlineKeyboardButton("📸 Instagram",       callback_data="ap_sub:Instagram")],
        [InlineKeyboardButton("➕ Add New Sub-Product", callback_data="ap_sub:__custom__")],
        [InlineKeyboardButton("◀️ Back",             callback_data="product:add")],
    ])


def _kb_sub_subtype(subs: list) -> InlineKeyboardMarkup:
    """Generic sub-type picker for Subscriptions."""
    rows = []
    row  = []
    for s in subs:
        row.append(InlineKeyboardButton(s, callback_data=f"ap_sub:{s}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("➕ Add New Sub-Product", callback_data="ap_sub:__custom__")])
    rows.append([InlineKeyboardButton("◀️ Back", callback_data="product:add")])
    return InlineKeyboardMarkup(rows)


# ── OTP → Telegram: country picker (35 popular countries) ──────────────

_OTP_TG_COUNTRIES: List[Tuple[str, str, str]] = [
    # (display_label, callback_slug, phone_prefix)
    ("🇮🇳 India",        "India",        "+91"),
    ("🇵🇰 Pakistan",     "Pakistan",     "+92"),
    ("🇧🇩 Bangladesh",   "Bangladesh",   "+880"),
    ("🇳🇬 Nigeria",      "Nigeria",      "+234"),
    ("🇺🇸 USA",          "USA",          "+1"),
    ("🇷🇺 Russia",       "Russia",       "+7"),
    ("🇧🇷 Brazil",       "Brazil",       "+55"),
    ("🇵🇭 Philippines",  "Philippines",  "+63"),
    ("🇮🇩 Indonesia",    "Indonesia",    "+62"),
    ("🇺🇦 Ukraine",      "Ukraine",      "+380"),
    ("🇬🇧 UK",           "UK",           "+44"),
    ("🇩🇪 Germany",      "Germany",      "+49"),
    ("🇫🇷 France",       "France",       "+33"),
    ("🇵🇱 Poland",       "Poland",       "+48"),
    ("🇲🇾 Malaysia",     "Malaysia",     "+60"),
    ("🇹🇷 Turkey",       "Turkey",       "+90"),
    ("🇪🇬 Egypt",        "Egypt",        "+20"),
    ("🇰🇿 Kazakhstan",   "Kazakhstan",   "+77"),
    ("🇺🇿 Uzbekistan",   "Uzbekistan",   "+998"),
    ("🇻🇳 Vietnam",      "Vietnam",      "+84"),
    ("🇸🇦 Saudi Arabia", "Saudi_Arabia", "+966"),
    ("🇦🇪 UAE",          "UAE",          "+971"),
    ("🇮🇶 Iraq",         "Iraq",         "+964"),
    ("🇮🇷 Iran",         "Iran",         "+98"),
    ("🇬🇭 Ghana",        "Ghana",        "+233"),
    ("🇪🇹 Ethiopia",     "Ethiopia",     "+251"),
    ("🇰🇪 Kenya",        "Kenya",        "+254"),
    ("🇹🇿 Tanzania",     "Tanzania",     "+255"),
    ("🇿🇦 South Africa", "South_Africa", "+27"),
    ("🇨🇴 Colombia",     "Colombia",     "+57"),
    ("🇦🇷 Argentina",    "Argentina",    "+54"),
    ("🇲🇽 Mexico",       "Mexico",       "+52"),
    ("🇹🇭 Thailand",     "Thailand",     "+66"),
    ("🇰🇷 South Korea",  "South_Korea",  "+82"),
    ("🇯🇵 Japan",        "Japan",        "+81"),
    ("🇸🇬 Singapore",    "Singapore",    "+65"),
]


def _kb_otp_tg_countries() -> InlineKeyboardMarkup:
    """36 country buttons displayed 3 per row, plus Back."""
    rows = []
    row  = []
    for label, slug, prefix in _OTP_TG_COUNTRIES:
        row.append(InlineKeyboardButton(label, callback_data=f"ap_tg_country:{slug}:{prefix}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("➕ Other / Custom", callback_data="ap_tg_country:__custom__:"),
                 InlineKeyboardButton("◀️ Back",           callback_data="ap_sub:Telegram")])
    return InlineKeyboardMarkup(rows)


async def ap_tg_country_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Callback: admin tapped a country button in the OTP→Telegram picker.
    Stores country + prefix in np, pre-fills a product name, then goes to ADD_PROD_NAME.
    """
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    parts  = update.callback_query.data.split(":", 2)
    slug   = parts[1]
    prefix = parts[2] if len(parts) > 2 else ""

    if slug == "__custom__":
        await _send(update,
            "✏️ *CUSTOM COUNTRY*\n\n"
            "Type the country name and send it:\n"
            "_e.g. 'Iraq', 'Ethiopia'_\n\n"
            "/cancel to abort")
        context.user_data.setdefault("np", {})["pending_country_custom"] = True
        return ADD_PROD_NAME   # first message will be taken as country name → then re-prompt for name

    country_label = slug.replace("_", " ")
    np = context.user_data.setdefault("np", {})
    np["country_slug"]   = slug
    np["country_prefix"] = prefix
    np["country_label"]  = country_label
    # Pre-fill a suggested product name — admin can override in the next step
    np["_name_hint"]     = f"Telegram OTP — {country_label} ({prefix})"

    await _send(update,
        f"🌍 *Country selected:* {country_label} {prefix}\n\n"
        f"*Next:* Confirm or edit the product name:\n"
        f"_Suggested:_ `{escape_md(np['_name_hint'])}`\n\n"
        f"Send the name or just press /skip to use the suggestion.\n"
        f"/cancel to abort")
    return ADD_PROD_NAME


async def start_add_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Step 0: show inline category buttons."""
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    categories = await get_categories()
    context.user_data["np"] = {}

    text = (
        "➕ *ADD NEW PRODUCT*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Select the *product category* below.\n\n"
        "📱 *OTP* — Telegram / WhatsApp / Instagram numbers\n"
        "📄 *Papers* — Digital documents delivered as a file\n"
        "🔐 *Subscriptions* — Netflix, Spotify, etc.\n"
        "➕ *Add New Category* — create your own custom category\n\n"
        "/cancel to abort"
    )
    await _send(update, text, _kb_add_product_category(categories))
    # No conversation state returned — category is picked via callback, not text
    return ConversationHandler.END   # conversation re-entered by ap_cat_chosen callback


async def ap_cat_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Callback: admin tapped a preset category button."""
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    cat = update.callback_query.data.split(":", 1)[1]

    if cat == "__custom__":
        # Ask admin to type a custom category name
        context.user_data["np"] = {}
        await _send(update,
            "✏️ *CUSTOM CATEGORY*\n\n"
            "Type your new category name and send it:\n"
            "_e.g. Accounts, Gaming, Other_\n\n"
            "/cancel to abort")
        return ADD_PROD_CAT_CUSTOM

    # Preset category chosen
    context.user_data["np"] = {"category": cat}

    # Check if this category has known sub-types
    sub_types = _KNOWN_CATEGORIES.get(cat, [])
    if sub_types or cat == "OTP":
        # Show sub-product picker
        return await _show_subtype_picker(update, context, cat)

    # No sub-types (like Papers) — go straight to name
    await _send(update,
        f"✅ *Category:* {escape_md(cat)}\n\n"
        f"*Step 1:* Send the *product name*:\n"
        f"_e.g. 'India +91 Number', 'Netflix 1 Month'_\n\n"
        f"/cancel to abort")
    return ADD_PROD_NAME


async def _show_subtype_picker(update: Update, context: ContextTypes.DEFAULT_TYPE, cat: str) -> int:
    """Show the sub-product selection keyboard."""
    if cat == "OTP":
        kb   = _kb_otp_subtype()
        text = (
            f"📱 *OTP — SELECT PLATFORM*\n\n"
            f"Which platform is this OTP number for?\n\n"
            f"• *Telegram* — Telegram account numbers (country picker follows)\n"
            f"• *WhatsApp* — WhatsApp account numbers\n"
            f"• *Instagram* — Instagram account numbers\n"
            f"• *Add New Sub-Product* — add your own platform"
        )
    else:
        subs = _KNOWN_CATEGORIES.get(cat, [])
        kb   = _kb_sub_subtype(subs)
        text = (
            f"🔐 *{escape_md(cat)} — SELECT SUB-TYPE*\n\n"
            f"Choose the specific sub-product type, or add a new one."
        )
    await _send(update, text, kb)
    return ConversationHandler.END  # sub-type picked via ap_sub_chosen callback


async def ap_sub_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Callback: admin tapped a sub-type button."""
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    sub = update.callback_query.data.split(":", 1)[1]
    cat = context.user_data.get("np", {}).get("category", "")

    if sub == "__custom__":
        await _send(update,
            "✏️ *ADD NEW SUB-PRODUCT*\n\n"
            "Type the sub-product name and send it:\n"
            "_e.g. Twitter, TikTok, Snapchat_\n\n"
            "/cancel to abort")
        return ADD_PROD_SUBTYPE_CUSTOM

    # ── OTP → Telegram: show country picker instead of name entry ──────
    if cat == "OTP" and sub == "Telegram":
        context.user_data["np"]["sub_type"] = "Telegram"
        await _send(update,
            "🌍 *OTP — TELEGRAM — SELECT COUNTRY*\n\n"
            "Choose the country for this OTP product.\n"
            "Each country becomes its own product in the shop.\n\n"
            "_Tap a country to continue:_",
            _kb_otp_tg_countries())
        return ConversationHandler.END   # country picked via ap_tg_country: callback

    # Combine category + sub-type as the final product name seed
    context.user_data["np"]["sub_type"] = sub
    full_name_hint = f"{cat} — {sub}"

    await _send(update,
        f"✅ *Category:* {escape_md(cat)}\n"
        f"✅ *Sub-Type:* {escape_md(sub)}\n\n"
        f"*Next:* Send the *product name*:\n"
        f"_Suggested: `{escape_md(full_name_hint)}`_\n"
        f"_Or type anything you prefer._\n\n"
        f"/cancel to abort")
    return ADD_PROD_NAME


async def ap_subtype_custom(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Admin typed a custom sub-type name."""
    sub = update.message.text.strip()
    cat = context.user_data.get("np", {}).get("category", "")
    context.user_data["np"]["sub_type"] = sub
    full_name_hint = f"{cat} — {sub}"

    await update.message.reply_text(
        f"✅ *Sub-Type set:* {escape_md(sub)}\n\n"
        f"*Next:* Send the *product name*:\n"
        f"_Suggested: `{escape_md(full_name_hint)}`_\n"
        f"_Or type anything you prefer._\n\n"
        f"/cancel to abort",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_NAME


async def ap_cat_custom_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Admin typed a custom category name."""
    cat = update.message.text.strip()
    context.user_data["np"]["category"] = cat
    await update.message.reply_text(
        f"✅ *Category set:* {escape_md(cat)}\n\n"
        f"*Step 1:* Send the *product name*:\n"
        f"/cancel to abort",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_NAME


async def ap_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    np   = context.user_data.setdefault("np", {})

    # /skip → use the pre-filled country-based hint (e.g. "Telegram OTP — India (+91)")
    if text in ("/skip", "") and np.get("_name_hint"):
        np["name"] = np.pop("_name_hint")
    else:
        np.pop("_name_hint", None)
        if not text or text.startswith("/"):
            await update.message.reply_text(
                "❌ Please send a valid product name, or /skip to use the suggested name.",
                parse_mode=ParseMode.MARKDOWN)
            return ADD_PROD_NAME
        np["name"] = text

    await update.message.reply_text(
        f"✅ *Name set:* {escape_md(np['name'])}\n\n"
        "*Next:* Send a *description* for this product, or /skip:\n"
        "_Descriptions appear on the product page in the shop. "
        "Include key details like country, validity, or format._",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_DESC


async def ap_desc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    # Handles both /skip from CommandHandler and typed "/skip"
    context.user_data["np"]["description"] = "" if (text == "/skip" or text == "") else text
    await update.message.reply_text(
        "✅ *Description set!*\n\n"
        "*Step 4:* Enter your *purchase/cost price* in USD:\n"
        "_This is what you pay per unit — used internally for profit calculations. "
        "Customers never see this price._ Example: `1.50`",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_BUY


async def ap_buy_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        price = float(update.message.text.strip())
        if price < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a valid positive number.", parse_mode=ParseMode.MARKDOWN)
        return ADD_PROD_BUY

    context.user_data["np"]["purchase_price"] = price
    await update.message.reply_text(
        f"✅ *Cost price set:* ${price:.2f}\n\n"
        f"*Step 5:* Enter the *selling price* in USD (what customers pay):\n"
        f"_Must be higher than cost price for positive margin._ Example: `2.50`",
        parse_mode=ParseMode.MARKDOWN)
    return ADD_PROD_SELL


async def ap_sell_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        price = float(update.message.text.strip())
        if price < 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Enter a valid positive number.", parse_mode=ParseMode.MARKDOWN)
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
            f"⚠️ *Negative margin warning!*\n\n"
            f"Selling (${price:.2f}) < Purchase (${buy:.2f})\n"
            f"Loss per unit: ${abs(profit):.2f}\n\n"
            "Proceed anyway?",
            parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return ADD_PROD_SELL   # stay in state until inline confirmation

    await update.message.reply_text(
        f"✅ *Selling price set:* ${price:.2f}\n"
        f"💰 *Margin:* ${profit:.2f} profit per unit ({margin:.1f}%)\n\n"
        f"*Step 6:* Send a *demo file* (optional — image or PDF shown to buyers before purchase), or /skip:\n"
        f"_Demo files help buyers understand what they're purchasing._",
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
        f"✅ Proceeding with ${price:.2f} (loss ${abs(price-buy):.2f}/unit)\n\n"
        "Send *demo file* (optional) or /skip:",
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
        await update.message.reply_text(
            "📄 *Papers category detected!*\n\n"
            "*Step 7:* Upload the *main delivery file* for this product.\n"
            "_This is the actual file sent to the buyer upon purchase — "
            "e.g. a PDF, image, or document. Or /skip if not ready yet._",
            parse_mode=ParseMode.MARKDOWN)
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

    margin_note = f"📈 Profit: ${profit:.2f}/unit ({margin:.1f}%)" if profit >= 0 else f"⚠️ Loss: ${abs(profit):.2f}/unit ({margin:.1f}%)"
    summary = (
        f"📦 *PRODUCT SUMMARY — REVIEW*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📂 *Category:* {escape_md(np['category'])}\n"
        f"🏷️ *Name:* {escape_md(np['name'])}\n"
        f"📝 *Description:* {escape_md(np.get('description') or '—')}\n\n"
        f"💸 *Cost Price:* ${np['purchase_price']:.2f}\n"
        f"💰 *Selling Price:* ${np['selling_price']:.2f}\n"
        f"{margin_note}\n\n"
        f"🖼️ *Demo File:* {'✅ Attached' if np.get('demo_file_id') else '❌ None'}\n"
        f"📄 *Main File:* {'✅ Attached' if np.get('main_file_id') else '❌ None'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_Everything look correct? Tap Create to publish this product, "
        f"or Cancel to discard._"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅  Create Product",  callback_data="product:confirm_create"),
         InlineKeyboardButton("❌  Discard",         callback_data="product:cancel_create")],
    ])
    await update.effective_message.reply_text(summary, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    return ConversationHandler.END


async def confirm_create_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    np = context.user_data.get("np")
    if not np:
        await _send(update, "❌ Session expired. Start over with Add Product.", _kb_back("products"))
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
        cat_lower = np.get("category","").lower()
        delivery_note = {
            "papers":        "📄 Upload a main file via *Add Stock* → stock items are ignored for this category.",
            "otp":           "📱 Add stock as text lines: `+CountryCode PhoneNumber | SessionString`",
            "subscriptions": "🔐 Add stock as text lines: `email@example.com:password`",
            "accounts":      "👤 Add stock as text lines: `username:password` or any format you choose.",
        }.get(cat_lower, "📦 Add stock as text lines — one item per line.")
        await _send(update,
            f"✅ *Product Created Successfully!*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 *Product ID:* `{product['id']}`\n"
            f"🏷️ *Name:* {escape_md(product['name'])}\n\n"
            f"⚡ *Next step — Add Stock:*\n{delivery_note}\n\n"
            f"_The product is live but shows as out of stock until you add items._",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("📦  Add Stock Now",   callback_data=f"stock:add:{product['id']}")],
                [InlineKeyboardButton("◀️  Back to Products", callback_data="menu:products")],
            ]))
    else:
        await _send(update, "❌ *Failed to create product.*\n\nThe database rejected the insert — check that all fields are valid and try again.", _kb_back("products"))


async def cancel_create_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    context.user_data.pop("np", None)
    await _send(update,
        "❌ *Product creation cancelled.*\n\n"
        "No product was created. You can start again anytime from the Products menu.",
        _kb_back("products"))


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
        await _send(update, f"✅ Product #{product_id} deactivated.", _kb_back("products"))
    else:
        await _send(update, "❌ Failed to delete product.", _kb_back("products"))


# ── Stock management ────────────────────────────────────

async def show_stock_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    categories = await get_categories()
    if not categories:
        await _send(update, "No products found.", _kb_back("products"))
        return

    rows = []
    text = "📦 *SELECT PRODUCT FOR STOCK*\n\n"
    # Parallel fetch — replaces N sequential awaits
    all_products_list = await asyncio.gather(*[
        get_products_by_category(cat) for cat in categories
    ])
    for products in all_products_list:
        for p in products:
            stock = p.get("stock_count", 0)
            text += f"• {escape_md(p['name'])} ({stock} left)\n"
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
        await _send(update, "❌ Product not found.")
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
        "Send items one per message.\n"
        "Send /done when finished.\n"
        "Send /cancel to abort.\n\n"
        "OTP format example:\n`\\+1234567890 \\| India \\| 2FA: pass`")
    return STOCK_MANUAL


async def stock_choose_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    await _send(update,
        "📄 *BULK STOCK UPLOAD*\n\n"
        "Send a `.txt` file — one item per line.\n"
        "Max 5,000 lines per upload.\n\n"
        "/cancel to abort.")
    return STOCK_FILE


async def stock_add_item(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # /done is handled by CommandHandler("done", _finalize_stock) in the ConversationHandler.
    # filters.TEXT & ~filters.COMMAND drops commands, so never check for /done here.
    text = update.message.text.strip()
    if not text:
        return STOCK_MANUAL

    items = context.user_data.setdefault("stock_items", [])
    items.append(text)
    await update.message.reply_text(
        f"✅ Item {len(items)} added. Send more or /done to save.",
        parse_mode=ParseMode.MARKDOWN)
    return STOCK_MANUAL

async def stock_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message.document:
        await update.message.reply_text("❌ Please send a .txt file.", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    doc = update.message.document
    if doc.file_size and doc.file_size > 5 * 1024 * 1024:   # 5 MB hard limit
        await update.message.reply_text("❌ File too large (max 5MB).", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    tg_file = await doc.get_file()
    content = await tg_file.download_as_bytearray()

    try:
        lines = content.decode("utf-8").strip().splitlines()
    except UnicodeDecodeError:
        await update.message.reply_text("❌ File must be UTF-8 encoded.", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    items = [l.strip() for l in lines if l.strip()][:5000]   # cap at 5000 lines
    if not items:
        await update.message.reply_text("❌ File is empty.", parse_mode=ParseMode.MARKDOWN)
        return STOCK_FILE

    context.user_data["stock_items"] = items
    await update.message.reply_text(
        f"✅ Parsed *{len(items)}* items. Processing…",
        parse_mode=ParseMode.MARKDOWN)
    return await _finalize_stock(update, context)


async def _finalize_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    product_id = context.user_data.get("stock_product_id")
    items      = context.user_data.get("stock_items", [])
    admin_id   = update.effective_user.id

    # Guard: product_id must be present — if lost, DB insert fails silently with @db_op
    if not product_id:
        await update.effective_message.reply_text(
            "❌ *Session error:* Product ID was lost.\n\n"
            "This can happen if the bot restarted mid-flow.\n"
            "Please tap *Add Stock* again from the product menu.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("products"))
        return ConversationHandler.END

    if not items:
        await update.effective_message.reply_text("❌ No items to add.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    await update.effective_message.reply_text(
        f"⏳ Saving *{len(items)}* item(s) to database…",
        parse_mode=ParseMode.MARKDOWN)

    # bulk_add_stock is now chunked internally — no 6MB crash
    count = await bulk_add_stock(product_id, items, admin_id)
    new_total = await get_stock_count(product_id)

    context.user_data.pop("stock_product_id", None)
    context.user_data.pop("stock_items", None)

    if count and count > 0:
        await update.effective_message.reply_text(
            f"✅ *Stock saved to database!*\n\n"
            f"Added: {count}/{len(items)} items\n"
            f"Total stock now: {new_total}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("products"))
    else:
        await update.effective_message.reply_text(
            f"❌ *Stock save failed!*\n\n"
            f"0 of {len(items)} items were saved.\n"
            f"Check bot logs for the database error.\n\n"
            f"Common causes: product deleted, DB connection error.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("products"))
    return ConversationHandler.END


async def show_low_stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    low = await get_low_stock_products()
    if not low:
        await _send(update, "✅ *All products well-stocked!*", _kb_back("products"))
        return

    text = "⚠️ *LOW STOCK ALERT*\n\n"
    rows = []
    for p in low:
        stock = p.get("stock_count", 0)
        flag  = "❌ OUT" if stock == 0 else f"⚠️ {stock} left"
        text += f"{flag} — {escape_md(p['name'])} ({escape_md(p['category'])})\n"
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
        await update.message.reply_text("❌ No users found.", parse_mode=ParseMode.MARKDOWN)
        return USER_SEARCH

    rows = []
    for u in results:
        name = escape_md(u["name"])
        rows.append([InlineKeyboardButton(
            f"👤 {u['name']} (ID: {u['user_id']})",
            callback_data=f"user:view:{u['user_id']}"
        )])

    await update.message.reply_text(
        f"Found {len(results)} user(s):",
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
        f"Username: {md_username(username)}\n"
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
        f"{'✅ User banned.' if ok else '❌ Failed to ban user.'}",
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
    await _send(update, f"{'✅ User unbanned.' if ok else '❌ Failed.'}",
                InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"user:view:{user_id}")]]))


async def start_balance_adjust(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    user_id = int(update.callback_query.data.split(":")[2])
    context.user_data["bal_user_id"] = user_id
    await _send(update,
        f"💰 *BALANCE ADJUST — User {user_id}*\n\n"
        "Send amount in USD (positive to add, negative to deduct):\n"
        "Example: `5.00` or `-2.50`\n\n/cancel to abort")
    return USER_BAL_AMOUNT


async def do_balance_adjust(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        amount  = float(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid amount.", parse_mode=ParseMode.MARKDOWN)
        return USER_BAL_AMOUNT

    user_id  = context.user_data.pop("bal_user_id", None)
    admin_id = update.effective_user.id

    if not user_id:
        return ConversationHandler.END

    ok = await update_balance(user_id, amount)
    action = "added to" if amount >= 0 else "deducted from"
    await update.message.reply_text(
        f"{'✅' if ok else '❌'} ${abs(amount):.2f} {action} user {user_id}'s balance.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_kb_back("users"))

    if ok:
        await log_admin_action(admin_id, "adjust_balance", "user", str(user_id), {"amount": amount})
    return ConversationHandler.END


async def show_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User statistics overview — was missing (button existed but no handler)."""
    if not await require_admin(update):
        return
    await _answer(update)

    from db import _run
    total_result = await _run(
        lambda: supabase.table("users").select("user_id", count="exact").execute()
    )
    banned_result = await _run(
        lambda: supabase.table("users").select("user_id", count="exact").eq("is_banned", True).execute()
    )
    rank_counts = {}
    for rank in ("Bronze", "Silver", "Gold", "VIP"):
        r = await _run(
            lambda rk=rank: supabase.table("users")
                .select("user_id", count="exact")
                .eq("rank", rk)
                .execute()
        )
        rank_counts[rank] = r.count or 0

    total  = total_result.count or 0
    banned = banned_result.count or 0

    text = (
        f"📊 *USER STATISTICS*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 *Total Users:* {total}\n"
        f"🚫 *Banned:* {banned}\n"
        f"✅ *Active:* {total - banned}\n\n"
        f"🏆 *Rank Distribution:*\n"
        f"  🥉 Bronze: {rank_counts.get('Bronze', 0)}\n"
        f"  🥈 Silver: {rank_counts.get('Silver', 0)}\n"
        f"  🥇 Gold:   {rank_counts.get('Gold', 0)}\n"
        f"  💎 VIP:    {rank_counts.get('VIP', 0)}\n"
    )
    await _send(update, text, _kb_back("users"))


async def show_banned_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List all banned users with links to their profiles."""
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
        await _send(update, "✅ *No banned users.*", _kb_back("users"))
        return

    rows = []
    text = f"🚫 *BANNED USERS* ({len(users)})\n\n"
    for u in users:
        name = escape_md(str(u.get("name","?")))
        text += f"• `{u['user_id']}` {name}\n"
        rows.append([InlineKeyboardButton(
            f"👤 {u.get('name','?')}", callback_data=f"user:view:{u['user_id']}"
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
        [InlineKeyboardButton("⏳  Pending Review",     callback_data="pay:pending")],
        [InlineKeyboardButton("✅  Approved History",   callback_data="pay:history:approved")],
        [InlineKeyboardButton("❌  Rejected History",   callback_data="pay:history:rejected")],
        [InlineKeyboardButton("◀️  Back to Main Panel", callback_data="menu:main")],
    ])
    await _send(update,
        "💰 *PAYMENT MANAGEMENT*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Review and action incoming UPI payment requests. "
        "Always verify the screenshot amount matches before approving.", kb)


async def show_pending_payments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)
    pending = await get_pending_payments()

    if not pending:
        await _send(update, "✅ *No pending payments.*", _kb_back("payments"))
        return

    text = (
        f"⏳ *PENDING PAYMENTS*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_{len(pending)} payment{'s' if len(pending)!=1 else ''} awaiting review_\n\n"
    )
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

    # Guard: already actioned
    if payment.get("status") != "pending":
        await _answer(update,
            f"⚠️ Payment already {payment.get('status', 'actioned')}.", alert=True)
        await show_pending_payments(update, context)
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
        f"Amount: {format_inr(payment['amount_inr'])} (${amount_usd:.2f})\n"
        f"User: {name} (@{escape_md(uname)})\n"
        f"ID: `{payment['user_id']}`\n"
        f"Submitted: {escape_md(ago)}\n"
        f"Screenshot: {'✅' if payment.get('screenshot_file_id') else '❌ None'}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ APPROVE", callback_data=f"pay:approve:{payment_id}"),
         InlineKeyboardButton("❌ REJECT",  callback_data=f"pay:reject:{payment_id}")],
        [InlineKeyboardButton("◀️ Back",    callback_data="pay:pending")],
    ])

    if payment.get("screenshot_file_id"):
        # Send screenshot as a new photo message with approve/reject buttons.
        # Then silently remove the old menu message so stale buttons can't be
        # clicked.  The NEW photo message owns the callback — so when admin
        # taps Approve/Reject, query.message is the photo and _safe_edit with
        # is_photo=True works correctly.
        try:
            await update.effective_chat.send_photo(
                photo        = payment["screenshot_file_id"],
                caption      = text,
                parse_mode   = ParseMode.MARKDOWN,
                reply_markup = kb,
            )
            # Silently delete the old text message (pending-list or menu)
            try:
                await update.callback_query.message.delete()
            except Exception:
                pass
            return
        except Exception as exc:
            logger.warning("Could not send screenshot photo: %s", exc)

    # No screenshot — just edit the current message as text
    await _send(update, text + "\n\n⚠️ No screenshot submitted yet.", kb)


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
        f"✅ *Payment {escape_md(ref)} APPROVED!*\n\nBalance credited to user."
        if ok else
        f"❌ *Approval failed:* {escape_md(err or 'Unknown error')}"
    )
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️  Back to Pending Payments", callback_data="pay:pending")]])

    # Edit the message (photo caption or text — try both)
    query = update.callback_query
    try:
        await query.edit_message_caption(
            caption=result_text, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb
        )
    except BadRequest:
        try:
            await query.edit_message_text(
                text=result_text, parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb
            )
        except BadRequest:
            pass

    # ── Notify the user via SHOP BOT token ──────────────────────────────
    # The user's relationship is with the shop bot, not admin bot.
    # We create a temporary Bot object using the shop bot token so the
    # notification arrives in the same chat the user knows.
    if ok and payment:
        try:
            from telegram import Bot as TGBot
            shop_bot = TGBot(token=Config.SHOP_BOT_TOKEN)
            amount_usd = Config.inr_to_usd(payment["amount_inr"])
            await shop_bot.send_message(
                chat_id    = payment["user_id"],
                text       = (
                    f"✅ *PAYMENT APPROVED!*\n\n"
                    f"{format_inr(payment['amount_inr'])} (${amount_usd:.2f}) "
                    f"has been added to your balance.\n\n"
                    f"You can now use your balance to make purchases!\n"
                    f"🛒 Tap *Browse Store* to start shopping."
                ),
                parse_mode = ParseMode.MARKDOWN,
            )
            logger.info("User %d notified of payment approval %s via shop bot", payment["user_id"], ref)
        except Exception as exc:
            logger.warning("Could not notify user %d of approval via shop bot: %s", payment["user_id"], exc)


async def start_reject_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    payment_id = int(update.callback_query.data.split(":")[2])
    context.user_data["reject_payment_id"] = payment_id

    prompt = "❌ *REJECT PAYMENT*\n\nSend rejection reason, or /skip for no reason:"
    query  = update.callback_query
    # Try caption edit (photo message) first, fall back to text
    try:
        await query.edit_message_caption(
            caption=prompt, parse_mode=ParseMode.MARKDOWN, reply_markup=None
        )
    except BadRequest:
        try:
            await query.edit_message_text(
                text=prompt, parse_mode=ParseMode.MARKDOWN, reply_markup=None
            )
        except BadRequest:
            pass

    return PAY_REJECT_REASON


async def do_reject_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text       = update.message.text.strip()
    reason     = None if text.lower() == "/skip" else text
    payment_id = context.user_data.pop("reject_payment_id", None)
    admin_id   = update.effective_user.id

    if not payment_id:
        return ConversationHandler.END

    ok, err  = await reject_payment(payment_id, admin_id, reason)
    payment  = await get_payment(payment_id)

    if ok:
        ref = payment["payment_ref"] if payment else f"#{payment_id}"
        status_text = f"✅ *Payment {escape_md(ref)} rejected.*"
    else:
        status_text = f"❌ *Rejection failed:* {escape_md(err or 'Unknown error')}"

    # Show result then immediately show updated pending list
    await update.message.reply_text(
        status_text,
        parse_mode=ParseMode.MARKDOWN,
    )

    # ── Notify user of rejection via SHOP BOT token ─────────────────────
    if ok and payment:
        try:
            from telegram import Bot as TGBot
            shop_bot    = TGBot(token=Config.SHOP_BOT_TOKEN)
            reason_line = f"\n*Reason:* {escape_md(reason)}" if reason else ""
            await shop_bot.send_message(
                chat_id    = payment["user_id"],
                text       = (
                    f"❌ *Payment Rejected*\n\n"
                    f"Your payment of {format_inr(payment['amount_inr'])} was not approved."
                    f"{reason_line}\n\n"
                    f"Contact {md_username(Config.SUPPORT_USERNAME)} for help."
                ),
                parse_mode = ParseMode.MARKDOWN,
            )
            logger.info("User %d notified of payment rejection %s via shop bot", payment["user_id"], payment.get("payment_ref"))
        except Exception as exc:
            logger.warning("Could not notify user %d of rejection via shop bot: %s", payment.get("user_id"), exc)

    # Refresh the pending list so the rejected payment disappears
    pending = await get_pending_payments()
    if not pending:
        await update.message.reply_text(
            "✅ *No more pending payments.*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("payments"),
        )
    else:
        txt  = f"⏳ *PENDING PAYMENTS* ({len(pending)})\n\n"
        rows = []
        for p in pending:
            u    = p.get("users") or {}
            nm   = escape_md(str(u.get("name","?")))
            ago  = time_ago(parse_utc(str(p.get("created_at",""))))
            txt += f"• `{p['payment_ref']}` — {format_inr(p['amount_inr'])} — {nm} — {escape_md(ago)}\n"
            rows.append([InlineKeyboardButton(
                f"💰 {p['payment_ref']}", callback_data=f"pay:review:{p['id']}"
            )])
        rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="pay:pending"),
                     InlineKeyboardButton("◀️ Back",    callback_data="menu:payments")])
        await update.message.reply_text(
            txt.strip(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(rows),
        )

    return ConversationHandler.END


async def view_payment_detail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    View a single approved/rejected payment with full details.
    If the payment has a screenshot, sends it as a photo.
    Reached from pay:detail:{id} — used in approved/rejected history.
    """
    if not await require_admin(update):
        return
    await _answer(update)

    payment_id = int(update.callback_query.data.split(":")[2])
    payment    = await get_payment(payment_id)
    if not payment:
        await _answer(update, "❌ Payment not found.", alert=True)
        return

    user       = payment.get("users") or {}
    name       = escape_md(str(user.get("name", "?")))
    uname      = user.get("username") or "none"
    status     = payment.get("status", "unknown")
    created    = parse_utc(str(payment.get("created_at", "")))
    actioned   = parse_utc(str(payment.get("actioned_at", "")))
    amount_usd = Config.inr_to_usd(payment["amount_inr"])

    status_emoji = {"approved": "✅", "rejected": "❌", "pending": "⏳"}.get(status, "❓")
    reason_line  = ""
    if status == "rejected" and payment.get("admin_rejection_reason"):
        reason_line = f"\n*Reason:* {escape_md(str(payment['admin_rejection_reason']))}"

    text = (
        f"💰 *PAYMENT DETAIL*\n\n"
        f"*Ref:* `{payment['payment_ref']}`\n"
        f"*Amount:* {format_inr(payment['amount_inr'])} (${amount_usd:.2f})\n"
        f"*Status:* {status_emoji} {escape_md(status.capitalize())}{reason_line}\n\n"
        f"*User:* {name} (@{escape_md(uname)})\n"
        f"*User ID:* `{payment['user_id']}`\n\n"
        f"*Submitted:* {escape_md(time_ago(created))}\n"
        f"*Actioned:* {escape_md(actioned.strftime('%Y-%m-%d %H:%M UTC') if actioned else 'N/A')}\n"
        f"*Screenshot:* {'✅ Attached' if payment.get('screenshot_file_id') else '❌ None'}"
    )
    back_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("◀️ Back", callback_data=f"pay:history:{status}")
    ]])

    if payment.get("screenshot_file_id"):
        try:
            await update.effective_chat.send_photo(
                photo        = payment["screenshot_file_id"],
                caption      = text,
                parse_mode   = ParseMode.MARKDOWN,
                reply_markup = back_kb,
            )
            try:
                await update.callback_query.message.delete()
            except Exception:
                pass
            return
        except Exception as exc:
            logger.warning("Could not send screenshot for detail view: %s", exc)

    await _send(update, text + "\n\n⚠️ _No screenshot on file._", back_kb)


async def show_payment_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show approved or rejected payment history — was missing its async def header."""
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
        await _send(update, f"No {status} payments found.", _kb_back("payments"))
        return

    emoji = "✅" if status == "approved" else "❌"
    text  = f"{emoji} *{status.upper()} PAYMENTS* (last 20)\n\n"
    rows  = []
    for p in payments:
        user  = p.get("users") or {}
        name  = escape_md(str(user.get("name","?")))
        dt    = parse_utc(str(p.get("actioned_at","")))
        ds    = dt.strftime("%m/%d") if dt else "?"
        has_ss = "📸" if p.get("screenshot_file_id") else "  "
        text += f"• `{p['payment_ref']}` {format_inr(p['amount_inr'])} — {name} — {escape_md(ds)} {has_ss}\n"
        rows.append([InlineKeyboardButton(
            f"{emoji} {p['payment_ref']} — {format_inr(p['amount_inr'])}",
            callback_data=f"pay:detail:{p['id']}"
        )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:payments")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


# ═══════════════════════════════════════════════════════
#  STATISTICS
# ═══════════════════════════════════════════════════════

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update, "Loading…")

    stats = await get_stats()
    if not stats:
        await _send(update, "❌ Could not load statistics.", _kb_back())
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
        "Send your message. It will be forwarded to all users.\n"
        "Supports Markdown.\n\n"
        "/cancel to abort.")
    return BCAST_MSG


async def do_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    msg_text = update.message.text.strip()
    admin_id = update.effective_user.id

    users = await get_all_users(limit=5000)
    if not users:
        await update.message.reply_text("No users found.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    sent = failed = 0
    status_msg = await update.message.reply_text(f"📢 Sending to {len(users)} users…")

    for user in users:
        try:
            try:
                await context.bot.send_message(
                    chat_id    = user["user_id"],
                    text       = msg_text,
                    parse_mode = ParseMode.MARKDOWN,
                )
            except BadRequest:
                # Admin's message may contain unmatched markdown — retry as plain text
                await context.bot.send_message(
                    chat_id = user["user_id"],
                    text    = msg_text,
                )
            sent += 1
        except Forbidden:
            failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)   # stay within Telegram rate limits

    await status_msg.edit_text(
        f"📢 *Broadcast complete!*\n\n✅ Sent: {sent}\n❌ Failed: {failed}",
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
        await _send(update, "No promo codes found.", _kb_back("promos"))
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
    await _send(update, "🎟️ *CREATE PROMO CODE*\n\nSend the promo code (e.g. SAVE10):\n\n/cancel to abort")
    return PROMO_CODE


async def promo_code_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    code = update.message.text.strip().upper()
    if not code.isalnum():
        await update.message.reply_text("❌ Code must be alphanumeric only.", parse_mode=ParseMode.MARKDOWN)
        return PROMO_CODE
    context.user_data["new_promo"]["code"] = code
    await update.message.reply_text(
        "Send discount. Examples:\n`10%` for 10% off\n`2.50` for $2.50 off",
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
            await update.message.reply_text("❌ Invalid percentage.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_DISCOUNT
    else:
        try:
            promo["discount_fixed"]   = float(text)
            promo["discount_percent"] = None
        except ValueError:
            await update.message.reply_text("❌ Invalid amount.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_DISCOUNT

    await update.message.reply_text("Min purchase in USD? (or /skip for none):", parse_mode=ParseMode.MARKDOWN)
    return PROMO_MIN


async def promo_min_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    if text == "/skip":
        context.user_data["new_promo"]["min_purchase"] = 0.0
    else:
        try:
            context.user_data["new_promo"]["min_purchase"] = float(text)
        except ValueError:
            await update.message.reply_text("❌ Invalid amount.", parse_mode=ParseMode.MARKDOWN)
            return PROMO_MIN

    await update.message.reply_text("Max uses? (or /skip for unlimited):", parse_mode=ParseMode.MARKDOWN)
    return PROMO_USES


async def promo_uses_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    if text == "/skip":
        context.user_data["new_promo"]["max_uses"] = None
    else:
        try:
            context.user_data["new_promo"]["max_uses"] = int(text)
        except ValueError:
            await update.message.reply_text("❌ Must be a whole number.", parse_mode=ParseMode.MARKDOWN)
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
            await update.message.reply_text("❌ Format must be YYYY-MM-DD.", parse_mode=ParseMode.MARKDOWN)
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
            f"✅ *Promo `{escape_md(promo['code'])}` created!*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("promos"))
    else:
        await update.message.reply_text(
            "❌ Failed (code may already exist).",
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
        f"{'✅ Promo deactivated.' if ok else '❌ Failed.'}",
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
        await _send(update, "✅ *No pending refunds.*", _kb_back())
        return

    text = f"🔄 *PENDING REFUNDS* ({len(refunds)})\n\n"
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
        f"{'✅ Refund approved — balance credited.' if ok else f'❌ Failed: {escape_md(err or chr(34))}'}",
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
        f"{'✅ Refund rejected.' if ok else '❌ Failed.'}",
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
        await _send(update, "📋 *No audit log entries.*", _kb_back())
        return

    text = "📋 *AUDIT LOG* (last 20)\n\n"
    for a in actions:
        dt   = parse_utc(str(a.get("created_at","")))
        ds   = dt.strftime("%m/%d %H:%M") if dt else "?"
        ref  = f" → `{escape_md(str(a['target_ref']))}`" if a.get("target_ref") else ""
        text += f"• `{a['admin_id']}` {escape_md(a['action'])}{ref} ({escape_md(ds)})\n"

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
    elif data == "menu:tgaccounts": await show_tg_accounts_menu(update, context)

    # Products
    elif data == "product:add":    await start_add_product(update, context)
    elif data == "product:list":   await list_products(update, context)
    elif data == "product:confirm_create": await confirm_create_product(update, context)
    elif data == "product:cancel_create":  await cancel_create_product(update, context)
    elif data.startswith("product:edit:"): await show_product_edit(update, context)
    elif data.startswith("product:delete:"): await delete_product_confirm(update, context)
    elif data.startswith("ap_confirm_margin:"): await ap_confirm_margin(update, context)

    # New product category & sub-type pickers (inline buttons, not conversation text)
    # ap_cat / ap_sub / ap_tg_country are ConversationHandler entry_points — NOT handled here
    # Routing them through route_callback would discard the returned conversation state integer
    elif data.startswith("ap_cat:"): await ap_cat_chosen(update, context)
    elif data.startswith("ap_sub:"): await ap_sub_chosen(update, context)

    # Stock
    elif data == "stock:select":   await show_stock_select(update, context)
    elif data == "stock:low":      await show_low_stock(update, context)

    # Users
    elif data == "user:search":    await _answer(update)   # handled by conversation
    elif data == "user:banned":    await show_banned_users(update, context)
    elif data == "user:stats":     await show_user_stats(update, context)
    elif data.startswith("user:view:"): await view_user(update, context)
    elif data.startswith("user:unban:"): await do_unban_user(update, context)

    # Payments
    elif data == "pay:pending":    await show_pending_payments(update, context)
    elif data.startswith("pay:review:"): await review_payment(update, context)
    elif data.startswith("pay:approve:"): await approve_payment_action(update, context)
    elif data.startswith("pay:history:"): await show_payment_history(update, context)
    elif data.startswith("pay:detail:"): await view_payment_detail(update, context)
    # pay:reject is handled by its ConversationHandler — do NOT handle here
    # (the conversation handler is registered before the catch-all router)

    # Promos
    elif data == "promo:list":     await show_promo_list(update, context)
    elif data.startswith("promo:deactivate:"): await deactivate_promo(update, context)

    # Refunds
    elif data.startswith("refund:review:"): await review_refund(update, context)
    elif data.startswith("refund:approve:"): await approve_refund_action(update, context)
    # refund:reject is handled by its ConversationHandler

    # Stats
    elif data == "stats:export":   await export_csv(update, context)

    # TG Accounts
    elif data == "tg:list":        await list_tg_accounts(update, context)
    elif data == "tg:health":      await run_health_check(update, context)
    elif data.startswith("tg:detail:"): await view_tg_account_detail(update, context)
    elif data.startswith("tg:delete:"): await delete_tg_account(update, context)
    elif data.startswith("tg_pool:"):   await view_tg_product_pool(update, context)
    elif data.startswith("tg_link_prod:") or data == "tg_link_skip":
        await tg_acct_link_product(update, context)
    elif data.startswith("tg_link_start:"):
        account_id = int(data.split(":")[1])
        await _answer(update)
        await _ask_link_product(update, context, account_id)

    else:
        await update.callback_query.answer("Unknown action.")


# ═══════════════════════════════════════════════════════
#  TG ACCOUNTS  (admin adds userbot accounts via OTP)
# ═══════════════════════════════════════════════════════

async def show_tg_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    from db import _run
    result = await _run(
        lambda: supabase.table("tg_accounts")
            .select("id", count="exact")
            .eq("is_sold", False)
            .execute()
    )
    healthy_result = await _run(
        lambda: supabase.table("tg_accounts")
            .select("id", count="exact")
            .eq("is_sold", False)
            .eq("is_healthy", True)
            .execute()
    )
    total   = result.count or 0
    healthy = healthy_result.count or 0

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Account (OTP)",   callback_data="tg:add")],
        [InlineKeyboardButton("📋 List All Accounts",   callback_data="tg:list")],
        [InlineKeyboardButton("🏥 Health Check Now",    callback_data="tg:health")],
        [InlineKeyboardButton("◀️ Back to Main Panel",  callback_data="menu:main")],
    ])
    await _send(update,
        f"📱 *TG ACCOUNTS*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 *Pool Status:*\n"
        f"  Total (unsold): {total}\n"
        f"  ✅ Healthy: {healthy}\n"
        f"  ❌ Unhealthy: {total - healthy}\n\n"
        f"_These are Pyrogram session accounts used to deliver OTPs._", kb)


async def list_tg_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    from db import _run
    result = await _run(
        lambda: supabase.table("tg_accounts")
            .select(
                "id, phone, country, country_flag, is_sold, is_healthy, "
                "health_error, first_name, last_name, tg_username, "
                "is_premium, dc_id, added_at"
            )
            .order("added_at", desc=True)
            .limit(30)
            .execute()
    )
    accounts = result.data or []

    if not accounts:
        await _send(update, "📱 *No TG accounts on file.*", _kb_back("tgaccounts"))
        return

    text = f"📱 *TG ACCOUNTS* ({len(accounts)} shown)\n\n"
    rows = []
    for acc in accounts:
        flag   = acc.get("country_flag") or "🌍"
        name   = acc.get("first_name") or ""
        if acc.get("last_name"):
            name += f" {acc['last_name']}"
        uname  = md_username(acc.get("tg_username")) if acc.get("tg_username") else "no username"
        health = "✅" if acc.get("is_healthy") else "❌"
        sold   = " 🛒sold" if acc.get("is_sold") else ""
        prem   = " 💎" if acc.get("is_premium") else ""
        dc     = f" DC{acc['dc_id']}" if acc.get("dc_id") else ""
        text  += (
            f"{health}{sold} {flag} `{acc['phone']}`\n"
            f"  {escape_md(name or '?')} ({escape_md(uname)}){prem}{escape_md(dc)}\n"
        )
        rows.append([InlineKeyboardButton(
            f"{health} {acc['phone']} {flag}", callback_data=f"tg:detail:{acc['id']}"
        )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="menu:tgaccounts")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


async def view_tg_account_detail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    account_id = int(update.callback_query.data.split(":")[2])
    from db import _run
    result = await _run(
        lambda: supabase.table("tg_accounts")
            .select("*")
            .eq("id", account_id)
            .single()
            .execute()
    )
    if not result.data:
        await _answer(update, "❌ Account not found.", alert=True)
        return

    acc = result.data
    flag     = acc.get("country_flag") or "🌍"
    country  = acc.get("country")     or "Unknown"
    fname    = acc.get("first_name")  or ""
    lname    = acc.get("last_name")   or ""
    full_name = f"{fname} {lname}".strip() or "—"
    uname    = acc.get("tg_username") or "—"
    bio      = acc.get("bio")         or "—"
    prem     = "💎 Yes" if acc.get("is_premium") else "No"
    dc       = acc.get("dc_id")       or "—"
    tg_id    = acc.get("tg_user_id")  or "—"
    health   = "✅ Healthy" if acc.get("is_healthy") else f"❌ {acc.get('health_error','?')[:60]}"
    sold_s   = "🛒 Yes" if acc.get("is_sold") else "No"
    added    = parse_utc(str(acc.get("added_at", "")))
    added_s  = added.strftime("%Y-%m-%d") if added else "—"

    text = (
        f"📱 *TG ACCOUNT DETAIL*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📞 *Phone:* `{acc['phone']}`\n"
        f"🌍 *Country:* {flag} {escape_md(country)}\n"
        f"🆔 *TG User ID:* `{tg_id}`\n\n"
        f"👤 *Name:* {escape_md(full_name)}\n"
        f"🔗 *Username:* {md_username(uname) if uname != '—' else '—'}\n"
        f"📝 *Bio:* {escape_md(bio[:100])}\n"
        f"💎 *Premium:* {prem}\n"
        f"🏢 *DC:* {dc}\n\n"
        f"🏥 *Health:* {escape_md(health)}\n"
        f"🛒 *Sold:* {sold_s}\n"
        f"📅 *Added:* {escape_md(added_s)}\n"
        f"🆔 *API ID:* `{acc.get('api_id','—')}`"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Link to Product", callback_data=f"tg_link_start:{account_id}"),
         InlineKeyboardButton("🗑️ Delete Account", callback_data=f"tg:delete:{account_id}")],
        [InlineKeyboardButton("◀️ Back to List",   callback_data="tg:list")],
    ])
    await _send(update, text, kb)


async def delete_tg_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update)

    account_id = int(update.callback_query.data.split(":")[2])
    admin_id   = update.effective_user.id
    from db import _run
    result = await _run(
        lambda: supabase.table("tg_accounts")
            .delete()
            .eq("id", account_id)
            .execute()
    )
    if result.data:
        await log_admin_action(admin_id, "delete_tg_account", "tg_account", str(account_id), {})
        await _send(update, f"✅ Account #{account_id} deleted.", _kb_back("tgaccounts"))
    else:
        await _send(update, "❌ Failed to delete account.", _kb_back("tgaccounts"))


async def run_health_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin(update):
        return
    await _answer(update, "Running health check…")
    msg = await update.effective_chat.send_message(
        "🏥 *Health check running…* This may take a minute.",
        parse_mode=ParseMode.MARKDOWN
    )
    try:
        from userbot import HealthMonitor
        results = await HealthMonitor.check_all_accounts()
        await msg.edit_text(
            f"🏥 *Health Check Complete*\n\n"
            f"✅ Checked: {results['checked']}\n"
            f"✅ Healthy: {results['healthy']}\n"
            f"❌ Unhealthy: {results['unhealthy']}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("tgaccounts")
        )
    except Exception as exc:
        await msg.edit_text(
            f"❌ Health check failed: {escape_md(str(exc)[:200])}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("tgaccounts")
        )


# ── Add TG Account via phone OTP ─────────────────────────────────────────

async def start_add_tg_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)
    context.user_data.pop("tg_acct", None)
    context.user_data["tg_acct"] = {}
    await _send(update,
        "📱 *ADD TG ACCOUNT*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "*Step 1:* Send the phone number in international format:\n"
        "_e.g._ `+917012345678` or `+14155552671`\n\n"
        "/cancel to abort")
    return TG_ACCT_PHONE


async def tg_acct_phone_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = update.message.text.strip()
    if not phone.startswith("+"):
        phone = f"+{phone}"

    await update.message.reply_text(
        f"📤 Sending OTP to `{escape_md(phone)}`…",
        parse_mode=ParseMode.MARKDOWN)

    try:
        from userbot import add_account_via_phone
        ok, msg, phone_code_hash = await add_account_via_phone(phone)
    except Exception as exc:
        await update.message.reply_text(
            f"❌ Error: {escape_md(str(exc)[:200])}\n\nTry again or /cancel",
            parse_mode=ParseMode.MARKDOWN)
        return TG_ACCT_PHONE

    if not ok:
        await update.message.reply_text(
            f"❌ {escape_md(msg)}\n\nSend a valid phone number or /cancel",
            parse_mode=ParseMode.MARKDOWN)
        return TG_ACCT_PHONE

    context.user_data["tg_acct"]["phone"] = phone
    await update.message.reply_text(
        f"✅ OTP sent to `{escape_md(phone)}`!\n\n"
        f"*Step 2:* Enter the OTP code you received in Telegram:\n"
        f"_Format: 5-digit code from Telegram service message_\n\n"
        f"/cancel to abort",
        parse_mode=ParseMode.MARKDOWN)
    return TG_ACCT_CODE


async def tg_acct_code_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    code  = update.message.text.strip().replace(" ", "").replace("-", "")
    phone = context.user_data.get("tg_acct", {}).get("phone", "")
    admin_id = update.effective_user.id

    if not phone:
        await update.message.reply_text("❌ Session expired. Start over.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    await update.message.reply_text("🔄 Verifying…", parse_mode=ParseMode.MARKDOWN)

    try:
        from userbot import verify_otp_and_save
        ok, result_msg = await verify_otp_and_save(phone, code, added_by=admin_id)
    except Exception as exc:
        err = str(exc)
        # 2FA needed — ask for password
        if "2FA" in err or "password" in err.lower() or "SessionPasswordNeeded" in err:
            context.user_data["tg_acct"]["pending_code"] = code
            await update.message.reply_text(
                "🔐 *2FA Required*\n\n"
                "This account has Two-Step Verification enabled.\n"
                "Send your 2FA password:\n\n"
                "/cancel to abort",
                parse_mode=ParseMode.MARKDOWN)
            return TG_ACCT_2FA
        await update.message.reply_text(
            f"❌ Error: {escape_md(err[:200])}\n\nTry again or /cancel",
            parse_mode=ParseMode.MARKDOWN)
        return TG_ACCT_CODE

    if not ok and "2FA" in result_msg:
        context.user_data["tg_acct"]["pending_code"] = code
        await update.message.reply_text(
            "🔐 *2FA Required*\n\n"
            "Send your 2FA password:\n\n/cancel to abort",
            parse_mode=ParseMode.MARKDOWN)
        return TG_ACCT_2FA

    context.user_data.pop("tg_acct", None)

    if ok:
        await update.message.reply_text(
            f"✅ *Account Added Successfully!*\n\n"
            f"{escape_md(result_msg)}\n\n"
            f"_All account details have been fetched and stored._",
            parse_mode=ParseMode.MARKDOWN)
        await log_admin_action(admin_id, "add_tg_account", "tg_account", phone, {})
        # Offer to link this account to a product
        from db import _run
        acc = await _run(
            lambda: supabase.table("tg_accounts").select("id").eq("phone", phone).single().execute()
        )
        if acc.data:
            return await _ask_link_product(update, context, acc.data["id"])
        await update.message.reply_text("", reply_markup=_kb_back("tgaccounts"))
    else:
        await update.message.reply_text(
            f"❌ {escape_md(result_msg)}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("tgaccounts"))
    return ConversationHandler.END


async def tg_acct_2fa_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    password = update.message.text.strip()
    phone    = context.user_data.get("tg_acct", {}).get("phone", "")
    code     = context.user_data.get("tg_acct", {}).get("pending_code", "")
    admin_id = update.effective_user.id

    if not phone:
        await update.message.reply_text("❌ Session expired. Start over.", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    if not code:
        await update.message.reply_text(
            "❌ OTP code was lost (bot may have restarted).\n\n"
            "Please start the Add TG Account flow again from the menu.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("tgaccounts"))
        return ConversationHandler.END

    await update.message.reply_text("🔄 Verifying 2FA…", parse_mode=ParseMode.MARKDOWN)

    try:
        from userbot import verify_otp_and_save
        ok, result_msg = await verify_otp_and_save(phone, code, twofa_password=password, added_by=admin_id)
    except Exception as exc:
        await update.message.reply_text(
            f"❌ 2FA failed: {escape_md(str(exc)[:200])}\n\nSend correct password or /cancel",
            parse_mode=ParseMode.MARKDOWN)
        return TG_ACCT_2FA

    context.user_data.pop("tg_acct", None)

    if ok:
        await update.message.reply_text(
            f"✅ *Account Added Successfully!*\n\n"
            f"{escape_md(result_msg)}\n\n"
            f"_All account details have been fetched and stored._",
            parse_mode=ParseMode.MARKDOWN)
        await log_admin_action(admin_id, "add_tg_account", "tg_account", phone, {"2fa": True})
        from db import _run
        acc = await _run(
            lambda: supabase.table("tg_accounts").select("id").eq("phone", phone).single().execute()
        )
        if acc.data:
            return await _ask_link_product(update, context, acc.data["id"])
        await update.message.reply_text("", reply_markup=_kb_back("tgaccounts"))
    else:
        await update.message.reply_text(
            f"❌ {escape_md(result_msg)}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("tgaccounts"))
    return ConversationHandler.END

async def _ask_link_product(update: Update, context: ContextTypes.DEFAULT_TYPE, account_id: int) -> int:
    """
    After a TG account is added, offer to link it to a specific OTP product.
    Shows all active products as inline buttons.
    """
    context.user_data["link_account_id"] = account_id
    categories = await get_categories()
    if not categories:
        # No products yet — skip linking
        await update.effective_message.reply_text(
            "ℹ️ No products found to link this account to.\n"
            "You can link it later from the account detail view.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_kb_back("tgaccounts"))
        return ConversationHandler.END

    rows = []
    all_products = await asyncio.gather(*[get_products_by_category(cat) for cat in categories])
    for products in all_products:
        for p in products:
            rows.append([InlineKeyboardButton(
                f"📦 {p['name']}", callback_data=f"tg_link_prod:{account_id}:{p['id']}"
            )])
    rows.append([InlineKeyboardButton("⏭️ Skip (link later)", callback_data="tg_link_skip")])

    await update.effective_message.reply_text(
        "🔗 *LINK ACCOUNT TO PRODUCT*\n\n"
        "Which OTP product should this account serve?\n"
        "_Select a product or skip to link later from the account detail._",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(rows))
    return TG_ACCT_LINK


async def tg_acct_link_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Callback: admin tapped a product button to link the new account."""
    if not await require_admin(update):
        return ConversationHandler.END
    await _answer(update)

    data = update.callback_query.data
    if data == "tg_link_skip":
        context.user_data.pop("link_account_id", None)
        await _send(update,
            "⏭️ *Link skipped.*\n\nYou can link this account to a product anytime "
            "from *TG Accounts → Account Detail → Link to Product*.",
            _kb_back("tgaccounts"))
        return ConversationHandler.END

    parts      = data.split(":")   # tg_link_prod:{account_id}:{product_id}
    account_id = int(parts[1])
    product_id = int(parts[2])
    admin_id   = update.effective_user.id

    ok = await link_tg_account_to_product(account_id, product_id, admin_id)
    product = await get_product(product_id)
    pname   = escape_md(product["name"]) if product else f"#{product_id}"

    if ok:
        # Show how many accounts are now available for this product
        count = await get_tg_account_stock_count(product_id)
        await _send(update,
            f"✅ *Account linked to* {pname}\n\n"
            f"📊 Pool now has *{count}* available account(s) for this product.",
            _kb_back("tgaccounts"))
    else:
        await _send(update, "❌ Failed to link account.", _kb_back("tgaccounts"))

    context.user_data.pop("link_account_id", None)
    return ConversationHandler.END


async def view_tg_product_pool(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the TG account pool for a specific OTP product. callback: tg_pool:{product_id}"""
    if not await require_admin(update):
        return
    await _answer(update)

    product_id = int(update.callback_query.data.split(":")[1])
    product    = await get_product(product_id)
    if not product:
        await _answer(update, "❌ Product not found.", alert=True)
        return

    accounts   = await get_tg_accounts_for_product(product_id)
    available  = sum(1 for a in accounts if not a.get("is_sold") and a.get("is_healthy"))
    sold       = sum(1 for a in accounts if a.get("is_sold"))

    text = (
        f"📱 *OTP POOL — {escape_md(product['name'])}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Available: {available}\n"
        f"🛒 Sold: {sold}\n"
        f"📊 Total: {len(accounts)}\n\n"
    )
    rows = []
    for acc in accounts[:20]:
        flag   = acc.get("country_flag") or "🌍"
        health = "✅" if acc.get("is_healthy") else "❌"
        sold_s = " 🛒" if acc.get("is_sold") else ""
        text  += f"{health}{sold_s} {flag} `{acc['phone']}`\n"
        rows.append([InlineKeyboardButton(
            f"{health}{sold_s} {acc['phone']}", callback_data=f"tg:detail:{acc['id']}"
        )])

    rows.append([InlineKeyboardButton("◀️ Back", callback_data="tg:list")])
    await _send(update, text.strip(), InlineKeyboardMarkup(rows))


# ═══════════════════════════════════════════════════════
#  Error handler
# ═══════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", context.error, exc_info=context.error)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    for key in ("np","stock_product_id","stock_items","ban_user_id","bal_user_id",
                "reject_payment_id","reject_refund_id","new_promo"):
        context.user_data.pop(key, None)
    await update.effective_message.reply_text("❌ Cancelled.", parse_mode=ParseMode.MARKDOWN,
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
    from telegram.request import HTTPXRequest
    app = (
        Application.builder()
        .token(Config.ADMIN_BOT_TOKEN)
        # Larger connection pool: default is 1 connection — on a VPS with concurrent
        # users, requests queue behind each other. 8 keeps throughput high.
        .request(HTTPXRequest(
            connection_pool_size = 8,
            connect_timeout      = 10.0,
            read_timeout         = 10.0,
            write_timeout        = 10.0,
            pool_timeout         = 10.0,   # default is 1s — causes queuing!
        ))
        # get_updates uses a separate connection; tune independently
        .get_updates_request(HTTPXRequest(
            connection_pool_size = 2,
            connect_timeout      = 20.0,
            read_timeout         = 20.0,
        ))
        # Process updates from different users concurrently (same user still serialized)
        .concurrent_updates(True)
        .build()
    )

    # ── Add Product conversation ─────────────────────────────────────────
    # Entry: category inline button → ap_cat_chosen (callback)
    #        ap_cat_chosen may return ADD_PROD_CAT_CUSTOM (admin types custom cat)
    #                      or ADD_PROD_NAME (preset cat, no sub-types)
    #        ap_cat_chosen may also trigger ap_sub flow (returns END — sub picked via callback)
    # Sub-product entry: ap_sub_chosen (callback) → ADD_PROD_NAME or ADD_PROD_SUBTYPE_CUSTOM
    # All text steps after that are linear: NAME → DESC → BUY → SELL → DEMO → FILE → confirm
    app.add_handler(ConversationHandler(
        entry_points=[
            # Initial "Add Product" button in products menu
            CallbackQueryHandler(start_add_product,    pattern="^product:add$"),
            # Category preset picked via inline button
            CallbackQueryHandler(ap_cat_chosen,        pattern=r"^ap_cat:.+$"),
            # Sub-type picked via inline button
            CallbackQueryHandler(ap_sub_chosen,        pattern=r"^ap_sub:.+$"),
            # Country chosen from OTP→Telegram country picker
            # MUST be an entry_point — returning ADD_PROD_NAME from route_callback has zero effect
            CallbackQueryHandler(ap_tg_country_chosen, pattern=r"^ap_tg_country:.+$"),
        ],
        states={
            # Admin typed a custom category name
            ADD_PROD_CAT_CUSTOM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ap_cat_custom_text),
            ],
            # Admin typed a custom sub-type name
            ADD_PROD_SUBTYPE_CUSTOM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ap_subtype_custom),
            ],
            # Product name — CommandHandler("skip") so /skip works for the country-hint pre-fill
            ADD_PROD_NAME: [
                CommandHandler("skip", ap_name),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ap_name),
            ],
            # Description
            ADD_PROD_DESC: [
                CommandHandler("skip", ap_desc),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ap_desc),
            ],
            # Purchase price
            ADD_PROD_BUY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ap_buy_price),
            ],
            # Selling price (may show inline margin confirmation)
            ADD_PROD_SELL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ap_sell_price),
                CallbackQueryHandler(ap_confirm_margin, pattern=r"^ap_confirm_margin:.+$"),
            ],
            # Demo file (optional)
            ADD_PROD_DEMO: [
                MessageHandler(filters.Document.ALL | filters.PHOTO, ap_demo),
                CommandHandler("skip", ap_demo),
            ],
            # Main delivery file (Papers category only)
            ADD_PROD_FILE: [
                MessageHandler(filters.Document.ALL, ap_main_file),
                CommandHandler("skip", ap_main_file),
            ],
        },
        fallbacks=_fallback(),
        allow_reentry=True,   # allow re-entry from category/sub-type callbacks
    ))

    # ── Add Stock conversation ───────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_add_stock, pattern=r"^stock:add:\d+$")],
        states={
            STOCK_METHOD: [
                CallbackQueryHandler(stock_choose_manual, pattern="^stock_method:manual$"),
                CallbackQueryHandler(stock_choose_file,   pattern="^stock_method:file$"),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern="^stock_method:cancel$"),
            ],
            STOCK_MANUAL: [
                # /done MUST be a CommandHandler — filters.TEXT & ~filters.COMMAND drops commands
                CommandHandler("done",   _finalize_stock),
                MessageHandler(filters.TEXT & ~filters.COMMAND, stock_add_item),
            ],
            STOCK_FILE:   [MessageHandler(filters.Document.ALL, stock_file_upload)],
        },
        fallbacks=_fallback(),
    ))

    # ── User search conversation ─────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_user_search, pattern="^user:search$")],
        states={USER_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_user_search)]},
        fallbacks=_fallback(),
    ))

    # ── Ban user conversation ────────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_ban_user, pattern=r"^user:ban:\d+$")],
        states={USER_BAN_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_ban_user)]},
        fallbacks=_fallback(),
    ))

    # ── Balance adjust conversation ──────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_balance_adjust, pattern=r"^user:balance:\d+$")],
        states={USER_BAL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_balance_adjust)]},
        fallbacks=_fallback(),
    ))

    # ── Payment reject conversation ──────────────────────────────────────
    # IMPORTANT: registered BEFORE the catch-all CallbackQueryHandler so
    # pay:reject:* callbacks are intercepted here, not in route_callback.
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_reject_payment, pattern=r"^pay:reject:\d+$")],
        states={PAY_REJECT_REASON: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, do_reject_payment),
            CommandHandler("skip", do_reject_payment),
        ]},
        fallbacks=_fallback(),
    ))

    # ── Promo create conversation ────────────────────────────────────────
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

    # ── Broadcast conversation ───────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_broadcast, pattern="^menu:broadcast$")],
        states={BCAST_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_broadcast)]},
        fallbacks=_fallback(),
    ))

    # ── Refund reject conversation ───────────────────────────────────────
    # Registered before catch-all so refund:reject:* is intercepted here.
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_reject_refund, pattern=r"^refund:reject:\d+$")],
        states={REFUND_REJECT_NOTE: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, do_reject_refund),
            CommandHandler("skip", do_reject_refund),
        ]},
        fallbacks=_fallback(),
    ))

    # ── TG Account (add via phone OTP) conversation ──────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(start_add_tg_account, pattern="^tg:add$")],
        states={
            TG_ACCT_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, tg_acct_phone_input)],
            TG_ACCT_CODE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, tg_acct_code_input)],
            TG_ACCT_2FA:   [MessageHandler(filters.TEXT & ~filters.COMMAND, tg_acct_2fa_input)],
            TG_ACCT_LINK:  [
                CallbackQueryHandler(tg_acct_link_product, pattern=r"^tg_link_prod:\d+:\d+$"),
                CallbackQueryHandler(tg_acct_link_product, pattern="^tg_link_skip$"),
            ],
        },
        fallbacks=_fallback(),
    ))

    # ── Commands ─────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    # ── Catch-all callback router ────────────────────────────────────────
    # Must be registered AFTER all ConversationHandlers so their entry_points
    # get priority. route_callback has admin check built in.
    app.add_handler(CallbackQueryHandler(route_callback))

    app.add_error_handler(error_handler)
    app.job_queue.run_repeating(job_cleanup, interval=900, first=60)

    logger.info("✅ Admin Bot running!")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
