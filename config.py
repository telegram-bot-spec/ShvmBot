"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Configuration                                        ║
║  Fixed: safe OWNER_IDS parse, Fernet validation,              ║
║         configurable min/max deposit, all values from env      ║
╚════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import logging
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _parse_int_list(env_key: str) -> List[int]:
    """
    Parse a comma-separated list of integers from an env variable.
    Skips blank entries (handles trailing commas).
    Raises SystemExit with a clear message on invalid values.
    """
    raw = os.getenv(env_key, "")
    result: List[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.append(int(part))
        except ValueError:
            print(f"❌  {env_key} contains a non-integer value: {part!r}")
            sys.exit(1)
    return result


def _get_float(env_key: str, default: float) -> float:
    raw = os.getenv(env_key, str(default))
    try:
        return float(raw)
    except ValueError:
        print(f"❌  {env_key} must be a number, got: {raw!r}")
        sys.exit(1)


def _get_int(env_key: str, default: int) -> int:
    raw = os.getenv(env_key, str(default))
    try:
        return int(raw)
    except ValueError:
        print(f"❌  {env_key} must be an integer, got: {raw!r}")
        sys.exit(1)


class Config:
    """Central, validated configuration. All values read once at import."""

    # ── Telegram bots ──────────────────────────────────────────────
    SHOP_BOT_TOKEN:  str = os.getenv("SHOP_BOT_TOKEN",  "")
    ADMIN_BOT_TOKEN: str = os.getenv("ADMIN_BOT_TOKEN", "")

    # ── Access control ─────────────────────────────────────────────
    OWNER_IDS: List[int] = _parse_int_list("OWNER_IDS")
    ADMIN_IDS: List[int] = _parse_int_list("ADMIN_IDS")

    # ── Supabase ───────────────────────────────────────────────────
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")   # service role key

    # ── Pyrogram (userbot) ─────────────────────────────────────────
    TG_API_ID:   int = _get_int("TG_API_ID", 0)
    TG_API_HASH: str = os.getenv("TG_API_HASH", "")

    # ── Payment ────────────────────────────────────────────────────
    UPI_ID:          str   = os.getenv("UPI_ID", "")
    UPI_QR_NAME:     str   = os.getenv("UPI_QR_NAME", "TGFlow")
    USD_TO_INR:      float = _get_float("USD_TO_INR", 94.0)
    MIN_DEPOSIT_INR: int   = _get_int("MIN_DEPOSIT_INR", 10)    # whole rupees
    MAX_DEPOSIT_INR: int   = _get_int("MAX_DEPOSIT_INR", 100000)

    # ── Referral ───────────────────────────────────────────────────
    REFERRAL_PERCENT: float = _get_float("REFERRAL_PERCENT", 2.0)

    # ── UI / branding ──────────────────────────────────────────────
    START_STICKER_ID:  str = os.getenv("START_STICKER_ID", "")
    SUPPORT_USERNAME:  str = os.getenv("SUPPORT_USERNAME", "support")
    SHOP_NAME:         str = os.getenv("SHOP_NAME", "TGFlow Shop")

    # ── Security ───────────────────────────────────────────────────
    ENCRYPTION_KEY: str = os.getenv("ENCRYPTION_KEY", "")

    # ── Terms of Service ───────────────────────────────────────────
    TOS_TEXT: str = os.getenv("TOS_TEXT", "By using this bot you agree to our Terms of Service.")

    # ── Rank thresholds (USD total spent) ──────────────────────────
    RANK_SILVER: float = _get_float("RANK_SILVER", 50.0)
    RANK_GOLD:   float = _get_float("RANK_GOLD",  200.0)
    RANK_VIP:    float = _get_float("RANK_VIP",   500.0)

    # ── Low-stock alert threshold ──────────────────────────────────
    LOW_STOCK_THRESHOLD: int = _get_int("LOW_STOCK_THRESHOLD", 5)

    # ── Health-check interval (seconds) ───────────────────────────
    HEALTH_CHECK_INTERVAL: int = _get_int("HEALTH_CHECK_INTERVAL", 10800)  # 3 h

    # ──────────────────────────────────────────────────────────────
    #  Validation
    # ──────────────────────────────────────────────────────────────

    @classmethod
    def validate(cls) -> bool:
        """
        Validate all critical settings.
        Prints clear error messages and returns False on failure.
        Does NOT import Fernet here — that is done lazily in utils.py
        to avoid crashing before this method can report the problem.
        """
        ok = True

        def _require(name: str, value: str) -> None:
            nonlocal ok
            if not value:
                print(f"❌  Missing required config: {name}")
                ok = False

        _require("SHOP_BOT_TOKEN",  cls.SHOP_BOT_TOKEN)
        _require("ADMIN_BOT_TOKEN", cls.ADMIN_BOT_TOKEN)
        _require("SUPABASE_URL",    cls.SUPABASE_URL)
        _require("SUPABASE_KEY",    cls.SUPABASE_KEY)
        _require("ENCRYPTION_KEY",  cls.ENCRYPTION_KEY)
        _require("UPI_ID",          cls.UPI_ID)

        if not cls.OWNER_IDS:
            print("❌  OWNER_IDS must have at least one ID")
            ok = False

        if cls.MIN_DEPOSIT_INR <= 0:
            print("❌  MIN_DEPOSIT_INR must be > 0")
            ok = False

        if cls.MAX_DEPOSIT_INR <= cls.MIN_DEPOSIT_INR:
            print("❌  MAX_DEPOSIT_INR must be > MIN_DEPOSIT_INR")
            ok = False

        # Validate Fernet key format without importing at module level
        if cls.ENCRYPTION_KEY:
            try:
                import base64
                decoded = base64.urlsafe_b64decode(cls.ENCRYPTION_KEY + "==")
                if len(decoded) != 32:
                    print("❌  ENCRYPTION_KEY must be a 32-byte URL-safe base64 string")
                    ok = False
            except Exception:
                print("❌  ENCRYPTION_KEY is not valid URL-safe base64")
                ok = False

        return ok

    # ──────────────────────────────────────────────────────────────
    #  Role helpers
    # ──────────────────────────────────────────────────────────────

    @classmethod
    def is_owner(cls, user_id: int) -> bool:
        return user_id in cls.OWNER_IDS

    @classmethod
    def is_admin(cls, user_id: int) -> bool:
        return user_id in cls.OWNER_IDS or user_id in cls.ADMIN_IDS

    # ──────────────────────────────────────────────────────────────
    #  Currency helpers
    # ──────────────────────────────────────────────────────────────

    @classmethod
    def usd_to_inr(cls, usd: float) -> float:
        return round(usd * cls.USD_TO_INR, 2)

    @classmethod
    def inr_to_usd(cls, inr: float) -> float:
        return round(inr / cls.USD_TO_INR, 4)


# ═══════════════════════════════════════════════════════
#  Formatting helpers  (pure functions, no side-effects)
# ═══════════════════════════════════════════════════════

def escape_md(text: str) -> str:
    """
    Escape Markdown special characters in user-supplied text
    (names, usernames) before embedding in MARKDOWN parse_mode messages.
    Prevents Markdown injection / broken layout.
    """
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def format_currency(amount_usd: float, show_inr: bool = True) -> str:
    """'$5.00 (₹470)' or just '₹470'"""
    inr = Config.usd_to_inr(amount_usd)
    if show_inr:
        return f"${amount_usd:.2f} (₹{inr:.0f})"
    return f"₹{inr:.0f}"


def format_usd(amount: float) -> str:
    return f"${amount:.2f}"


def format_inr(amount_inr: float) -> str:
    return f"₹{int(round(amount_inr))}"


def format_profile_card(user: dict) -> str:
    """
    Build profile card text.
    Accepts joined as either datetime or ISO string — handles both safely.
    Escapes user-controlled fields.
    """
    from datetime import datetime, timezone

    joined = user.get("joined", "")
    if isinstance(joined, str):
        try:
            joined = datetime.fromisoformat(joined.replace("Z", "+00:00"))
        except Exception:
            joined = None
    joined_str = joined.strftime("%Y-%m-%d") if joined else "Unknown"

    rank_emoji = {"Bronze": "🥉", "Silver": "🥈", "Gold": "🥇", "VIP": "💎"}.get(
        user.get("rank", "Bronze"), "🏅"
    )

    name     = escape_md(str(user.get("name", "User")))
    ref_code = user.get("referral_code", "—")

    return (
        f"👤 *CLIENT CARD*\n\n"
        f"🆔 ID: `{user['user_id']}`\n"
        f"👤 Name: {name}\n"
        f"💰 Balance: {format_currency(float(user.get('balance', 0)))}\n"
        f"{rank_emoji} Rank: {user.get('rank', 'Bronze')}\n"
        f"📦 Total Spent: {format_currency(float(user.get('total_spent', 0)))}\n"
        f"📅 Joined: {joined_str}\n"
        f"🎁 Referral Code: `{ref_code}`"
    )


def format_product_card(product: dict) -> str:
    """Build product card. Escapes name and description."""
    name = escape_md(str(product.get("name", "")))
    desc = escape_md(str(product.get("description", "")))
    stock = product.get("stock_count")

    lines = [
        f"📦 *{name}*\n",
        f"💰 Price: {format_currency(float(product.get('selling_price', 0)))}",
    ]
    if desc:
        lines.append(f"📝 {desc}")
    if stock is not None:
        if stock > 0:
            lines.append(f"📊 Stock: {stock} available")
        else:
            lines.append("❌ *OUT OF STOCK*")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════
#  CLI validation
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    print("🔍 Validating configuration...")
    if Config.validate():
        print("✅ Configuration valid!")
        print(f"   Owners : {Config.OWNER_IDS}")
        print(f"   Admins : {Config.ADMIN_IDS}")
        print(f"   Rate   : 1 USD = ₹{Config.USD_TO_INR}")
        print(f"   Deposit: ₹{Config.MIN_DEPOSIT_INR} – ₹{Config.MAX_DEPOSIT_INR}")
    else:
        sys.exit(1)
