"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Payment Processing                                   ║
║  Fixed: image-byte hashing, configurable limits,              ║
║         integer INR amounts, clean message formatting          ║
╚════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import logging
from io import BytesIO
from typing import Tuple, Optional

import qrcode
import qrcode.constants

from config import Config, format_inr, format_currency
from utils import hash_image_bytes

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════
#  Amount validation
# ═══════════════════════════════════════════════════════

def validate_payment_amount(amount_inr: float) -> Tuple[bool, str]:
    """
    Validate a payment amount in INR.

    Limits come from Config (configurable via .env) — NOT hardcoded.
    Amount is rounded to the nearest whole rupee (UPI requires integers).

    Returns:
        (True, "")        — valid
        (False, reason)   — invalid with human-readable reason
    """
    try:
        amount_int = int(round(amount_inr))
    except (TypeError, ValueError):
        return False, "Please enter a valid number."

    if amount_int <= 0:
        return False, "Amount must be greater than zero."

    if amount_int < Config.MIN_DEPOSIT_INR:
        return False, f"Minimum deposit is {format_inr(Config.MIN_DEPOSIT_INR)}."

    if amount_int > Config.MAX_DEPOSIT_INR:
        return False, f"Maximum deposit is {format_inr(Config.MAX_DEPOSIT_INR)}."

    return True, ""


def normalise_inr(amount: float) -> int:
    """Round INR to nearest whole rupee (UPI does not accept paise)."""
    return int(round(amount))


# ═══════════════════════════════════════════════════════
#  UPI QR Code
# ═══════════════════════════════════════════════════════

def generate_upi_qr(amount_inr: int, payment_ref: str) -> BytesIO:
    """
    Generate a UPI deep-link QR code.

    Args:
        amount_inr:  Amount in whole rupees (integer)
        payment_ref: Unique payment reference (e.g. PAY-ABC123)

    Returns:
        BytesIO PNG image, seeked to position 0, ready for Telegram upload.
    """
    upi_string = (
        f"upi://pay?"
        f"pa={Config.UPI_ID}&"
        f"pn={Config.UPI_QR_NAME}&"
        f"am={amount_inr}&"          # integer — no decimal point
        f"cu=INR&"
        f"tn={payment_ref}"
    )

    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_M,   # medium — more robust
        box_size=10,
        border=4,
    )
    qr.add_data(upi_string)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")

    buf = BytesIO()
    buf.name = f"upi_{payment_ref}.png"
    img.save(buf, "PNG")
    buf.seek(0)

    logger.debug("Generated UPI QR for %s (₹%d)", payment_ref, amount_inr)
    return buf


# ═══════════════════════════════════════════════════════
#  Screenshot duplicate detection
# ═══════════════════════════════════════════════════════

async def hash_screenshot(bot, file_id: str) -> Optional[str]:
    """
    Download the screenshot image and return SHA-256 of its raw bytes.

    This is the CORRECT duplicate-detection approach.
    Hashing the file_id string is wrong — Telegram reassigns file_ids
    on every upload, so the same image gets a different ID each time.

    Returns None if download fails (don't block the user — just log).
    """
    try:
        tg_file = await bot.get_file(file_id)
        buf      = BytesIO()
        await tg_file.download_to_memory(buf)
        return hash_image_bytes(buf.getvalue())
    except Exception as exc:
        logger.warning("Could not download screenshot for hashing: %s", exc)
        return None


# ═══════════════════════════════════════════════════════
#  Message templates
# ═══════════════════════════════════════════════════════

def msg_payment_instructions(
    amount_inr: int,
    payment_ref: str,
) -> str:
    """
    Caption shown on the QR code photo.
    Uses Markdown (*bold*, `code`).
    'Click below' refers to the inline buttons attached to the same message.
    """
    amount_usd = Config.inr_to_usd(amount_inr)
    return (
        f"💰 *ADD FUNDS*\n\n"
        f"*Amount:* {format_inr(amount_inr)} (${amount_usd:.2f})\n"
        f"*Payment ID:* `{payment_ref}`\n\n"
        f"📱 *How to pay:*\n"
        f"1. Scan the QR code with any UPI app\n"
        f"2. Or pay manually to: `{Config.UPI_ID}`\n"
        f"3. Amount must be *exactly {format_inr(amount_inr)}*\n"
        f"4. Tap *✅ I've Paid* below after payment\n\n"
        f"⚠️ *Important:*\n"
        f"• Wrong amount = automatic rejection\n"
        f"• Screenshot must clearly show amount + transaction ID\n"
        f"• Support: @{Config.SUPPORT_USERNAME}"
    )


def msg_request_screenshot() -> str:
    """Shown after user taps 'I've Paid' — asks for screenshot."""
    return (
        "📸 *PAYMENT SCREENSHOT*\n\n"
        "Please send a *clear screenshot* of your payment confirmation.\n\n"
        "✅ *Screenshot must show:*\n"
        "• Payment amount\n"
        "• Transaction ID\n"
        "• Date and time\n"
        "• Recipient UPI ID\n\n"
        "Tap ❌ Cancel below to abort."
    )


def msg_payment_submitted(payment_ref: str) -> str:
    """Confirmation sent to user after screenshot upload."""
    return (
        "✅ *PAYMENT SUBMITTED*\n\n"
        "Your screenshot is under review.\n\n"
        f"🔖 *Payment ID:* `{payment_ref}`\n"
        "⏱ *Review time:* Usually 5–30 minutes\n\n"
        "You'll receive a notification once approved.\n"
        f"Questions? Contact @{Config.SUPPORT_USERNAME}"
    )


def msg_payment_approved(amount_inr: int) -> str:
    """DM sent to user when admin approves their payment."""
    amount_usd = Config.inr_to_usd(amount_inr)
    return (
        "✅ *PAYMENT APPROVED!*\n\n"
        f"{format_inr(amount_inr)} (${amount_usd:.2f}) has been added to your balance.\n\n"
        "You can now use your balance to make purchases!\n"
        "🛒 Tap *Browse Store* to start shopping."
    )


def msg_payment_rejected(amount_inr: int, reason: Optional[str] = None) -> str:
    """DM sent to user when admin rejects their payment."""
    lines = [
        "❌ *PAYMENT REJECTED*\n",
        f"Your payment of {format_inr(amount_inr)} was not approved.",
    ]
    if reason:
        from config import escape_md
        lines.append(f"\n*Reason:* {escape_md(reason)}")
    lines += [
        f"\n*What to do:*",
        "• Make sure the amount matches exactly",
        "• Ensure your screenshot is clear and complete",
        f"• Contact @{Config.SUPPORT_USERNAME} for help",
    ]
    return "\n".join(lines)


def msg_admin_new_payment(payment: dict) -> str:
    """
    Message shown to admins when reviewing a payment.
    payment dict must have: payment_ref, amount_inr, user_id,
    users.name, users.username, created_at
    """
    from utils import time_ago, parse_utc
    from config import escape_md

    user     = payment.get("users") or {}
    name     = escape_md(str(user.get("name", "Unknown")))
    username = user.get("username") or "none"
    created  = parse_utc(str(payment.get("created_at", "")))
    ago      = time_ago(created)
    amount_inr = int(payment.get("amount_inr", 0))
    amount_usd = Config.inr_to_usd(amount_inr)

    return (
        f"💰 *PAYMENT REVIEW*\n\n"
        f"*Payment ID:* `{payment['payment_ref']}`\n"
        f"*Amount:* {format_inr(amount_inr)} (${amount_usd:.2f})\n\n"
        f"*User:* {name}\n"
        f"*Username:* @{username}\n"
        f"*User ID:* `{payment['user_id']}`\n\n"
        f"⏰ *Submitted:* {ago}"
    )


# ═══════════════════════════════════════════════════════
#  CLI self-test
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    import os
    os.environ.setdefault("UPI_ID",          "test@upi")
    os.environ.setdefault("UPI_QR_NAME",     "TestShop")
    os.environ.setdefault("USD_TO_INR",      "84")
    os.environ.setdefault("MIN_DEPOSIT_INR", "10")
    os.environ.setdefault("MAX_DEPOSIT_INR", "100000")
    os.environ.setdefault("SUPPORT_USERNAME","support")
    os.environ.setdefault("OWNER_IDS",       "1")

    print("💰  Testing payments module…\n")

    # Validation
    ok, msg = validate_payment_amount(500)
    assert ok,                     f"Expected valid: {msg}"
    ok, msg = validate_payment_amount(5)
    assert not ok,                 "Should reject below minimum"
    ok, msg = validate_payment_amount(-10)
    assert not ok,                 "Should reject negative"
    ok, msg = validate_payment_amount(200000)
    assert not ok,                 "Should reject above maximum"
    print("✅  validate_payment_amount OK")

    # Normalise
    assert normalise_inr(499.6) == 500
    assert normalise_inr(499.4) == 499
    print("✅  normalise_inr OK")

    # QR generation
    buf = generate_upi_qr(500, "PAY-TEST01")
    assert buf.read(4) == b"\x89PNG", "Not a PNG"
    print(f"✅  QR code generated (PNG confirmed)")

    # Image hash is content-based
    fake_img = b"\x89PNG fake image bytes"
    h1 = hash_image_bytes(fake_img)
    h2 = hash_image_bytes(fake_img)
    assert h1 == h2 and len(h1) == 64
    print(f"✅  Image hash stable: {h1[:16]}…")

    # Message formatting
    msg_instr = msg_payment_instructions(500, "PAY-TEST01")
    assert "PAY-TEST01" in msg_instr
    assert "500" in msg_instr
    print("✅  msg_payment_instructions OK")

    msg_app = msg_payment_approved(500)
    assert "500" in msg_app
    print("✅  msg_payment_approved OK")

    msg_rej = msg_payment_rejected(500, "Screenshot unclear")
    assert "Screenshot unclear" in msg_rej
    print("✅  msg_payment_rejected OK")

    print("\n✅  All payment tests passed!")
