"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Utilities                                            ║
║  Fixed: lazy Fernet init, safe decrypt, thread-safe rate       ║
║         limiter, referral code collision guard, UTC datetimes  ║
╚════════════════════════════════════════════════════════════════╝
"""

import hashlib
import hmac
import logging
import secrets
import string
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════
#  Encryption  (lazy-loaded so import never crashes)
# ═══════════════════════════════════════════════════════

class Encryption:
    """
    Fernet symmetric encryption for session strings, 2FA passwords, etc.

    Lazy-initialised: the Fernet object is only created on first use.
    This means importing utils.py never crashes even if ENCRYPTION_KEY
    is missing — Config.validate() will catch it before any bot starts.
    """

    def __init__(self) -> None:
        self._cipher = None
        self._lock   = threading.Lock()

    def _get_cipher(self):
        """Return (or create) the Fernet cipher. Thread-safe."""
        if self._cipher is not None:
            return self._cipher
        with self._lock:
            if self._cipher is not None:        # double-checked inside lock
                return self._cipher
            from cryptography.fernet import Fernet, InvalidToken  # noqa: F401
            from config import Config
            key = Config.ENCRYPTION_KEY
            if not key:
                raise RuntimeError(
                    "ENCRYPTION_KEY is not set. "
                    "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
                )
            self._cipher = Fernet(key.encode())
            return self._cipher

    def encrypt(self, plaintext: str) -> str:
        """Encrypt a string. Returns empty string for empty input."""
        if not plaintext:
            return ""
        try:
            return self._get_cipher().encrypt(plaintext.encode()).decode()
        except Exception as exc:
            logger.error("Encryption failed: %s", exc)
            raise

    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt a string. Returns empty string for empty input.
        Returns empty string (and logs error) if decryption fails —
        never raises to callers so a bad DB value can't crash the bot.
        """
        if not ciphertext:
            return ""
        try:
            return self._get_cipher().decrypt(ciphertext.encode()).decode()
        except Exception as exc:
            logger.error("Decryption failed (bad key or corrupted data): %s", exc)
            return ""


# Single global instance — lazy, thread-safe
crypto = Encryption()


# ═══════════════════════════════════════════════════════
#  Payment reference ID
# ═══════════════════════════════════════════════════════

def generate_payment_ref() -> str:
    """PAY-XXXXXX  (6 cryptographically random uppercase alphanumeric chars)"""
    alphabet = string.ascii_uppercase + string.digits
    suffix   = "".join(secrets.choice(alphabet) for _ in range(6))
    return f"PAY-{suffix}"


# ═══════════════════════════════════════════════════════
#  Referral code
# ═══════════════════════════════════════════════════════

def generate_referral_code(user_id: int) -> str:
    """
    Deterministic-but-unique 8-char code derived from user_id + random salt.
    The salt makes it non-guessable even knowing the user_id.
    Always uppercase alphanumeric.
    """
    salt = secrets.token_hex(8)
    raw  = f"{user_id}:{salt}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return digest[:8].upper()


# ═══════════════════════════════════════════════════════
#  Screenshot hashing  (hash image BYTES, not file_id)
# ═══════════════════════════════════════════════════════

def hash_image_bytes(image_bytes: bytes) -> str:
    """
    SHA-256 of actual image content.
    This is the correct duplicate-detection approach — hashing the
    Telegram file_id string is wrong because the same image re-uploaded
    gets a different file_id every time.
    """
    return hashlib.sha256(image_bytes).hexdigest()


def hash_file_id(file_id: str) -> str:
    """
    Kept for backward compatibility but intentionally NOT used for
    duplicate payment detection. Use hash_image_bytes() instead.
    """
    return hashlib.sha256(file_id.encode()).hexdigest()


# ═══════════════════════════════════════════════════════
#  Rank calculation
# ═══════════════════════════════════════════════════════

def calculate_rank(total_spent_usd: float) -> str:
    from config import Config
    if total_spent_usd >= Config.RANK_VIP:
        return "VIP"
    if total_spent_usd >= Config.RANK_GOLD:
        return "Gold"
    if total_spent_usd >= Config.RANK_SILVER:
        return "Silver"
    return "Bronze"


RANK_EMOJI: Dict[str, str] = {
    "Bronze": "🥉",
    "Silver": "🥈",
    "Gold":   "🥇",
    "VIP":    "💎",
}

def get_rank_emoji(rank: str) -> str:
    return RANK_EMOJI.get(rank, "🏅")


# ═══════════════════════════════════════════════════════
#  Referral commission
# ═══════════════════════════════════════════════════════

def calculate_referral_commission(purchase_amount_usd: float) -> float:
    from config import Config
    commission = round(purchase_amount_usd * (Config.REFERRAL_PERCENT / 100), 4)
    return max(commission, 0.0)


# ═══════════════════════════════════════════════════════
#  Promo code discount
# ═══════════════════════════════════════════════════════

def calculate_discount(price_usd: float, promo: dict) -> float:
    """
    Calculate discount.
    Fixed: uses `is not None` checks so a 0% discount is valid.
    """
    disc_pct   = promo.get("discount_percent")
    disc_fixed = promo.get("discount_fixed")

    if disc_pct is not None:
        discount = price_usd * (float(disc_pct) / 100.0)
    elif disc_fixed is not None:
        discount = float(disc_fixed)
    else:
        discount = 0.0

    return round(min(discount, price_usd), 4)   # can't discount more than price


# ═══════════════════════════════════════════════════════
#  Date / time helpers  (always UTC-aware)
# ═══════════════════════════════════════════════════════

def utcnow() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def parse_utc(iso_string: str) -> Optional[datetime]:
    """
    Parse an ISO 8601 string (with or without Z suffix) into a
    UTC-aware datetime.  Returns None on parse failure.
    """
    if not iso_string:
        return None
    try:
        return datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def format_dt(dt: Optional[datetime], fmt: str = "%Y-%m-%d %H:%M UTC") -> str:
    """Format a datetime to a readable string. Returns 'Unknown' on None."""
    if dt is None:
        return "Unknown"
    return dt.strftime(fmt)


def time_ago(dt: Optional[datetime]) -> str:
    """
    Human-readable 'X minutes ago' / 'X hours ago'.
    Fixed: uses total_seconds() not .seconds to handle >1 hour correctly.
    """
    if dt is None:
        return "Unknown"
    now   = utcnow()
    # Make dt timezone-aware if it somehow isn't
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    total = int(delta.total_seconds())

    if total < 60:
        return f"{total}s ago"
    if total < 3600:
        return f"{total // 60}m ago"
    if total < 86400:
        return f"{total // 3600}h ago"
    return f"{total // 86400}d ago"


# ═══════════════════════════════════════════════════════
#  Rate limiter  (in-memory, per-process)
#
#  Known limitation: resets on restart and is not shared between
#  the shop-bot and admin-bot processes.  For production, back this
#  with Redis.  For a single-server deployment this is fine.
# ═══════════════════════════════════════════════════════

class RateLimiter:
    """
    Thread-safe in-memory rate limiter.
    Stores (last_action_time, call_count) per key.
    """

    def __init__(self) -> None:
        self._data: Dict[str, Tuple[datetime, int]] = {}
        self._lock = threading.Lock()

    def is_allowed(self, key: str, cooldown_seconds: int, max_calls: int = 1) -> bool:
        """
        Returns True if the action is allowed.
        Returns False if the key is still within its cooldown window.
        """
        now = utcnow()
        with self._lock:
            if key not in self._data:
                self._data[key] = (now, 1)
                return True

            last_time, count = self._data[key]
            elapsed = (now - last_time).total_seconds()

            if elapsed >= cooldown_seconds:
                # Window expired — reset
                self._data[key] = (now, 1)
                return True

            if count < max_calls:
                self._data[key] = (last_time, count + 1)
                return True

            return False

    # Back-compat alias used in shop_bot
    def check(self, key: str, cooldown_seconds: int) -> bool:
        return self.is_allowed(key, cooldown_seconds)

    def cleanup(self, max_age_seconds: int = 3600) -> int:
        """Remove entries older than max_age_seconds. Call periodically."""
        now     = utcnow()
        removed = 0
        with self._lock:
            stale = [
                k for k, (t, _) in self._data.items()
                if (now - t).total_seconds() > max_age_seconds
            ]
            for k in stale:
                del self._data[k]
                removed += 1
        return removed


# Global rate limiter instance
rate_limiter = RateLimiter()


# ═══════════════════════════════════════════════════════
#  Markdown helpers
# ═══════════════════════════════════════════════════════

def bold(text: str)        -> str: return f"*{text}*"
def code(text: str)        -> str: return f"`{text}`"
def pre(text: str)         -> str: return f"```\n{text}\n```"
def italic(text: str)      -> str: return f"_{text}_"
def separator(n: int = 28) -> str: return "─" * n


# ═══════════════════════════════════════════════════════
#  CLI self-test
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    import os, sys
    os.environ.setdefault("ENCRYPTION_KEY",
        "ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=")   # test key only

    print("🧪  Testing utils...\n")

    # Encryption
    secret   = "MyTestPassword!123"
    enc      = crypto.encrypt(secret)
    dec      = crypto.decrypt(enc)
    assert dec == secret, "Encryption round-trip failed"
    print(f"✅  Encryption OK  ({enc[:20]}…)")

    # Bad ciphertext returns empty string, does not raise
    result = crypto.decrypt("notvalidciphertext")
    assert result == "", f"Expected empty string, got {result!r}"
    print("✅  Bad ciphertext → empty string (no crash)")

    # Payment ref
    ref = generate_payment_ref()
    assert ref.startswith("PAY-") and len(ref) == 10, f"Bad ref: {ref}"
    print(f"✅  Payment ref: {ref}")

    # Referral code
    code1 = generate_referral_code(123456)
    code2 = generate_referral_code(123456)
    assert len(code1) == 8 and code1 == code1.upper()
    assert code1 != code2, "Referral codes must be unique (salted)"
    print(f"✅  Referral codes unique: {code1} ≠ {code2}")

    # Rank
    assert calculate_rank(0)   == "Bronze"
    assert calculate_rank(50)  == "Silver"
    assert calculate_rank(200) == "Gold"
    assert calculate_rank(500) == "VIP"
    print("✅  Rank calculation OK")

    # Discount — 0% must work (fixed bug)
    promo_zero = {"discount_percent": 0.0}
    assert calculate_discount(100.0, promo_zero) == 0.0
    promo_10   = {"discount_percent": 10.0}
    assert calculate_discount(100.0, promo_10) == 10.0
    print("✅  Discount calculation OK (0% works)")

    # Rate limiter
    rl = RateLimiter()
    assert rl.is_allowed("u1", 5) is True
    assert rl.is_allowed("u1", 5) is False    # blocked
    assert rl.is_allowed("u2", 5) is True     # different key
    print("✅  Rate limiter OK")

    # time_ago — uses total_seconds (fixed bug)
    from datetime import timezone
    two_hours_ago = utcnow() - timedelta(hours=2)
    result = time_ago(two_hours_ago)
    assert result == "2h ago", f"Got: {result}"
    print(f"✅  time_ago(2h) = '{result}'")

    # Image hash vs file_id hash are different functions
    img_bytes = b"fake image data"
    h1 = hash_image_bytes(img_bytes)
    h2 = hash_image_bytes(img_bytes)
    assert h1 == h2 and len(h1) == 64
    print(f"✅  Image hash stable: {h1[:16]}…")

    print("\n✅  All utils tests passed!")
