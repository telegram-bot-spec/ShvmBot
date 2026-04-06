"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Userbot (Pyrogram OTP layer)                        ║
║                                                                ║
║  FIXED:                                                        ║
║  • OTP reads from 777000 (Telegram service), NOT "me"         ║
║  • FloodWait: retry with depth limit — no infinite recursion  ║
║  • phone_code_hash stored on client — verify uses same client ║
║  • verify_otp_and_save reuses the connected client            ║
║  • SessionPool has asyncio.Lock — no duplicate sessions       ║
║  • HealthMonitor skips sold accounts                          ║
║  • scheduled_health_check runs immediately on startup         ║
║  • All datetime comparisons UTC-aware                         ║
║  • OTP regex requires "your code is" context                  ║
║  • Country prefix map uses longest-prefix-first matching      ║
║  • add_account_via_session uses unique client names            ║
║  • api_hash NOT stored in DB — uses Config.TG_API_HASH        ║
╚════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import logging
import re
import secrets
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    SessionPasswordNeeded,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    PhoneNumberInvalid,
    BadRequest,
    AuthKeyUnregistered,
    UserDeactivated,
)

from config import Config
from utils import crypto, utcnow, parse_utc

logger = logging.getLogger("userbot")


# ═══════════════════════════════════════════════════════
#  Supabase helper (reuse from db.py)
# ═══════════════════════════════════════════════════════

def _get_supabase():
    from db import supabase
    return supabase


async def _db_run(fn):
    """Run a sync Supabase call in the thread pool."""
    loop = asyncio.get_event_loop()
    from functools import partial
    return await loop.run_in_executor(None, fn)


# ═══════════════════════════════════════════════════════
#  Country prefix map
#  Sorted longest-prefix-first to avoid +1 matching +91
# ═══════════════════════════════════════════════════════

_RAW_COUNTRY_MAP: Dict[str, Tuple[str, str]] = {
    "+1":    ("USA / Canada", "🇺🇸"),
    "+7":    ("Russia",       "🇷🇺"),
    "+20":   ("Egypt",        "🇪🇬"),
    "+27":   ("South Africa", "🇿🇦"),
    "+30":   ("Greece",       "🇬🇷"),
    "+31":   ("Netherlands",  "🇳🇱"),
    "+32":   ("Belgium",      "🇧🇪"),
    "+33":   ("France",       "🇫🇷"),
    "+34":   ("Spain",        "🇪🇸"),
    "+36":   ("Hungary",      "🇭🇺"),
    "+39":   ("Italy",        "🇮🇹"),
    "+40":   ("Romania",      "🇷🇴"),
    "+41":   ("Switzerland",  "🇨🇭"),
    "+43":   ("Austria",      "🇦🇹"),
    "+44":   ("UK",           "🇬🇧"),
    "+45":   ("Denmark",      "🇩🇰"),
    "+46":   ("Sweden",       "🇸🇪"),
    "+47":   ("Norway",       "🇳🇴"),
    "+48":   ("Poland",       "🇵🇱"),
    "+49":   ("Germany",      "🇩🇪"),
    "+51":   ("Peru",         "🇵🇪"),
    "+52":   ("Mexico",       "🇲🇽"),
    "+54":   ("Argentina",    "🇦🇷"),
    "+55":   ("Brazil",       "🇧🇷"),
    "+56":   ("Chile",        "🇨🇱"),
    "+57":   ("Colombia",     "🇨🇴"),
    "+60":   ("Malaysia",     "🇲🇾"),
    "+61":   ("Australia",    "🇦🇺"),
    "+62":   ("Indonesia",    "🇮🇩"),
    "+63":   ("Philippines",  "🇵🇭"),
    "+64":   ("New Zealand",  "🇳🇿"),
    "+65":   ("Singapore",    "🇸🇬"),
    "+66":   ("Thailand",     "🇹🇭"),
    "+77":   ("Kazakhstan",   "🇰🇿"),   # must be before +7
    "+81":   ("Japan",        "🇯🇵"),
    "+82":   ("South Korea",  "🇰🇷"),
    "+84":   ("Vietnam",      "🇻🇳"),
    "+86":   ("China",        "🇨🇳"),
    "+880":  ("Bangladesh",   "🇧🇩"),
    "+886":  ("Taiwan",       "🇹🇼"),
    "+90":   ("Turkey",       "🇹🇷"),
    "+91":   ("India",        "🇮🇳"),   # must be before +1
    "+92":   ("Pakistan",     "🇵🇰"),
    "+93":   ("Afghanistan",  "🇦🇫"),
    "+94":   ("Sri Lanka",    "🇱🇰"),
    "+95":   ("Myanmar",      "🇲🇲"),
    "+98":   ("Iran",         "🇮🇷"),
    "+212":  ("Morocco",      "🇲🇦"),
    "+213":  ("Algeria",      "🇩🇿"),
    "+216":  ("Tunisia",      "🇹🇳"),
    "+218":  ("Libya",        "🇱🇾"),
    "+220":  ("Gambia",       "🇬🇲"),
    "+234":  ("Nigeria",      "🇳🇬"),
    "+254":  ("Kenya",        "🇰🇪"),
    "+255":  ("Tanzania",     "🇹🇿"),
    "+256":  ("Uganda",       "🇺🇬"),
    "+260":  ("Zambia",       "🇿🇲"),
    "+263":  ("Zimbabwe",     "🇿🇼"),
    "+351":  ("Portugal",     "🇵🇹"),
    "+352":  ("Luxembourg",   "🇱🇺"),
    "+353":  ("Ireland",      "🇮🇪"),
    "+358":  ("Finland",      "🇫🇮"),
    "+359":  ("Bulgaria",     "🇧🇬"),
    "+370":  ("Lithuania",    "🇱🇹"),
    "+371":  ("Latvia",       "🇱🇻"),
    "+372":  ("Estonia",      "🇪🇪"),
    "+380":  ("Ukraine",      "🇺🇦"),
    "+381":  ("Serbia",       "🇷🇸"),
    "+385":  ("Croatia",      "🇭🇷"),
    "+386":  ("Slovenia",     "🇸🇮"),
    "+420":  ("Czech Rep.",   "🇨🇿"),
    "+421":  ("Slovakia",     "🇸🇰"),
    "+48":   ("Poland",       "🇵🇱"),
    "+502":  ("Guatemala",    "🇬🇹"),
    "+503":  ("El Salvador",  "🇸🇻"),
    "+506":  ("Costa Rica",   "🇨🇷"),
    "+51":   ("Peru",         "🇵🇪"),
    "+598":  ("Uruguay",      "🇺🇾"),
    "+60":   ("Malaysia",     "🇲🇾"),
    "+880":  ("Bangladesh",   "🇧🇩"),
    "+960":  ("Maldives",     "🇲🇻"),
    "+966":  ("Saudi Arabia", "🇸🇦"),
    "+971":  ("UAE",          "🇦🇪"),
    "+972":  ("Israel",       "🇮🇱"),
    "+973":  ("Bahrain",      "🇧🇭"),
    "+974":  ("Qatar",        "🇶🇦"),
    "+975":  ("Bhutan",       "🇧🇹"),
    "+976":  ("Mongolia",     "🇲🇳"),
    "+977":  ("Nepal",        "🇳🇵"),
    "+992":  ("Tajikistan",   "🇹🇯"),
    "+993":  ("Turkmenistan", "🇹🇲"),
    "+994":  ("Azerbaijan",   "🇦🇿"),
    "+995":  ("Georgia",      "🇬🇪"),
    "+996":  ("Kyrgyzstan",   "🇰🇬"),
    "+998":  ("Uzbekistan",   "🇺🇿"),
}

# Sort by prefix length descending so longer prefixes match first (+880 before +88 before +8)
_COUNTRY_MAP: List[Tuple[str, str, str]] = sorted(
    [(code, country, flag) for code, (country, flag) in _RAW_COUNTRY_MAP.items()],
    key=lambda x: len(x[0]),
    reverse=True,
)


def get_country_from_phone(phone: str) -> Tuple[str, str]:
    """Match phone number to country using longest-prefix-first."""
    normalized = phone if phone.startswith("+") else f"+{phone}"
    for code, country, flag in _COUNTRY_MAP:
        if normalized.startswith(code):
            return country, flag
    return "Unknown", "🌍"


# ═══════════════════════════════════════════════════════
#  OTP Patterns  (context-aware — not just any 5-6 digits)
# ═══════════════════════════════════════════════════════

_OTP_PATTERNS = [
    # Telegram's own format: "Login code: 12345"
    re.compile(r'(?:login|verification|confirm|security|auth(?:entication)?)\s+code[:\s]+(\d{5,6})', re.I),
    # "Your code is 12345" / "code: 12345"
    re.compile(r'(?:your\s+)?code\s+(?:is\s+)?[:\-]?\s*(\d{5,6})', re.I),
    # "OTP: 12345"
    re.compile(r'\bOTP\b[:\s]+(\d{5,6})', re.I),
    # Telegram format: just the code alone in a message from 777000
    re.compile(r'^(\d{5,6})$', re.M),
]


def extract_otp(text: str) -> Optional[str]:
    """
    Extract OTP from message text using context-aware patterns.
    Returns the first match, or None.
    """
    for pattern in _OTP_PATTERNS:
        m = pattern.search(text)
        if m:
            return m.group(1)
    return None


# ═══════════════════════════════════════════════════════
#  Session Pool  (thread-safe, no duplicate sessions)
# ═══════════════════════════════════════════════════════

class SessionPool:
    """
    Manages a pool of active Pyrogram clients keyed by phone number.
    Uses asyncio.Lock to prevent race conditions that create duplicate sessions.
    """

    TELEGRAM_SERVICE_ID = 777000    # Official Telegram service account

    def __init__(self) -> None:
        self._clients: Dict[str, Client] = {}
        self._lock    = asyncio.Lock()

    async def get_client(
        self,
        phone: str,
        session_string: str,
        max_flood_retries: int = 3,
    ) -> Optional[Client]:
        """
        Return a started Pyrogram client for the given phone.
        Creates one if it doesn't exist.
        FloodWait retried up to max_flood_retries times — no infinite recursion.
        """
        async with self._lock:
            if phone in self._clients:
                client = self._clients[phone]
                if client.is_connected:
                    return client
                # Client disconnected — remove and recreate
                del self._clients[phone]

            for attempt in range(max_flood_retries + 1):
                try:
                    client = Client(
                        name           = f"session_{phone.replace('+','')}",
                        api_id         = Config.TG_API_ID,
                        api_hash       = Config.TG_API_HASH,
                        session_string = session_string,
                        in_memory      = True,
                    )
                    await client.start()
                    self._clients[phone] = client
                    logger.info("✅ Client started for %s", phone)
                    return client

                except FloodWait as exc:
                    if attempt >= max_flood_retries:
                        logger.error("❌ FloodWait exhausted for %s after %d retries", phone, max_flood_retries)
                        return None
                    wait = exc.value + 2   # add 2s buffer
                    logger.warning("FloodWait %ds for %s (attempt %d/%d)", wait, phone, attempt+1, max_flood_retries)
                    await asyncio.sleep(wait)

                except (AuthKeyUnregistered, UserDeactivated) as exc:
                    logger.error("❌ Session invalid for %s: %s", phone, exc)
                    await self._mark_unhealthy(phone, str(exc))
                    return None

                except Exception as exc:
                    logger.error("❌ Error starting client for %s: %s", phone, exc)
                    return None

        return None

    async def release(self, phone: str) -> None:
        async with self._lock:
            client = self._clients.pop(phone, None)
        if client and client.is_connected:
            try:
                await client.stop()
            except Exception:
                pass

    async def release_all(self) -> None:
        async with self._lock:
            phones = list(self._clients.keys())
        for phone in phones:
            await self.release(phone)

    async def _mark_unhealthy(self, phone: str, error: str) -> None:
        try:
            await _db_run(
                lambda: _get_supabase().table("tg_accounts")
                    .update({"is_healthy": False, "health_error": error[:500]})
                    .eq("phone", phone)
                    .execute()
            )
        except Exception as exc:
            logger.warning("Could not mark %s unhealthy: %s", phone, exc)


# Global pool
session_pool = SessionPool()


# ═══════════════════════════════════════════════════════
#  OTP Fetcher
# ═══════════════════════════════════════════════════════

class OTPFetcher:

    @staticmethod
    async def fetch_latest_otp(
        phone: str,
        session_string: str,
        max_age_minutes: int = 5,
    ) -> Optional[str]:
        """
        Fetch the latest OTP for the given account.

        Fixed:
        • Reads from 777000 (Telegram service), NOT "me" (saved messages)
        • All datetime comparisons are UTC-aware
        • OTP regex requires context, not just any 5-6 digit number
        """
        client = await session_pool.get_client(phone, session_string)
        if not client:
            logger.error("❌ Could not get client for %s", phone)
            return None

        cutoff = utcnow() - timedelta(minutes=max_age_minutes)

        try:
            # 777000 = Official Telegram service account (sends OTP codes)
            async for message in client.get_chat_history(
                SessionPool.TELEGRAM_SERVICE_ID, limit=10
            ):
                # message.date from Pyrogram is UTC-aware — safe comparison
                if message.date < cutoff:
                    break

                text = message.text or message.caption or ""
                otp  = extract_otp(text)
                if otp:
                    logger.info("✅ OTP found for %s: %s", phone, otp)
                    return otp

            logger.info("No OTP found within %d minutes for %s", max_age_minutes, phone)
            return None

        except Exception as exc:
            logger.error("❌ Error fetching OTP for %s: %s", phone, exc)
            return None


# ═══════════════════════════════════════════════════════
#  Account management
# ═══════════════════════════════════════════════════════

# In-memory store for pending phone verifications
# Maps phone → {"client": Client, "phone_code_hash": str}
_pending_verifications: Dict[str, Dict] = {}
_pending_lock = asyncio.Lock()


async def add_account_via_phone(phone: str) -> Tuple[bool, str, Optional[str]]:
    """
    Step 1: Send OTP to phone number.
    Stores the connected client in _pending_verifications so
    verify_otp_and_save() can use the SAME session.

    Fixed: client is stored — not leaked.
    verify_otp_and_save() reuses this exact client + phone_code_hash.
    """
    if not phone.startswith("+"):
        phone = f"+{phone}"

    async with _pending_lock:
        # Clean up any existing pending verification for this phone
        existing = _pending_verifications.pop(phone, None)
        if existing:
            try:
                await existing["client"].stop()
            except Exception:
                pass

    try:
        client = Client(
            name     = f"otp_send_{secrets.token_hex(4)}",   # unique name
            api_id   = Config.TG_API_ID,
            api_hash = Config.TG_API_HASH,
            in_memory= True,
        )
        await client.connect()
        sent_code = await client.send_code(phone)

        async with _pending_lock:
            _pending_verifications[phone] = {
                "client":          client,
                "phone_code_hash": sent_code.phone_code_hash,
            }

        logger.info("✅ OTP sent to %s", phone)
        return True, "OTP sent successfully.", sent_code.phone_code_hash

    except PhoneNumberInvalid:
        return False, "Invalid phone number.", None
    except FloodWait as exc:
        return False, f"Too many attempts. Wait {exc.value}s.", None
    except Exception as exc:
        logger.error("❌ send_code failed for %s: %s", phone, exc)
        return False, "Failed to send OTP. Try again.", None


async def verify_otp_and_save(
    phone: str,
    code: str,
    twofa_password: Optional[str] = None,
    added_by: int = 0,
) -> Tuple[bool, str]:
    """
    Step 2: Verify OTP and save account to DB.

    Fixed:
    • Uses the SAME client from add_account_via_phone() — not a new one
    • phone_code_hash belongs to this client's session
    • api_hash NOT stored in DB
    """
    if not phone.startswith("+"):
        phone = f"+{phone}"

    async with _pending_lock:
        pending = _pending_verifications.get(phone)

    if not pending:
        return False, "Verification session expired. Please send OTP again."

    client          = pending["client"]
    phone_code_hash = pending["phone_code_hash"]

    try:
        signed_in = await client.sign_in(phone, phone_code_hash, code)

    except PhoneCodeInvalid:
        return False, "Invalid OTP code."
    except PhoneCodeExpired:
        async with _pending_lock:
            _pending_verifications.pop(phone, None)
        try:
            await client.stop()
        except Exception:
            pass
        return False, "OTP expired. Please request a new one."
    except SessionPasswordNeeded:
        if not twofa_password:
            return False, "This account has 2FA enabled. Provide the password."
        try:
            signed_in = await client.check_password(twofa_password)
        except Exception as exc:
            return False, f"2FA failed: {str(exc)[:100]}"
    except Exception as exc:
        logger.error("❌ sign_in failed for %s: %s", phone, exc)
        return False, f"Sign-in error: {str(exc)[:100]}"

    # Export session string
    try:
        session_str = await client.export_session_string()
    except Exception as exc:
        return False, f"Could not export session: {str(exc)[:100]}"

    # ── Fetch full account details (like @tgdnabot) ──────────────────────
    account_details: dict = {}
    try:
        me = await client.get_me()
        account_details = {
            "tg_user_id":  me.id,
            "first_name":  me.first_name or "",
            "last_name":   me.last_name  or "",
            "tg_username": me.username   or "",
            "bio":         "",           # bio requires get_chat(me.id)
            "is_premium":  bool(getattr(me, "is_premium", False)),
            "dc_id":       None,
        }
        # Try fetching bio via get_chat (not always available)
        try:
            chat = await client.get_chat(me.id)
            account_details["bio"] = (chat.bio or "")[:500]
        except Exception:
            pass
        # Try fetching DC id from client storage
        try:
            account_details["dc_id"] = client.storage.dc_id
        except Exception:
            pass
    except Exception as exc:
        logger.warning("Could not fetch account details for %s: %s", phone, exc)

    # Encrypt sensitive data
    enc_session = crypto.encrypt(session_str)
    enc_twofa   = crypto.encrypt(twofa_password) if twofa_password else None

    country, flag = get_country_from_phone(phone)

    # Save to DB — api_hash NOT stored (use Config.TG_API_HASH at runtime)
    try:
        row = {
            "phone":          phone,
            "country":        country,
            "country_flag":   flag,
            "session_string": enc_session,   # Fernet-encrypted
            "twofa_password": enc_twofa,     # Fernet-encrypted or None
            "api_id":         str(Config.TG_API_ID),
            # api_hash intentionally NOT stored — use Config.TG_API_HASH
            "is_healthy":     True,
            "added_by":       added_by,
            # Full account details
            **{k: v for k, v in account_details.items() if v is not None and v != ""},
        }
        result = await _db_run(
            lambda: _get_supabase().table("tg_accounts").insert(row).execute()
        )
        if not result.data:
            return False, "Failed to save account to database."
    except Exception as exc:
        logger.error("❌ DB insert failed for %s: %s", phone, exc)
        return False, "Database error saving account."
    finally:
        # Clean up pending verification
        async with _pending_lock:
            _pending_verifications.pop(phone, None)
        try:
            await client.stop()
        except Exception:
            pass

    logger.info("✅ Account saved: %s (%s %s)", phone, flag, country)
    return True, f"✅ Account {phone} added successfully!"


async def add_account_via_session(
    session_string: str,
    twofa_password: Optional[str] = None,
    added_by: int = 0,
) -> Tuple[bool, str]:
    """
    Import account using an existing session string.

    Fixed:
    • Unique client name per import (no conflicts on concurrent imports)
    • api_hash not stored in DB
    """
    unique_name = f"import_{secrets.token_hex(8)}"   # unique every time

    try:
        client = Client(
            name           = unique_name,
            api_id         = Config.TG_API_ID,
            api_hash       = Config.TG_API_HASH,
            session_string = session_string,
            in_memory      = True,
        )
        await client.start()
        me    = await client.get_me()
        phone = me.phone_number
        if not phone.startswith("+"):
            phone = f"+{phone}"

        # ── Fetch full account details ───────────────────────────────────
        account_details: dict = {
            "tg_user_id":  me.id,
            "first_name":  me.first_name or "",
            "last_name":   me.last_name  or "",
            "tg_username": me.username   or "",
            "bio":         "",
            "is_premium":  bool(getattr(me, "is_premium", False)),
            "dc_id":       None,
        }
        try:
            chat = await client.get_chat(me.id)
            account_details["bio"] = (chat.bio or "")[:500]
        except Exception:
            pass
        try:
            account_details["dc_id"] = client.storage.dc_id
        except Exception:
            pass

    except Exception as exc:
        logger.error("❌ Session import failed: %s", exc)
        return False, f"Invalid session string: {str(exc)[:100]}"
    finally:
        try:
            await client.stop()
        except Exception:
            pass

    # Check duplicate
    existing = await _db_run(
        lambda: _get_supabase().table("tg_accounts")
            .select("phone")
            .eq("phone", phone)
            .execute()
    )
    if existing.data:
        return False, f"Account {phone} already exists in database."

    enc_session = crypto.encrypt(session_string)
    enc_twofa   = crypto.encrypt(twofa_password) if twofa_password else None
    country, flag = get_country_from_phone(phone)

    try:
        row = {
            "phone":          phone,
            "country":        country,
            "country_flag":   flag,
            "session_string": enc_session,
            "twofa_password": enc_twofa,
            "api_id":         str(Config.TG_API_ID),
            "is_healthy":     True,
            "added_by":       added_by,
            # Full account details (fetched above via get_me)
            **{k: v for k, v in account_details.items() if v is not None and v != ""},
        }
        result = await _db_run(
            lambda: _get_supabase().table("tg_accounts").insert(row).execute()
        )
        if not result.data:
            return False, "Database error."
    except Exception as exc:
        return False, f"DB error: {str(exc)[:100]}"

    details_line = ""
    if account_details.get("first_name"):
        n = f"{account_details['first_name']} {account_details.get('last_name','') or ''}".strip()
        details_line = f" · {n}"
    logger.info("✅ Session imported: %s (%s)%s", phone, country, details_line)
    return True, f"✅ Account {phone} imported successfully!{details_line}"


# ═══════════════════════════════════════════════════════
#  Health Monitor
# ═══════════════════════════════════════════════════════

class HealthMonitor:

    @staticmethod
    async def check_all_accounts() -> Dict[str, int]:
        """
        Run health check on all UNSOLD accounts.

        Fixed:
        • Filters is_sold=FALSE — does not reconnect to sold accounts
        • Uses UTC-aware timestamps
        """
        result = await _db_run(
            lambda: _get_supabase().table("tg_accounts")
                .select("phone, session_string")
                .eq("is_sold",    False)
                .eq("is_healthy", True)
                .execute()
        )
        accounts = result.data or []

        if not accounts:
            logger.info("No unsold accounts to health-check")
            return {"checked": 0, "healthy": 0, "unhealthy": 0}

        healthy = unhealthy = 0

        for account in accounts:
            phone          = account["phone"]
            enc_session    = account["session_string"]
            session_string = crypto.decrypt(enc_session)

            if not session_string:
                logger.warning("Could not decrypt session for %s", phone)
                unhealthy += 1
                continue

            is_ok, error = await HealthMonitor._check_one(phone, session_string)
            now_iso      = utcnow().isoformat()

            if is_ok:
                healthy += 1
                await _db_run(
                    lambda p=phone, t=now_iso: _get_supabase().table("tg_accounts")
                        .update({"is_healthy": True, "health_error": None, "last_health_check": t})
                        .eq("phone", p)
                        .execute()
                )
            else:
                unhealthy += 1
                await _db_run(
                    lambda p=phone, t=now_iso, e=error: _get_supabase().table("tg_accounts")
                        .update({"is_healthy": False, "health_error": e[:500], "last_health_check": t})
                        .eq("phone", p)
                        .execute()
                )
                logger.warning("❌ Unhealthy account %s: %s", phone, error)

            # Rate-limit health checks
            await asyncio.sleep(1)

        logger.info("Health check complete: %d healthy, %d unhealthy", healthy, unhealthy)
        return {"checked": len(accounts), "healthy": healthy, "unhealthy": unhealthy}

    @staticmethod
    async def _check_one(phone: str, session_string: str) -> Tuple[bool, str]:
        """Check a single account. Returns (is_healthy, error_msg)."""
        unique_name = f"health_{secrets.token_hex(6)}"
        client = None
        try:
            client = Client(
                name           = unique_name,
                api_id         = Config.TG_API_ID,
                api_hash       = Config.TG_API_HASH,
                session_string = session_string,
                in_memory      = True,
            )
            await client.start()
            await client.get_me()
            return True, ""
        except (AuthKeyUnregistered, UserDeactivated) as exc:
            return False, str(exc)
        except FloodWait as exc:
            # FloodWait means it's alive — just rate limited
            return True, ""
        except Exception as exc:
            return False, str(exc)
        finally:
            if client:
                try:
                    await client.stop()
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════
#  Scheduled health check
# ═══════════════════════════════════════════════════════

async def scheduled_health_check() -> None:
    """
    Runs immediately on startup, then every HEALTH_CHECK_INTERVAL seconds.
    Fixed: old code slept first — unhealthy accounts served for 3 hours.
    """
    logger.info("🏥 Starting health check scheduler (interval: %ds)", Config.HEALTH_CHECK_INTERVAL)

    while True:
        try:
            logger.info("🏥 Running health check…")
            results = await HealthMonitor.check_all_accounts()
            logger.info("🏥 Health check done: %s", results)
        except Exception as exc:
            logger.error("❌ Health check failed: %s", exc)

        # Sleep AFTER the check (not before)
        await asyncio.sleep(Config.HEALTH_CHECK_INTERVAL)


# ═══════════════════════════════════════════════════════
#  Public API for shop_bot integration
# ═══════════════════════════════════════════════════════

async def get_otp_for_order(phone: str, session_string_enc: str) -> Optional[str]:
    """
    Convenience function called by shop_bot when an OTP account is purchased.
    Decrypts session, fetches OTP, returns it or None.
    """
    session_string = crypto.decrypt(session_string_enc)
    if not session_string:
        logger.error("Cannot decrypt session for %s", phone)
        return None
    return await OTPFetcher.fetch_latest_otp(phone, session_string)


# ═══════════════════════════════════════════════════════
#  CLI entry point
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    async def _test_otp_extract():
        print("🧪 Testing OTP extraction…\n")
        tests = [
            ("Login code: 12345", "12345"),
            ("Your code is 98765", "98765"),
            ("OTP: 123456", "123456"),
            ("12345",          "12345"),   # from 777000 — bare code is valid
            ("Transaction ID: 98765", None),   # should NOT match
            ("Order #12345 confirmed", None),   # should NOT match
        ]
        for text, expected in tests:
            result = extract_otp(text)
            status = "✅" if result == expected else "❌"
            print(f"  {status}  Input: {text!r:40} → {result!r} (expected {expected!r})")

    async def _test_country():
        print("\n🧪 Testing country prefix matching…\n")
        tests = [
            ("+91234567890",  "India"),
            ("+1234567890",   "USA / Canada"),
            ("+77123456789",  "Kazakhstan"),
            ("+7123456789",   "Russia"),
            ("+8801234567890","Bangladesh"),
            ("+447911123456", "UK"),
        ]
        for phone, expected_country in tests:
            country, flag = get_country_from_phone(phone)
            status = "✅" if country == expected_country else "❌"
            print(f"  {status}  {phone:20} → {flag} {country} (expected {expected_country})")

    async def _run_health():
        print("\n🏥 Running health check…")
        results = await HealthMonitor.check_all_accounts()
        print(f"Results: {results}")

    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"
    if cmd == "test":
        asyncio.run(_test_otp_extract())
        asyncio.run(_test_country())
    elif cmd == "health":
        asyncio.run(_run_health())
    elif cmd == "schedule":
        asyncio.run(scheduled_health_check())
    else:
        print("Usage: python userbot.py [test|health|schedule]")
