#!/usr/bin/env python3
"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Setup Verification                                   ║
║  Run before starting bots to catch config/dependency issues   ║
╚════════════════════════════════════════════════════════════════╝

Usage:
    python test_setup.py
"""

from __future__ import annotations

import asyncio
import sys
from typing import List, Tuple


# ═══════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════

_errors:   List[str] = []
_warnings: List[str] = []


def ok(msg: str)   -> None: print(f"  ✅  {msg}")
def warn(msg: str) -> None:
    print(f"  ⚠️   {msg}")
    _warnings.append(msg)
def fail(msg: str) -> None:
    print(f"  ❌  {msg}")
    _errors.append(msg)


# ═══════════════════════════════════════════════════════
#  1. Imports
# ═══════════════════════════════════════════════════════

def test_imports() -> None:
    print("\n🔍  Checking imports…")

    required = [
        ("telegram",        "python-telegram-bot"),
        ("telegram.ext",    "python-telegram-bot"),
        ("pyrogram",        "pyrogram"),
        ("supabase",        "supabase"),
        ("qrcode",          "qrcode[pil]"),
        ("PIL",             "Pillow"),
        ("dotenv",          "python-dotenv"),
        ("cryptography",    "cryptography"),
    ]

    for module, package in required:
        try:
            __import__(module)
            ok(f"{module}")
        except ImportError as exc:
            fail(f"{module} — install with: pip install {package}  ({exc})")


# ═══════════════════════════════════════════════════════
#  2. Configuration
# ═══════════════════════════════════════════════════════

def test_config() -> None:
    print("\n🔍  Checking configuration…")
    try:
        from config import Config
        if Config.validate():
            ok("All required config values present")
            ok(f"Owners  : {Config.OWNER_IDS}")
            ok(f"Admins  : {Config.ADMIN_IDS}")
            ok(f"Rate    : 1 USD = ₹{Config.USD_TO_INR}")
            ok(f"Deposit : ₹{Config.MIN_DEPOSIT_INR} – ₹{Config.MAX_DEPOSIT_INR}")
        else:
            fail("Configuration validation failed — see errors above")
    except SystemExit:
        fail("Config crashed on import (likely bad OWNER_IDS or ADMIN_IDS value in .env)")
    except Exception as exc:
        fail(f"Config error: {exc}")


# ═══════════════════════════════════════════════════════
#  3. Encryption
# ═══════════════════════════════════════════════════════

def test_encryption() -> None:
    print("\n🔍  Checking encryption…")
    try:
        from utils import crypto
        plaintext = "TGFlowTestSecret!@#"
        encrypted = crypto.encrypt(plaintext)
        decrypted = crypto.decrypt(encrypted)
        if decrypted == plaintext:
            ok("Fernet encrypt/decrypt round-trip passed")
        else:
            fail(f"Encryption mismatch: got {decrypted!r}")

        # Bad ciphertext must NOT raise
        result = crypto.decrypt("thisisnotvalidciphertext")
        if result == "":
            ok("Bad ciphertext returns empty string (no crash)")
        else:
            fail(f"Bad ciphertext returned {result!r} instead of empty string")
    except RuntimeError as exc:
        fail(f"Encryption init failed: {exc}")
    except Exception as exc:
        fail(f"Encryption error: {exc}")


# ═══════════════════════════════════════════════════════
#  4. Database connectivity
# ═══════════════════════════════════════════════════════

async def test_database() -> None:
    print("\n🔍  Checking database connectivity…")
    try:
        # Import db module — Supabase client initialised at import time
        from db import supabase, _run
        result = await _run(
            lambda: supabase.table("users").select("user_id", count="exact").execute()
        )
        ok(f"Supabase connected — {result.count or 0} users in DB")
    except Exception as exc:
        fail(f"Database connection failed: {exc}")
        fail("  → Check SUPABASE_URL and SUPABASE_KEY in .env")
        fail("  → Ensure you have run init_database.sql in Supabase SQL Editor")


# ═══════════════════════════════════════════════════════
#  5. QR code generation
# ═══════════════════════════════════════════════════════

def test_qr() -> None:
    print("\n🔍  Checking QR code generation…")
    try:
        from payments import generate_upi_qr, validate_payment_amount
        buf = generate_upi_qr(500, "PAY-TEST01")
        header = buf.read(4)
        if header == b"\x89PNG":
            ok(f"UPI QR generated successfully (PNG confirmed, {len(buf.getvalue())} bytes)")
        else:
            fail(f"QR output is not a valid PNG (header: {header!r})")

        # Validate amount
        ok_v, _ = validate_payment_amount(500)
        bad_v, msg = validate_payment_amount(1)
        if ok_v and not bad_v:
            ok("Payment amount validation working")
        else:
            fail("Payment validation returned unexpected results")
    except Exception as exc:
        fail(f"QR generation error: {exc}")


# ═══════════════════════════════════════════════════════
#  6. Userbot / Pyrogram config check
# ═══════════════════════════════════════════════════════

def test_pyrogram_config() -> None:
    print("\n🔍  Checking Pyrogram config…")
    try:
        from config import Config
        if Config.TG_API_ID and Config.TG_API_HASH:
            ok(f"TG_API_ID set: {Config.TG_API_ID}")
            ok("TG_API_HASH set")
        else:
            warn("TG_API_ID / TG_API_HASH not set — OTP accounts will not work")
    except Exception as exc:
        fail(f"Pyrogram config check error: {exc}")


# ═══════════════════════════════════════════════════════
#  7. OTP extraction unit test
# ═══════════════════════════════════════════════════════

def test_otp_extraction() -> None:
    print("\n🔍  Checking OTP extraction…")
    try:
        from userbot import extract_otp
        cases = [
            ("Login code: 12345",           "12345"),
            ("Your code is 98765",          "98765"),
            ("OTP: 123456",                 "123456"),
            ("12345",                       "12345"),
            ("Transaction ID: 98765",       None),
            ("Order #12345 confirmed",      None),
        ]
        all_ok = True
        for text, expected in cases:
            result = extract_otp(text)
            if result != expected:
                fail(f"OTP extract: {text!r} → {result!r} (expected {expected!r})")
                all_ok = False
        if all_ok:
            ok("All OTP extraction cases passed")
    except Exception as exc:
        fail(f"OTP extraction test error: {exc}")


# ═══════════════════════════════════════════════════════
#  8. Country prefix matching
# ═══════════════════════════════════════════════════════

def test_country_matching() -> None:
    print("\n🔍  Checking country prefix matching…")
    try:
        from userbot import get_country_from_phone
        cases = [
            ("+91234567890",   "India"),
            ("+1234567890",    "USA / Canada"),
            ("+77123456789",   "Kazakhstan"),
            ("+7123456789",    "Russia"),
            ("+8801234567890", "Bangladesh"),
            ("+447911123456",  "UK"),
        ]
        all_ok = True
        for phone, expected in cases:
            country, _ = get_country_from_phone(phone)
            if country != expected:
                fail(f"Country match: {phone} → {country!r} (expected {expected!r})")
                all_ok = False
        if all_ok:
            ok("All country prefix cases matched correctly")
    except Exception as exc:
        fail(f"Country matching test error: {exc}")


# ═══════════════════════════════════════════════════════
#  9. Atomic stored procedures exist in DB
# ═══════════════════════════════════════════════════════

async def test_stored_procedures() -> None:
    print("\n🔍  Checking atomic stored procedures in DB…")
    procs = ["deduct_balance", "add_balance", "reserve_stock_atomic",
             "use_promo_code_atomic", "cleanup_expired_reservations"]
    try:
        from db import _run
        from db import supabase
        for proc in procs:
            try:
                # Check procedure exists via information_schema
                result = await _run(
                    lambda p=proc: supabase.table("information_schema.routines")
                        .select("routine_name")
                        .eq("routine_name", p)
                        .execute()
                )
                if result.data:
                    ok(f"Procedure exists: {proc}()")
                else:
                    # Try calling it — if it exists, even with bad params it'll respond
                    warn(f"Procedure not found in schema check: {proc}() — run init_database.sql")
            except Exception:
                warn(f"Could not verify {proc}() — run init_database.sql if not done")
    except Exception as exc:
        warn(f"Stored procedure check skipped: {exc}")


# ═══════════════════════════════════════════════════════
#  Runner
# ═══════════════════════════════════════════════════════

async def main() -> None:
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  TGFLOW — Setup Verification                                ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    # Sync tests
    test_imports()
    test_config()
    test_encryption()
    test_qr()
    test_pyrogram_config()
    test_otp_extraction()
    test_country_matching()

    # Async tests
    await test_database()
    await test_stored_procedures()

    # Summary
    print("\n" + "═" * 64)
    if _errors:
        print(f"❌  FAILED — {len(_errors)} error(s), {len(_warnings)} warning(s)\n")
        for e in _errors:
            print(f"   ✗  {e}")
        if _warnings:
            print()
            for w in _warnings:
                print(f"   ⚠  {w}")
        print("\n🔧  Fix the errors above then run this script again.")
        sys.exit(1)
    else:
        print(f"✅  ALL TESTS PASSED  ({len(_warnings)} warning(s))\n")
        if _warnings:
            for w in _warnings:
                print(f"   ⚠  {w}")
            print()
        print("🚀  Ready to run:")
        print("   Shop bot  : python shop_bot.py")
        print("   Admin bot : python admin_bot.py")
        print("   Userbot   : python userbot.py schedule   (optional)")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
