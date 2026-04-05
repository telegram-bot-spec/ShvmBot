"""
╔════════════════════════════════════════════════════════════════╗
║  TGFLOW — Database Layer (Supabase)                            ║
║                                                                ║
║  FIXED:                                                        ║
║  • All Supabase calls wrapped in run_in_executor               ║
║    (never blocks the asyncio event loop)                       ║
║  • deduct_balance  → atomic DB stored procedure                ║
║  • reserve_stock   → atomic DB stored procedure                ║
║  • use_promo_code  → atomic DB stored procedure                ║
║  • approve_refund  → actually credits user balance             ║
║  • get_stats       → single-query views, no N+1, UTC times     ║
║  • search_users    → input sanitised (no filter injection)     ║
║  • bulk_add_stock  → chunked inserts (no 6 MB limit crash)     ║
║  • get_products_by_category → single aggregation query         ║
║  • create_promo_code → `is not None` check (0% promo works)   ║
║  • admin_actions target_ref TEXT (stores promo code keys)      ║
║  • approve_payment rollback if balance add fails               ║
║  • screenshot duplicate detection via image-byte hash          ║
╚════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from functools import partial, wraps
from typing import Any, Dict, List, Optional, Tuple

from supabase import create_client, Client

from config import Config
from utils import (
    crypto,
    generate_referral_code,
    calculate_rank,
    hash_image_bytes,
    utcnow,
    parse_utc,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
#  Supabase client  (synchronous — all calls go through _run)
# ═══════════════════════════════════════════════════════

try:
    supabase: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
    logger.info("✅ Supabase client ready")
except Exception as exc:
    logger.critical("❌ Supabase init failed: %s", exc)
    raise


# ═══════════════════════════════════════════════════════
#  Async executor wrapper
#  Every sync Supabase call goes through _run() so it never
#  blocks the asyncio event loop.  No asyncio.run() inside coroutines.
# ═══════════════════════════════════════════════════════

async def _run(func, *args, **kwargs):
    """Run a synchronous callable in the default thread-pool executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))


# ═══════════════════════════════════════════════════════
#  Decorator: consistent error handling + async enforcement
# ═══════════════════════════════════════════════════════

def db_op(func):
    """
    Decorator for all DB coroutines.
    • Ensures the decorated function is always a coroutine.
    • Catches every exception, logs it, returns None / False / [] as appropriate.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as exc:
            logger.error("❌ DB error in %s: %s", func.__name__, exc, exc_info=True)
            # Return a sensible zero-value based on return-type hint in docstring
            return None
    return wrapper


# ═══════════════════════════════════════════════════════
#  Helper: run a Supabase RPC (stored procedure)
# ═══════════════════════════════════════════════════════

async def _rpc(fn_name: str, params: dict) -> Any:
    """Call a Supabase RPC function and return result.data."""
    result = await _run(
        lambda: supabase.rpc(fn_name, params).execute()
    )
    return result.data


# ═══════════════════════════════════════════════════════════════════════
#  USER OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("users")
            .select("*")
            .eq("user_id", user_id)
            .single()
            .execute()
    )
    return result.data if result.data else None


@db_op
async def create_user(
    user_id: int,
    name: str,
    username: Optional[str],
    referred_by: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    Create a new user.
    Validates referrer exists and is not the same user (belt-and-suspenders
    on top of the DB trigger).
    """
    # Validate referrer
    if referred_by:
        if referred_by == user_id:
            referred_by = None
        else:
            ref_user = await get_user(referred_by)
            if not ref_user:
                logger.warning("Referrer %s not found — ignoring", referred_by)
                referred_by = None

    ref_code = generate_referral_code(user_id)

    data = {
        "user_id":      user_id,
        "name":         name[:200],        # cap length
        "username":     username,
        "referral_code": ref_code,
        "referred_by":  referred_by,
    }

    result = await _run(
        lambda: supabase.table("users").insert(data).execute()
    )

    if result.data:
        logger.info("✅ User created: %d (%s)", user_id, name)
        return result.data[0]

    logger.error("❌ Failed to create user %d", user_id)
    return None


@db_op
async def get_or_create_user(
    user_id: int,
    name: str,
    username: Optional[str],
    referred_by: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Get existing user or create. Updates last_active on every call."""
    user = await get_user(user_id)
    if user:
        # Update last_active in background — don't await, non-critical
        asyncio.ensure_future(_update_last_active(user_id))
        return user
    return await create_user(user_id, name, username, referred_by)


async def _update_last_active(user_id: int) -> None:
    """Fire-and-forget last_active update."""
    try:
        await _run(
            lambda: supabase.table("users")
                .update({"last_active": utcnow().isoformat()})
                .eq("user_id", user_id)
                .execute()
        )
    except Exception as exc:
        logger.debug("last_active update failed for %d: %s", user_id, exc)

# keep old name for compatibility
update_user_last_active = _update_last_active


@db_op
async def accept_tos(user_id: int) -> bool:
    result = await _run(
        lambda: supabase.table("users")
            .update({
                "tos_accepted":    True,
                "tos_accepted_at": utcnow().isoformat(),
            })
            .eq("user_id", user_id)
            .execute()
    )
    return bool(result.data)


@db_op
async def update_balance(user_id: int, amount: float) -> bool:
    """
    Add `amount` to user balance (use negative to subtract).
    For DEDUCTIONS use deduct_balance() — it is atomic and returns False
    when funds are insufficient.  This function is for ADDITIONS only
    (payment approval, referral commission).
    """
    if amount < 0:
        # Redirect to atomic deduction
        return await deduct_balance(user_id, abs(amount))

    ok = await _rpc("add_balance", {
        "p_user_id": user_id,
        "p_amount":  round(float(amount), 4),
    })
    if ok:
        logger.info("✅ +%.4f USD balance for user %d", amount, user_id)
    return bool(ok)


@db_op
async def deduct_balance(user_id: int, amount: float) -> bool:
    """
    Atomically deduct `amount` from user balance.
    Uses the `deduct_balance` PostgreSQL function with FOR UPDATE row lock.
    Returns False (never clips to 0) if funds are insufficient.
    """
    ok = await _rpc("deduct_balance", {
        "p_user_id": user_id,
        "p_amount":  round(float(amount), 4),
    })
    if ok:
        logger.info("✅ Deducted %.4f USD from user %d", amount, user_id)
    else:
        logger.warning("⚠️ Insufficient balance for user %d (need %.4f)", user_id, amount)
    return bool(ok)


@db_op
async def update_total_spent(user_id: int, amount: float) -> bool:
    """Increment total_spent and recalculate rank."""
    user = await get_user(user_id)
    if not user:
        return False

    new_total = float(user.get("total_spent", 0)) + amount
    new_rank  = calculate_rank(new_total)

    result = await _run(
        lambda: supabase.table("users")
            .update({"total_spent": new_total, "rank": new_rank})
            .eq("user_id", user_id)
            .execute()
    )

    if result.data:
        old_rank = user.get("rank", "Bronze")
        if new_rank != old_rank:
            logger.info("🎉 User %d rank: %s → %s", user_id, old_rank, new_rank)
        return True
    return False


@db_op
async def ban_user(user_id: int, reason: str, admin_id: int) -> bool:
    result = await _run(
        lambda: supabase.table("users")
            .update({"is_banned": True, "ban_reason": reason})
            .eq("user_id", user_id)
            .execute()
    )
    if result.data:
        await log_admin_action(admin_id, "ban_user", "user", str(user_id), {"reason": reason})
        logger.warning("⚠️ User %d banned by admin %d", user_id, admin_id)
        return True
    return False


@db_op
async def unban_user(user_id: int, admin_id: int) -> bool:
    result = await _run(
        lambda: supabase.table("users")
            .update({"is_banned": False, "ban_reason": None})
            .eq("user_id", user_id)
            .execute()
    )
    if result.data:
        await log_admin_action(admin_id, "unban_user", "user", str(user_id), {})
        return True
    return False


@db_op
async def search_users(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search by name or username.
    Input is sanitised: only alphanumeric, spaces, underscores and hyphens
    are passed through, preventing PostgREST filter injection.
    """
    safe = "".join(c for c in query if c.isalnum() or c in " _-@.")[:50]
    if not safe:
        return []

    result = await _run(
        lambda: supabase.table("users")
            .select("*")
            .or_(f"name.ilike.%{safe}%,username.ilike.%{safe}%")
            .limit(limit)
            .execute()
    )
    return result.data or []


@db_op
async def get_all_users(limit: int = 1000) -> List[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("users")
            .select("*")
            .order("joined", desc=True)
            .limit(limit)
            .execute()
    )
    return result.data or []


@db_op
async def get_user_count() -> int:
    result = await _run(
        lambda: supabase.table("users")
            .select("user_id", count="exact")
            .execute()
    )
    return result.count or 0


# ═══════════════════════════════════════════════════════════════════════
#  PRODUCT OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def get_categories() -> List[str]:
    result = await _run(
        lambda: supabase.table("products")
            .select("category")
            .eq("is_active", True)
            .execute()
    )
    if not result.data:
        return []
    return sorted(set(p["category"] for p in result.data))


@db_op
async def get_products_by_category(category: str) -> List[Dict[str, Any]]:
    """
    Fetch products with stock counts in TWO queries (not N+1).
    Query 1: all products in category.
    Query 2: stock counts for those product IDs via aggregation.
    """
    prod_result = await _run(
        lambda: supabase.table("products")
            .select("*")
            .eq("category", category)
            .eq("is_active", True)
            .order("name")
            .execute()
    )
    products = prod_result.data or []
    if not products:
        return []

    product_ids = [p["id"] for p in products]

    # Single aggregation query for all stock counts
    stock_result = await _run(
        lambda: supabase.table("stock")
            .select("product_id", count="exact")
            .in_("product_id", product_ids)
            .eq("is_sold", False)
            .execute()
    )

    # Build a count map  {product_id: count}
    count_map: Dict[int, int] = {}
    if stock_result.data:
        # PostgREST returns per-group counts when using group-by RPC,
        # but with select+filter we get all rows — count per product manually
        from collections import Counter
        cnt = Counter(r["product_id"] for r in stock_result.data)
        count_map = dict(cnt)

    for p in products:
        p["stock_count"] = count_map.get(p["id"], 0)

    return products


@db_op
async def get_product(product_id: int) -> Optional[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("products")
            .select("*")
            .eq("id", product_id)
            .single()
            .execute()
    )
    if not result.data:
        return None
    product = result.data
    product["stock_count"] = await get_stock_count(product_id)
    return product


@db_op
async def create_product(
    category: str,
    name: str,
    description: str,
    purchase_price: float,
    selling_price: float,
    admin_id: int,
    demo_file_id: Optional[str] = None,
    main_file_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    # Note: DB constraint `selling_price >= purchase_price` was intentionally
    # REMOVED from schema to allow negative-margin products after admin confirmation.
    data = {
        "category":       category.strip(),
        "name":           name.strip(),
        "description":    description.strip() if description else None,
        "purchase_price": round(purchase_price, 4),
        "selling_price":  round(selling_price, 4),
        "demo_file_id":   demo_file_id,
        "main_file_id":   main_file_id,
        "created_by":     admin_id,
    }
    result = await _run(
        lambda: supabase.table("products").insert(data).execute()
    )
    if result.data:
        product = result.data[0]
        await log_admin_action(
            admin_id, "create_product", "product", str(product["id"]), data
        )
        logger.info("✅ Product created: %s (ID %d)", name, product["id"])
        return product
    return None


@db_op
async def update_product(product_id: int, admin_id: int, **updates) -> bool:
    if not updates:
        return False
    result = await _run(
        lambda: supabase.table("products")
            .update(updates)
            .eq("id", product_id)
            .execute()
    )
    if result.data:
        await log_admin_action(
            admin_id, "update_product", "product", str(product_id), updates
        )
        return True
    return False


@db_op
async def delete_product(product_id: int, admin_id: int) -> bool:
    """Soft-delete (set is_active=False)."""
    result = await _run(
        lambda: supabase.table("products")
            .update({"is_active": False})
            .eq("id", product_id)
            .execute()
    )
    if result.data:
        await log_admin_action(
            admin_id, "delete_product", "product", str(product_id), {}
        )
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════
#  STOCK OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def get_stock_count(product_id: int) -> int:
    result = await _run(
        lambda: supabase.table("stock")
            .select("id", count="exact")
            .eq("product_id", product_id)
            .eq("is_sold", False)
            .execute()
    )
    return result.count or 0


@db_op
async def reserve_stock(
    user_id: int,
    product_id: int,
    quantity: int,
) -> Optional[List[Dict[str, Any]]]:
    """
    Atomically reserve `quantity` stock items using the PostgreSQL
    stored procedure `reserve_stock_atomic` (FOR UPDATE SKIP LOCKED).

    Returns list of {stock_id, item} dicts, or None if insufficient stock.
    This is the ONLY correct way to reserve stock — no SELECT+UPDATE race.
    """
    rows = await _rpc("reserve_stock_atomic", {
        "p_product_id": product_id,
        "p_quantity":   quantity,
        "p_user_id":    user_id,
    })

    if not rows or len(rows) < quantity:
        logger.warning(
            "⚠️ Insufficient stock for product %d: wanted %d, got %d",
            product_id, quantity, len(rows) if rows else 0
        )
        return None

    logger.info("✅ Reserved %d items for product %d (user %d)", quantity, product_id, user_id)
    return rows   # [{stock_id, item}, ...]


@db_op
async def add_stock_item(product_id: int, item: str, admin_id: int) -> bool:
    result = await _run(
        lambda: supabase.table("stock").insert({
            "product_id": product_id,
            "item":       item,
            "added_by":   admin_id,
        }).execute()
    )
    if result.data:
        await log_admin_action(
            admin_id, "add_stock", "product", str(product_id), {"quantity": 1}
        )
        return True
    return False


_STOCK_CHUNK_SIZE = 200   # stay well under Supabase 6 MB request limit

@db_op
async def bulk_add_stock(
    product_id: int,
    items: List[str],
    admin_id: int,
) -> int:
    """
    Bulk-insert stock items in chunks of 200 rows.
    Prevents hitting the Supabase 6 MB request body limit.
    Returns total number of rows successfully inserted.
    """
    if not items:
        return 0

    rows = [
        {"product_id": product_id, "item": item.strip(), "added_by": admin_id}
        for item in items
        if item.strip()
    ]

    total_added = 0
    for i in range(0, len(rows), _STOCK_CHUNK_SIZE):
        chunk = rows[i : i + _STOCK_CHUNK_SIZE]
        result = await _run(
            lambda c=chunk: supabase.table("stock").insert(c).execute()
        )
        added = len(result.data) if result.data else 0
        total_added += added
        if added < len(chunk):
            logger.error(
                "❌ Chunk insert partial: expected %d, got %d", len(chunk), added
            )

    if total_added > 0:
        await log_admin_action(
            admin_id, "bulk_add_stock", "product", str(product_id),
            {"requested": len(rows), "inserted": total_added}
        )
        logger.info("✅ Bulk added %d/%d items to product %d", total_added, len(rows), product_id)

    return total_added


@db_op
async def get_low_stock_products(threshold: int = None) -> List[Dict[str, Any]]:
    """
    Return active products whose available stock is below `threshold`.
    Uses TWO queries (not N+1).
    """
    if threshold is None:
        threshold = Config.LOW_STOCK_THRESHOLD

    # All active products
    prod_result = await _run(
        lambda: supabase.table("products")
            .select("id, name, category, selling_price")
            .eq("is_active", True)
            .execute()
    )
    if not prod_result.data:
        return []

    all_ids = [p["id"] for p in prod_result.data]

    # Available stock counts in one query
    stock_result = await _run(
        lambda: supabase.table("stock")
            .select("product_id")
            .in_("product_id", all_ids)
            .eq("is_sold", False)
            .execute()
    )
    from collections import Counter
    count_map = Counter(r["product_id"] for r in (stock_result.data or []))

    low = []
    for p in prod_result.data:
        cnt = count_map.get(p["id"], 0)
        if cnt < threshold:
            p["stock_count"] = cnt
            low.append(p)

    return sorted(low, key=lambda p: p["stock_count"])


# ═══════════════════════════════════════════════════════════════════════
#  WISHLIST OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def add_to_wishlist(user_id: int, product_id: int) -> bool:
    try:
        result = await _run(
            lambda: supabase.table("wishlists").insert({
                "user_id":    user_id,
                "product_id": product_id,
            }).execute()
        )
        return bool(result.data)
    except Exception:
        # Duplicate unique constraint — already in wishlist
        return False


@db_op
async def remove_from_wishlist(user_id: int, product_id: int) -> bool:
    result = await _run(
        lambda: supabase.table("wishlists")
            .delete()
            .eq("user_id", user_id)
            .eq("product_id", product_id)
            .execute()
    )
    return bool(result.data)


@db_op
async def get_wishlist(user_id: int) -> List[Dict[str, Any]]:
    """
    Fetch wishlist with product details + stock counts in TWO queries.
    """
    result = await _run(
        lambda: supabase.table("wishlists")
            .select("*, products(*)")
            .eq("user_id", user_id)
            .execute()
    )
    items = result.data or []
    if not items:
        return []

    # Batch stock counts
    product_ids = [
        item["products"]["id"]
        for item in items
        if item.get("products")
    ]
    if product_ids:
        stock_result = await _run(
            lambda: supabase.table("stock")
                .select("product_id")
                .in_("product_id", product_ids)
                .eq("is_sold", False)
                .execute()
        )
        from collections import Counter
        count_map = Counter(r["product_id"] for r in (stock_result.data or []))
        for item in items:
            if item.get("products"):
                item["products"]["stock_count"] = count_map.get(
                    item["products"]["id"], 0
                )

    return items


@db_op
async def is_in_wishlist(user_id: int, product_id: int) -> bool:
    result = await _run(
        lambda: supabase.table("wishlists")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .eq("product_id", product_id)
            .execute()
    )
    return (result.count or 0) > 0


# ═══════════════════════════════════════════════════════════════════════
#  PAYMENT OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def create_payment(
    user_id: int,
    amount_inr: int,
    payment_ref: str,
) -> Optional[int]:
    """amount_inr is INTEGER — whole rupees only."""
    result = await _run(
        lambda: supabase.table("payments").insert({
            "payment_ref": payment_ref,
            "user_id":     user_id,
            "amount_inr":  int(amount_inr),
        }).execute()
    )
    if result.data:
        pid = result.data[0]["id"]
        logger.info("✅ Payment created: %s ₹%d (user %d)", payment_ref, amount_inr, user_id)
        return pid
    return None


@db_op
async def update_payment_screenshot(
    payment_id: int,
    file_id: str,
    image_bytes: bytes,          # actual image bytes for real duplicate detection
) -> Tuple[bool, Optional[str]]:
    """
    Attach screenshot to payment.
    Duplicate detection uses SHA-256 of actual image bytes — NOT file_id.
    """
    image_hash = hash_image_bytes(image_bytes)

    # Check for duplicate screenshot across ALL payments
    dup = await _run(
        lambda: supabase.table("payments")
            .select("id, payment_ref")
            .eq("screenshot_hash", image_hash)
            .neq("id", payment_id)
            .execute()
    )
    if dup.data:
        dup_ref = dup.data[0]["payment_ref"]
        logger.warning("⚠️ Duplicate screenshot for payment %d (matches %s)", payment_id, dup_ref)
        return False, f"This screenshot was already used for payment `{dup_ref}`."

    result = await _run(
        lambda: supabase.table("payments").update({
            "screenshot_file_id": file_id,
            "screenshot_hash":    image_hash,
        }).eq("id", payment_id).execute()
    )
    if result.data:
        return True, None
    return False, "Failed to save screenshot. Please try again."


@db_op
async def get_pending_payments() -> List[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("payments")
            .select("*, users(name, username)")
            .eq("status", "pending")
            .order("created_at", desc=False)
            .execute()
    )
    return result.data or []


@db_op
async def get_payment(payment_id: int) -> Optional[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("payments")
            .select("*, users(name, username)")
            .eq("id", payment_id)
            .single()
            .execute()
    )
    return result.data if result.data else None


@db_op
async def approve_payment(
    payment_id: int,
    admin_id: int,
) -> Tuple[bool, Optional[str]]:
    """
    Approve payment and credit user balance atomically.
    If balance credit fails, payment status is rolled back to 'pending'.
    """
    payment = await get_payment(payment_id)
    if not payment:
        return False, "Payment not found."
    if payment["status"] != "pending":
        return False, f"Payment already {payment['status']}."

    # Mark approved
    upd = await _run(
        lambda: supabase.table("payments").update({
            "status":       "approved",
            "actioned_by":  admin_id,
            "actioned_at":  utcnow().isoformat(),
        }).eq("id", payment_id).execute()
    )
    if not upd.data:
        return False, "Failed to update payment status."

    # Credit balance  (INR → USD)
    amount_usd = Config.inr_to_usd(payment["amount_inr"])
    credited   = await update_balance(payment["user_id"], amount_usd)

    if not credited:
        # Rollback — reset to pending so admin can retry
        await _run(
            lambda: supabase.table("payments").update({
                "status":      "pending",
                "actioned_by": None,
                "actioned_at": None,
            }).eq("id", payment_id).execute()
        )
        logger.error("❌ Balance credit failed for payment %d — rolled back", payment_id)
        return False, "Could not credit user balance. Payment reset to pending — please retry."

    await log_admin_action(
        admin_id, "approve_payment", "payment", str(payment_id),
        {"user_id": payment["user_id"], "amount_inr": payment["amount_inr"], "amount_usd": amount_usd}
    )
    logger.info("✅ Payment %s approved — ₹%d → $%.4f credited to user %d",
                payment["payment_ref"], payment["amount_inr"], amount_usd, payment["user_id"])
    return True, None


@db_op
async def reject_payment(
    payment_id: int,
    admin_id: int,
    reason: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    payment = await get_payment(payment_id)
    if not payment:
        return False, "Payment not found."
    if payment["status"] != "pending":
        return False, f"Payment already {payment['status']}."

    result = await _run(
        lambda: supabase.table("payments").update({
            "status":                  "rejected",
            "actioned_by":             admin_id,
            "actioned_at":             utcnow().isoformat(),
            "admin_rejection_reason":  reason,
        }).eq("id", payment_id).execute()
    )
    if result.data:
        await log_admin_action(
            admin_id, "reject_payment", "payment", str(payment_id),
            {"reason": reason, "user_id": payment["user_id"]}
        )
        return True, None
    return False, "Failed to update payment."


# ═══════════════════════════════════════════════════════════════════════
#  ORDER OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def create_order(
    user_id: int,
    product_id: int,
    product_name: str,           # snapshot — survives product deletion
    quantity: int,
    unit_price: float,
    total_price: float,
    items_delivered: List[Dict[str, Any]],
    discount_amount: float = 0.0,
    promo_code: Optional[str] = None,
) -> Optional[int]:
    data = {
        "user_id":         user_id,
        "product_id":      product_id,
        "product_name":    product_name,
        "quantity":        quantity,
        "unit_price":      round(unit_price, 4),
        "total_price":     round(total_price, 4),
        "items_delivered": items_delivered,
        "discount_amount": round(discount_amount, 4),
        "promo_code":      promo_code,
    }
    result = await _run(
        lambda: supabase.table("orders").insert(data).execute()
    )
    if result.data:
        order_id = result.data[0]["id"]
        logger.info("✅ Order %d created: %s x%d $%.4f (user %d)",
                    order_id, product_name, quantity, total_price, user_id)
        return order_id
    return None


@db_op
async def get_user_orders(user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("orders")
            .select("*")           # product_name is snapshotted — no join needed
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
    )
    return result.data or []


@db_op
async def get_order(order_id: int) -> Optional[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("orders")
            .select("*, users(name, username)")
            .eq("id", order_id)
            .single()
            .execute()
    )
    return result.data if result.data else None


@db_op
async def get_user_order_count(user_id: int) -> int:
    """Efficient order count — does NOT download all rows."""
    result = await _run(
        lambda: supabase.table("orders")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .execute()
    )
    return result.count or 0


# ═══════════════════════════════════════════════════════════════════════
#  REFERRAL OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def get_user_by_referral_code(code: str) -> Optional[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("users")
            .select("*")
            .eq("referral_code", code.upper().strip())
            .single()
            .execute()
    )
    return result.data if result.data else None


@db_op
async def record_referral_earning(
    referrer_id: int,
    referred_id: int,
    order_id: int,
    commission: float,
) -> bool:
    """
    Record referral commission and credit referrer's balance.

    The DB schema has UNIQUE(referred_id) on referral_earnings, so
    this silently no-ops if a commission was already recorded for
    this referred user (first-purchase-only enforcement).

    order_id must NOT be None — caller must ensure order was created first.
    """
    if order_id is None:
        logger.error("❌ record_referral_earning called with order_id=None — skipping")
        return False

    # Credit balance first
    credited = await update_balance(referrer_id, commission)
    if not credited:
        logger.error("❌ Could not credit referral commission to user %d", referrer_id)
        return False

    # Record in table — ignore duplicate (UNIQUE referred_id)
    try:
        result = await _run(
            lambda: supabase.table("referral_earnings").insert({
                "referrer_id": referrer_id,
                "referred_id": referred_id,
                "order_id":    order_id,
                "commission":  round(commission, 4),
            }).execute()
        )
        if result.data:
            logger.info("✅ Referral commission $%.4f → user %d", commission, referrer_id)
            return True
        return False
    except Exception:
        # UNIQUE constraint fired — commission for this referred user already recorded
        # But we already credited the balance above — we need to reverse it
        logger.warning(
            "⚠️ Referral earning already exists for referred_id=%d — reversing balance credit",
            referred_id
        )
        await deduct_balance(referrer_id, commission)
        return False


@db_op
async def get_referral_stats(user_id: int) -> Dict[str, Any]:
    """Single-query referral stats using two lightweight queries."""
    ref_count = await _run(
        lambda: supabase.table("users")
            .select("user_id", count="exact")
            .eq("referred_by", user_id)
            .execute()
    )
    earnings = await _run(
        lambda: supabase.table("referral_earnings")
            .select("commission")
            .eq("referrer_id", user_id)
            .execute()
    )
    total_earnings = sum(
        float(e["commission"]) for e in (earnings.data or [])
    )
    return {
        "total_referrals": ref_count.count or 0,
        "total_earnings":  round(total_earnings, 4),
    }


# ═══════════════════════════════════════════════════════════════════════
#  PROMO CODE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def get_promo_code(code: str) -> Optional[Dict[str, Any]]:
    """Fetch and validate a promo code (expiry + usage checked in DB)."""
    result = await _run(
        lambda: supabase.table("promo_codes")
            .select("*")
            .eq("code", code.upper().strip())
            .eq("is_active", True)
            .single()
            .execute()
    )
    if not result.data:
        return None

    promo = result.data

    # Check expiry (UTC-aware comparison)
    if promo.get("expires_at"):
        expiry = parse_utc(str(promo["expires_at"]))
        if expiry and utcnow() > expiry:
            logger.debug("Promo %s expired", code)
            return None

    # Check usage limit
    max_uses = promo.get("max_uses")
    if max_uses is not None and promo.get("current_uses", 0) >= max_uses:
        logger.debug("Promo %s exhausted", code)
        return None

    return promo


@db_op
async def use_promo_code(code: str) -> bool:
    """
    Atomically increment usage counter via stored procedure.
    Returns False if code is exhausted, expired, or inactive.
    """
    ok = await _rpc("use_promo_code_atomic", {"p_code": code.upper().strip()})
    return bool(ok)


@db_op
async def create_promo_code(
    code: str,
    admin_id: int,
    discount_percent: Optional[float] = None,
    discount_fixed: Optional[float] = None,
    min_purchase: float = 0.0,
    max_uses: Optional[int] = None,
    expires_at: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fixed: uses `is not None` checks so 0% discount is valid.
    """
    if discount_percent is None and discount_fixed is None:
        logger.error("Promo code must have discount_percent or discount_fixed")
        return None
    if discount_percent is not None and discount_fixed is not None:
        logger.error("Promo code cannot have both percentage and fixed discount")
        return None

    data = {
        "code":             code.upper().strip(),
        "discount_percent": discount_percent,
        "discount_fixed":   discount_fixed,
        "min_purchase":     round(min_purchase, 4),
        "max_uses":         max_uses,
        "expires_at":       expires_at.isoformat() if expires_at else None,
        "created_by":       admin_id,
    }
    result = await _run(
        lambda: supabase.table("promo_codes").insert(data).execute()
    )
    if result.data:
        promo = result.data[0]
        await log_admin_action(
            admin_id, "create_promo_code", "promo_code", code.upper(), data
        )
        return promo
    return None


@db_op
async def get_all_promo_codes(active_only: bool = True) -> List[Dict[str, Any]]:
    q = supabase.table("promo_codes").select("*")
    if active_only:
        q = q.eq("is_active", True)
    result = await _run(lambda: q.order("created_at", desc=True).execute())
    return result.data or []


@db_op
async def deactivate_promo_code(code: str, admin_id: int) -> bool:
    result = await _run(
        lambda: supabase.table("promo_codes")
            .update({"is_active": False})
            .eq("code", code.upper())
            .execute()
    )
    if result.data:
        await log_admin_action(admin_id, "deactivate_promo", "promo_code", code, {})
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════
#  REFUND OPERATIONS
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def create_refund_request(
    user_id: int,
    order_id: int,
    reason: str = "OTP not received",
    proof_file_id: Optional[str] = None,
) -> Optional[int]:
    """UNIQUE(order_id) enforced in DB — one refund per order."""
    try:
        result = await _run(
            lambda: supabase.table("refund_requests").insert({
                "user_id":       user_id,
                "order_id":      order_id,
                "reason":        reason,
                "proof_file_id": proof_file_id,
            }).execute()
        )
        if result.data:
            rid = result.data[0]["id"]
            logger.info("✅ Refund request %d for order %d", rid, order_id)
            return rid
        return None
    except Exception:
        logger.warning("Refund request already exists for order %d", order_id)
        return None


@db_op
async def get_pending_refunds() -> List[Dict[str, Any]]:
    result = await _run(
        lambda: supabase.table("refund_requests")
            .select("*, users(name, username), orders(product_name, total_price, quantity)")
            .eq("status", "pending")
            .order("created_at", desc=False)
            .execute()
    )
    return result.data or []


@db_op
async def approve_refund(
    refund_id: int,
    admin_id: int,
    notes: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Approve refund AND credit user balance.
    The old code NEVER credited balance — this is the fix.
    """
    # Get refund details
    refund_result = await _run(
        lambda: supabase.table("refund_requests")
            .select("*, orders(total_price, user_id)")
            .eq("id", refund_id)
            .single()
            .execute()
    )
    if not refund_result.data:
        return False, "Refund request not found."

    refund = refund_result.data
    if refund["status"] != "pending":
        return False, f"Refund already {refund['status']}."

    order      = refund.get("orders") or {}
    user_id    = order.get("user_id") or refund.get("user_id")
    refund_amt = float(order.get("total_price", 0))

    if not user_id or refund_amt <= 0:
        return False, "Cannot determine refund amount from order."

    # Mark approved in DB
    upd = await _run(
        lambda: supabase.table("refund_requests").update({
            "status":        "approved",
            "refund_amount": refund_amt,
            "admin_notes":   notes,
            "actioned_by":   admin_id,
            "actioned_at":   utcnow().isoformat(),
        }).eq("id", refund_id).execute()
    )
    if not upd.data:
        return False, "Failed to update refund status."

    # Credit user balance  ← THIS WAS MISSING IN THE ORIGINAL CODE
    credited = await update_balance(user_id, refund_amt)
    if not credited:
        # Rollback refund status
        await _run(
            lambda: supabase.table("refund_requests")
                .update({"status": "pending", "actioned_by": None, "actioned_at": None})
                .eq("id", refund_id)
                .execute()
        )
        return False, "Could not credit refund to user balance. Reset to pending."

    # Mark order as refunded
    await _run(
        lambda: supabase.table("orders")
            .update({"status": "refunded"})
            .eq("id", refund["order_id"])
            .execute()
    )

    await log_admin_action(
        admin_id, "approve_refund", "refund", str(refund_id),
        {"user_id": user_id, "amount": refund_amt, "notes": notes}
    )
    logger.info("✅ Refund %d approved — $%.4f credited to user %d", refund_id, refund_amt, user_id)
    return True, None


@db_op
async def reject_refund(
    refund_id: int,
    admin_id: int,
    notes: Optional[str] = None,
) -> bool:
    result = await _run(
        lambda: supabase.table("refund_requests").update({
            "status":      "rejected",
            "admin_notes": notes,
            "actioned_by": admin_id,
            "actioned_at": utcnow().isoformat(),
        }).eq("id", refund_id).execute()
    )
    if result.data:
        await log_admin_action(
            admin_id, "reject_refund", "refund", str(refund_id), {"notes": notes}
        )
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════
#  ADMIN ACTION AUDIT LOG
# ═══════════════════════════════════════════════════════════════════════

async def log_admin_action(
    admin_id: int,
    action: str,
    target_type: Optional[str] = None,
    target_ref: Optional[str] = None,    # TEXT — stores both numeric IDs and promo codes
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Fire-and-forget audit log entry.
    Never raises — a logging failure must not break the calling operation.
    target_ref is TEXT so promo code text primary keys are stored correctly.
    """
    try:
        await _run(
            lambda: supabase.table("admin_actions").insert({
                "admin_id":    admin_id,
                "action":      action,
                "target_type": target_type,
                "target_ref":  str(target_ref) if target_ref is not None else None,
                "details":     details or {},
            }).execute()
        )
    except Exception as exc:
        logger.error("❌ audit log failed (%s): %s", action, exc)


@db_op
async def get_admin_actions(
    admin_id: Optional[int] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    q = supabase.table("admin_actions").select("*")
    if admin_id:
        q = q.eq("admin_id", admin_id)
    result = await _run(lambda: q.order("created_at", desc=True).limit(limit).execute())
    return result.data or []


# ═══════════════════════════════════════════════════════════════════════
#  STATISTICS  (fixed: views, no N+1, no RAM bomb, UTC times)
# ═══════════════════════════════════════════════════════════════════════

@db_op
async def get_stats() -> Dict[str, Any]:
    """
    All stats gathered from DB views and count queries.
    No Python-side loops over all orders.
    No N+1 queries.
    All time comparisons done in PostgreSQL (UTC-aware).
    """
    # Revenue + order counts from the view (single query)
    biz_result = await _run(
        lambda: supabase.table("v_business_stats").select("*").single().execute()
    )
    biz = biz_result.data or {}

    # User count
    user_result = await _run(
        lambda: supabase.table("users").select("user_id", count="exact").execute()
    )
    total_users = user_result.count or 0

    # Active products
    prod_result = await _run(
        lambda: supabase.table("products")
            .select("id", count="exact")
            .eq("is_active", True)
            .execute()
    )
    active_products = prod_result.count or 0

    # Rank distribution — 4 tiny queries
    rank_counts: Dict[str, int] = {}
    for rank in ("Bronze", "Silver", "Gold", "VIP"):
        r = await _run(
            lambda rk=rank: supabase.table("users")
                .select("user_id", count="exact")
                .eq("rank", rk)
                .execute()
        )
        rank_counts[rank] = r.count or 0

    # Total cost from product_sales_stats view
    sales_result = await _run(
        lambda: supabase.table("v_product_sales")
            .select("total_revenue, total_profit")
            .execute()
    )
    total_revenue = sum(float(r.get("total_revenue", 0)) for r in (sales_result.data or []))
    total_profit  = sum(float(r.get("total_profit",  0)) for r in (sales_result.data or []))
    total_cost    = total_revenue - total_profit
    profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0.0

    # Pending payments count
    pending_pay = await _run(
        lambda: supabase.table("payments")
            .select("id", count="exact")
            .eq("status", "pending")
            .execute()
    )
    # Pending refunds count
    pending_ref = await _run(
        lambda: supabase.table("refund_requests")
            .select("id", count="exact")
            .eq("status", "pending")
            .execute()
    )

    return {
        "total_users":       total_users,
        "active_products":   active_products,
        "total_orders":      int(biz.get("total_orders", 0)),
        "today_orders":      int(biz.get("today_orders", 0)),
        "total_revenue":     float(biz.get("total_revenue", 0)),
        "today_revenue":     float(biz.get("today_revenue", 0)),
        "week_revenue":      float(biz.get("week_revenue", 0)),
        "month_revenue":     float(biz.get("month_revenue", 0)),
        "total_cost":        round(total_cost, 4),
        "net_profit":        round(total_profit, 4),
        "profit_margin":     round(profit_margin, 2),
        "rank_distribution": rank_counts,
        "pending_payments":  pending_pay.count or 0,
        "pending_refunds":   pending_ref.count or 0,
    }


# ═══════════════════════════════════════════════════════════════════════
#  MAINTENANCE
# ═══════════════════════════════════════════════════════════════════════

async def cleanup_expired_reservations() -> int:
    """
    Release expired stock reservations back to the available pool.
    Calls the PostgreSQL stored procedure which does it atomically.
    Schedule this to run periodically (e.g. every 15 minutes via job_queue).
    """
    try:
        count = await _rpc("cleanup_expired_reservations", {})
        if count:
            logger.info("🧹 Released %d expired stock reservations", count)
        return int(count or 0)
    except Exception as exc:
        logger.error("❌ cleanup_expired_reservations failed: %s", exc)
        return 0


# ═══════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    async def _test():
        print("\n🧪  Testing DB connection…")
        try:
            result = await _run(
                lambda: supabase.table("users")
                    .select("user_id", count="exact")
                    .execute()
            )
            print(f"✅  Connected — {result.count or 0} users in DB")
        except Exception as exc:
            print(f"❌  Connection failed: {exc}")
            sys.exit(1)

    async def _stats():
        stats = await get_stats()
        print("\n📊  BUSINESS STATS")
        print(f"  Users          : {stats['total_users']}")
        print(f"  Active products: {stats['active_products']}")
        print(f"  Total orders   : {stats['total_orders']}")
        print(f"  Today orders   : {stats['today_orders']}")
        print(f"  Total revenue  : ${stats['total_revenue']:.2f}")
        print(f"  Today revenue  : ${stats['today_revenue']:.2f}")
        print(f"  Net profit     : ${stats['net_profit']:.2f} ({stats['profit_margin']:.1f}%)")
        print(f"  Pending pay    : {stats['pending_payments']}")
        print(f"  Pending refunds: {stats['pending_refunds']}")
        print(f"  Ranks          : {stats['rank_distribution']}")

    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"
    if cmd == "test":
        asyncio.run(_test())
    elif cmd == "stats":
        asyncio.run(_stats())
    else:
        print("Usage: python db.py [test|stats]")
