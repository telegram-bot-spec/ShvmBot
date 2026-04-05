#!/bin/bash

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  🧪 TGFLOW - COMPLETE SYSTEM TEST                             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
    echo -e "${GREEN}✅ Virtual environment activated${NC}"
else
    echo -e "${RED}❌ Virtual environment not found${NC}"
    echo "Run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1️⃣  Testing Configuration..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python config.py
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Configuration valid${NC}"
else
    echo -e "${RED}❌ Configuration failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2️⃣  Testing Database Connection..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python db.py test
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Database connected${NC}"
else
    echo -e "${RED}❌ Database connection failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3️⃣  Testing Encryption..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python -c "
from utils import crypto
test = 'SecretPassword123'
enc = crypto.encrypt(test)
dec = crypto.decrypt(enc)
print(f'Original: {test}')
print(f'Encrypted: {enc[:30]}...')
print(f'Decrypted: {dec}')
assert test == dec, 'Encryption failed!'
print('✅ Encryption working')
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Encryption test passed${NC}"
else
    echo -e "${RED}❌ Encryption test failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4️⃣  Testing Payment QR Generation..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python -c "
from payments import generate_upi_qr
qr = generate_upi_qr(100.0, 'TEST-123')
print(f'QR Size: {len(qr.getvalue())} bytes')
assert len(qr.getvalue()) > 0, 'QR generation failed!'
print('✅ QR code generated')
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Payment QR test passed${NC}"
else
    echo -e "${RED}❌ Payment QR test failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5️⃣  Testing Database Operations..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python -c "
import asyncio
from db import get_categories, get_stats

async def test():
    # Test get categories
    cats = await get_categories()
    print(f'Categories found: {len(cats)}')
    
    # Test stats
    stats = await get_stats()
    print(f'Total users: {stats.get(\"total_users\", 0)}')
    print(f'Total orders: {stats.get(\"total_orders\", 0)}')
    print('✅ Database operations working')

asyncio.run(test())
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Database operations test passed${NC}"
else
    echo -e "${RED}❌ Database operations test failed${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "6️⃣  Checking Bot Files..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

files=("shop_bot.py" "admin_bot.py" "config.py" "db.py" "utils.py" "payments.py" "userbot.py" ".env")
all_exist=true

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo -e "${GREEN}✅ $file${NC}"
    else
        echo -e "${RED}❌ $file (missing)${NC}"
        all_exist=false
    fi
done

if [ "$all_exist" = false ]; then
    echo -e "${RED}❌ Some files are missing${NC}"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "7️⃣  Checking .env Configuration..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

required_vars=("SHOP_BOT_TOKEN" "ADMIN_BOT_TOKEN" "SUPABASE_URL" "SUPABASE_KEY" "ENCRYPTION_KEY" "TG_API_ID" "TG_API_HASH")
all_configured=true

for var in "${required_vars[@]}"; do
    if grep -q "^${var}=.\+" .env 2>/dev/null; then
        echo -e "${GREEN}✅ $var configured${NC}"
    else
        echo -e "${YELLOW}⚠️  $var not configured${NC}"
        all_configured=false
    fi
done

if [ "$all_configured" = false ]; then
    echo -e "${YELLOW}⚠️  Some variables need configuration in .env${NC}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "8️⃣  Testing Bot Imports..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python -c "
print('Testing shop_bot.py imports...')
import shop_bot
print('✅ shop_bot.py imports OK')

print('Testing admin_bot.py imports...')
import admin_bot
print('✅ admin_bot.py imports OK')

print('Testing userbot.py imports...')
import userbot
print('✅ userbot.py imports OK')
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ All bot imports successful${NC}"
else
    echo -e "${RED}❌ Bot import failed${NC}"
    exit 1
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  ✅ ALL TESTS PASSED!                                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "🎯 Your system is ready to run!"
echo ""
echo "📋 Next Steps:"
echo ""
echo "  Start Shop Bot:"
echo "    python shop_bot.py"
echo ""
echo "  Start Admin Bot:"
echo "    python admin_bot.py"
echo ""
echo "  Start Both:"
echo "    ./run.sh"
echo ""
echo "  View Statistics:"
echo "    python db.py stats"
echo ""
echo "🚀 Happy coding!"
echo ""

