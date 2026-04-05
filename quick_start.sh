#!/bin/bash

###############################################################################
#  TGFLOW - Quick Start Script
#  One-command setup for new installations
###############################################################################

set -e  # Exit on error

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  TGFLOW - Quick Start Setup                                    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if running in Termux
if [ -d "/data/data/com.termux" ]; then
    echo "✅ Detected Termux environment"
    PKG_MANAGER="pkg"
else
    echo "✅ Detected standard Linux environment"
    PKG_MANAGER="apt"
fi

# Step 1: Check Python
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1️⃣  Checking Python installation..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 not found${NC}"
    echo "Installing Python..."
    $PKG_MANAGER install -y python
else
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✅ $PYTHON_VERSION found${NC}"
fi

# Step 2: Create virtual environment
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2️⃣  Setting up virtual environment..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -d "venv" ]; then
    echo -e "${YELLOW}⚠️  Virtual environment already exists${NC}"
    read -p "Do you want to recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf venv
        python3 -m venv venv
        echo -e "${GREEN}✅ Virtual environment recreated${NC}"
    fi
else
    python3 -m venv venv
    echo -e "${GREEN}✅ Virtual environment created${NC}"
fi

# Activate virtual environment
source venv/bin/activate

# Step 3: Upgrade pip
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3️⃣  Upgrading pip..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

pip install --upgrade pip
echo -e "${GREEN}✅ Pip upgraded${NC}"

# Step 4: Install dependencies
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4️⃣  Installing dependencies..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

pip install -r requirements.txt
echo -e "${GREEN}✅ All dependencies installed${NC}"

# Step 5: Check .env file
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5️⃣  Checking configuration..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  No .env file found${NC}"
    cp .env.example .env
    echo -e "${GREEN}✅ Created .env from template${NC}"
    echo ""
    echo -e "${YELLOW}⚠️  IMPORTANT: You must edit .env file with your credentials!${NC}"
    echo ""
    echo "Required configurations:"
    echo "  • SHOP_BOT_TOKEN (from @BotFather)"
    echo "  • ADMIN_BOT_TOKEN (from @BotFather)"
    echo "  • SUPABASE_URL (from Supabase dashboard)"
    echo "  • SUPABASE_KEY (service role key)"
    echo "  • ENCRYPTION_KEY (generate with command below)"
    echo ""
    echo "Generate encryption key:"
    echo "  python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    echo ""
    read -p "Press Enter to open .env file with nano..."
    nano .env
else
    echo -e "${GREEN}✅ .env file exists${NC}"
fi

# Step 6: Generate encryption key if missing
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "6️⃣  Checking encryption key..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if ! grep -q "ENCRYPTION_KEY=.\+" .env; then
    echo -e "${YELLOW}⚠️  Generating encryption key...${NC}"
    KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    echo "ENCRYPTION_KEY=$KEY" >> .env
    echo -e "${GREEN}✅ Encryption key generated and added to .env${NC}"
else
    echo -e "${GREEN}✅ Encryption key already configured${NC}"
fi

# Step 7: Test setup
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "7️⃣  Running setup verification..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python test_setup.py

# Step 8: Initialize database
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "8️⃣  Database setup..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo -e "${YELLOW}📋 Database Initialization Steps:${NC}"
echo ""
echo "1. Open your Supabase project dashboard"
echo "2. Go to SQL Editor"
echo "3. Copy the contents of: init_database.sql"
echo "4. Paste and run in SQL Editor"
echo ""
read -p "Press Enter once database is initialized..."

python db.py test

# Final message
echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  ✅ SETUP COMPLETE!                                            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "🎯 What's next?"
echo ""
echo "  Run bots:"
echo "    ./run.sh                 # Start both bots"
echo "    python shop_bot.py       # Customer bot only"
echo "    python admin_bot.py      # Admin bot only (Phase 2)"
echo ""
echo "  Verify:"
echo "    python db.py test        # Test database"
echo "    python db.py stats       # View statistics"
echo "    python test_setup.py     # Run all tests"
echo ""
echo "  Git (push to GitHub):"
echo "    git init"
echo "    git add ."
echo "    git commit -m 'Initial commit'"
echo "    git remote add origin YOUR_REPO_URL"
echo "    git push -u origin main"
echo ""
echo "📖 Documentation: README.md"
echo "🆘 Support: Check logs in shop_bot.log"
echo ""
echo -e "${GREEN}Happy coding! 🚀${NC}"
echo ""

