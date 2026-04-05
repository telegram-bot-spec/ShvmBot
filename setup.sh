#!/bin/bash

###############################################################################
#  TGFLOW - Initial Setup Script
#  Run this once to set up the project
###############################################################################

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  TGFLOW - Project Setup                                        ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed!"
    echo "   Install it with: pkg install python"
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"
echo ""

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate
echo "✅ Virtual environment created"
echo ""

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip
echo "✅ Pip upgraded"
echo ""

# Install requirements
echo "📥 Installing dependencies..."
pip install -r requirements.txt
echo "✅ All dependencies installed"
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  No .env file found!"
    echo "   Copying .env.example to .env..."
    cp .env.example .env
    echo "✅ .env file created"
    echo ""
    echo "🔧 NEXT STEPS:"
    echo "   1. Edit .env file and add your credentials:"
    echo "      nano .env"
    echo ""
    echo "   2. Initialize database schema:"
    echo "      python db.py init"
    echo "      (Then run the SQL in your Supabase dashboard)"
    echo ""
    echo "   3. Generate encryption key:"
    echo "      python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    echo "      (Add this to .env as ENCRYPTION_KEY)"
    echo ""
    echo "   4. Start the bots:"
    echo "      ./run.sh"
else
    echo "✅ .env file already exists"
    echo ""
    echo "🔍 Validating configuration..."
    python config.py
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Setup Complete!                                               ║"
echo "╚════════════════════════════════════════════════════════════════╝"
