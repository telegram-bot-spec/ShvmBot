#!/bin/bash

###############################################################################
#  TGFLOW - Bot Runner
#  Runs both shop and admin bots
###############################################################################

# Activate virtual environment
source venv/bin/activate

echo "🚀 Starting TGFlow Bots..."
echo ""

# Run both bots in background
python shop_bot.py &
SHOP_PID=$!

python admin_bot.py &
ADMIN_PID=$!

echo "✅ Shop Bot started (PID: $SHOP_PID)"
echo "✅ Admin Bot started (PID: $ADMIN_PID)"
echo ""
echo "Press Ctrl+C to stop both bots"
echo ""

# Wait for Ctrl+C
trap "kill $SHOP_PID $ADMIN_PID; echo ''; echo '🛑 Bots stopped'; exit" INT

# Keep script running
wait
