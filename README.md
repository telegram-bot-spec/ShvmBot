# 🤖 TGFLOW - Telegram Shop Bot

A complete Telegram-based digital marketplace with customer and admin bots sharing one Supabase database.

## 📦 Features

### Customer Bot (`shop_bot.py`)
- 🛒 Browse products by category
- 💰 Add funds via UPI
- 📱 Purchase OTP accounts with live OTP fetching
- 🎁 Referral system (2% commission)
- ⭐ Wishlist
- 📋 Order history
- 🔄 Refund requests (OTP-only)

### Admin Bot (`admin_bot.py`)
- 👑 Owner/Admin role system
- 📦 Product management with profit tracking
- 💰 Payment approval/rejection
- 👥 User management (ban, balance adjust)
- 📊 Business statistics dashboard
- 📢 Broadcast messages
- 🎟️ Promo code system
- 📥 CSV export
- 📝 Audit logs

## 🚀 Quick Start

### 1. Setup
```bash
chmod +x setup.sh
./setup.sh
```

### 2. Configure
Edit `.env` with your credentials:
```bash
nano .env
```

### 3. Initialize Database
```bash
python db.py init
```
Copy the SQL output and run it in your Supabase SQL Editor.

### 4. Generate Encryption Key
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
Add this to `.env` as `ENCRYPTION_KEY`.

### 5. Run Bots
```bash
./run.sh
```

## 📁 File Structure

```
tgflow/
├── shop_bot.py          # Customer-facing bot
├── admin_bot.py         # Admin management bot
├── config.py            # Configuration loader
├── db.py                # Database operations
├── utils.py             # Utilities (encryption, rank, etc)
├── payments.py          # Payment & QR generation
├── userbot.py           # Pyrogram OTP layer (Phase 3)
├── pdf.py               # Receipt generation (Phase 5)
├── requirements.txt     # Dependencies
├── .env                 # Your credentials (DO NOT COMMIT)
├── .env.example         # Template
├── setup.sh             # Setup script
├── run.sh               # Bot runner
└── README.md            # This file
```

## 🔐 Security Notes

- **Never commit .env** - Add to .gitignore
- Session strings are encrypted with Fernet
- 2FA passwords are encrypted
- Payment screenshots hashed for duplicate detection
- All admin actions are logged

## 📝 Environment Variables

See `.env.example` for full list. Critical ones:

```env
SHOP_BOT_TOKEN=         # From @BotFather
ADMIN_BOT_TOKEN=        # From @BotFather
OWNER_IDS=              # Your Telegram user ID
SUPABASE_URL=           # From Supabase dashboard
SUPABASE_KEY=           # Service role key
ENCRYPTION_KEY=         # Generate with Fernet
UPI_ID=                 # Your UPI ID
```

## 🗄️ Database

Uses Supabase (PostgreSQL). Schema includes:
- `users` - Customer accounts
- `products` - Product catalog
- `stock` - Inventory items
- `tg_accounts` - Telegram accounts for OTP
- `orders` - Purchase history
- `payments` - Payment requests
- `promo_codes` - Discount codes
- `wishlists` - Saved products
- `refund_requests` - Refund claims
- `admin_actions` - Audit trail

## 🤝 Support

For issues or questions:
- Check logs: `tail -f shop_bot.log`
- Test DB connection: `python db.py test`
- Validate config: `python config.py`

## 📄 License

Private project - All rights reserved.
