import os

# Telegram API Credentials
API_ID = 29241108
API_HASH = "2fc11b5216ddf760ba9179011679e5dc"
BOT_TOKEN = "8452917477:AAGy5OospmHJzzYttdVDvrVe1ojCriZ0hLg"
LOGGER_BOT_TOKEN = "7891280232:AAF2Vvlokjxyh3oYJ03IQozrvOH_dVDx-MU"
BOT_USERNAME = "SnNoiBot"
BOT_NAME = "QUANTOM"
LOGGER_BOT_USERNAME = "ToxicVPSBot"

# Admin Settings
ADMIN_ID = 8233966309
ADMIN_USERNAME = "szxns"
ADMIN_IDS = [8233966309]

# Image URLs
START_IMAGE = "https://i.ibb.co/mVRtxk9g/x.jpg"
BROADCAST_IMAGE = "https://i.ibb.co/mVRtxk9g/x.jpg"
FORCE_JOIN_IMAGE = "https://i.ibb.co/mVRtxk9g/x.jpg"

# Force Join Settings
ENABLE_FORCE_JOIN = False
MUST_JOIN = "quantumadz"
MUSTJOIN = "sector_market"

# Channel and Group IDs
SETUP_GROUP_ID = -1002843633996
VOUCH_CHANNEL_ID = -1002843633996
TECH_LOG_CHANNEL_ID = -1002843633996

# External Links
PRIVACY_POLICY_URL = "https://telegra.ph/Privacy-Policy-12-31"
SUPPORT_GROUP_URL = "https://t.me/sector_market"
UPDATES_CHANNEL_URL = "https://t.me/quantumadz"
VOUCH_CHANNEL_URL = "https://t.me/luxxad_vouch"
GUIDE_URL = "https://t.me/Quantumadz/445"
WEBAPP_URL = "https://luxxad.vercel.app/"

# Vouch Message
VOUCH_MESSAGE = "One more legend joined the network! 🚀"

# Encryption Key
ENCRYPTION_KEY = "RnVa0xtPfK1pm3qu_POAvFI9qkSyISKFShE37_JSQ2w="

# Database Configuration
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://Kittu:Kittu@kittu.nvijrco.mongodb.net/?retryWrites=true&w=majority&appName=Kittu"
)
DB_NAME = "adsbot_db"

# Guide Text
GUIDE_TEXT = """
> *Quatum Bot Guide*

*Step 1: Host Your Account*
- Click Host Account button
- Enter your phone number with country code
- Enter the OTP you receive
- If 2FA is enabled, enter your cloud password

*Step 2: Set Your Ad*
- Click Set Ad Message
- Type your advertisement text
- Set delay between messages (optional)

*Step 3: Start Broadcasting*
- Click Start Ads to begin
- Your ads will be sent to all groups automatically
- Use Stop Ads to pause anytime

*Tips*
- Keep ad messages engaging but not spammy
- Use appropriate delays to avoid bans
- Monitor broadcast logs for performance
- Contact @luxxadsupport for help
"""

# Broadcast Settings
DEFAULT_DELAY = 30
MIN_DELAY = 10
MAX_DELAY = 3600
MAX_BROADCASTS_PER_DAY = 50
MAX_GROUPS_PER_BROADCAST = 20

# OTP Settings
OTP_LENGTH = 5
OTP_EXPIRY = 300

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FILE = "logs/luxxad_bot.log"

# Feature Toggles
ENABLE_FORCE_JOIN = False
ENABLE_OTP_VERIFICATION = True
ENABLE_BROADCASTING = True
ENABLE_ANALYTICS = True

# Success Messages
SUCCESS_MESSAGES = {
    "account_added": "Account added successfully!",
    "otp_sent": "OTP sent to your phone number!",
    "broadcast_started": "Broadcast started successfully!",
    "broadcast_completed": "Broadcast completed successfully!"
}

# Error Messages
ERROR_MESSAGES = {
    "account_limit": "You've reached your account limit of 5!",
    "invalid_phone": "Invalid phone number format! Use +1234567890",
    "otp_expired": "OTP has expired. Please restart hosting.",
    "invalid_otp": "Invalid OTP. Please try again.",
    "login_failed": "Failed to login to Telegram account!",
    "no_groups": "No groups found in your account!",
    "no_messages": "No messages found in Saved Messages!",
    "broadcast_limit": "Daily broadcast limit reached!",
    "unauthorized": "You are not authorized to perform this action!",
    "force_join_required": "Join required channels to access this feature!"
}

# Auto-Reply Configuration
AUTO_REPLY_DEFAULT_MESSAGE = "Thank you for your message! Our team will get back to you soon."
AUTO_REPLY_MAX_LENGTH = 200

# Session Storage
SESSION_STORAGE_PATH = "sessions/"
