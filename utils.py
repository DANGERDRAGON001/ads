import re
import random
import string
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import logging
import config

logger = logging.getLogger(__name__)

def validate_phone_number(phone: str) -> bool:
    """Validate phone number format"""
    cleaned = re.sub(r'[^\d+]', '', phone)
    pattern = r'^\+\d{10,15}$'
    return bool(re.match(pattern, cleaned))

def generate_progress_bar(completed: int, total: int, length: int = 10) -> str:
    """Generate visual progress bar"""
    if total == 0:
        return "▓" * length + " 0%"
    
    percentage = (completed / total) * 100
    filled = int((completed / total) * length)
    bar = "▓" * filled + "░" * (length - filled)
    
    return f"{bar} {percentage:.1f}%"

def format_duration(td: timedelta) -> str:
    """Format timedelta to human readable string"""
    total_seconds = int(td.total_seconds())
    
    if total_seconds < 60:
        return f"{total_seconds}s"
    elif total_seconds < 3600:
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}m {seconds}s"
    else:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours}h {minutes}m"

def generate_transaction_id() -> str:
    """Generate unique transaction ID"""
    return f"#TXN{''.join(random.choices(string.digits, k=6))}"

def generate_order_id() -> str:
    """Generate unique order ID"""
    return f"ORD{''.join(random.choices(string.ascii_uppercase + string.digits, k=8))}"

def format_plan_features(features: List[str]) -> str:
    """Format plan features for display"""
    return "\n".join([f"<blockquote>{feature}</blockquote>" for feature in features])

def calculate_success_rate(sent: int, failed: int) -> float:
    """Calculate success rate percentage"""
    total = sent + failed
    if total == 0:
        return 0.0
    return (sent / total) * 100

def format_currency(amount: float, currency: str = "$") -> str:
    """Format currency amount in USD"""
    return f"{currency}{amount:,.2f}"

def time_until_expiry(expires_at: str) -> Dict[str, Any]:
    """Calculate time until expiry"""
    try:
        expiry_date = datetime.fromisoformat(expires_at)
        now = datetime.now()
        
        if now >= expiry_date:
            return {"days": 0, "hours": 0, "minutes": 0, "expired": True}
        
        diff = expiry_date - now
        days = diff.days
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        
        return {"days": days, "hours": hours, "minutes": minutes, "expired": False}
    except Exception as e:
        logger.error(f"time_until_expiry error: {e}")
        return {"days": 0, "hours": 0, "minutes": 0, "expired": True}

def sanitize_username(username: str) -> str:
    """Sanitize username for display"""
    if not username:
        return "N/A"
    return username.replace("@", "").strip()

def format_user_display(user_data: Dict) -> str:
    """Format user data for display"""
    username = sanitize_username(user_data.get('username', ''))
    first_name = user_data.get('first_name', 'User')
    user_id = user_data.get('user_id', 'Unknown')
    
    if username and username != "N/A":
        return f"@{username} ({user_id})"
    else:
        return f"{first_name} ({user_id})"

def validate_delay(delay_str: str) -> tuple[bool, int]:
    """Validate and return delay value"""
    try:
        delay = int(delay_str)
        if delay < 10 or delay > 600:
            return False, 0
        return True, delay
    except ValueError:
        return False, 0

def generate_referral_code(user_id: int) -> str:
    """Generate referral code for user"""
    return f"REF{user_id}{random.randint(1000, 9999)}"

def parse_referral_code(code: str) -> int:
    """Parse referral code to get user ID"""
    try:
        if code.startswith("REF"):
            user_part = code[3:-4]
            return int(user_part)
    except:
        pass
    return 0

def format_broadcast_summary(sent: int, failed: int, duration: timedelta) -> str:
    """Format broadcast completion summary"""
    total = sent + failed
    success_rate = (sent / total * 100) if total > 0 else 0
    
    return (
        f"📊 <blockquote><b>BROADCAST SUMMARY</b></blockquote>\n\n"
        f"<blockquote>✅ <b>Sent:</b> {sent:,}</blockquote>\n"
        f"<blockquote>❌ <b>Failed:</b> {failed:,}</blockquote>\n"
        f"<blockquote>📈 <b>Success Rate:</b> {success_rate:.1f}%</blockquote>\n"
        f"<blockquote>⏰ <b>Duration:</b> {format_duration(duration)}</blockquote>\n"
        f"<blockquote>🎯 <b>Performance:</b> {generate_progress_bar(sent, total)}</blockquote>"
    )

def create_vouch_message(user_data: Dict, plan_data: Dict, transaction_id: str, admin_username: str) -> str:
    """Create formatted vouch message"""
    username = sanitize_username(user_data.get('username', ''))
    user_id = user_data.get('user_id')
    
    return (
        f"🎉 <blockquote><b>LUXXAD PREMIUM GRANTED</b></blockquote>\n\n"
        f"👤 <b>User:</b> @{username} <code>{user_id}</code>\n"
        f"🏷️ <b>Transaction:</b> <blockquote>{transaction_id}</blockquote>\n\n"
        f"📋 <b>Plan:</b> <blockquote>{plan_data['name']} Plan</blockquote>\n"
        f"💰 <b>Value:</b> <blockquote>${plan_data['price']:.2f}</blockquote>\n\n"
        f"✨ <b>Features Unlocked:</b>\n"
        f"{format_plan_features(plan_data['features'])}\n\n"
        f"👨‍💼 <b>Granted by:</b> @{admin_username}"
    )

def log_user_action(user_id: int, action: str, details: str = "") -> str:
    """Create formatted log message for user actions"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return (
        f"📝 <b>USER ACTION LOG</b>\n\n"
        f"👤 <b>User:</b> <code>{user_id}</code>\n"
        f"🎯 <b>Action:</b> {action}\n"
        f"📋 <b>Details:</b> {details}\n"
        f"⏰ <b>Time:</b> {timestamp} IST"
    )

def validate_message_content(content: str) -> tuple[bool, str]:
    """Validate ad message content"""
    if not content or not content.strip():
        return False, "❌ Message cannot be empty"
    if len(content) > 4096:
        return False, "❌ Message too long (max 4096 characters)"
    spam_indicators = ['🔥🔥🔥', 'URGENT!!!', 'CLICK NOW!!!']
    for indicator in spam_indicators:
        if indicator.lower() in content.lower():
            return False, f"❌ Avoid spam words like '{indicator}'"
    return True, "✅ Message validated"

def create_analytics_summary(analytics: Dict) -> str:
    """Create formatted analytics summary"""
    total_sent = analytics.get('total_sent', 0)
    total_failed = analytics.get('total_failed', 0)
    success_rate = calculate_success_rate(total_sent, total_failed)
    
    return (
        f"📊 <blockquote><b>PERFORMANCE ANALYTICS</b></blockquote>\n\n"
        f"<blockquote>📈 <b>Broadcasts:</b> {analytics.get('total_broadcasts', 0):,}</blockquote>\n"
        f"<blockquote>✅ <b>Sent:</b> {total_sent:,}</blockquote>\n"
        f"<blockquote>❌ <b>Failed:</b> {total_failed:,}</blockquote>\n"
        f"<blockquote>🎯 <b>Success Rate:</b> {success_rate:.1f}%</blockquote>\n"
        f"<blockquote>📱 <b>Accounts:</b> {analytics.get('total_accounts', 0)}</blockquote>"
    )

def format_error_message(error_type: str, context: str = "") -> str:
    """Format error messages consistently"""
    base_message = config.ERROR_MESSAGES.get(error_type, "❌ An error occurred")
    if context:
        return f"{base_message}\n\n<blockquote>🔍 <b>Context:</b> {context}</blockquote>"
    return base_message

def format_success_message(success_type: str, context: str = "") -> str:
    """Format success messages consistently"""
    base_message = config.SUCCESS_MESSAGES.get(success_type, "✅ Operation successful")
    if context:
        return f"{base_message}\n\n<blockquote>📋 <b>Details:</b> {context}</blockquote>"
    return base_message

def kb(buttons: List[List[Any]]) -> InlineKeyboardMarkup:
    """Create inline keyboard from button list"""
    keyboard = []
    for row in buttons:
        row_buttons = []
        for button in row:
            if isinstance(button, dict):
                if 'url' in button:
                    row_buttons.append(InlineKeyboardButton(button['text'], url=button['url']))
                else:
                    row_buttons.append(InlineKeyboardButton(button['text'], callback_data=button['callback_data']))
            else:
                row_buttons.append(button)
        keyboard.append(row_buttons)
    return InlineKeyboardMarkup(keyboard)