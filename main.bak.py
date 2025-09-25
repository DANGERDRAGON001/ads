import asyncio
import random
import string
import re
import json
from datetime import datetime, timedelta
from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    UpdateAppToLoginError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    SessionExpiredError,
    PasswordHashInvalidError
)
from pyrogram import Client as PyroClient, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from pyrogram.errors import UserNotParticipant, PeerIdInvalid, ChatWriteForbidden, FloodWait
from pyrogram.enums import ParseMode
import config
from database import EnhancedDatabaseManager
from utils import validate_phone_number, generate_progress_bar, format_duration
import os
import logging
from cryptography.fernet import Fernet

# Logging setup
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/QUANTUM_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

print("QUANTUM Bot Free Version Started. 🚀")

# Define desired bio and name suffix
DESIRED_BIO = "Automated ads via @QuantumAdsBot (FREE)"
NAME_SUFFIX = " -- via @QuantumAdsBot (FREE)"

# Initialize encryption key
ENCRYPTION_KEY = getattr(config, 'ENCRYPTION_KEY', None)
if not ENCRYPTION_KEY:
    logger.warning("No ENCRYPTION_KEY in config. Generating new key.")
    ENCRYPTION_KEY = Fernet.generate_key().decode()
cipher_suite = Fernet(ENCRYPTION_KEY.encode())

# Initialize database
db = EnhancedDatabaseManager()

# Admin check
ADMIN_IDS = [config.ADMIN_ID]
def is_owner(uid):
    return uid in ADMIN_IDS

# Inline keyboard helper
def kb(rows):
    if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
        logger.error("Invalid rows format for InlineKeyboardMarkup")
        raise ValueError("Rows must be a list of lists")
    return InlineKeyboardMarkup(rows)

# Initialize Pyrogram clients
pyro = PyroClient("QUANTUM_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)
logger_client = PyroClient("logger_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.LOGGER_BOT_TOKEN)

# Async function to send logs via logger bot to user DM
async def send_dm_log(user_id, log_message):
    try:
        await logger_client.send_message(user_id, log_message, parse_mode=ParseMode.HTML)
        logger.info(f"DM log sent to {user_id}: {log_message[:50]}...")
    except Exception as e:
        logger.error(f"DM log failed for {user_id}: {e} - Message: {log_message[:50]}...")
        db.log_logger_failure(user_id, str(e))

# Logger bot start command to mark user as active
@logger_client.on_message(filters.command(["start"]))
async def logger_start(client, m):
    uid = m.from_user.id
    username = m.from_user.username or "Unknown"
    first_name = m.from_user.first_name or "User"
    
    db.create_user(uid, username, first_name)
    db.set_logger_status(uid, is_active=True)
    await m.reply(
        f"<b>🚀 Welcome to QUANTUM Logger Bot! ✨</b>\n\n"
        f"<u>Logs for your ad broadcasts will be sent here.</u>\n"
        f"Start the main bot (@{config.BOT_USERNAME}) to begin broadcasting! 🌟",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Logger bot started by user {uid}")

# Updated log_vouch function with peer resolution and retry logic
async def log_vouch(client, message):
    """Log vouch events to vouch channel with retry logic."""
    max_retries = 3
    retry_delay = 1
    chat_id = config.VOUCH_CHANNEL_ID
    
    try:
        await client.resolve_peer(chat_id)
        logger.info(f"Resolved peer for vouch channel {chat_id}")
    except (PeerIdInvalid, KeyError, ValueError) as e:
        logger.error(f"Failed to resolve peer for vouch channel {chat_id}: {e}")
        with open('logs/vouch_errors.log', 'a') as f:
            f.write(f"{datetime.now()} - Failed to resolve vouch channel {chat_id}: {e} - Message: {message[:50]}...\n")
        db.increment_vouch_failure(chat_id, str(e))
        for admin_id in ADMIN_IDS:
            try:
                await client.resolve_peer(admin_id)
                await client.send_message(
                    admin_id,
                    f"Failed to resolve vouch channel {chat_id}: {e}. Please ensure bot is in the channel."
                )
                break
            except Exception as admin_e:
                logger.error(f"Failed to notify admin {admin_id}: {admin_e}")
        return

    for attempt in range(max_retries):
        try:
            await client.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            logger.info(f"Vouch log sent to {chat_id}: {message[:50]}...")
            db.increment_vouch_success(chat_id)
            return
        except PeerIdInvalid as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} - Peer ID invalid for vouch channel {chat_id}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
        except ChatWriteForbidden as e:
            logger.error(f"Bot lacks permission to post in vouch channel {chat_id}: {e}")
            db.increment_vouch_failure(chat_id, str(e))
            for admin_id in ADMIN_IDS:
                try:
                    await client.resolve_peer(admin_id)
                    await client.send_message(
                        admin_id,
                        f"Vouch log failed: No post permission in {chat_id}. Please grant permissions."
                    )
                    break
                except Exception as admin_e:
                    logger.error(f"Failed to notify admin {admin_id}: {admin_e}")
            break
        except FloodWait as e:
            logger.warning(f"Flood wait in vouch channel {chat_id}: Wait {e.value} seconds")
            await asyncio.sleep(e.value)
            db.increment_vouch_failure(chat_id, f"FloodWaitError ({e.value}s)")
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} - Failed to send vouch log to {chat_id}: {e}. Message: {message[:50]}...")
            db.increment_vouch_failure(chat_id, str(e))
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
    
    with open('logs/vouch_errors.log', 'a') as f:
        f.write(f"{datetime.now()} - Failed to send vouch to {chat_id}: Max retries reached - Message: {message[:50]}...\n")
    db.increment_vouch_failure(chat_id, "Max retries reached")
    for admin_id in ADMIN_IDS:
        try:
            await client.resolve_peer(admin_id)
            await client.send_message(
                admin_id,
                f"Vouch log failed after {max_retries} retries: {chat_id}. Message: {message[:50]}..."
            )
            break
        except Exception as admin_e:
            logger.error(f"Failed to notify admin {admin_id}: {admin_e}")

async def is_joined(client, uid, chat_id):
    try:
        await client.get_chat_member(chat_id, uid)
        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.error(f"Failed to check join status for {uid} in {chat_id}: {e}")
        return False

async def is_joined_all(client, uid):
    channel_joined = await is_joined(client, uid, config.MUST_JOIN)
    group_joined = await is_joined(client, uid, config.MUSTJOIN)
    return channel_joined and group_joined

async def validate_session(session_str):
    """Validate a Telegram session string."""
    try:
        tg_client = TelegramClient(StringSession(session_str), config.API_ID, config.API_HASH)
        await tg_client.connect()
        is_valid = await tg_client.is_user_authorized()
        await tg_client.disconnect()
        return is_valid
    except Exception as e:
        logger.error(f"Session validation failed: {e}")
        return False

async def stop_broadcast_task(uid):
    """Helper function to stop broadcast task and clean up."""
    state = db.get_broadcast_state(uid)
    running = state.get("running", False)
    if not running:
        return False

    task = db.get_temp_data(uid)
    if task:
        try:
            task.cancel()
            logger.info(f"Cancelled broadcast task for {uid}")
        except Exception as e:
            logger.error(f"Failed to cancel broadcast task for {uid}: {e}")
    
    db.set_broadcast_state(uid, running=False)
    db.set_temp_data(uid, None)
    return True

async def run_broadcast(client, uid):
    """Send ads to all groups with minimal delay and cycle interval."""
    try:
        sent_count = 0
        failed_count = 0
        msg = db.get_user_ad_messages(uid)
        msg = msg[0]["message"] if msg else None
        if not msg:
            await client.send_message(uid, "No ad message set! 😔", parse_mode=ParseMode.HTML)
            return
        delay = db.get_user_ad_delay(uid)
        accounts = db.get_user_accounts(uid)
        target_groups = db.get_target_groups(uid)
        group_ids = [g['group_id'] for g in target_groups] if target_groups else None
        clients = {}
        error_summary = []

        for acc in accounts:
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                if not await validate_session(session_str):
                    db.deactivate_account(acc['_id'])
                    logger.warning(f"Deactivated invalid session for {acc['phone_number']}")
                    continue
                tg_client = TelegramClient(StringSession(session_str), config.API_ID, config.API_HASH)
                await tg_client.start()
                
                me = await tg_client.get_me()
                about = getattr(me, 'about', None) or ""
                if about != DESIRED_BIO:
                    try:
                        await tg_client(functions.account.UpdateProfileRequest(
                            about=DESIRED_BIO
                        ))
                        logger.info(f"Updated bio for account {acc['phone_number']}")
                    except Exception as e:
                        logger.warning(f"Failed to update bio for {acc['phone_number']}: {e}")
                
                current_last = getattr(me, 'last_name', "") or ""
                if not current_last.endswith(NAME_SUFFIX):
                    new_last = current_last + NAME_SUFFIX
                    try:
                        await tg_client(functions.account.UpdateProfileRequest(
                            last_name=new_last
                        ))
                        logger.info(f"Updated last name for account {acc['phone_number']}")
                    except Exception as e:
                        logger.warning(f"Failed to update last name for {acc['phone_number']}: {e}")
                
                clients[acc['_id']] = tg_client
            except Exception as e:
                logger.error(f"Failed to start client for {acc['phone_number']}: {e}")
                failed_count += 1
                db.increment_broadcast_stats(uid, False)
                error_summary.append(f"Account {acc['phone_number']}: {str(e)}")
                await send_dm_log(uid, f"<b>❌ Failed to start account {acc['phone_number']}:</b> {str(e)} 😔")
        if not clients:
            await client.send_message(uid, "No valid accounts found! 😔", parse_mode=ParseMode.HTML)
            return

        db.set_broadcast_state(uid, running=True)

        try:
            while db.get_broadcast_state(uid).get("running", False):
                for acc in accounts:
                    tg_client = clients.get(acc['_id'])
                    if not tg_client:
                        continue
                    async for dialog in tg_client.iter_dialogs():
                        if dialog.is_group and (not group_ids or dialog.id in group_ids):
                            try:
                                await tg_client.send_message(dialog.id, msg)
                                sent_count += 1
                                db.increment_broadcast_stats(uid, True)
                                await send_dm_log(uid, f"<b>✅ Sent to {dialog.name} ({dialog.id})</b> using account {acc['phone_number']} 🚀")
                            except FloodWaitError as e:
                                logger.warning(f"Flood wait in group {dialog.id}: Wait {e.seconds} seconds")
                                await asyncio.sleep(e.seconds)
                                failed_count += 1
                                db.increment_broadcast_stats(uid, False)
                                error_summary.append(f"Group {dialog.id}: FloodWaitError ({e.seconds}s)")
                                await send_dm_log(uid, f"<b>⚠️ Flood wait in {dialog.name} ({dialog.id}):</b> Waiting {e.seconds}s 😔")
                            except Exception as e:
                                logger.error(f"Failed to send message to group {dialog.id}: {e}")
                                failed_count += 1
                                db.increment_broadcast_stats(uid, False)
                                error_summary.append(f"Group {dialog.id}: {str(e)}")
                                await send_dm_log(uid, f"<b>❌ Failed to send to {dialog.name} ({dialog.id}):</b> {str(e)} 😔")
                            await asyncio.sleep(random.uniform(3, 4))
                if error_summary:
                    logger.warning(f"Broadcast errors for user {uid}: {len(error_summary)} failures - {', '.join(error_summary[:5])}")
                    error_summary = []
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logger.info(f"Broadcast task cancelled for {uid}")
        finally:
            for tg_client in clients.values():
                try:
                    await tg_client.disconnect()
                except Exception as e:
                    logger.error(f"Failed to disconnect client: {e}")
            db.set_broadcast_state(uid, running=False)
            await client.send_message(
                uid,
                f"<blockquote><b>🏁 Broadcast Completed! ✨</b></blockquote>\n\n"
                f"<u>Check analytics for stats! 📈</u>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Analytics 📈", callback_data="analytics")]])
            )
            await send_dm_log(uid, f"<b>🏁 Broadcast Completed! Check analytics for stats ✨</b>")
    except Exception as e:
        logger.error(f"Broadcast task failed for user {uid}: {e}")
        db.increment_broadcast_stats(uid, False)
        db.set_broadcast_state(uid, running=False)
        await send_dm_log(uid, f"<b>❌ Broadcast task failed:</b> {str(e)} 😔")
        for admin_id in ADMIN_IDS:
            try:
                await client.resolve_peer(admin_id)
                await client.send_message(
                    admin_id,
                    f"Broadcast task failed for user {uid}: {e}"
                )
                break
            except Exception as admin_e:
                logger.error(f"Failed to notify admin {admin_id}: {admin_e}")

# OTP Inline Keyboard
def get_otp_keyboard():
    rows = [
        [InlineKeyboardButton("1", callback_data="otp_1"), InlineKeyboardButton("2", callback_data="otp_2"), InlineKeyboardButton("3", callback_data="otp_3")],
        [InlineKeyboardButton("4", callback_data="otp_4"), InlineKeyboardButton("5", callback_data="otp_5"), InlineKeyboardButton("6", callback_data="otp_6")],
        [InlineKeyboardButton("7", callback_data="otp_7"), InlineKeyboardButton("8", callback_data="otp_8"), InlineKeyboardButton("9", callback_data="otp_9")],
        [InlineKeyboardButton("⌫", callback_data="otp_back"), InlineKeyboardButton("0", callback_data="otp_0"), InlineKeyboardButton("❌", callback_data="otp_cancel")],
        [InlineKeyboardButton("Show Code", url="tg://openmessage?user_id=777000")]
    ]
    return kb(rows)

@pyro.on_callback_query(filters.regex("^otp_"))
async def otp_callback(client, cb):
    uid = cb.from_user.id
    state = db.get_user_state(uid)
    if state != "telethon_wait_otp":
        await cb.answer("Invalid state! Please restart with /start. 😔", show_alert=True)
        return

    temp_data = db.get_temp_data(uid)
    if not temp_data:
        await cb.answer("Session expired! Please restart. 😔", show_alert=True)
        db.set_user_state(uid, "")
        return

    parts = temp_data.split("::")
    if len(parts) < 3:
        logger.error(f"Invalid temp_data format for user {uid}: {temp_data}")
        await cb.answer("Error: Invalid session data. Please restart. 😔", show_alert=True)
        db.set_user_state(uid, "")
        db.set_temp_data(uid, None)
        return

    phone, session_str, phone_code_hash = parts[:3]
    otp = parts[3] if len(parts) > 3 else ""

    # Validate session string
    try:
        if not session_str or not StringSession(session_str).is_valid():
            logger.error(f"Invalid session string for user {uid}: {session_str}")
            await cb.answer("Error: Invalid session. Please restart. 😔", show_alert=True)
            db.set_user_state(uid, "")
            db.set_temp_data(uid, None)
            return
    except Exception as e:
        logger.error(f"Session validation failed for user {uid}: {e}")
        await cb.answer("Error: Invalid session format. Please restart. 😔", show_alert=True)
        db.set_user_state(uid, "")
        db.set_temp_data(uid, None)
        return

    action = cb.data.replace("otp_", "")
    if action.isdigit():
        if len(otp) < 5:
            otp += action
    elif action == "back":
        otp = otp[:-1] if otp else ""
    elif action == "cancel":
        db.set_user_state(uid, "")
        db.set_temp_data(uid, None)
        await cb.message.edit_text("OTP entry cancelled. 😔", reply_markup=None)
        return

    db.set_temp_data(uid, f"{phone}::{session_str}::{phone_code_hash}::{otp}")

    masked = " ".join("*" for _ in otp) if otp else "_____"
    caption = (
        f"Phone: {phone}\n\n"
        f"<blockquote><b>✅ OTP sent! 🚀</b></blockquote>\n\n"
        f"<u>Enter the OTP using the keypad below ✨</u>\n"
        f"<b>Current:</b> <code>{masked}</code>\n"
        f"<b>Format:</b> <code>12345</code> (no spaces needed) 🌟\n"
        f"<i>Valid for:</i> <u>{config.OTP_EXPIRY // 60} minutes</u>"
    )

    await cb.message.edit_caption(
        caption=caption,
        parse_mode=ParseMode.HTML,
        reply_markup=get_otp_keyboard()
    )

    if len(otp) == 5:
        await cb.message.edit_caption(caption + "\n\n<b>Verifying...</b>", parse_mode=ParseMode.HTML, reply_markup=None)
        tg = TelegramClient(StringSession(session_str), config.API_ID, config.API_HASH)
        try:
            await tg.connect()
            await tg.sign_in(phone, code=otp, phone_code_hash=phone_code_hash)

            session_encrypted = cipher_suite.encrypt(session_str.encode()).decode()
            db.add_user_account(uid, phone, session_encrypted)

            await cb.message.edit_caption(
                f"<blockquote><b>✅ Account added! 🚀</b></blockquote>\n\n"
                f"<u>Phone:</u> <code>{phone}</code> ✨\n"
                "<i>Account is ready for broadcasting! 🌟</i>\n"
                "<i>Note: Profile bio and name will be updated during the first broadcast.</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>✅ Account added successfully:</b> <code>{phone}</code> ✨")
            db.set_user_state(uid, "")
            db.set_temp_data(uid, None)
        except SessionPasswordNeededError:
            db.set_user_state(uid, "telethon_wait_password")
            db.set_temp_data(uid, f"{phone}::{session_str}")
            await cb.message.edit_caption(
                caption + "\n\n<blockquote><b>🔐 2FA Detected! 🚀</b></blockquote>\n\n"
                "<u>Please send your Telegram cloud password ✨:</u>",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
        except PhoneCodeInvalidError:
            await cb.message.edit_caption(
                caption + "\n\n<b>❌ Invalid OTP! Try again.</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_otp_keyboard()
            )
            db.set_temp_data(uid, f"{phone}::{session_str}::{phone_code_hash}::")
        except PhoneCodeExpiredError:
            await cb.message.edit_caption(
                caption + "\n\n<b>❌ OTP expired! Please restart.</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
            db.set_user_state(uid, "")
            db.set_temp_data(uid, None)
        except Exception as e:
            logger.error(f"Error signing in for {uid}: {e}")
            await cb.message.edit_caption(
                caption + f"\n\n<blockquote><b>❌ Login failed:</b> <i>{str(e)}</i> 😔</blockquote>\n\n"
                f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=None
            )
            await send_dm_log(uid, f"<b>❌ Account login failed:</b> {str(e)} 😔")
            db.set_user_state(uid, "")
            db.set_temp_data(uid, None)
        finally:
            await tg.disconnect()

@pyro.on_message(filters.command(["start"]))
async def start(client, m):
    uid = m.from_user.id
    username = m.from_user.username or "Unknown"
    first_name = m.from_user.first_name or "User"
    
    db.create_user(uid, username, first_name)
    db.update_user_last_interaction(uid)
    
    if config.ENABLE_FORCE_JOIN:
        if not await is_joined_all(client, uid):
            try:
                await m.reply_photo(
                    photo=config.FORCE_JOIN_IMAGE,
                    caption="""<blockquote>🔐 QUANTUM ACCESS REQUIRED 🚀</blockquote>\n\n"""
                            """To unlock the full <b>L</b> experience, please join our official channels first!\n\n"""
                            """Your <i>premium automation journey</i> starts here ✨""",
                    reply_markup=kb([
                        [InlineKeyboardButton("JOIN CHANNEL 🌟", url=f"https://t.me/{config.MUST_JOIN}")],
                        [InlineKeyboardButton("JOIN GROUP 🌟", url=f"https://t.me/{config.MUSTJOIN}")],
                        [InlineKeyboardButton("I Joined ✅", callback_data="joined_check")]
                    ]),
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Failed to send force join message to {uid}: {e}")
                await m.reply("Please join our channels to proceed. Contact support if this persists. 😔")
            return
    
    try:
        await m.reply_photo(
            photo=config.START_IMAGE,
            caption=f"""<blockquote>🚀 Welcome to <b>QUANTUM</b> — The Future of Telegram Automation ✨</blockquote>\n\n"""
                    f"<u>Premium Ad Broadcasting</u> • <i>Smart Delays</i> • <b>Multi-Account Support</b>\n\n"
                    f"Admin: @{config.ADMIN_USERNAME} 🌟",
            reply_markup=kb([
                [InlineKeyboardButton("Enter Dashboard 🚪", callback_data="menu_main")],
                [InlineKeyboardButton("Privacy Policy 🔒", url=config.PRIVACY_POLICY_URL),
                 InlineKeyboardButton("Support Group 💬", url=config.SUPPORT_GROUP_URL)],
                [InlineKeyboardButton("How To Use 📖", url=config.GUIDE_URL)],
                [InlineKeyboardButton("Updates Channel 📢", url=config.UPDATES_CHANNEL_URL)]
            ]),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Failed to send start message to {uid}: {e}")
        await m.reply("Error starting bot. Please try again or contact support. 😔")

@pyro.on_callback_query(filters.regex("joined_check"))
async def joined_check(client, cb):
    if not await is_joined_all(client, cb.from_user.id):
        await cb.answer("Please join both channel and group first! 😔", show_alert=True)
        return
    await cb.message.delete()
    await start(client, cb.message)

@pyro.on_callback_query(filters.regex("back_to_start"))
async def back_to_start(client, cb):
    await cb.message.delete()
    await start(client, cb.message)

@pyro.on_callback_query(filters.regex("menu_main"))
async def menu_main(client, cb):
    try:
        uid = cb.from_user.id
        db.update_user_last_interaction(uid)
        user = db.get_user(uid)
        
        if not user:
            await cb.answer("Please restart with /start 😔", show_alert=True)
            return
        
        accounts_count = db.get_user_accounts_count(uid)
        saved_msgs = db.get_user_ad_messages(uid)
        ad_msg_status = "Set ✅" if saved_msgs else "Not Set 😔"
        current_delay = db.get_user_ad_delay(uid)
        broadcast_state = db.get_broadcast_state(uid)
        running = broadcast_state.get("running", False)
        broadcast_status = "Running 🚀" if running else "Stopped ⏹️"
        
        dashboard_caption = (
            f"<blockquote>📊 <b>QUANTUM DASHBOARD ✨</b></blockquote>\n\n"
            f"Hosted Accounts: <code>{accounts_count}/5</code> 🌟\n"
            f"Ad Message: <i>{ad_msg_status}</i>\n"
            f"Cycle Interval: <u>{current_delay}s</u> ⏱️\n"
            f"Broadcast: <b>{broadcast_status}</b>\n\n"
            "<blockquote>Choose an action below to continue 🚀</blockquote>"
        )
        
        menu = [
            [InlineKeyboardButton("Add Accounts 📱", callback_data="host_account"),
             InlineKeyboardButton("My Accounts 👥", callback_data="view_accounts")],
            [InlineKeyboardButton("Set Ad Message 📝", callback_data="set_msg"),
             InlineKeyboardButton("Set Time Interval ⏱️", callback_data="set_delay")],
            [InlineKeyboardButton("Start Ads 🚀", callback_data="start_broadcast")],
            [InlineKeyboardButton("Stop Ads ⏹️", callback_data="stop_broadcast")],
            [InlineKeyboardButton("Analytics 📈", callback_data="analytics"),
             InlineKeyboardButton("Target Groups 🎯", callback_data="target_groups")],
            [InlineKeyboardButton("Back 🔙", callback_data="back_to_start")]
        ]
        
        try:
            await cb.message.edit_media(
                media=InputMediaPhoto(
                    media=config.START_IMAGE,
                    caption=dashboard_caption,
                    parse_mode=ParseMode.HTML
                ),
                reply_markup=kb(menu)
            )
        except Exception as e:
            logger.error(f"Error editing media in menu_main: {e}")
            await cb.message.edit_caption(
                caption=dashboard_caption,
                reply_markup=kb(menu),
                parse_mode=ParseMode.HTML
            )
        logger.info(f"Menu main accessed by user {uid}, callback_data: {cb.data}")
    except Exception as e:
        logger.error(f"Error in menu_main for user {uid}: {e}")
        await cb.answer("Error loading dashboard. Try /start. 😔", show_alert=True)

@pyro.on_callback_query(filters.regex("host_account"))
async def host_account(client, cb):
    uid = cb.from_user.id
    user = db.get_user(uid)
    
    if not user:
        await cb.answer("Please restart with /start 😔", show_alert=True)
        return
    
    accounts_count = db.get_user_accounts_count(uid)
    limit = user.get("accounts_limit", 5)
    if isinstance(limit, str):
        if limit.lower() == "unlimited":
            limit = 999
            logger.info(f"User {uid} has 'Unlimited' accounts_limit, setting to {limit}")
        else:
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                logger.error(f"Invalid accounts_limit for user {uid}: {limit}. Defaulting to 5")
                limit = 5
    
    if not is_owner(uid) and accounts_count >= limit:
        await cb.answer(f"Account limit reached ({accounts_count}/{limit}) 😔", show_alert=True)
        return
    
    try:
        db.set_user_state(uid, "telethon_wait_phone")
        db.set_temp_data(uid, None)
    except Exception as e:
        logger.error(f"Failed to set user state for {uid}: {e}")
        await cb.answer("Error initiating account hosting. Try again. 😔", show_alert=True)
        return
    
    await cb.message.edit_media(
        media=InputMediaPhoto(
            media=config.FORCE_JOIN_IMAGE,
            caption="""<blockquote>🔐 <b>HOST NEW ACCOUNT 🚀</b></blockquote>\n\n"""
                    """<u>Secure Account Hosting ✨</u>\n\n"""
                    """Enter your phone number with country code:\n\n"""
                    """<blockquote>Example: <code>+1234567890</code> 🌟</blockquote>\n\n"""
                    """<i>Your data is encrypted and secure 🔒</i>""",
            parse_mode=ParseMode.HTML
        ),
        reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
    )

@pyro.on_callback_query(filters.regex("view_accounts"))
async def view_accounts(client, cb):
    uid = cb.from_user.id
    accounts = db.get_user_accounts(uid)
    if not accounts:
        await cb.message.edit_caption(
            caption="""<blockquote>📱 <b>NO ACCOUNTS HOSTED 😔</b></blockquote>\n\n"""
                    """<u>Add an account to start broadcasting! 🚀</u>""",
            reply_markup=kb([[InlineKeyboardButton("Add Account 📱", callback_data="host_account"),
                            InlineKeyboardButton("Back 🔙", callback_data="menu_main")]]),
            parse_mode=ParseMode.HTML
        )
        return
    
    caption = "<blockquote><b>📱 HOSTED ACCOUNTS ✨</b></blockquote>\n\n"
    for i, acc in enumerate(accounts, 1):
        status = "Active ✅" if acc['is_active'] else "Inactive 😔"
        caption += f"{i}. <code>{acc['phone_number']}</code> - <i>{status}</i>\n"
    
    caption += "\n<blockquote><u>Choose an action:</u> 🌟</blockquote>"
    
    await cb.message.edit_caption(
        caption=caption,
        reply_markup=kb([
            [InlineKeyboardButton("Add Account 📱", callback_data="host_account")],
            [InlineKeyboardButton("Back 🔙", callback_data="menu_main")]
        ]),
        parse_mode=ParseMode.HTML
    )

@pyro.on_callback_query(filters.regex("set_msg"))
async def set_msg(client, cb):
    uid = cb.from_user.id
    db.set_user_state(uid, "waiting_broadcast_msg")
    
    await cb.message.edit_media(
        media=InputMediaPhoto(
            media=config.START_IMAGE,
            caption="""<blockquote>📝 <b>SET YOUR AD MESSAGE 🚀</b></blockquote>\n\n"""
                    """<u>Tips for effective ads ✨:</u>\n"""
                    """- <i>Keep it concise and engaging 🌟</i>\n"""
                    """- <b>Use premium emojis for flair 😊</b>\n"""
                    """- <u>Include clear call-to-action 📞</u>\n"""
                    """- <i>Avoid excessive caps or spam words ⚠️</i>\n\n"""
                    """<blockquote>Send your ad message now 🌟:</blockquote>""",
            parse_mode=ParseMode.HTML
        ),
        reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
    )

@pyro.on_callback_query(filters.regex("set_delay"))
async def set_delay(client, cb):
    uid = cb.from_user.id
    current_delay = db.get_user_ad_delay(uid)
    
    await cb.message.edit_media(
        media=InputMediaPhoto(
            media=config.START_IMAGE,
            caption=f"""<blockquote>⏱️ <b>SET BROADCAST CYCLE INTERVAL 🚀</b></blockquote>\n\n"""
                    f"<u>Current Interval:</u> <code>{current_delay} seconds</code> ✨\n\n"
                    f"<b>Recommended Intervals 🌟:</b>\n"
                    f"- <i>300s - Safe & Balanced (5 min) 😊</i>\n"
                    f"- <u>600s - Conservative (10 min) ⚖️</u>\n"
                    f"- <b>120s - Aggressive (2 min) ⚡</b>\n\n"
                    f"<blockquote>Send a number (in seconds) 🌟:</blockquote>",
            parse_mode=ParseMode.HTML
        ),
        reply_markup=kb([
            [InlineKeyboardButton("120s ⚡", callback_data="quick_delay_120"),
             InlineKeyboardButton("300s 😊", callback_data="quick_delay_300"),
             InlineKeyboardButton("600s ⚖️", callback_data="quick_delay_600")],
            [InlineKeyboardButton("Back 🔙", callback_data="menu_main")]
        ])
    )
    db.set_user_state(uid, "waiting_broadcast_delay")

@pyro.on_callback_query(filters.regex("quick_delay_"))
async def quick_delay(client, cb):
    uid = cb.from_user.id
    delay = int(cb.data.split("_")[-1])
    
    try:
        db.set_user_ad_delay(uid, delay)
    except Exception as e:
        logger.error(f"Failed to set ad delay for user {uid}: {e}")
        await cb.answer("Error setting delay. Try again. 😔", show_alert=True)
        return
    
    mode = "Balanced 😊" if delay >= 300 else "Conservative ⚖️" if delay >= 600 else "Aggressive ⚡"
    
    await cb.message.edit_caption(
        caption=f"""<blockquote>✅ <b>CYCLE INTERVAL UPDATED! 🚀</b></blockquote>\n\n"""
                f"<u>New Interval:</u> <code>{delay} seconds</code> ✨\n"
                f"<b>Mode:</b> <i>{mode}</i>\n\n"
                f"<blockquote>Ready for broadcasting! 🌟</blockquote>",
        reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]]),
        parse_mode=ParseMode.HTML
    )
    await send_dm_log(uid, f"<b>⏱️ Broadcast interval updated:</b> {delay} seconds ({mode}) ✨")
    db.set_user_state(uid, "")

@pyro.on_callback_query(filters.regex("start_broadcast"))
async def start_broadcast(client, cb):
    uid = cb.from_user.id
    if db.get_broadcast_state(uid).get("running"):
        await cb.answer("Broadcast already running! 🚀", show_alert=True)
        return
    
    if not db.get_user_ad_messages(uid):
        await cb.answer("Please set an ad message first! 😔", show_alert=True)
        return
    
    accounts = db.get_user_accounts(uid)
    if not accounts:
        await cb.answer("No accounts hosted! 😔", show_alert=True)
        return
    
    current_task = db.get_temp_data(uid)
    if current_task:
        try:
            current_task.cancel()
            logger.info(f"Cancelled previous broadcast for {uid}")
        except Exception as e:
            logger.error(f"Failed to cancel previous broadcast task for {uid}: {e}")
    
    task = asyncio.create_task(run_broadcast(client, uid))
    db.set_temp_data(uid, task)
    db.set_broadcast_state(uid, running=True)
    await cb.answer("Broadcast started! 🚀", show_alert=True)
    await cb.message.edit_caption(
        caption="""<blockquote>🚀 <b>BROADCAST STARTED! ✨</b></blockquote>\n\n"""
                """<u>Your ads are now being sent to targeted groups.</u> 🌟\n"""
                """<i>Check analytics for real-time stats. 📈</i>""",
        reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]]),
        parse_mode=ParseMode.HTML
    )
    await send_dm_log(uid, "<b>🚀 Broadcast started! Logs will come here ✨</b>")

@pyro.on_callback_query(filters.regex("stop_broadcast"))
async def stop_broadcast(client, cb):
    uid = cb.from_user.id
    stopped = await stop_broadcast_task(uid)
    if not stopped:
        await cb.answer("No broadcast running! 😔", show_alert=True)
        return
    
    await cb.answer("Broadcast stopped! ⏹️", show_alert=True)
    await cb.message.edit_caption(
        caption="""<blockquote>⏹️ <b>BROADCAST STOPPED! ✨</b></blockquote>\n\n"""
                """<u>Your broadcast has been stopped.</u> 🌟\n"""
                """<i>Check analytics for final stats. 📈</i>""",
        reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]]),
        parse_mode=ParseMode.HTML
    )
    await send_dm_log(uid, f"<b>⏹️ Broadcast stopped! ✨</b>")
    logger.info(f"Broadcast stopped via callback for user {uid}")

@pyro.on_callback_query(filters.regex("target_groups"))
async def target_groups(client, cb):
    uid = cb.from_user.id
    groups = db.get_target_groups(uid)
    if not groups:
        caption = (
            "<blockquote>🎯 <b>TARGET GROUPS ✨</b></blockquote>\n\n"
            "<u>No groups selected. Send group invite links to add.</u> 🌟\n\n"
        )
    else:
        caption = "<blockquote><b>🎯 TARGET GROUPS ✨</b></blockquote>\n\n"
        for g in groups:
            caption += f"- <i>{g['group_name']}</i>\n"
        caption += "\n<u>Send a group invite link to add more.</u> 🌟"
    db.set_user_state(uid, "waiting_group_link")
    await cb.message.edit_caption(
        caption=caption,
        reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]]),
        parse_mode=ParseMode.HTML
    )

@pyro.on_callback_query(filters.regex("analytics"))
async def analytics(client, cb):
    uid = cb.from_user.id
    user_stats = db.get_user_analytics(uid)
    accounts = db.get_user_accounts(uid)
    logger_failures = len(db.get_logger_failures(uid))
    
    analytics_text = (
        f"<blockquote><b>📈 QUANTUM ANALYTICS ✨</b></blockquote>\n\n"
        f"<u>Total Broadcasts:</u> <code>{user_stats.get('total_broadcasts', 0)}</code> 🚀\n"
        f"<b>Messages Sent:</b> <i>{user_stats.get('total_sent', 0)}</i> 📤\n"
        f"<u>Failed Sends:</u> <code>{user_stats.get('total_failed', 0)}</code> 😔\n"
        f"<b>Vouch Successes:</b> <i>{user_stats.get('vouch_successes', 0)}</i> ✅\n"
        f"<u>Vouch Failures:</u> <code>{user_stats.get('vouch_failures', 0)}</code> 😔\n"
        f"<b>Logger Failures:</b> <i>{logger_failures}</i> 📩\n"
        f"<b>Active Accounts:</b> <i>{len([a for a in accounts if a['is_active']])}</i> ✅\n"
        f"<u>Avg Delay:</u> <code>{db.get_user_ad_delay(uid)}s</code> ⏱️\n\n"
        f"<blockquote>Success Rate: {generate_progress_bar(user_stats.get('total_sent', 0), user_stats.get('total_sent', 0) + user_stats.get('total_failed', 0))} 🌟</blockquote>"
    )
    
    await cb.message.edit_caption(
        caption=analytics_text,
        reply_markup=kb([
            [InlineKeyboardButton("Detailed Report 📊", callback_data="detailed_report")],
            [InlineKeyboardButton("Back 🔙", callback_data="menu_main")]
        ]),
        parse_mode=ParseMode.HTML
    )

@pyro.on_callback_query(filters.regex("detailed_report"))
async def detailed_report(client, cb):
    uid = cb.from_user.id
    user_stats = db.get_user_analytics(uid)
    accounts = db.get_user_accounts(uid)
    logger_failures = db.get_logger_failures(uid)
    
    detailed_text = (
        f"<blockquote><b>📊 DETAILED ANALYTICS REPORT ✨</b></blockquote>\n\n"
        f"<u>Date:</u> <i>{datetime.now().strftime('%d/%m/%y')}</i> 📅\n"
        f"<b>User ID:</b> <code>{uid}</code>\n\n"
        "<b>Broadcast Stats 🚀:</b>\n"
        f"- <u>Total Sent:</u> <code>{user_stats.get('total_sent', 0)}</code> 📤\n"
        f"- <i>Total Failed:</i> <b>{user_stats.get('total_failed', 0)}</b> 😔\n"
        f"- <u>Total Broadcasts:</u> <code>{user_stats.get('total_broadcasts', 0)}</code>\n\n"
        "<b>Vouch Stats 📢:</b>\n"
        f"- <u>Vouch Successes:</u> <code>{user_stats.get('vouch_successes', 0)}</code> ✅\n"
        f"- <i>Vouch Failures:</i> <b>{user_stats.get('vouch_failures', 0)}</b> 😔\n\n"
        "<b>Logger Stats 📩:</b>\n"
        f"- <u>Logger Failures:</u> <code>{len(logger_failures)}</code> 😔\n"
        f"- <i>Last Failure:</i> <b>{logger_failures[-1]['error'] if logger_failures else 'None'}</b>\n\n"
        "<b>Account Stats 📱:</b>\n"
        f"- <i>Total Accounts:</i> <u>{len(accounts)}</u>\n"
        f"- <b>Active Accounts:</b> <code>{len([a for a in accounts if a['is_active']])}</code> ✅\n"
        f"- <u>Inactive Accounts:</u> <i>{len([a for a in accounts if not a['is_active']])}</i> 😔\n\n"
        f"<blockquote><b>Current Delay:</b> <code>{db.get_user_ad_delay(uid)}s</code> ⏱️</blockquote>"
    )
    
    await cb.message.edit_caption(
        caption=detailed_text,
        reply_markup=kb([
            [InlineKeyboardButton("Back 🔙", callback_data="analytics")]
        ]),
        parse_mode=ParseMode.HTML
    )

@pyro.on_message(filters.command("stats") & filters.user(ADMIN_IDS))
async def admin_stats(client, m):
    try:
        stats = db.get_admin_stats()
        
        stats_text = (
            f"<blockquote><b>👑 QUANTUM ADMIN DASHBOARD ✨</b></blockquote>\n\n"
            f"<u>Report Date:</u> <i>{datetime.now().strftime('%d/%m/%y • %I:%M %p')}</i> 📅\n\n"
            "<b>USER STATISTICS 🌟:</b>\n"
            f"- <u>Total Users:</u> <code>{stats.get('total_users', 0)}</code> 👥\n"
            f"- <b>Hosted Accounts:</b> <code>{stats.get('total_accounts', 0)}</code> 📱\n"
            f"- <u>Total Forwards:</u> <i>{stats.get('total_forwards', 0)}</i> 📤\n"
            f"- <b>Active Logger Users:</b> <code>{stats.get('active_logger_users', 0)}</code> 📩\n"
            "<b>VOUCH STATISTICS 📢:</b>\n"
            f"- <u>Vouch Successes:</u> <code>{stats.get('vouch_successes', 0)}</code> ✅\n"
            f"- <b>Vouch Failures:</b> <code>{stats.get('vouch_failures', 0)}</code> 😔\n"
        )
        
        await m.reply_photo(
            photo=config.START_IMAGE,
            caption=stats_text,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await m.reply(f"Error generating stats: {str(e)} 😔", parse_mode=ParseMode.HTML)

@pyro.on_message(filters.command("bd"))
async def admin_broadcast(client, m):
    uid = m.from_user.id
    if not is_owner(uid):
        await m.reply("Admin only command. 😔", parse_mode=ParseMode.HTML)
        return
    
    if not m.reply_to_message:
        await m.reply("Reply to a message to broadcast it. 😔", parse_mode=ParseMode.HTML)
        return
    
    all_users = db.get_all_users()
    if not all_users:
        await m.reply("No users found. 😔", parse_mode=ParseMode.HTML)
        return
    
    status_msg = await m.reply(
        """<blockquote><b>📢 QUANTUM ADMIN BROADCAST 🚀</b></blockquote>\n\n"""
        "<u>Status: Initializing...</u> ✨",
        parse_mode=ParseMode.HTML
    )
    
    sent_count = 0
    failed_count = 0
    total_users = len(all_users)
    
    reply_msg = m.reply_to_message
    media = None
    caption = reply_msg.caption or reply_msg.text or ""
    
    if reply_msg.photo:
        media = reply_msg.photo.file_id
    elif reply_msg.document:
        media = reply_msg.document.file_id
    elif reply_msg.video:
        media = reply_msg.video.file_id
    
    for user in all_users:
        user_id = user['user_id']
        try:
            if media:
                await client.send_photo(
                    chat_id=user_id,
                    photo=media,
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
            else:
                await client.send_message(
                    chat_id=user_id,
                    text=caption,
                    parse_mode=ParseMode.HTML
                )
            sent_count += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to user {user_id}: {e}")
            failed_count += 1
            await send_dm_log(user_id, f"<b>❌ Admin broadcast failed:</b> {str(e)} 😔")
        if (sent_count + failed_count) % 10 == 0 or (sent_count + failed_count) == total_users:
            try:
                await status_msg.edit_text(
                    f"""<blockquote><b>📢 QUANTUM ADMIN BROADCAST 🚀</b></blockquote>\n\n"""
                    f"<u>Status: In Progress...</u> ✨\n"
                    f"<b>Sent:</b> <code>{sent_count}/{total_users}</code>\n"
                    f"<i>Failed:</i> <u>{failed_count}</u>\n"
                    f"<blockquote>Progress: {generate_progress_bar(sent_count + failed_count, total_users)} 🌟</blockquote>",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Failed to update broadcast status: {e}")
        await asyncio.sleep(0.5)
    
    await status_msg.edit_text(
        f"""<blockquote><b>✅ QUANTUM ADMIN BROADCAST COMPLETED ✨</b></blockquote>\n\n"""
        f"<u>Sent:</u> <code>{sent_count}/{total_users}</code>\n"
        f"<b>Failed:</b> <i>{failed_count}</i> 😔\n"
        f"<blockquote>Success Rate: {generate_progress_bar(sent_count, total_users)} 🌟</blockquote>",
        parse_mode=ParseMode.HTML
    )
    await send_dm_log(uid, f"<b>🏁 Admin broadcast completed:</b> Sent {sent_count}/{total_users}, Failed {failed_count} ✨")

@pyro.on_message(filters.command("me"))
async def user_info(client, m):
    uid = m.from_user.id
    user = db.get_user(uid)
    
    if not user:
        await m.reply("You're not registered. Please /start first. 😔", parse_mode=ParseMode.HTML)
        return
    
    accounts_count = db.get_user_accounts_count(uid)
    
    status_text = (
        f"<blockquote><b>🔓 QUANTUM FREE USER ✨</b></blockquote>\n\n"
        f"<u>User ID:</u> <code>{uid}</code>\n"
        f"<b>Username:</b> <i>@{user.get('username', 'N/A')}</i>\n"
        "<blockquote><u>Status: FREE USER 🌟</u></blockquote>\n"
        f"<i>Hosted Accounts:</i> <u>{accounts_count}/5</u> 📱\n"
        f"<b>Logger Active:</b> <i>{'Yes ✅' if db.get_logger_status(uid) else 'No 😔'}</i>\n"
        "<b>Features:</b>\n"
        "- <u>Up to 5 account hosting 📱</u>\n"
        "- <i>Automated broadcasting 🚀</i>\n"
        "- <b>Group targeting 🎯</b>\n"
        "- <u>Real-time analytics 📈</u>\n"
        "- <i>DM logging via logger bot 📩</i>\n"
    )
    
    status_buttons = [
        [InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")],
        [InlineKeyboardButton("Support 💬", url=config.SUPPORT_GROUP_URL)]
    ]
    
    await m.reply_photo(
        photo=config.START_IMAGE,
        caption=status_text,
        reply_markup=InlineKeyboardMarkup(status_buttons),
        parse_mode=ParseMode.HTML
    )

@pyro.on_message(filters.text & filters.regex(r"https?://t\.me/.*") & ~filters.command(["start", "bd", "me", "stats", "stop"]))
async def handle_group_link(client, m):
    uid = m.from_user.id
    state = db.get_user_state(uid)
    if state != "waiting_group_link":
        return
    link = m.text.strip()
    try:
        tg_client = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
        await tg_client.connect()
        chat = await tg_client.get_entity(link)
        db.add_target_group(uid, chat.id, chat.title)
        await m.reply(f"<blockquote><b>✅ Group <i>{chat.title}</i> added! ✨</b></blockquote>", parse_mode=ParseMode.HTML)
        await send_dm_log(uid, f"<b>🎯 Group added:</b> <i>{chat.title}</i> ✨")
        db.set_user_state(uid, "")
        await tg_client.disconnect()
    except Exception as e:
        await m.reply(f"<blockquote><b>❌ Failed to add group:</b> <i>{str(e)}</i> 😔</blockquote>", parse_mode=ParseMode.HTML)
        await send_dm_log(uid, f"<b>❌ Failed to add group:</b> {str(e)} 😔")
        logger.error(f"Failed to add group for {uid}: {e}")

@pyro.on_message(filters.text & ~filters.command(["start", "bd", "me", "stats", "stop"]))
async def handle_text_message(client, m):
    uid = m.from_user.id
    state = db.get_user_state(uid)
    text = m.text.strip()
    
    if state == "waiting_broadcast_msg":
        try:
            db.add_user_ad_message(uid, text, datetime.now())
            db.set_user_state(uid, "")
            await m.reply(
                f"<blockquote><b>✅ AD MESSAGE SET! 🚀</b></blockquote>\n\n"
                f"<u>Message Preview:</u>\n<code>{text}</code>\n\n"
                f"<b>Ready to broadcast! 🌟</b>\n"
                f"<i>Start your campaign from the dashboard.</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>📝 Ad message updated:</b> <code>{text[:50]}{'...' if len(text) > 50 else ''}</code> ✨")
            logger.info(f"Ad message set for user {uid}: {text[:50]}...")
        except Exception as e:
            logger.error(f"Failed to add ad message for user {uid}: {e}")
            db.set_user_state(uid, "")
            await m.reply(
                f"<blockquote><b>❌ Failed to save ad message! 😔</b></blockquote>\n\n"
                f"<u>Error:</u> <i>{str(e)}</i>\n"
                f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>❌ Failed to set ad message:</b> {str(e)} 😔")
    elif state == "waiting_broadcast_delay":
        try:
            delay = int(text)
            if delay < 60:
                await m.reply(
                    f"<blockquote><b>❌ Invalid interval! 😔</b></blockquote>\n\n"
                    f"<u>Minimum interval is 60 seconds.</u> 🌟\n"
                    f"<i>Please enter a valid number.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
                )
                return
            db.set_user_ad_delay(uid, delay)
            db.set_user_state(uid, "")
            mode = "Conservative ⚖️" if delay >= 600 else "Balanced 😊" if delay >= 300 else "Aggressive ⚡"
            await m.reply(
                f"<blockquote><b>✅ CYCLE INTERVAL UPDATED! 🚀</b></blockquote>\n\n"
                f"<u>New Interval:</u> <code>{delay} seconds</code> ✨\n"
                f"<b>Mode:</b> <i>{mode}</i>\n\n"
                f"<blockquote>Ready for broadcasting! 🌟</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>⏱️ Broadcast interval updated:</b> {delay} seconds ({mode}) ✨")
            logger.info(f"Broadcast delay set for user {uid}: {delay}s")
        except ValueError:
            await m.reply(
                f"<blockquote><b>❌ Invalid input! 😔</b></blockquote>\n\n"
                f"<u>Please enter a number (in seconds).</u> 🌟\n"
                f"<i>Example: <code>300</code> for 5 minutes.</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
            )
        except Exception as e:
            logger.error(f"Failed to set broadcast delay for user {uid}: {e}")
            db.set_user_state(uid, "")
            await m.reply(
                f"<blockquote><b>❌ Failed to set interval! 😔</b></blockquote>\n\n"
                f"<u>Error:</u> <i>{str(e)}</i>\n"
                f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>❌ Failed to set broadcast interval:</b> {str(e)} 😔")
    elif state == "telethon_wait_phone":
        if not validate_phone_number(text):
            await m.reply(
                f"<blockquote><b>❌ Invalid phone number! 😔</b></blockquote>\n\n"
                f"<u>Please use international format.</u> 🌟\n"
                f"<i>Example: <code>+1234567890</code></i>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
            )
            return
        status_msg = await m.reply(
            f"<blockquote><b>⏳ Hold! We’re trying to send OTP... 🚀</b></blockquote>\n\n"
            f"<u>Phone:</u> <code>{text}</code> ✨\n"
            f"<i>Please wait a moment.</i> 🌟",
            parse_mode=ParseMode.HTML
        )
        try:
            tg = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
            await tg.connect()
            sent_code = await tg.send_code_request(text)
            session_str = tg.session.save()
            session_encrypted = cipher_suite.encrypt(session_str.encode()).decode()
            db.set_user_state(uid, "telethon_wait_otp")
            db.set_temp_data(uid, f"{text}::{session_encrypted}::{sent_code.phone_code_hash}")
            await status_msg.edit(
                f"<blockquote><b>✅ OTP sent to <code>{text}</code>! 🚀</b></blockquote>\n\n"
                f"<u>Enter the OTP using the keypad below ✨</u>\n"
                f"<b>Current:</b> <code>_____</code>\n"
                f"<b>Format:</b> <code>12345</code> (no spaces needed) 🌟\n"
                f"<i>Valid for:</i> <u>{config.OTP_EXPIRY // 60} minutes</u>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_otp_keyboard()
            )
            await send_dm_log(uid, f"<b>📱 OTP requested for phone number:</b> <code>{text}</code> ✨")
        except PhoneNumberInvalidError:
            await status_msg.edit(
                f"<blockquote><b>❌ Invalid phone number! 😔</b></blockquote>\n\n"
                f"<u>Please check the number and try again.</u> 🌟",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
            )
        except Exception as e:
            logger.error(f"Failed to send OTP for {uid}: {e}")
            db.set_user_state(uid, "")
            await status_msg.edit(
                f"<blockquote><b>❌ Failed to send OTP! 😔</b></blockquote>\n\n"
                f"<u>Error:</u> <i>{str(e)}</i>\n"
                f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>❌ Failed to send OTP for phone:</b> {str(e)} 😔")
        finally:
            await tg.disconnect()
    elif state == "telethon_wait_password":
        temp_data = db.get_temp_data(uid)
        if not temp_data:
            await m.reply(
                f"<blockquote><b>❌ Session expired! 😔</b></blockquote>\n\n"
                f"<u>Please restart the process.</u> 🌟",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
            )
            db.set_user_state(uid, "")
            return
        phone, session_str = temp_data.split("::")
        tg = TelegramClient(StringSession(session_str), config.API_ID, config.API_HASH)
        try:
            await tg.connect()
            await tg.sign_in(password=text)
            session_encrypted = cipher_suite.encrypt(session_str.encode()).decode()
            db.add_user_account(uid, phone, session_encrypted)
            await m.reply(
                f"<blockquote><b>✅ Account added! 🚀</b></blockquote>\n\n"
                f"<u>Phone:</u> <code>{phone}</code> ✨\n"
                "<i>Account is ready for broadcasting! 🌟</i>\n"
                "<i>Note: Profile bio and name will be updated during the first broadcast.</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>✅ Account added successfully:</b> <code>{phone}</code> ✨")
            db.set_user_state(uid, "")
            db.set_temp_data(uid, None)
        except PasswordHashInvalidError:
            await m.reply(
                f"<blockquote><b>❌ Invalid password! 😔</b></blockquote>\n\n"
                f"<u>Please try again.</u> 🌟",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
            )
        except Exception as e:
            logger.error(f"Failed to sign in with password for {uid}: {e}")
            db.set_user_state(uid, "")
            db.set_temp_data(uid, None)
            await m.reply(
                f"<blockquote><b>❌ Login failed! 😔</b></blockquote>\n\n"
                f"<u>Error:</u> <i>{str(e)}</i>\n"
                f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
            )
            await send_dm_log(uid, f"<b>❌ Account login failed:</b> {str(e)} 😔")
        finally:
            await tg.disconnect()
    else:
        await m.reply(
            f"<blockquote><b>🚀 Welcome back! ✨</b></blockquote>\n\n"
            f"<u>Use the dashboard to manage your campaigns.</u> 🌟",
            parse_mode=ParseMode.HTML,
            reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
        )

# Run both bots
async def main():
    await pyro.start()
    await logger_client.start()
    await idle()

if __name__ == "__main__":
    pyro.run(main())