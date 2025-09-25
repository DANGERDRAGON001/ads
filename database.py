import logging
from datetime import datetime, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
import config
from bson.objectid import ObjectId
import time
import json

# Logging setup - INFO only, no DEBUG spam for clean logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/QUANTUM_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedDatabaseManager:
    def __init__(self):
        self.client = None
        self.db = None
        self._init_db()
        self._load_persistent_globals()

    def _init_db(self):
        """Initialize MongoDB connection with exponential backoff retries and robust index handling."""
        max_retries = 3
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                self.client = pymongo.MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
                self.client.admin.command('ping')
                self.db = self.client[config.DB_NAME]
                logger.info("MongoDB initialized successfully")

                # Helper function to ensure index with specific options
                def ensure_index(collection, key, **kwargs):
                    # Handle single or compound keys
                    index_key = key if isinstance(key, list) else [(key, pymongo.ASCENDING)]
                    # Generate default index name based on MongoDB convention
                    index_name = "_".join(f"{k}_{v}" for k, v in index_key)
                    try:
                        existing_indexes = collection.index_information()
                        if index_name in existing_indexes:
                            existing_unique = existing_indexes[index_name].get('unique', False)
                            desired_unique = kwargs.get('unique', False)
                            if existing_unique != desired_unique:
                                collection.drop_index(index_name)
                                logger.info(f"Dropped conflicting index {index_name} on {collection.name}")
                            else:
                                logger.info(f"Index {index_name} on {collection.name} already exists with correct specs")
                                return
                        collection.create_index(key, name=index_name, **kwargs)
                        logger.info(f"Created index {index_name} on {collection.name}")
                    except OperationFailure as e:
                        logger.error(f"Failed to create index {index_name} on {collection.name}: {e}")
                        raise

                # Create indexes for efficient queries
                ensure_index(self.db.users, "user_id", unique=True)
                ensure_index(self.db.accounts, [("user_id", pymongo.ASCENDING), ("phone_number", pymongo.ASCENDING)])
                ensure_index(self.db.ad_messages, "user_id")
                ensure_index(self.db.ad_delays, "user_id", unique=True)
                ensure_index(self.db.broadcast_states, "user_id", unique=True)
                ensure_index(self.db.target_groups, [("user_id", pymongo.ASCENDING), ("group_id", pymongo.ASCENDING)])
                ensure_index(self.db.analytics, "user_id", unique=True)
                ensure_index(self.db.broadcast_logs, "user_id")
                ensure_index(self.db.broadcast_activity, "user_id")
                ensure_index(self.db.temp_data, [("user_id", pymongo.ASCENDING), ("key", pymongo.ASCENDING)], unique=True)
                ensure_index(self.db.logger_status, "user_id", unique=True)
                ensure_index(self.db.logger_failures, "user_id")
                return
            except ConnectionFailure as e:
                logger.error(f"MongoDB connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("Max retries reached for MongoDB connection")
                    raise
            except OperationFailure as e:
                logger.error(f"Failed to initialize MongoDB: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error during MongoDB init: {e}")
                raise

    def _load_persistent_globals(self):
        """Load persistent user data like ad messages, delays, broadcast states from DB."""
        try:
            ad_msgs = self.db.ad_messages.find({}, {"user_id": 1, "message": 1})
            for doc in ad_msgs:
                logger.info(f"Loaded ad msg for {doc['user_id']}")
            delays = self.db.ad_delays.find({}, {"user_id": 1, "delay": 1})
            for doc in delays:
                logger.info(f"Loaded delay {doc['delay']}s for {doc['user_id']}")
            states = self.db.broadcast_states.find({}, {"user_id": 1, "paused": 1, "running": 1})
            for doc in states:
                logger.info(f"Loaded broadcast state for {doc['user_id']}: running={doc.get('running', False)}")
            logger_statuses = self.db.logger_status.find({}, {"user_id": 1, "is_active": 1})
            for doc in logger_statuses:
                logger.info(f"Loaded logger status for {doc['user_id']}: is_active={doc.get('is_active', False)}")
        except Exception as e:
            logger.error(f"Failed to load persistent globals: {e}")

    def create_user(self, user_id, username, first_name):
        """Create or update a user with fixed 5-account limit and vouch tracking."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "username": username or "Unknown",
                        "first_name": first_name or "User",
                        "last_interaction": datetime.now()
                    },
                    "$setOnInsert": {
                        "created_at": datetime.now(),
                        "accounts_limit": 5,
                        "has_joined_vouch": False,
                        "state": ""
                    }
                },
                upsert=True
            )
            logger.info(f"User created/updated: {user_id}")
        except Exception as e:
            logger.error(f"Failed to create user {user_id}: {e}")
            raise

    def get_user(self, user_id):
        """Fetch user data."""
        try:
            user = self.db.users.find_one({"user_id": user_id})
            return user if user else None
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
            return None

    def update_user_last_interaction(self, user_id):
        """Update user's last interaction timestamp."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"last_interaction": datetime.now()}}
            )
        except Exception as e:
            logger.error(f"Failed to update last interaction for {user_id}: {e}")
            raise

    def has_vouch_sent(self, user_id):
        """Check if vouch message has been sent for a user."""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"has_joined_vouch": 1})
            return user.get("has_joined_vouch", False) if user else False
        except Exception as e:
            logger.error(f"Failed to check vouch status for {user_id}: {e}")
            return False

    def set_vouch_sent(self, user_id):
        """Mark vouch message as sent for a user."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"has_joined_vouch": True}}
            )
            logger.info(f"Vouch marked as sent for {user_id}")
        except Exception as e:
            logger.error(f"Failed to set vouch sent for {user_id}: {e}")
            raise

    def get_user_accounts(self, user_id):
        """Fetch all accounts for a user."""
        try:
            return list(self.db.accounts.find({"user_id": user_id}))
        except Exception as e:
            logger.error(f"Failed to get accounts for {user_id}: {e}")
            return []

    def get_user_accounts_count(self, user_id):
        """Count user's accounts."""
        try:
            return self.db.accounts.count_documents({"user_id": user_id})
        except Exception as e:
            logger.error(f"Failed to count accounts for {user_id}: {e}")
            return 0

    def add_user_account(self, user_id, phone_number, session_string, **kwargs):
        """Add a user account with fixed 5-account limit enforcement."""
        try:
            accounts_count = self.get_user_accounts_count(user_id)
            if accounts_count >= 5:
                logger.warning(f"Account limit exceeded for {user_id}: {accounts_count}/5")
                return False
            first_name = kwargs.get('first_name', '')
            last_name = kwargs.get('last_name', '')
            self.db.accounts.insert_one({
                "user_id": user_id,
                "phone_number": phone_number,
                "session_string": session_string,
                "first_name": first_name,
                "last_name": last_name,
                "is_active": True,
                "created_at": datetime.now()
            })
            logger.info(f"Account added for user {user_id}: {phone_number}")
            return True
        except Exception as e:
            logger.error(f"Failed to add account for {user_id}: {e}")
            return False

    def deactivate_account(self, account_id):
        """Deactivate an account."""
        try:
            self.db.accounts.update_one(
                {"_id": ObjectId(account_id)},
                {"$set": {"is_active": False, "updated_at": datetime.now()}}
            )
            logger.info(f"Deactivated account {account_id}")
        except Exception as e:
            logger.error(f"Failed to deactivate account {account_id}: {e}")
            raise

    def get_user_ad_messages(self, user_id):
        """Fetch user's ad messages."""
        try:
            return list(self.db.ad_messages.find({"user_id": user_id}, sort=[("created_at", -1)]))
        except Exception as e:
            logger.error(f"Failed to get ad messages for {user_id}: {e}")
            return []

    def add_user_ad_message(self, user_id, message, created_at):
        """Add an ad message for a user."""
        try:
            self.db.ad_messages.update_one(
                {"user_id": user_id},
                {"$set": {"message": message, "created_at": created_at, "updated_at": datetime.now()}},
                upsert=True
            )
            logger.info(f"Ad message added for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to add ad message for {user_id}: {e}")
            raise

    def get_user_ad_delay(self, user_id):
        """Get user's ad delay."""
        try:
            doc = self.db.ad_delays.find_one({"user_id": user_id}, {"delay": 1})
            return doc.get("delay", 300) if doc else 300
        except Exception as e:
            logger.error(f"Failed to get ad delay for {user_id}: {e}")
            return 300

    def set_user_ad_delay(self, user_id, delay):
        """Set user's ad delay."""
        try:
            self.db.ad_delays.update_one(
                {"user_id": user_id},
                {"$set": {"delay": delay, "updated_at": datetime.now()}},
                upsert=True
            )
            logger.info(f"Ad delay set for {user_id}: {delay}s")
        except Exception as e:
            logger.error(f"Failed to set ad delay for {user_id}: {e}")
            raise

    def get_broadcast_state(self, user_id):
        """Get user's broadcast state."""
        try:
            doc = self.db.broadcast_states.find_one({"user_id": user_id}, {"running": 1, "paused": 1})
            return doc if doc else {"running": False, "paused": False}
        except Exception as e:
            logger.error(f"Failed to get broadcast state for {user_id}: {e}")
            return {"running": False, "paused": False}

    def set_broadcast_state(self, user_id, running=False, paused=False):
        """Set user's broadcast state."""
        try:
            self.db.broadcast_states.update_one(
                {"user_id": user_id},
                {"$set": {"running": running, "paused": paused, "updated_at": datetime.now()}},
                upsert=True
            )
            logger.info(f"Broadcast state updated for {user_id}: running={running}, paused={paused}")
        except Exception as e:
            logger.error(f"Failed to set broadcast state for {user_id}: {e}")
            raise

    def increment_broadcast_cycle(self, user_id):
        """Increment the broadcast cycle count for a user."""
        try:
            self.db.analytics.update_one(
                {"user_id": user_id},
                {
                    "$inc": {"total_cycles": 1},
                    "$set": {"updated_at": datetime.now()}
                },
                upsert=True
            )
            logger.info(f"Incremented broadcast cycle for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to increment broadcast cycle for {user_id}: {e}")
            raise

    def get_target_groups(self, user_id):
        """Fetch user's target groups."""
        try:
            return list(self.db.target_groups.find({"user_id": user_id}))
        except Exception as e:
            logger.error(f"Failed to get target groups for {user_id}: {e}")
            return []

    def add_target_group(self, user_id, group_id, group_name):
        """Add a target group for a user."""
        try:
            self.db.target_groups.update_one(
                {"user_id": user_id, "group_id": group_id},
                {
                    "$set": {
                        "group_name": group_name,
                        "created_at": datetime.now(),
                        "updated_at": datetime.now()
                    }
                },
                upsert=True
            )
            logger.info(f"Target group {group_name} added for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to add target group for {user_id}: {e}")
            raise

    def get_user_analytics(self, user_id):
        """Fetch analytics for a user."""
        try:
            stats = self.db.analytics.find_one({"user_id": user_id})
            return stats if stats else {
                "total_broadcasts": 0,
                "total_sent": 0,
                "total_failed": 0,
                "total_cycles": 0,
                "vouch_successes": 0,
                "vouch_failures": 0
            }
        except Exception as e:
            logger.error(f"Failed to get analytics for {user_id}: {e}")
            return {
                "total_broadcasts": 0,
                "total_sent": 0,
                "total_failed": 0,
                "total_cycles": 0,
                "vouch_successes": 0,
                "vouch_failures": 0
            }

    def increment_broadcast_stats(self, user_id, success):
        """Increment broadcast stats for a user."""
        try:
            field = "total_sent" if success else "total_failed"
            self.db.analytics.update_one(
                {"user_id": user_id},
                {
                    "$inc": {field: 1, "total_broadcasts": 1},
                    "$set": {"updated_at": datetime.now()}
                },
                upsert=True
            )
            logger.info(f"Updated broadcast stats for user {user_id}: {field}+1")
        except Exception as e:
            logger.error(f"Failed to update broadcast stats for {user_id}: {e}")
            raise

    def increment_vouch_success(self, channel_id):
        """Increment vouch success count."""
        try:
            self.db.analytics.update_one(
                {"channel_id": channel_id},
                {
                    "$inc": {"vouch_successes": 1},
                    "$set": {"updated_at": datetime.now()}
                },
                upsert=True
            )
            logger.info(f"Incremented vouch success for channel {channel_id}")
        except Exception as e:
            logger.error(f"Failed to increment vouch success for {channel_id}: {e}")
            raise

    def increment_vouch_failure(self, channel_id, error):
        """Increment vouch failure count."""
        try:
            self.db.analytics.update_one(
                {"channel_id": channel_id},
                {
                    "$inc": {"vouch_failures": 1},
                    "$set": {"updated_at": datetime.now(), "last_error": str(error)}
                },
                upsert=True
            )
            logger.info(f"Incremented vouch failure for channel {channel_id}: {error}")
        except Exception as e:
            logger.error(f"Failed to increment vouch failure for {channel_id}: {e}")
            raise

    def log_broadcast(self, user_id, message, accounts_count, groups_count, sent_count, failed_count, status):
        """Log a broadcast event."""
        try:
            self.db.broadcast_logs.insert_one({
                "user_id": user_id,
                "message": message,
                "accounts_count": accounts_count,
                "groups_count": groups_count,
                "sent_count": sent_count,
                "failed_count": failed_count,
                "status": status,
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            })
            logger.info(f"Broadcast logged for user {user_id}: {status}")
        except Exception as e:
            logger.error(f"Failed to log broadcast for {user_id}: {e}")
            raise

    def update_broadcast_log(self, user_id, sent_count, failed_count, status):
        """Update broadcast log."""
        try:
            self.db.broadcast_logs.update_one(
                {"user_id": user_id, "status": "running"},
                {
                    "$set": {
                        "sent_count": sent_count,
                        "failed_count": failed_count,
                        "status": status,
                        "updated_at": datetime.now()
                    }
                }
            )
            logger.info(f"Broadcast log updated for user {user_id}: {status}")
        except Exception as e:
            logger.error(f"Failed to update broadcast log for {user_id}: {e}")
            raise

    def log_broadcast_activity(self, user_id, sent_count, failed_count):
        """Log broadcast activity."""
        try:
            self.db.broadcast_activity.insert_one({
                "user_id": user_id,
                "sent_count": sent_count,
                "failed_count": failed_count,
                "timestamp": datetime.now()
            })
            logger.info(f"Broadcast activity logged for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to log broadcast activity for {user_id}: {e}")
            raise

    def get_all_users(self, page=0, limit=100):
        """Fetch all users with pagination."""
        try:
            skip = page * limit
            return list(self.db.users.find({}).skip(skip).limit(limit))
        except Exception as e:
            logger.error(f"Failed to get all users: {e}")
            return []

    def get_admin_stats(self):
        """Fetch admin statistics."""
        try:
            total_users = self.db.users.count_documents({})
            total_accounts = self.db.accounts.count_documents({})
            forwards_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$total_sent"}}}]
            forwards_doc = list(self.db.analytics.aggregate(forwards_pipeline))
            total_forwards = forwards_doc[0]["total"] if forwards_doc else 0
            logger_stats = self.db.logger_status.count_documents({"is_active": True})
            vouch_stats = self.db.analytics.aggregate([
                {"$group": {"_id": None, "vouch_successes": {"$sum": "$vouch_successes"}, "vouch_failures": {"$sum": "$vouch_failures"}}}
            ])
            vouch_data = list(vouch_stats)[0] if list(vouch_stats) else {"vouch_successes": 0, "vouch_failures": 0}
            return {
                "total_users": total_users,
                "total_forwards": total_forwards,
                "total_accounts": total_accounts,
                "active_logger_users": logger_stats,
                "vouch_successes": vouch_data["vouch_successes"],
                "vouch_failures": vouch_data["vouch_failures"]
            }
        except Exception as e:
            logger.error(f"Failed to get admin stats: {e}")
            return {}

    def set_user_state(self, user_id, state):
        """Set user state for conversation flow."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"state": state, "updated_at": datetime.now()}}
            )
            logger.info(f"Set user state for {user_id}: {state}")
        except Exception as e:
            logger.error(f"Failed to set user state for {user_id}: {e}")
            raise

    def get_user_state(self, user_id):
        """Get user state."""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"state": 1})
            return user.get("state", "") if user else ""
        except Exception as e:
            logger.error(f"Failed to get user state for {user_id}: {e}")
            return ""

    def set_temp_data(self, user_id, data):
        """Set temporary data for user in temp_data collection."""
        try:
            self.db.temp_data.update_one(
                {"user_id": user_id, "key": "session"},
                {"$set": {"value": data, "updated_at": datetime.now()}},
                upsert=True
            )
            logger.info(f"Set temp data for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to set temp data for user {user_id}: {e}")
            raise

    def get_temp_data(self, user_id):
        """Get temporary data for user from temp_data collection."""
        try:
            doc = self.db.temp_data.find_one({"user_id": user_id, "key": "session"}, {"value": 1})
            return doc.get("value") if doc else None
        except Exception as e:
            logger.error(f"Failed to get temp data for user {user_id}: {e}")
            return None

    def set_user_temp_data(self, user_id, key, value):
        """Set temporary data for a user with a specific key (for future use)."""
        try:
            self.db.temp_data.update_one(
                {"user_id": user_id, "key": key},
                {"$set": {"value": json.dumps(value), "updated_at": datetime.now()}},
                upsert=True
            )
            logger.info(f"Set temp data for user {user_id}, key: {key}")
        except Exception as e:
            logger.error(f"Failed to set temp data for user {user_id}, key: {key}: {e}")
            raise

    def get_user_temp_data(self, user_id, key):
        """Get temporary data for a user with a specific key (for future use)."""
        try:
            doc = self.db.temp_data.find_one({"user_id": user_id, "key": key}, {"value": 1})
            return json.loads(doc.get("value")) if doc and doc.get("value") else None
        except Exception as e:
            logger.error(f"Failed to get temp data for user {user_id}, key: {key}: {e}")
            return None

    def set_logger_status(self, user_id, is_active=True):
        """Mark if user has started the logger bot."""
        try:
            self.db.logger_status.update_one(
                {"user_id": user_id},
                {"$set": {"is_active": is_active, "updated_at": datetime.now()}},
                upsert=True
            )
            logger.info(f"Logger status set for {user_id}: is_active={is_active}")
        except Exception as e:
            logger.error(f"Failed to set logger status for {user_id}: {e}")
            raise

    def get_logger_status(self, user_id):
        """Check if user has started the logger bot."""
        try:
            doc = self.db.logger_status.find_one({"user_id": user_id}, {"is_active": 1})
            return doc.get("is_active", False) if doc else False
        except Exception as e:
            logger.error(f"Failed to get logger status for {user_id}: {e}")
            return False

    def log_logger_failure(self, user_id, error):
        """Log a failure when sending a DM via logger bot."""
        try:
            self.db.logger_failures.insert_one({
                "user_id": user_id,
                "error": str(error),
                "timestamp": datetime.now()
            })
            logger.info(f"Logged logger failure for user {user_id}: {error}")
        except Exception as e:
            logger.error(f"Failed to log logger failure for {user_id}: {e}")
            raise

    def get_logger_failures(self, user_id):
        """Fetch logger failure stats for a user."""
        try:
            return list(self.db.logger_failures.find({"user_id": user_id}))
        except Exception as e:
            logger.error(f"Failed to get logger failures for {user_id}: {e}")
            return []

    def close(self):
        """Close MongoDB connection."""
        try:
            if self.client:
                self.client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Failed to close MongoDB connection: {e}")
            raise