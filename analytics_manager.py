import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
from pyrogram import Client
from pyrogram.enums import ParseMode
import config  # Assume config has TECH_LOG_CHANNEL_ID

logger = logging.getLogger(__name__)

class AnalyticsManager:
    """Manages comprehensive analytics and reporting for LUXXAD bot with async operations and persistent storage."""
    
    def __init__(self, db, client: Client):
        self.db = db
        self.client = client
        
    async def generate_user_report(self, user_id: int) -> Dict:
        """Generate comprehensive user analytics report asynchronously."""
        try:
            user = await self.db.get_user(user_id)
            if not user:
                logger.error(f"User {user_id} not found for report generation")
                return {}
            
            analytics = await self.db.get_user_analytics(user_id)
            accounts = await self.db.get_user_accounts(user_id)
            premium_info = await self.db.get_user_premium_info(user_id)
            
            # Calculate success rate
            total_sent = analytics.get('total_sent', 0)
            total_failed = analytics.get('total_failed', 0)
            total_messages = total_sent + total_failed
            success_rate = (total_sent / total_messages * 100) if total_messages > 0 else 0
            
            # Calculate account performance
            active_accounts = len([acc for acc in accounts if acc.get('is_active', False)])
            
            report = {
                'user_info': {
                    'user_id': user_id,
                    'username': user.get('username', 'N/A'),
                    'first_name': user.get('first_name', 'User'),
                    'member_since': user.get('created_at'),
                    'last_active': user.get('updated_at')
                },
                'subscription': {
                    'is_premium': premium_info.get('is_premium', False),
                    'plan': premium_info.get('premium_plan', None),
                    'expires_at': premium_info.get('premium_expires_at', None),
                    'accounts_limit': premium_info.get('accounts_limit', 0),
                    'duration_type': premium_info.get('duration_type', 'weekly')  # Enhanced for weekly/monthly plans
                },
                'broadcasting': {
                    'total_broadcasts': analytics.get('total_broadcasts', 0),
                    'total_sent': total_sent,
                    'total_failed': total_failed,
                    'success_rate': success_rate,
                    'performance_grade': self.calculate_performance_grade(success_rate, total_sent)
                },
                'accounts': {
                    'total_accounts': len(accounts),
                    'active_accounts': active_accounts
                },
                'generated_at': datetime.now().isoformat()
            }
            
            await self.log_tech(f"Generated user report for {user_id}: {json.dumps(report, default=str)}")
            return report
            
        except Exception as e:
            logger.error(f"Error generating user report for {user_id}: {e}")
            await self.log_tech(f"Error generating user report for {user_id}: {str(e)}", "ERROR")
            return {}
    
    def calculate_performance_grade(self, success_rate: float, total_sent: int) -> str:
        """Calculate performance grade based on success rate and volume."""
        if total_sent == 0:
            return "No Data"
        elif success_rate >= 95 and total_sent >= 1000:
            return "A+ Elite"
        elif success_rate >= 90 and total_sent >= 500:
            return "A Excellent"
        elif success_rate >= 80 and total_sent >= 100:
            return "B+ Good"
        elif success_rate >= 70:
            return "B Average"
        elif success_rate >= 50:
            return "C Below Average"
        else:
            return "D Poor"
    
    async def generate_admin_dashboard(self) -> Dict:
        """Generate comprehensive admin dashboard data asynchronously."""
        try:
            # Use existing DB methods; add new ones if needed (e.g., get_total_users via get_admin_stats)
            admin_stats = await self.db.get_admin_stats()
            total_users = admin_stats.get('total_users', 0)
            premium_users = admin_stats.get('premium_users', 0)
            trial_users = admin_stats.get('trial_users', 0)
            total_broadcasts = admin_stats.get('total_broadcasts', 0)  # Assume added to get_admin_stats
            total_sent = admin_stats.get('total_sent', 0)
            total_failed = admin_stats.get('total_failed', 0)
            
            # Calculate additional metrics
            conversion_rate = (premium_users / total_users * 100) if total_users > 0 else 0
            
            dashboard = {
                'overview': {
                    'total_users': total_users,
                    'premium_users': premium_users,
                    'trial_users': trial_users,
                    'conversion_rate': conversion_rate
                },
                'broadcasting': {
                    'total_broadcasts': total_broadcasts,
                    'total_sent': total_sent,
                    'total_failed': total_failed
                },
                'generated_at': datetime.now().isoformat()
            }
            
            await self.log_tech(f"Generated admin dashboard: {json.dumps(dashboard)}")
            return dashboard
            
        except Exception as e:
            logger.error(f"Error generating admin dashboard: {e}")
            await self.log_tech(f"Error generating admin dashboard: {str(e)}", "ERROR")
            return {}
    
    async def generate_performance_insights(self, user_id: int) -> List[str]:
        """Generate performance insights and recommendations asynchronously."""
        try:
            analytics = await self.db.get_user_analytics(user_id)
            accounts = await self.db.get_user_accounts(user_id)
            
            insights = []
            total_sent = analytics.get('total_sent', 0)
            total_failed = analytics.get('total_failed', 0)
            success_rate = self.calculate_success_rate(total_sent, total_failed)
            
            # Success rate insights
            if success_rate < 70:
                insights.append("Consider increasing delays between messages to improve success rate")
            elif success_rate > 90:
                insights.append("Excellent success rate. Your settings are optimized")
            
            # Account insights
            if len(accounts) == 1:
                insights.append("Consider hosting additional accounts for better distribution")
            elif len(accounts) > 5:
                insights.append("Great account diversity. This improves delivery reliability")
            
            # Volume insights
            if total_sent < 100:
                insights.append("Increase broadcasting frequency to reach more audiences")
            elif total_sent > 1000:
                insights.append("High volume broadcaster. Consider premium features for optimization")
            
            await self.log_tech(f"Generated insights for user {user_id}: {insights}")
            return insights
            
        except Exception as e:
            logger.error(f"Error generating insights for {user_id}: {e}")
            await self.log_tech(f"Error generating insights for {user_id}: {str(e)}", "ERROR")
            return []
    
    def calculate_success_rate(self, sent: int, failed: int) -> float:
        """Calculate success rate percentage."""
        total = sent + failed
        return (sent / total * 100) if total > 0 else 0
    
    def format_analytics_for_display(self, analytics: Dict) -> str:
        """Format analytics data for Telegram display."""
        try:
            broadcasting = analytics.get('broadcasting', {})
            accounts = analytics.get('accounts', {})
            
            return (
                f"> **LUXXAD ANALYTICS REPORT**\n\n"
                f"• **Broadcasts:** {broadcasting.get('total_broadcasts', 0):,}\n"
                f"• **Sent:** {broadcasting.get('total_sent', 0):,}\n"
                f"• **Failed:** {broadcasting.get('total_failed', 0):,}\n"
                f"• **Success Rate:** {broadcasting.get('success_rate', 0):.1f}%\n"
                f"• **Grade:** {broadcasting.get('performance_grade', 'N/A')}\n\n"
                f"• **Active Accounts:** {accounts.get('active_accounts', 0)}\n"
            )
            
        except Exception as e:
            logger.error(f"Error formatting analytics: {e}")
            asyncio.create_task(self.log_tech(f"Error formatting analytics: {str(e)}", "ERROR"))
            return "> **Error generating analytics report**"
    
    async def export_user_data(self, user_id: int, format_type: str = 'json') -> Optional[str]:
        """Export user data in specified format asynchronously."""
        try:
            report = await self.generate_user_report(user_id)
            if not report:
                return None
            
            if format_type == 'json':
                data = json.dumps(report, indent=2, default=str)
                await self.log_tech(f"Exported user data for {user_id} in JSON format")
                return data
            elif format_type == 'csv':
                data = self.convert_to_csv(report)
                await self.log_tech(f"Exported user data for {user_id} in CSV format")
                return data
            else:
                await self.log_tech(f"Invalid export format requested for user {user_id}: {format_type}", "ERROR")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting user data for {user_id}: {e}")
            await self.log_tech(f"Error exporting user data for {user_id}: {str(e)}", "ERROR")
            return None
    
    def convert_to_csv(self, data: Dict) -> str:
        """Convert analytics data to CSV format."""
        try:
            lines = ["Metric,Value"]
            for section, values in data.items():
                if isinstance(values, dict):
                    for key, value in values.items():
                        lines.append(f"{section}_{key},{value}")
                else:
                    lines.append(f"{section},{values}")
            return "\n".join(lines)
            
        except Exception as e:
            logger.error(f"Error converting to CSV: {e}")
            asyncio.create_task(self.log_tech(f"Error converting analytics to CSV: {str(e)}", "ERROR"))
            return ""
    
    async def schedule_analytics_cleanup(self):
        """Schedule cleanup of old analytics data using Postgres queries."""
        try:
            cutoff_date = datetime.now() - timedelta(days=90)
            async with self.db.pool.acquire() as conn:  # Direct pool access for cleanup
                await conn.execute("""
                    DELETE FROM user_analytics 
                    WHERE date < $1
                """, cutoff_date.date())
                await conn.execute("""
                    DELETE FROM broadcast_logs 
                    WHERE started_at < $1
                """, cutoff_date)
            await self.log_tech("Analytics cleanup completed: Removed data older than 90 days")
            
        except Exception as e:
            logger.error(f"Analytics cleanup error: {e}")
            await self.log_tech(f"Analytics cleanup error: {str(e)}", "ERROR")
    
    async def log_tech(self, message: str, level: str = "INFO"):
        """Log message to config.TECH_LOG_CHANNEL_ID asynchronously."""
        try:
            formatted_msg = f"<b>{level} LOG</b>\n\n{message}\n\n<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST</i>"
            await self.client.send_message(
                chat_id=config.TECH_LOG_CHANNEL_ID,
                text=formatted_msg,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to send tech log: {e}")