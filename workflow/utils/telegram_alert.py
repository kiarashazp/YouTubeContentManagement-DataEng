from telegram import Bot
import logging
import asyncio

# Set up logging
logger = logging.getLogger(__name__)

# Fetch Telegram credentials from Airflow Variables
# TELEGRAM_BOT_TOKEN = Variable.get("telegram_bot_token")
# TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")

TELEGRAM_BOT_TOKEN = "7200432955:AAELxQ0cp_NvypQmwh3WFiarGp_GW19knZE" 
TELEGRAM_CHAT_ID = "-1002362833220" 

# Initialize Bot
bot = Bot(token=TELEGRAM_BOT_TOKEN)

async def send_telegram_alert(message: str):
    """
    Sends a message to a Telegram group.

    Args:
        message (str): The message to send.
    """
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        logger.info("Telegram alert sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")

# Functions to send alerts on task success, failure, and retry
def notify_on_failure(context):
    """
    Sends a Telegram alert when a task fails.

    Args:
        context (dict): Airflow context containing task instance and execution details.
    """
    task_id = context.get("task_instance").task_id
    execution_date = context.get("logical_date")
    exception = context.get("exception")

    message = (
        f"🚨 Task Failed 🚨\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Error: {str(exception)}"
    )

    asyncio.run(send_telegram_alert(message))

def notify_on_success(context):
    """
    Sends a Telegram alert when a task succeeds.

    Args:
        context (dict): Airflow context containing task instance and execution details.
    """
    task_id = context.get("task_instance").task_id
    execution_date = context.get("logical_date")

    message = (
        f"✅ Task Succeeded ✅\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}"
    )

    asyncio.run(send_telegram_alert(message))

def notify_on_retry(context):
    """
    Sends a Telegram alert when a task is retried.

    Args:
        context (dict): Airflow context containing task instance and execution details.
    """
    task_id = context.get("task_instance").task_id
    execution_date = context.get("logical_date")
    exception = context.get("exception")

    message = (
        f"🔄 Task Retry 🔄\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Error: {str(exception)}"
    )

    asyncio.run(send_telegram_alert(message))

# Test the Telegram alert
if __name__ == "__main__":
    message = "🚨 This is a test alert. 🚨"
    asyncio.run(send_telegram_alert(message))
    logger.info("Telegram alert sent successfully.")
