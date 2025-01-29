from telegram import Bot
import logging
import asyncio
from clickhouse_driver import Client
from airflow.models import Variable

# Set up logging
logger = logging.getLogger(__name__)

# Initialize Bot and Telegram channel
TELEGRAM_BOT_TOKEN = "7200432955:AAELxQ0cp_NvypQmwh3WFiarGp_GW19knZE"
TELEGRAM_CHAT_ID = "-1002362833220" 

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


# Function to send alerts on task failure
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


# Function to send alerts on task success
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


# Function to send alerts on task retry
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
