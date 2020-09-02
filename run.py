import logging
import os
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

# package import
from xerta_bot import commands
from xerta_bot.database import manager
from xerta_bot.database.managers.jokes import insert_joke

# os.environ
import secrets


# Enable logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s,"
)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------
def main():
    updater = Updater(os.getenv('TOKEN'), use_context=True)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher
    commands.setup(dp)

    # Start the Bot
    updater.start_polling()

    # Run the bot until process receives SIGINT, SIGTERM or SIGABRT.
    updater.idle()


# --------------------------------------------------------------------------------
# setup jokes table
# --------------------------------------------------------------------------------
def setup_jokes():
    path = f'data/jokes.txt'
    with open(path, 'r') as f:
        jokes = f.read()
    
    jokes = jokes.split('\n\n')

    conn = manager.connect()
    for joke in jokes:
        joke = ' '.join(joke.split())
        
        insert_joke(conn, joke)


# --------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
