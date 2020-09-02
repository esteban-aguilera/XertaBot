import pathlib
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters


# package imports
from .wrappers import command, public, private, restricted
from .database import manager
from .database.managers.jokes import random_joke
from .database.managers.users import get_users


# --------------------------------------------------------------------------------
# setup commands
# --------------------------------------------------------------------------------
def setup(dp):
    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(CommandHandler('help', start))
    dp.add_handler(CommandHandler('joke', joke))
    dp.add_handler(CommandHandler('users', users))

    # on noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, message))


# --------------------------------------------------------------------------------
# Telegram commands
# --------------------------------------------------------------------------------
@public
@command
def start(update, context):
    """Send a message when the command /start is issued."""
    path = f'{pathlib.Path(__file__).parent.absolute()}/database/messages/start.txt'

    with open(path, mode='r') as f:
        msg = f.read()

    update.message.reply_text(msg)


@public
@command
def joke(update, context):
    """Send a message when the command /joke is issued."""
    conn = manager.connect()
    update.message.reply_text(random_joke(conn))


# --------------------------------------------------------------------------------
# Message Handler
# --------------------------------------------------------------------------------
@public
@command
def message(update, context):
    """Handles the event where the user writes a message."""
    update.message.reply_text('Jajaja, you are very funny.')


# --------------------------------------------------------------------------------
# Message Handler
# --------------------------------------------------------------------------------
@restricted
@command
def users(update, context):
    """Send a message when the command /joke is issued."""
    conn = manager.connect()
    df = get_users(conn, privilege=0)
    
    df = df[['first_name', 'last_name']].dropna()
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    
    users_msg = '\n'.join( df['full_name'].values )
    update.message.reply_text(f'Users connected: \n{users_msg}')