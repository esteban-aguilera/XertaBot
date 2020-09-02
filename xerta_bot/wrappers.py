import os

from .database import manager
from .database.managers.commands import insert_command
from .database.managers.users import get_users, insert_user


# --------------------------------------------------------------------------------
# decorators
# --------------------------------------------------------------------------------
def public(func):
    def wrapper(update, context):
        user = update.message.from_user
        
        user_id = int( user.id )
        username = user.username
        first_name = user.first_name
        last_name = user.last_name
        language_code = user.language_code
            
        conn = manager.connect()
        insert_user(conn,
            ['id', 'username', 'first_name', 'last_name', 'language_code'],
            [user_id, username, first_name, last_name, language_code]
        )

        func(update, context)
    
    return wrapper


def private(func):
    def wrapper(update, context):            
        user_id = int(update.message.from_user.id)
        
        conn = manager.connect()
        df = get_users(conn, privilege=1).join(get_users(conn, privilege=2))
        if(user_id in list(df['id'].values)):
            func(update, context)
        else:
            update.message.reply_text('Sorry, this method is private.')
    
    return wrapper


def restricted(func):
    def wrapper(update, context):            
        user_id = int(update.message.from_user.id)
        
        conn = manager.connect()
        df = get_users(conn, privilege=2)

        if(user_id in df['id'].values):
            func(update, context)
        else:
            update.message.reply_text('Sorry, this method is restricted.')
    
    return wrapper


# --------------------------------------------------------------------------------
# command
# --------------------------------------------------------------------------------
def command(func):
    def wrapper(update, context):
        user_id = str(update.message.from_user.id)
        
        conn = manager.connect()
        insert_command(conn, user_id, func.__name__)
        
        func(update, context)
    
    return wrapper