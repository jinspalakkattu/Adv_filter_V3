# (©)𝙐𝙁𝙎 𝘽𝙤𝙩𝙯

import base64
import re
import asyncio
from pyrogram import filters
from pyrogram.errors.exceptions.bad_request_400 import UserNotParticipant
from pyrogram.errors import FloodWait


async def encode(string):
    string_bytes = string.encode("ascii")
    base64_bytes = base64.b64encode(string_bytes)
    base64_string = base64_bytes.decode("ascii")
    return base64_string


async def decode(base64_string):
    base64_bytes = base64_string.encode("ascii")
    string_bytes = base64.b64decode(base64_bytes) 
    string = string_bytes.decode("ascii")
    return string


async def get_messages(client, message_id, channel_id):
    messages = []
    total_messages = 0
    while total_messages != len(message_id):
        temb_ids = message_id[total_messages:total_messages+200]
        # for id in message_id:
        try:
            msgs = await client.get_messages(
                chat_id=channel_id,
                message_ids=temb_ids
            )
        except FloodWait as e:
            await asyncio.sleep(e.x)
            msgs = await client.get_messages(
                chat_id=channel_id,
                message_ids=temb_ids
            )
        except:
            pass
        total_messages += len(temb_ids)
        messages.extend(msgs)
    return messages


async def get_message_id(client, message, fwd_channel_id):
    if message.forward_from_chat:
        if message.forward_from_chat.id == fwd_channel_id:
            return message.forward_from_message_id
        else:
            return 0
    elif message.forward_sender_name:
        return 0
    elif message.text:
        pattern = "https://t.me/(?:c/)?(.*)/(\d+)"
        matches = re.match(pattern, message.text)
        if not matches:
            return 0
        channel_id = matches.group(1)
        msg_id = int(matches.group(2))
        if channel_id.isdigit():
            if f"-100{channel_id}" == str(fwd_channel_id):
                return msg_id
        else:
            if channel_id == client.db_channel.username:
                return msg_id
    else:
        return 0
