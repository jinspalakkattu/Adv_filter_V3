import base64
import logging
import asyncio

from bot import Bot
from utils import temp
from struct import pack
from info import ADMINS
from pyrogram.file_id import FileId
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from database.batch_db import save_file
from plugins.helper_func import get_message_id, encode
from database.connections_mdb import active_connection
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
lock = asyncio.Lock()


@Bot.on_message(filters.command('batch') & filters.incoming)
async def batch_file(client: Client, message):
    userid = message.from_user.id if message.from_user else None
    if not userid:
        return await message.reply(f"You are anonymous admin. Use /connect {message.chat.id} in PM")
    chat_type = message.chat.type
    args = message.text.html.split(None, 1)

    if chat_type == "private":
        grpid = await active_connection(str(userid))
        if grpid is not None:
            grp_id = grpid
            try:
                chat = await client.get_chat(grpid)
                title = chat.title
            except:
                await message.reply_text("Make sure I'm present in your group!!", quote=True)
                return
        else:
            await message.reply_text("I'm not connected to any groups!", quote=True)
            return

    elif chat_type in ["group", "supergroup"]:
        grp_id = message.chat.id
        title = message.chat.title

    else:
        return

    st = await client.get_chat_member(grp_id, userid)
    if (
            st.status != "administrator"
            and st.status != "creator"
            and str(userid) not in ADMINS
    ):
        return

    while True:
        try:
            first_message = await client.ask(text="Forward the First Message from DB Channel (with Quotes)..\n\n"
                                                  "or Send the DB Channel Post Link", chat_id=message.from_user.id,
                                             filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
                                             timeout=60)
        except:
            return
        first_channel_id = first_message.forward_from_chat.id
        try:
            channel = await client.get_chat(first_channel_id)
        except Exception as e:
            await first_message.reply("❌ Error\n\nMake Sure Bot Is Admin In Forwarded Channel...", quote=True)
            return

        f_msg_id = await get_message_id(client, first_message, first_channel_id)
        if f_msg_id:
            break
        else:
            await first_message.reply("❌ Error\n\nthis Forwarded Post is not from my DB Channel or this Link is taken "
                                      "from DB Channel", quote=True)
            continue

    while True:
        try:
            second_message = await client.ask(text="Forward the Last Message from DB Channel (with Quotes)..\n"
                                                   "or Send the DB Channel Post link", chat_id=message.from_user.id,
                                              filters=(filters.forwarded | (filters.text & ~filters.forwarded)),
                                              timeout=60)
        except:
            return
        second_channel_id = second_message.forward_from_chat.id
        try:
            channel = await client.get_chat(second_channel_id)
        except Exception as e:
            await second_message.reply("❌ Error\n\nMake Sure Bot Is Admin In Forwarded Channel...", quote=True)
            return

        if first_channel_id == second_channel_id:
            s_msg_id = await get_message_id(client, second_message, second_channel_id)
            if s_msg_id:
                break
            else:
                await second_message.reply("❌ Error\n\nThis Forwarded Post Is Not From My DB Channel Or This "
                                           "Link Is Not Taken From DB Channel", quote=True)
                continue
        else:
            await second_message.reply("❌ Error\n\nthis Forwarded Post Is Not From My First Forwarded Channel Or This "
                                       "Link Is Taken From DB Channel", quote=True)

    string = f"get-{f_msg_id}-{s_msg_id}-{abs(grp_id)}"
    base64_string = await encode(string)
    link = f"https://t.me/{temp.U_NAME}?start={base64_string}"

    async with lock:
        try:
            total = s_msg_id + 1
            current = f_msg_id
            file_id = ''
            file_ref = ''
            caption = ''
            cap = None
            while current < total:
                try:
                    message = await client.get_messages(chat_id=second_channel_id, message_ids=current, replies=0)
                except FloodWait as e:
                    await asyncio.sleep(e.x)
                    message = await client.get_messages(
                        second_channel_id,
                        current,
                        replies=0
                    )
                except Exception as e:
                    logger.exception(e)

                try:
                    for file_type in ("document", "video", "audio"):
                        media = getattr(message, file_type, None)
                        if media is not None:
                            break
                        else:
                            continue
                    media.file_type = file_type
                    media.caption = message.caption

                    # TODO: Find better way to get same file_id for same media to avoid duplicates
                    _id, _ref = unpack_new_file_id(media.file_id)
                    file_id = file_id + _id + "#"
                    file_ref = file_ref + _ref + "#"
                    # file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))
                    cap = media.caption.html if media.caption else None
                    caption = caption + cap + "#"
                except Exception as e:
                    if "NoneType" in str(e):
                        logger.warning(
                            "Skipping deleted / Non-Media messages (if this continues for long, "
                            "use /setskip to set a skip number)")
                    else:
                        logger.exception(e)
                current += 1

            file_id = file_id[:-1]
            file_ref = file_ref[:-1]
            caption = caption[:-1]
            aynav, vnay = await save_file("Eva-V3", base64_string, file_id, file_ref, caption)

        except Exception as e:
            logger.exception(e)
            await second_message.edit(f'Error: {e}')
        else:
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Share URL",
                                                                       url=f'https://telegram.me/share/url?url={link}')]])
            await second_message.reply_text(f"<b>Here is your link</b>\n\n{link}", quote=True,
                                            reply_markup=reply_markup)


def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0

    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0

            r += bytes([i])

    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref