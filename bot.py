import os
import json
import asyncio
import sqlite3
from pathlib import Path
from datetime import datetime
import discord
from discord.ext import commands
from enum import Enum
from tqdm import tqdm

def load_env_from_file(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


intents = discord.Intents.all()
bot = commands.Bot(command_prefix='!', intents=intents)


def generate_progress_bar(current, total, length=20):
    if total <= 0:
        return "[--------------------] 0/0 (0%)"
    if current > total:
        current = total
    progress = int(length * current / total)
    bar = "█" * progress + "-" * (length - progress)
    percent = int(100 * current / total)
    return f"[{bar}] {current}/{total} ({percent}%)"


def sanitize_name(name):
    return "".join(c for c in name if c.isalnum() or c in (" ", "-", "_")).strip() or "unknown"


def default_backup_options(method, mode):
    normalized_method = (method or "json").lower()
    normalized_mode = (mode or "rapido").lower()
    if normalized_method not in ("json", "txt", "db"):
        normalized_method = "json"
    if normalized_mode not in ("rapido", "full"):
        normalized_mode = "rapido"
    return normalized_method, normalized_mode

# Extracts overwrite from channel-like types
def extract_overwrites(chs):
    return [{str(k): { "allow": v.pair()[0].value, "deny": v.pair()[1].value }} for k,v in chs.overwrites.items()]

# Obtains the guild structure
# Permission Set for this function: [NONE]
async def get_guild_structure(guild):
    # Permission: NONE
    base = {
        "id": guild.id,
        "name": guild.name,
        "owner_id": guild.owner_id,
        "member_count": guild.member_count,
        "created_at": guild.created_at.isoformat(),
    }

    # Permission: NONE
    roles = [
        {
            "id": role.id,
            "name": role.name,
            "position": role.position,
            "permissions": role.permissions.value,
            "color": role.color.value,
            "hoist": role.hoist,
            "mentionable": role.mentionable,
            "managed" : role.managed,
         } for role in guild.roles ]
    # Permission: NONE
    categories = [
        {
            "id": x.id,
            "name": x.name,
            "position": x.position,
            "overwrites": extract_overwrites(x)
        } for x in guild.categories ]
    # Permission: NONE
    channels = [
        {
            "id": x.id,
            "name": x.name,
            "category_id": x.category.id if x.category else None,
            "position": x.position,
            "topic": x.topic,
            "nsfw": x.is_nsfw(),
            "slowmode_delay": x.slowmode_delay,
            "overwrites": extract_overwrites(x)
        } for x in guild.text_channels ]
    # Permission: NONE
    emojis = [
        {
            "id": x.id,
            "name": x.name,
            "animated": x.animated,
            "url": str(x.url)
        } for x in guild.emojis]

    return {
        "info": base,
        "roles": roles,
        "categories":categories,
        "channels": channels,
        "emojis": emojis,
        "version": 1,
    }


class FileKind(Enum):
    GUILD_STRUCTURE_JSON = 1
    BACKUP_JSON = 2
    FILE_ATTACHMENT = 3

async def save_file(kind, base_dir, data, extra=None):
    def save_json(path, data):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    path = None

    if kind is FileKind.GUILD_STRUCTURE_JSON:
        path = base_dir / "backup_structure.json"
        save_json(path, data)
    elif kind is FileKind.FILE_ATTACHMENT:
        path = base_dir / f"{extra}_{data.filename}"
        await data.save(path)
    elif kind is FileKind.BACKUP_JSON:
        path = base_dir / "backup_data.json"
        save_json(path, data)
    return path

async def backup_guild_structure(guild, backup_dir):
   structure = await get_guild_structure(guild)
   return save_file(FileKind.GUILD_STRUCTURE_JSON, backup_dir, structure)

# Here base_dir is the channel media directory.
# Said directory MUST exist before calling this function
async def save_attachments(message, base_dir):
    for att in message.attachments:
        await save_file(FileKind.FILE_ATTACHMENT, base_dir, att, extra=message.id)

# Transforms a message into a .TXT file immediately appending it to the log file.
# Here log_file is a file-like object that allows appending into the end of itself.
# Said file MUST exist before calling this function
# Note to selves: *Maybe* this is a micro-optimization, cause we could collect all messages into RAM
# but for *really* large discords that might be impossible? I dont know.
# It is done "streaming" here because of possibility that some random discord server
# could bloat enough UTF8 data to OOM. Scary! And we certainly wouldn't wanna be in that server lel. 
async def transform_txt(message, log_file):
    timestamp = message.created_at.isoformat()
    author = f"{message.author} ({message.author.id})"
    content = message.clean_content or ""
    log_file.write(f"[{timestamp}] {author}: {content}\n")
    for (idx,embed) in enumerate(message.embeds):
        log_file.write(f"[EMBED {idx}] {json.dumps(embed.to_dict(), ensure_ascii=False)}\n")

# Transforms a message into a dict that can then be collected as a list
# Here base_dir is the channel media directory.
# It is not needed that the directory be created before calling this function
# since it is a pure function.
# Fuck, can JSON even be saved in a streamable way? Maybe `transform_txt` is a microop afterall. 
def transform_dict(message, base_dir):
    return {
        "id": message.id,
        "author_id": message.author.id,
        "author_tag": str(message.author),
        "content": message.clean_content or "",
        "created_at": message.created_at.isoformat(),
        "embeds": [embed.to_dict() for embed in message.embeds],
        "attachments": [{
            "filename": x.filename,
            "saved_path": base_dir / message.id + "_" + x.filename
        } for x in message.attachments]
    }



async def backup_messages_txt(guild, backup_dir):
    # Thats just handrolling TQDM...
    # Iona like it. 
    media_dir = backup_dir / "media"
    logs_dir = backup_dir / "logs"
    # Maybe os.makedirs? is recursive and recursive betterer
    media_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    for channel in tqdm(guild.text_channels, desc="[TXT] Discord Channels"):
        safe_name = sanitize_name(channel.name) or str(channel.id)
        log_path = logs_dir / f"{safe_name}.txt"
        ch_media_dir = media_dir / safe_name
        ch_media_dir.mkdir(exist_ok=True)
        with open(log_path, "w", encoding="utf-8") as log:
            async for message in channel.history(oldest_first=True, limit=None):
                await transform_txt(message, log)
                await save_attachments(message, ch_media_dir)

async def backup_messages_json(guild, backup_dir):
    media_dir = backup_dir / "media"
    logs_dir = backup_dir / "logs"

    media_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    base = {
        "guild_id": guild.id,
        "created_at": datetime.utcnow().isoformat(),
        "mode": "full",
        "channels": [],
        "version": 1,
    }

    for channel in tqdm(guild.text_channels, desc="[JSON] Discord Channels"):
        safe_name = sanitize_name(channel.name) or str(channel.id)
        log_path = logs_dir / f"{safe_name}.txt"
        ch_media_dir = media_dir / safe_name
        ch_media_dir.mkdir(exist_ok=True)

        ch_entry = {
            "id": channel.id,
            "name": channel.name,
            "category_id": channel.category.id if channel.category else None,
            "messages": [],
        }

        async for message in channel.history(oldest_first=True, limit=None):
            transformed = transform_dict(message, ch_media_dir)
            ch_entry["messages"].append(transformed)
            await save_attachments(message, ch_media_dir)

        base["channels"].append(ch_entry)

    return save_file(FileKind.BACKUP_JSON, backup_dir, base)

async def backup_messages_db(guild, backup_dir, mode, progress_msg):
    db_path = backup_dir / "backup.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, channel_id INTEGER, author_id INTEGER, author_tag TEXT, content TEXT, created_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS embeds (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, payload TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS attachments (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, filename TEXT, saved_path TEXT, error TEXT)"
    )
    conn.commit()
    text_channels = guild.text_channels
    total_channels = len(text_channels)
    completed_channels = 0
    media_dir = backup_dir / "media"
    media_dir.mkdir(exist_ok=True)

    async def progress_updater():
        while completed_channels < total_channels:
            bar = generate_progress_bar(completed_channels, total_channels)
            await progress_msg.edit(content=f"Saving messsages to DB: {bar}")
            await asyncio.sleep(2)

    updater_task = asyncio.create_task(progress_updater())

    try:
        for channel in text_channels:
            safe_name = sanitize_name(channel.name) or str(channel.id)
            channel_media_dir = media_dir / safe_name
            channel_media_dir.mkdir(exist_ok=True)
            try:
                limit = None
                if mode == "rapido":
                    limit = 500
                history_kwargs = {"limit": limit, "oldest_first": True}
                async for msg in channel.history(**history_kwargs):
                    conn.execute(
                        "INSERT OR REPLACE INTO messages (id, channel_id, author_id, author_tag, content, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            msg.id,
                            channel.id,
                            msg.author.id,
                            str(msg.author),
                            msg.clean_content or "",
                            msg.created_at.isoformat(),
                        ),
                    )
                    for embed in msg.embeds:
                        conn.execute(
                            "INSERT INTO embeds (message_id, payload) VALUES (?, ?)",
                            (msg.id, json.dumps(embed.to_dict(), ensure_ascii=False)),
                        )
                    if mode == "full":
                        for att in msg.attachments:
                            attachment_path = channel_media_dir / att.filename
                            error = None
                            try:
                                await att.save(attachment_path)
                            except Exception as e:
                                error = str(e)
                            conn.execute(
                                "INSERT INTO attachments (message_id, filename, saved_path, error) VALUES (?, ?, ?, ?)",
                                (
                                    msg.id,
                                    att.filename,
                                    str(attachment_path.relative_to(backup_dir)),
                                    error,
                                ),
                            )
                conn.commit()
            except Exception as e:
                conn.execute(
                    "INSERT INTO messages (id, channel_id, author_id, author_tag, content, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        None,
                        channel.id,
                        0,
                        "system",
                        f"Error during the backup of the channel {channel.id} ({channel.name}): {e}",
                        datetime.utcnow().isoformat(),
                    ),
                )
                conn.commit()
            completed_channels += 1
    finally:
        updater_task.cancel()
        conn.close()
    return db_path


async def create_metadata_file(backup_dir, guild, method, mode, structure_path, data_path):
    timestamp = datetime.utcnow().isoformat()
    meta = {
        "guild_id": guild.id,
        "guild_name": guild.name,
        "created_at": timestamp,
        "method": method,
        "mode": mode,
        "structure_file": str(structure_path),
        "data_file": str(data_path) if data_path else None,
        "version": 1,
    }
    safe_name = sanitize_name(guild.name) or str(guild.id)
    file_name = f"backup_{safe_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    meta_path = backup_dir / file_name
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta_path

@bot.command()
@commands.has_permissions(administrator=True)
async def backup(ctx, method: str = None):
    guild = ctx.guild
    method, mode = default_backup_options(method, None)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = sanitize_name(guild.name) or str(guild.id)
    backup_dir = Path(f"backup_{safe_name}_{timestamp}")
    backup_dir.mkdir(exist_ok=True)
    description = f"Method: {method.upper()} • Mode: {mode.upper()}"
    progress_msg = await ctx.send(f"📦 Starting backup of **{guild.name}**\n{description}")
    structure_path = await backup_guild_structure(guild, backup_dir)
    await progress_msg.edit(content=f"📁 Saved server structure\n{description}")
    data_path = None
    if method == "txt":
        await backup_messages_txt(guild, backup_dir)
    elif method == "json":
        data_path = await backup_messages_json(guild, backup_dir)
    elif method == "db":
        data_path = await backup_messages_db(guild, backup_dir, mode, progress_msg)
    meta_path = await create_metadata_file(backup_dir, guild, method, mode, structure_path, data_path)
    from discord import Embed
    embed = Embed(
        title="✅ Backup complete",
        description="The server backup was completed sucessfuly.",
        color=discord.Color.green(),
    )
    embed.add_field(name="Method", value=method.upper(), inline=True)
    embed.add_field(name="Mode", value=mode.upper(), inline=True)
    embed.add_field(name="File Metadata", value=str(meta_path.name), inline=False)
    await progress_msg.edit(content=None, embed=embed)


load_env_from_file()
TOKEN = os.getenv("DISCORD_BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("Missing DISCORD_BOT_TOKEN environment variable.")
bot.run(TOKEN)
