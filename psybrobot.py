import os
import re
import json
import requests
from gspread import utils

from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Optional

from telegram import Update
from telegram.constants import ChatType
from telegram.ext import (Application, ContextTypes, MessageHandler, CommandHandler, filters)

import gspread
from google.oauth2.service_account import Credentials

# ========================
# CONFIGURACIÓN Y ARRANQUE
# ========================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
SHEET_ID = os.environ.get("SHEET_ID")
ALLOWED_CHAT_ID = os.environ.get("ALLOWED_CHAT_ID")  # opcional

sa_info = json.loads(os.environ.get("GOOGLE_SHEETS_JSON", "{}"))
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
gclient = gspread.authorize(creds)
if SHEET_ID:
    sh = gclient.open_by_key(SHEET_ID)
else:
    print("No se ha configurado la variable de entorno SHEET_ID.")

MASTER_SHEET = "Master"
CACHE_URLS_REGISTERED = set()  # Cache para urls ya registradas en el master

# ==========
# CONSTANTES
# ==========

URL_RE = re.compile(r'(?P<url>(https?://|www\.)[^\s<>\]]+)', re.IGNORECASE)
TAG_RE = re.compile(r"#(?!ascucha\b)\w+")
ASC_LINK_RE = re.compile(r'#ascucha\s+((https?://|www\.)[^\s<>\]]+)', re.IGNORECASE)

YOUTUBE_HOSTS = {"youtu.be", "youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"}
SPOTIFY_HOSTS = {"open.spotify.com", "spotify.link"}
SOUNDCLOUD_HOSTS = {"soundcloud.com"}
APPLE_HOSTS = {"apple.com", "music.apple.com"}
BANDCAMP_HOSTS = {"bandcamp.com"}

HEADERS_MASTER = [
    "Timestamp","SharedBy","SourceChat","MessageLink",
    "Platform","Artist","Title","URL","Tags","Notes","Álbum","Año"
]

# =============
# UTILIDADES
# =============

def get_display_name(user) -> str:
    if user:
        if getattr(user, "username", None):
            return user.username
        elif getattr(user, "full_name", None):
            return user.full_name
    return ""

def get_source_chat(update: Update) -> str:
    chat = update.effective_chat
    if chat:
        if hasattr(chat, "title") and chat.title:
            return chat.title
        elif hasattr(chat, "username") and chat.username:
            return chat.username
    return ""

def detect_platform(url: str) -> Optional[str]:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return None
    if host in YOUTUBE_HOSTS:
        return "youtube"
    if host in SPOTIFY_HOSTS:
        return "spotify"
    if host in SOUNDCLOUD_HOSTS:
        return "soundcloud"
    if host in APPLE_HOSTS:
        return "appleMusic"
    if host in BANDCAMP_HOSTS:
        return "bandcamp"
    return None

def build_message_link(update: Update) -> str:
    msg = update.effective_message
    chat = update.effective_chat
    if not chat or chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP, ChatType.PRIVATE):
        return ""
    if chat.username and msg is not None:
        return f"https://t.me/{chat.username}/{msg.message_id}"
    return ""

def normalize_tags(tags: list[str]) -> list[str]:
    return [t[0].upper() + t[1:].lower() if len(t)>1 else t.upper() for t in tags]

def extract_notes(text: str, meta: str) -> str:
    """
    Extrae notas excluyendo hashtags, URLs (excepto links Telegram) y palabras de meta.
    """
    tags = set(TAG_RE.findall(text))
    urls = set(m.group("url") for m in URL_RE.finditer(text))
    meta_words = set(meta.split())
    fragments = text.split()

    notes_fragments = []
    for f in fragments:
        f_lower = f.lower()
        if f in tags or f in urls or f in meta_words or f == "/add" or f.startswith("#") or f_lower == "#ascucha":
            if f.startswith("https://t.me/") or f.startswith("http://t.me/"):
                notes_fragments.append(f)
        else:
            notes_fragments.append(f)

    return " ".join(notes_fragments)

def ensure_headers_in_sheet(sheet_name: str):
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=100, cols=len(HEADERS_MASTER))
        ws.append_row(HEADERS_MASTER, value_input_option=utils.ValueInputOption.raw)
        return

    first_row = ws.row_values(1)
    if [h.strip() for h in first_row] != HEADERS_MASTER:
        ws.clear()
        ws.append_row(HEADERS_MASTER, value_input_option=utils.ValueInputOption.raw)

def ensure_columns(ws, required_cols):
    headers = ws.row_values(1)
    added = False
    for col in required_cols:
        if col not in headers:
            headers.append(col)
            added = True
    if added:
        ws.update(range_name="1:1", values=[headers])

    return headers

def row_exists_by_url_in_sheet(url: str, sheet_name: str) -> bool:
    global CACHE_URLS_REGISTERED
    if sheet_name == MASTER_SHEET and url in CACHE_URLS_REGISTERED:
        return True
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return False
    urls = ws.col_values(8)
    url_set = {str(u).strip() for u in urls if u is not None}
    if sheet_name == MASTER_SHEET:
        CACHE_URLS_REGISTERED = url_set  # cache actualizado
    return str(url).strip() in url_set

# =============
# API SONG.LINK
# =============

def get_songlink_metadata(url: str):
    api_url = "https://api.song.link/v1-alpha.1/links"
    params = {"url": url}
    try:
        resp = requests.get(api_url, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()

        entities_by_platform = data.get("entitiesByUniqueId", {})
        links_by_platform = data.get("linksByPlatform", {})

        for platform in ["spotify", "appleMusic", "youtube", "soundcloud", "bandcamp"]:
            platform_info = links_by_platform.get(platform)
            if platform_info and "entityUniqueId" in platform_info:
                entity_id = platform_info["entityUniqueId"]
                entity = entities_by_platform.get(entity_id, {})
                artist = entity.get("artistName", "")
                title = entity.get("title", "")
                album = entity.get("albumName", "")
                year = entity.get("year", "")
                return {"artist": artist, "title": title, "album": album, "year": str(year)}

        main_entity_id = data.get("pageEntityUniqueId")
        if main_entity_id and main_entity_id in entities_by_platform:
            entity = entities_by_platform.get(main_entity_id, {})
            artist = entity.get("artistName", "")
            title = entity.get("title", "")
            album = entity.get("albumName", "")
            year = entity.get("year", "")
            return {"artist": artist, "title": title, "album": album, "year": str(year)}
    except Exception:
        pass
    return {}

# =============
# MANEJO DE FILAS EN HOJAS
# =============

async def append_row_to_sheet(sheet_name: str, row: list) -> None:
    try:
        ensure_headers_in_sheet(sheet_name)
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ensure_headers_in_sheet(sheet_name)
        ws = sh.add_worksheet(title=sheet_name, rows=100, cols=len(HEADERS_MASTER))
        ws.append_row(HEADERS_MASTER, value_input_option=utils.ValueInputOption.raw)
    ensure_columns(ws, ["Álbum", "Año"])
    ws.append_row(row, value_input_option=utils.ValueInputOption.raw)

async def append_row(context: ContextTypes.DEFAULT_TYPE, update: Update, *, shared_by: str, source_chat: str,
                    artist: str, title: str, url: str, message_link: str, tags: str = "", notes: str = "",
                    album: str = "", year: str = "") -> None:
    platform = detect_platform(url)
    ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    row = [ts, shared_by, source_chat, message_link,
        platform, artist, title, url, tags, notes, album, year]
    row = [x if x is not None else "" for x in row]

    if not row_exists_by_url_in_sheet(url, MASTER_SHEET):
        await append_row_to_sheet(MASTER_SHEET, row)

    if tags:
        for tag in tags.split():
            if tag and tag != "#ascucha":
                tag_name = tag.lstrip("#")
                if tag_name:
                    sheet_name = tag_name[0].upper() + tag_name[1:].lower()
                    if not row_exists_by_url_in_sheet(url, sheet_name):
                        await append_row_to_sheet(sheet_name, row)
    else:
    # Si no hay tags, añade a la hoja "Undefined"
            if not row_exists_by_url_in_sheet(url, "Undefined"):
                await append_row_to_sheet("Undefined", row)

# =============
# HANDLERS DE TELEGRAM
# =============

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text("¡Listo! Envíame un link o usa /add URL y lo registro en la hoja Master.")

async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_CHAT_ID and (not update.effective_chat or str(update.effective_chat.id) != str(ALLOWED_CHAT_ID)):
        return
    text = ((update.message.text if update.message else "") or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        if update.message:
            await update.message.reply_text("Uso: /add URL")
        return

    all_urls = [m.group("url") for m in URL_RE.finditer(parts[1])]
    if not all_urls:
        if update.message:
            await update.message.reply_text("No encontré un URL válido. Formato: /add URL")
        return

    first_url = all_urls[0]
    rest_urls = all_urls[1:]

    platform = detect_platform(first_url)
    if platform is None:
        if update and update.message:
            await update.message.reply_text("No reconozco la plataforma del URL. Solo YouTube, Spotify, Apple Music, SoundCloud y Bandcamp.")
        return
    if row_exists_by_url_in_sheet(first_url, MASTER_SHEET):
        if update.message:
            await update.message.reply_text("Ya estaba registrado ✅ (duplicado por URL).")
        return

    tags_raw = TAG_RE.findall(text)
    tags = [t for t in tags_raw if t.lower() != "ascucha"]
    tags_str = " ".join(f"#{t}" for t in tags) if tags else ""

    notes_str = extract_notes(text, "")
    if rest_urls:
        notes_str = (notes_str + " " + " ".join(rest_urls)).strip()

    metadata = get_songlink_metadata(first_url) or {}
    artist = metadata.get("artist", "")
    title = metadata.get("title", "")
    album = metadata.get("album", "")
    year = metadata.get("year", "")

    shared_by = get_display_name(update.effective_user)
    source_chat = get_source_chat(update)
    message_link = build_message_link(update)

    await append_row(context, update, shared_by=shared_by, source_chat=source_chat,
                    artist=artist, title=title, url=first_url, message_link=message_link,
                    tags=tags_str, notes=notes_str, album=album, year=year)
    if update.message:
        await update.message.reply_text("Anotado en Master ✅")

async def catch_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_CHAT_ID and (not update.effective_chat or str(update.effective_chat.id) != str(ALLOWED_CHAT_ID)):
        return
    text = (update.message.text_html if update.message else "") or ""

    # Busca links solo después de #ascucha
    ascucha_links = [m.group(1) for m in ASC_LINK_RE.finditer(text)]
    if not ascucha_links:
        return  # No hay links válidos tras #ascucha, no procesa nada

    first_url = ascucha_links[0]
    rest_urls = ascucha_links[1:]

    shared_by = get_display_name(update.effective_user)
    source_chat = get_source_chat(update)
    message_link = build_message_link(update)

    tags_raw = TAG_RE.findall(text)
    tags = [t for t in tags_raw if t.lower() != "ascucha"]
    tags_str = " ".join(f"#{t}" for t in tags) if tags else ""

    notes_str = extract_notes(text, "")
    if rest_urls:
        notes_str = (notes_str + " " + " ".join(rest_urls)).strip()

    platform = detect_platform(first_url)
    if platform is None:
        if update.message:
            await update.message.reply_text(
                f"No reconozco la plataforma del URL {first_url}. Solo YouTube, Spotify, Apple Music, SoundCloud y Bandcamp.")
        return
    if row_exists_by_url_in_sheet(first_url, MASTER_SHEET):
        if update.message:
            await update.message.reply_text("Ya estaba registrado ✅ (duplicado por URL).")
        return

    metadata = get_songlink_metadata(first_url) or {}
    artist = metadata.get("artist", "")
    title = metadata.get("title", "")
    album = metadata.get("album", "")
    year = metadata.get("year", "")

    await append_row(context, update, shared_by=shared_by, source_chat=source_chat,
                    artist=artist, title=title, url=first_url, message_link=message_link,
                    tags=tags_str, notes=notes_str, album=album, year=year)

    if update.message:
        await update.message.reply_text("Anotado en Master ✅")

# ===========
# MAIN
# ===========

def main():
    if not BOT_TOKEN:
        raise ValueError("Bot token no configurado en variable de entorno BOT_TOKEN")
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, catch_links))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
