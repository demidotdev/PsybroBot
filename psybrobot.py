from email.mime import text
import os
import re
import json
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Optional

from telegram import Update
from telegram.constants import ChatType
from telegram.ext import (Application, ContextTypes, MessageHandler, CommandHandler, filters)

import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
ALLOWED_CHAT_ID = os.environ.get("ALLOWED_CHAT_ID")  # opcional

# Cargar credenciales de Service Account desde env
sa_info = json.loads(os.environ["GOOGLE_SHEETS_JSON"])
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
gclient = gspread.authorize(creds)
sh = gclient.open_by_key(SHEET_ID)

MASTER_SHEET = "Master"


# Asegura que exista la hoja Master con encabezados
def ensure_master_headers():
    try:
        ws = sh.worksheet(MASTER_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=MASTER_SHEET, rows=100, cols=10)

    headers = ["Timestamp","SharedBy","SourceChat","MessageLink",
               "Platform","Artist","Title","URL","Tags","Notes"]
    try:
        first_row = ws.row_values(1)
        if [h.strip() for h in first_row] != headers:
            ws.clear()
            ws.append_row(headers)
    except Exception:
        ws.clear()
        ws.append_row(headers)

ensure_master_headers()

# ---------- Helpers ----------
def get_display_name(user) -> str:
    if user:
        if hasattr(user, "username") and user.username:
            return user.username
        elif hasattr(user, "full_name") and user.full_name:
            return user.full_name
    return ""

URL_RE = re.compile(
    r'(?P<url>(https?://|www\.)[^\s<>\]]+)', re.IGNORECASE
)
YOUTUBE_HOSTS = {"youtu.be", "youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"}
SPOTIFY_HOSTS = {"open.spotify.com", "spotify.link"}
SOUNDCLOUD_HOSTS = {"soundcloud.com"}
APPLE_HOSTS = {"apple.com", "music.apple.com"}
BANDCAMP_HOSTS = {"bandcamp.com"}

TAG_RE = re.compile(r"#(?!ascucha\b)\w+")


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
        return "apple"
    if host in BANDCAMP_HOSTS:
        return "bandcamp"
    return None


def build_message_link(update: Update) -> str:
    msg = update.effective_message
    chat = update.effective_chat
    if not chat or chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return ""
    # Para supergrupos públicos: t.me/username/123
    if chat.username and msg is not None:
        return f"https://t.me/{chat.username}/{msg.message_id}"
    # Para privados, se necesita el “internal link” (no siempre disponible sin extras)
    # Devolvemos vacío si no es posible construirlo.
    return ""


def row_exists_by_url_in_sheet(url: str, sheet_name: str) -> bool:
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return False
    urls = ws.col_values(8)  # URL en columna 8 (índice 1-based)
    url_set = {str(u).strip() for u in urls if u is not None}
    return str(url).strip() in url_set


async def append_row_to_sheet(sheet_name: str, row: list) -> None:
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=100, cols=10)
        headers = ["Timestamp","SharedBy","SourceChat","MessageLink",
                   "Platform","Artist","Title","URL","Tags","Notes"]
        ws.append_row(headers, value_input_option=gspread.utils.ValueInputOption.raw)
    ws.append_row(row, value_input_option=gspread.utils.ValueInputOption.raw)


async def append_row(context: ContextTypes.DEFAULT_TYPE, update: Update, *, shared_by: str, source_chat: str,
                    artist: str, title: str, url: str, message_link: str, tags: str = "", notes: str = "") -> None:
    platform = detect_platform(url)
    ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    row = [ts, shared_by, source_chat, message_link,
           platform, artist, title, url, tags, notes]
    row = [x if x is not None else "" for x in row]

    # Agrega a la hoja Master si no existe aún
    if not row_exists_by_url_in_sheet(url, MASTER_SHEET):
        await append_row_to_sheet(MASTER_SHEET, row)

    # Para cada etiqueta válida, agrega a la hoja correspondiente si no existe el URL
    if tags:
        for tag in tags.split():
            if tag and tag != "#ascucha":
                # Asumo que la etiqueta NO incluye el '#', si la incluye, quitarlo
                tag_name = tag.lstrip("#")
                if tag_name:
                    if not row_exists_by_url_in_sheet(url, tag_name):
                        await append_row_to_sheet(tag_name, row)


# ---------- Comandos ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(
            "¡Listo! Envíame un link o usa /add Artista - Título URL y lo registro en la hoja Master."
        )


def extract_notes(text: str, meta: str) -> str:
    tags = set(TAG_RE.findall(text))
    urls = set(m.group("url") for m in URL_RE.finditer(text))
    meta_words = set(meta.split())
    fragments = text.split()

    notes = [
        f for f in fragments
        if f not in tags and f not in urls and f not in meta_words and not f.startswith("#") and f != "/add"
    ]

    return " ".join(notes)


async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_CHAT_ID and (not update.effective_chat or str(update.effective_chat.id) != str(ALLOWED_CHAT_ID)):
        return
    text = ((update.message.text if update.message else "") or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        if update.message:
            await update.message.reply_text("Uso: /add Artista - Título URL")
        return
    payload = parts[1]
    m = URL_RE.search(payload)
    if not m:
        if update.message:
            await update.message.reply_text("No encontré un URL. Formato: /add Artista - Título URL")
        return
    url = m.group("url")
    meta = payload[:m.start()].strip()
    artist, title = "", ""
    if " - " in meta:
        artist, title = [s.strip() for s in meta.split(" - ", 1)]
    elif meta:
        title = meta
    platform = detect_platform(url)
    if platform is None:
        if update and update.message:
            await update.message.reply_text("No reconozco la plataforma del URL. Solo YouTube, Spotify, Apple Music, SoundCloud y Bandcamp.")
        return
    if row_exists_by_url_in_sheet(url, MASTER_SHEET):
        if update.message:
            await update.message.reply_text("Ya estaba registrado ✅ (duplicado por URL).")
        return

    tags = TAG_RE.findall(text)
    tags_str = " ".join(tags) if tags else ""
    notes_str = extract_notes(text, meta)

    user = update.effective_user
    shared_by = get_display_name(user)
    source_chat = ""
    if update.effective_chat:
        source_chat = (update.effective_chat.title if update.effective_chat and hasattr(update.effective_chat, "title") and update.effective_chat.title else
                        update.effective_chat.username if update.effective_chat and hasattr(update.effective_chat, "username") and update.effective_chat.username else
                        "")
    message_link = build_message_link(update)

    await append_row(context, update, shared_by=shared_by, source_chat=source_chat,
                    artist=artist, title=title, url=url, message_link=message_link,
                    tags=tags_str, notes=notes_str)
    if update.message:
        await update.message.reply_text("Anotado en Master ✅")


# ---------- Catch-all de mensajes con links ----------
async def catch_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ALLOWED_CHAT_ID and (not update.effective_chat or str(update.effective_chat.id) != str(ALLOWED_CHAT_ID)):
        return
    text = (update.message.text_html if update.message else "") or ""
    urls = [m.group("url") for m in URL_RE.finditer(text)]
    if not urls:
        return

    user = update.effective_user
    shared_by = get_display_name(user)
    source_chat = ""
    if update.effective_chat:
        source_chat = getattr(update.effective_chat, "title", None) or getattr(update.effective_chat, "username", None) or ""
    message_link = build_message_link(update)
    tags = TAG_RE.findall(text)
    tags_str = " ".join(tags) if tags else ""
    notes_str = extract_notes(text, meta="")

    added = 0
    for url in urls:
        platform = detect_platform(url)
        if platform is None:
            if update.message:
                await update.message.reply_text(
                    f"No reconozco la plataforma del URL {url}. Solo YouTube, Spotify, Apple Music, SoundCloud y Bandcamp.")
            return
        if row_exists_by_url_in_sheet(url, MASTER_SHEET):
            if update.message:
                await update.message.reply_text("Ya estaba registrado ✅ (duplicado por URL).")
            continue
        await append_row(context, update, shared_by=shared_by, source_chat=source_chat,
                        artist="", title="", url=url, message_link=message_link,
                        tags=tags_str, notes=notes_str)
        added += 1

    if added and update.message:
        await update.message.reply_text(f"Registré {added} link(s) en Master ✅")
    elif update.message:
        await update.message.reply_text("No encontré links nuevos de plataformas autorizadas para registrar.")


# ---------- Main ----------
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, catch_links))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
