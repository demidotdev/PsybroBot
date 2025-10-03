from email.mime import text
import os
import re
import json
import requests
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

BOT_TOKEN = os.environ["BOT_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]
ALLOWED_CHAT_ID = os.environ.get("ALLOWED_CHAT_ID")  # opcional

# Cargamos credenciales Google Sheets
sa_info = json.loads(os.environ["GOOGLE_SHEETS_JSON"])
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
gclient = gspread.authorize(creds)
sh = gclient.open_by_key(SHEET_ID)

# Definimos el nombre maestro de la hoja central
MASTER_SHEET = "Master"

# ==========
# UTILIDADES: Hoja principal y columnas
# ==========

def ensure_master_headers():
    """
    Se asegura de que la hoja Master exista y tenga los encabezados correctos:
    Agrega las columnas de artista/álbum/año si no están.
    """
    try:
        ws = sh.worksheet(MASTER_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=MASTER_SHEET, rows=100, cols=12)
    headers = [
        "Timestamp","SharedBy","SourceChat","MessageLink",
        "Platform","Artist","Title","URL","Tags","Notes","Álbum","año"
    ]
    try:
        first_row = ws.row_values(1)
        if [h.strip() for h in first_row] != headers:
            ws.clear()
            ws.append_row(headers)
    except Exception:
        ws.clear()
        ws.append_row(headers)

ensure_master_headers()

def ensure_columns(ws, required_cols):
    """
    Si faltan columnas en la hoja, las agrega al final y actualiza el encabezado
    sin borrar toda la fila, para evitar corrupción del encabezado.
    """
    headers = ws.row_values(1)
    added = False
    for col in required_cols:
        if col not in headers:
            headers.append(col)
            added = True
    if added:
        # Actualiza la fila 1 con los nuevos encabezados completos
        ws.update("1:1", [headers])
    return headers

# ===========
# REGEX Y AYUDANTES PARA PARSING DE PLATAFORMA, TAGS Y USUARIO
# ===========

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
    """
    Detecta la plataforma del link para normalizar el campo Platform.
    """
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
    """
    Genera el link directo al mensaje para supergrupos públicos o vacío en otros casos.
    """
    msg = update.effective_message
    chat = update.effective_chat
    if not chat or chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        return ""
    if chat.username and msg is not None:
        return f"https://t.me/{chat.username}/{msg.message_id}"
    return ""

def get_display_name(user) -> str:
    """
    Prioriza mostrar el username. Si no lo tiene, el nombre completo.
    """
    if user:
        if hasattr(user, "username") and user.username:
            return user.username
        elif hasattr(user, "full_name") and user.full_name:
            return user.full_name
    return ""

def row_exists_by_url_in_sheet(url: str, sheet_name: str) -> bool:
    """
    Devuelve True si la URL ya está registrada en la hoja sheet_name.
    """
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return False
    # URL está en columna 8
    urls = ws.col_values(8)
    url_set = {str(u).strip() for u in urls if u is not None}
    return str(url).strip() in url_set

# ===========
# API Song.link: Extracción de metadata musical multi-plataforma
# ===========

def get_songlink_metadata(url: str):
    """
    Llama a la API song.link y trata de extraer artista, título, álbum y año,
    priorizando las plataformas con más metadata disponible.
    """
    api_url = "https://api.song.link/v1-alpha.1/links"
    params = {"url": url}
    try:
        resp = requests.get(api_url, params=params)
        resp.raise_for_status()
        data = resp.json()

        entities_by_platform = data.get("entitiesByUniqueId", {})
        links_by_platform = data.get("linksByPlatform", {})

        # Orden preferencial de plataformas (puedes modificar este orden)
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
        
        # Si no se encuentra en las plataformas listadas, buscar metadata general
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

# ===========
# FUNCIONES PARA GUARDAR FILAS EN HOJAS  
# ===========

async def append_row_to_sheet(sheet_name: str, row: list) -> None:
    """
    Agrega una fila a una hoja del documento Google.
    Si la hoja no existe, la crea y agrega encabezados básicos (mas tarde se agregan extra con ensure_columns).
    """
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=100, cols=12)
        headers = [
            "Timestamp","SharedBy","SourceChat","MessageLink","Platform",
            "Artist","Title","URL","Tags","Notes","Álbum","año"
        ]
        ws.append_row(headers, value_input_option=gspread.utils.ValueInputOption.raw)
    ensure_columns(ws, ["Álbum", "año"])
    ws.append_row(row, value_input_option=gspread.utils.ValueInputOption.raw)

async def append_row(context: ContextTypes.DEFAULT_TYPE, update: Update, *, shared_by: str, source_chat: str,
                     artist: str, title: str, url: str, message_link: str, tags: str = "", notes: str = "",
                     album: str = "", year: str = "") -> None:
    """
    Registra la fila principal en Master, y si hay tags,
    propaga la misma fila a hojas por tag (una por cada uno).
    """
    platform = detect_platform(url)
    ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    row = [ts, shared_by, source_chat, message_link,
           platform, artist, title, url, tags, notes, album, year]
    row = [x if x is not None else "" for x in row]

    # Agrega a la hoja Master si no existe aún
    if not row_exists_by_url_in_sheet(url, MASTER_SHEET):
        await append_row_to_sheet(MASTER_SHEET, row)

    # Si tiene tags válidos, también propaga a otras hojas
    if tags:
        for tag in tags.split():
            if tag and tag != "#ascucha":
                tag_name = tag.lstrip("#")
                if tag_name:
                    sheet_name = tag_name[0].upper() + tag_name[1:].lower()
                    if not row_exists_by_url_in_sheet(url, sheet_name):
                        await append_row_to_sheet(sheet_name, row)

# ===========
# COMANDOS Y HANDLERS DE TELEGRAM
# ===========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Inicio del bot, instrucción básica.
    """
    if update.message:
        await update.message.reply_text(
            "¡Listo! Envíame un link o usa /add URL y lo registro en la hoja Master."
        )

def extract_notes(text: str, meta: str) -> str:
    """
    Extrae cualquier comentario textual que no sea hashtag o url.
    """
    tags = set(TAG_RE.findall(text))
    urls = set(m.group("url") for m in URL_RE.finditer(text))
    meta_words = set(meta.split())
    fragments = text.split()

    notes_fragments = []
    for f in fragments:
        if f in tags or f in urls or f in meta_words or f == "/add" or f.startswith("#") or f.lower() == "#ascucha":
            # Excluir hashtags normales y todas URLs excepto links Telegram
            if f.startswith("https://t.me/") or f.startswith("http://t.me/"):
                # Mantener los links Telegram en notas
                notes_fragments.append(f)
            # si no es un link Telegram, excluimos
        else:
            notes_fragments.append(f)

    return " ".join(notes_fragments)

async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Comando /add que registra manualmente un link, infiere los demás datos desde song.link.
    Procesa solo el primer link válido y agrega el resto de links, incluidos Telegram, en notas.
    """
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
    tags = [t for t in tags_raw if t.lower() != "ascucha"]  # excluye 'ascucha'
    tags_str = " ".join(f"#{t}" for t in tags) if tags else ""

    # Extrae notas, excluyendo hashtags y URLs excepto links Telegram
    notes_str = extract_notes(text, "")
    # Añadir resto de links (primer link ya procesado) a notas
    if rest_urls:
        notes_str = (notes_str + " " + " ".join(rest_urls)).strip()

    metadata = get_songlink_metadata(first_url) or {}
    artist = metadata.get("artist", "")
    title = metadata.get("title", "")
    album = metadata.get("album", "")
    year = metadata.get("year", "")

    user = update.effective_user
    shared_by = get_display_name(user)
    source_chat = ""
    if update.effective_chat:
        source_chat = (update.effective_chat.title if update.effective_chat and hasattr(update.effective_chat, "title") and update.effective_chat.title else
                        update.effective_chat.username if update.effective_chat and hasattr(update.effective_chat, "username") and update.effective_chat.username else
                        "")
    message_link = build_message_link(update)

    await append_row(context, update, shared_by=shared_by, source_chat=source_chat,
                    artist=artist, title=title, url=first_url, message_link=message_link,
                    tags=tags_str, notes=notes_str, album=album, year=year)
    if update.message:
        await update.message.reply_text("Anotado en Master ✅")


async def catch_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler general de texto: captura links en mensaje.
    Procesa solo el primer link válido, agrega el resto (incluidos Telegram) en notas.
    """
    if ALLOWED_CHAT_ID and (not update.effective_chat or str(update.effective_chat.id) != str(ALLOWED_CHAT_ID)):
        return
    text = (update.message.text_html if update.message else "") or ""
    all_urls = [m.group("url") for m in URL_RE.finditer(text)]
    if not all_urls:
        return

    first_url = all_urls[0]
    rest_urls = all_urls[1:]

    user = update.effective_user
    shared_by = get_display_name(user)
    source_chat = ""
    if update.effective_chat:
        source_chat = getattr(update.effective_chat, "title", None) or getattr(update.effective_chat, "username", None) or ""
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
# MAIN: ARMA EL BOT
# ===========

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, catch_links))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
