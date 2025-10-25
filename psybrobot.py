import os
import re
import json
import requests
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Optional

from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
import uvicorn

from collections import Counter
from typing import List, Dict, Optional

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)

import gspread
from gspread import utils
from google.oauth2.service_account import Credentials

# ========================
# Configuración de entorno y guard clauses globales
# ========================

# Guard clauses para variables críticas
BOT_TOKEN = os.environ.get("BOT_TOKEN")
SHEET_ID = os.environ.get("SHEET_ID")
GOOGLE_SHEETS_JSON = os.environ.get("GOOGLE_SHEETS_JSON")
if not BOT_TOKEN or not SHEET_ID or not GOOGLE_SHEETS_JSON:
    raise RuntimeError("Faltan variables de entorno requeridas: BOT_TOKEN, SHEET_ID o GOOGLE_SHEETS_JSON")

ALLOWED_CHAT_ID = os.environ.get("ALLOWED_CHAT_ID")  # Opcional

# Inicialización de credenciales y clientes de Google Sheets
sa_info = json.loads(GOOGLE_SHEETS_JSON)
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
gclient = gspread.authorize(creds)
sh = gclient.open_by_key(SHEET_ID)

MASTER_SHEET = "Master"
CACHE_URLS_REGISTERED = set()  # Cache en memoria para evitar duplicados

HEADERS_MASTER = [
    "Timestamp", "SharedBy", "SourceChat", "MessageLink",
    "Platform", "Artist", "Title", "URL", "Tags", "Notes", "Álbum", "Año"
]

# ========================
# Expresiones regulares y plataformas soportadas
# ========================

URL_RE = re.compile(r'(?P<url>(https?://|www\.)[^\s<>\]]+)', re.IGNORECASE)
TAG_RE = re.compile(r"#(?!ascucha\b)\w+")
ASC_LINK_RE = re.compile(r'#ascucha\s+((https?://|www\.)[^\s<>\]]+)', re.IGNORECASE)

PLATFORM_HOSTS = {
    "youtube": {"youtu.be", "youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"},
    "spotify": {"open.spotify.com", "spotify.link"},
    "soundcloud": {"soundcloud.com"},
    "appleMusic": {"apple.com", "music.apple.com"},
    "bandcamp": {"bandcamp.com"}
}

# ========================
# APIs de metadata musical
# ========================

def get_musicbrainz_metadata(artist: str, title: str) -> Dict:
    """Consulta MusicBrainz para obtener metadata musical."""
    try:
        # MusicBrainz API search
        search_url = "https://musicbrainz.org/ws/2/recording/"
        params = {
            "query": f'artist:"{artist}" AND recording:"{title}"',
            "fmt": "json",
            "limit": 1
        }
        headers = {"User-Agent": "PsybroBot/1.0 ( [email protected] )"}  # Requerido por MusicBrainz
        
        resp = requests.get(search_url, params=params, headers=headers, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("recordings"):
            recording = data["recordings"][0]
            # MusicBrainz tiene año en el release
            year = ""
            if recording.get("releases"):
                release_date = recording["releases"][0].get("date", "")
                if release_date:
                    year = release_date.split("-")[0]
            
            return {"year": year}
    except Exception as e:
        print(f"Error MusicBrainz: {e}")
    return {}

def get_spotify_metadata(url: str) -> Dict:
    """Extrae metadata de Spotify Web API (año, BPM, tono)."""
    try:
        # Spotify requiere OAuth, pero podemos obtener info básica del track ID
        # Nota: Para producción necesitas configurar Spotify API credentials
        # Por ahora retornamos vacío o puedes implementar con credentials
        pass
    except Exception:
        pass
    return {}

def get_discogs_metadata(artist: str, title: str) -> Dict:
    """Consulta Discogs para obtener año de release."""
    try:
        search_url = "https://api.discogs.com/database/search"
        params = {
            "artist": artist,
            "track": title,
            "type": "release"
        }
        headers = {"User-Agent": "PsybroBot/1.0"}
        
        resp = requests.get(search_url, params=params, headers=headers, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("results"):
            result = data["results"][0]
            return {"year": str(result.get("year", ""))}
    except Exception as e:
        print(f"Error Discogs: {e}")
    return {}

def get_tunebat_metadata(artist: str, title: str) -> Dict:
    """Consulta base de datos de Tunebat para BPM y Key (requiere scraping o API si disponible)."""
    # Tunebat no tiene API pública gratuita, requeriría scraping
    # Por ahora dejamos placeholder
    return {}

def get_getsongbpm_metadata(artist: str, title: str) -> Dict:
    """Consulta GetSongBPM API para obtener BPM y Key."""
    try:
        # GetSongBPM tiene API pero requiere API key
        # Placeholder para implementación futura
        pass
    except Exception:
        pass
    return {}

def consolidate_metadata(metadata_list: List[Dict]) -> Dict:
    """
    Consolida metadata de múltiples fuentes priorizando consenso.
    Si hay empate, se concatenan las opciones con 'ó'.
    """
    result = {}
    
    # Consolidar año
    years = [m.get("year") for m in metadata_list if m.get("year")]
    if years:
        year_counts = Counter(years)
        most_common = year_counts.most_common(2)
        if len(most_common) == 1:
            result["year"] = most_common[0][0]
        elif len(most_common) > 1 and most_common[0][1] == most_common[1][1]:
            # Empate
            result["year"] = f"{most_common[0][0]} ó {most_common[1][0]}"
        else:
            result["year"] = most_common[0][0]
    else:
        result["year"] = ""
    
    # Consolidar BPM
    bpms = [m.get("bpm") for m in metadata_list if m.get("bpm")]
    if bpms:
        bpm_counts = Counter(bpms)
        most_common = bpm_counts.most_common(2)
        if len(most_common) == 1:
            result["bpm"] = most_common[0][0]
        elif len(most_common) > 1 and most_common[0][1] == most_common[1][1]:
            result["bpm"] = f"{most_common[0][0]} ó {most_common[1][0]}"
        else:
            result["bpm"] = most_common[0][0]
    else:
        result["bpm"] = ""
    
    # Consolidar Tono/Key
    keys = [m.get("key") for m in metadata_list if m.get("key")]
    if keys:
        key_counts = Counter(keys)
        most_common = key_counts.most_common(2)
        if len(most_common) == 1:
            result["key"] = most_common[0][0]
        elif len(most_common) > 1 and most_common[0][1] == most_common[1][1]:
            result["key"] = f"{most_common[0][0]} ó {most_common[1][0]}"
        else:
            result["key"] = most_common[0][0]
    else:
        result["key"] = ""
    
    return result

async def get_enhanced_metadata(artist: str, title: str, url: str) -> Dict:
    """
    Obtiene metadata enriquecida consultando múltiples fuentes y consolidando resultados.
    Retorna: Dict con keys: artist, title, album, year, key, bpm
    """
    # Primero intentamos song.link para datos básicos
    songlink_data = get_songlink_metadata(url) or {}
    
    # Consultar múltiples fuentes para enriquecer
    metadata_sources = []
    
    # Solo consultar si tenemos artist y title
    if artist and title:
        mb_data = get_musicbrainz_metadata(artist, title)
        if mb_data:
            metadata_sources.append(mb_data)
        
        discogs_data = get_discogs_metadata(artist, title)
        if discogs_data:
            metadata_sources.append(discogs_data)
        
        # Aquí puedes agregar más APIs cuando tengas credenciales
        # spotify_data = get_spotify_metadata(url)
        # tunebat_data = get_tunebat_metadata(artist, title)
        # etc.
    
    # Consolidar metadata de todas las fuentes
    consolidated = consolidate_metadata(metadata_sources)
    
    # Combinar con datos de song.link
    return {
        "artist": artist or songlink_data.get("artist", ""),
        "title": title or songlink_data.get("title", ""),
        "album": songlink_data.get("album", ""),
        "year": consolidated.get("year", "") or songlink_data.get("year", ""),
        "key": consolidated.get("key", ""),
        "bpm": consolidated.get("bpm", "")
    }


# ========================
# Utilidades generales
# ========================

def get_display_name(user) -> str:
    """Devuelve el username si existe, si no el nombre completo, si no vacío."""
    if not user:
        return ""
    if getattr(user, "username", None):
        return user.username
    if getattr(user, "full_name", None):
        return user.full_name
    return ""

def get_source_chat(update: Update) -> str:
    """Devuelve el nombre o username del canal/grupo origen del mensaje."""
    chat = update.effective_chat
    if not chat:
        return ""
    title = getattr(chat, "title", None)
    if title:
        return title
    username = getattr(chat, "username", None)
    if username:
        return username
    return ""

def detect_platform(url: str) -> Optional[str]:
    """Detecta la plataforma del link a partir del host de la URL."""
    try:
        host = urlparse(url).netloc.lower()
        for platform, hosts in PLATFORM_HOSTS.items():
            if host in hosts:
                return platform
    except Exception:
        pass
    return None

def build_message_link(update: Update) -> str:
    """Construye el vínculo al mensaje de Telegram si es un grupo/supergrupo público."""
    msg = update.effective_message
    chat = update.effective_chat
    if not chat or chat.type not in ("supergroup", "group", "private"):
        return ""
    if chat.username and msg is not None:
        return f"https://t.me/{chat.username}/{msg.message_id}"
    return ""

def extract_notes(text: str, meta: str = "") -> str:
    """Extrae notas del texto excluyendo hashtags, URLs y palabras clave, conservando links telegram."""
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
    """Asegura que una hoja tenga los encabezados correctamente y en el orden esperado."""
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
    """Agrega columnas requeridas si faltan y actualiza encabezados."""
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
    """Verifica si una URL ya existe en la hoja dada (para evitar duplicados)."""
    global CACHE_URLS_REGISTERED
    if sheet_name == MASTER_SHEET and url in CACHE_URLS_REGISTERED:
        return True
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return False
    urls = ws.col_values(8)
    url_set = {str(u).strip() for u in urls if u}
    if sheet_name == MASTER_SHEET:
        CACHE_URLS_REGISTERED = url_set  # Actualiza cache
    return str(url).strip() in url_set

def get_songlink_metadata(url: str) -> Dict:
    """Obtiene metadatos musicales de song.link priorizando plataformas más populares."""
    api_url = "https://api.song.link/v1-alpha.1/links"
    params = {"url": url}
    try:
        resp = requests.get(api_url, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        entities_by_platform = data.get("entitiesByUniqueId", {})
        links_by_platform = data.get("linksByPlatform", {})
        for platform in ["spotify", "appleMusic", "youtube", "soundcloud", "bandcamp"]:
            info = links_by_platform.get(platform)
            if info and "entityUniqueId" in info:
                entity = entities_by_platform.get(info["entityUniqueId"], {})
                return {
                    "artist": entity.get("artistName", ""),
                    "title": entity.get("title", ""),
                    "album": entity.get("albumName", ""),
                    "year": str(entity.get("year", ""))
                }
        # Fallback genérico
        main_id = data.get("pageEntityUniqueId")
        if main_id and main_id in entities_by_platform:
            entity = entities_by_platform.get(main_id, {})
            return {
                "artist": entity.get("artistName", ""),
                "title": entity.get("title", ""),
                "album": entity.get("albumName", ""),
                "year": str(entity.get("year", ""))
            }
    except Exception:
        pass
    return {}

# ========================
# Operaciones de filas en las hojas de Google Sheets
# ========================

async def append_row_to_sheet(sheet_name: str, row: list):
    """Agrega una fila a una hoja, asegurando encabezados y columnas requeridas."""
    ensure_headers_in_sheet(sheet_name)
    ws = sh.worksheet(sheet_name)
    ensure_columns(ws, ["Álbum", "Año", "Tono", "BPM"])
    ws.append_row(row, value_input_option=utils.ValueInputOption.raw)

async def append_row(context: ContextTypes.DEFAULT_TYPE, update: Update, *, shared_by: str, source_chat: str,
                    artist: str, title: str, url: str, message_link: str, tags: str = "", notes: str = "",
                    album: str = "", year: str = "", key: str = "", bpm: str = ""):
    """Agrega una fila a Master y a las hojas correspondientes según etiquetas."""
    platform = detect_platform(url)
    ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    row = [ts, shared_by, source_chat, message_link,
        platform, artist, title, url, tags, notes, album, year, key, bpm]
    row = [x if x is not None else "" for x in row]
    
    if not row_exists_by_url_in_sheet(url, MASTER_SHEET):
        await append_row_to_sheet(MASTER_SHEET, row)
    
    tags_list = tags.split() if tags else []
    # Early return: Si no hay tags extras, registrar en "Undefined"
    if not tags_list:
        if not row_exists_by_url_in_sheet(url, "Undefined"):
            await append_row_to_sheet("Undefined", row)
        return
    
    for tag in tags_list:
        if tag and tag != "#ascucha":
            tag_name = tag.lstrip("#")
            if tag_name:
                sheet_name = tag_name[0].upper() + tag_name[1:].lower()
                if not row_exists_by_url_in_sheet(url, sheet_name):
                    await append_row_to_sheet(sheet_name, row)

    if not row_exists_by_url_in_sheet(url, "Undefined"):
        await append_row_to_sheet("Undefined", row)

# ========================
# Handlers para telegram - comandos/chat
# ========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start - Responde con un mensaje de bienvenida e instrucción."""
    if update.message:
        await update.message.reply_text("¡Listo! Envíame un link o usa /add URL y lo registro en la hoja Master.")

async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /add - Permite registrar manualmente un enlace con metadatos."""
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
    if not platform:
        if update.message:
            await update.message.reply_text("No reconozco la plataforma del URL.")
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

##    metadata = get_songlink_metadata(first_url) or {}
    ##shared_by = get_display_name(update.effective_user)
    ##source_chat = get_source_chat(update)
    ##message_link = build_message_link(update)
    ##await append_row(context, update, shared_by=shared_by, source_chat=source_chat,
                    ##artist=metadata.get("artist", ""), title=metadata.get("title", ""),
                    ##url=first_url, message_link=message_link,
                    ##tags=tags_str, notes=notes_str,
                    ##album=metadata.get("album", ""), year=metadata.get("year", ""))
    ##if update.message:
        ##await update.message.reply_text("Anotado en Master ✅")


# Obtener metadata básica primero
    basic_metadata = get_songlink_metadata(first_url) or {}
    artist = basic_metadata.get("artist", "")
    title = basic_metadata.get("title", "")
    
    # Obtener metadata enriquecida (año, tono, BPM)
    enhanced_metadata = await get_enhanced_metadata(artist, title, first_url)
    
    shared_by = get_display_name(update.effective_user)
    source_chat = get_source_chat(update)
    message_link = build_message_link(update)
    
    await append_row(context, update, 
                    shared_by=shared_by, source_chat=source_chat,
                    artist=enhanced_metadata.get("artist", ""), 
                    title=enhanced_metadata.get("title", ""),
                    url=first_url, message_link=message_link,
                    tags=tags_str, notes=notes_str,
                    album=enhanced_metadata.get("album", ""), 
                    year=enhanced_metadata.get("year", ""),
                    key=enhanced_metadata.get("key", ""),
                    bpm=enhanced_metadata.get("bpm", ""))
    
    if update.message:
        await update.message.reply_text("Anotado en Master ✅")

async def catch_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler que captura mensajes de texto con links precedidos por #ascucha."""
    if ALLOWED_CHAT_ID and (not update.effective_chat or str(update.effective_chat.id) != str(ALLOWED_CHAT_ID)):
        return
    text = (update.message.text_html if update.message else "") or ""
    ascucha_links = [m.group(1) for m in ASC_LINK_RE.finditer(text)]
    if not ascucha_links:
        return
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
    if not platform:
        if update.message:
            await update.message.reply_text(
                f"No reconozco la plataforma del URL {first_url}."
            )
        return
    if row_exists_by_url_in_sheet(first_url, MASTER_SHEET):
        if update.message:
            await update.message.reply_text("Ya estaba registrado ✅ (duplicado por URL).")
        return
   ## metadata = get_songlink_metadata(first_url) or {}
    ##await append_row(context, update, shared_by=shared_by, source_chat=source_chat,
                    ##artist=metadata.get("artist", ""), title=metadata.get("title", ""),
                    ##url=first_url, message_link=message_link,
                    ##tags=tags_str, notes=notes_str,
                    ##album=metadata.get("album", ""), year=metadata.get("year", ""))
    ##if update.message:
        ##await update.message.reply_text("Anotado en Master ✅")

# Obtener metadata básica primero
    basic_metadata = get_songlink_metadata(first_url) or {}
    artist = basic_metadata.get("artist", "")
    title = basic_metadata.get("title", "")
    
    # Obtener metadata enriquecida (año, tono, BPM)
    enhanced_metadata = await get_enhanced_metadata(artist, title, first_url)
    
    shared_by = get_display_name(update.effective_user)
    source_chat = get_source_chat(update)
    message_link = build_message_link(update)
    
    await append_row(context, update, 
                    shared_by=shared_by, source_chat=source_chat,
                    artist=enhanced_metadata.get("artist", ""), 
                    title=enhanced_metadata.get("title", ""),
                    url=first_url, message_link=message_link,
                    tags=tags_str, notes=notes_str,
                    album=enhanced_metadata.get("album", ""), 
                    year=enhanced_metadata.get("year", ""),
                    key=enhanced_metadata.get("key", ""),
                    bpm=enhanced_metadata.get("bpm", ""))
    
    if update.message:
        await update.message.reply_text("Anotado en Master ✅")

# ====================
# FASTAPI + Telegram Application para webhooks con lifespan
# ====================

# Inicialización global de la aplicación de Telegram
telegram_app = None

async def init_telegram_app():
    """Inicializa la aplicación de Telegram de forma segura."""
    global telegram_app
    if telegram_app is None:
        telegram_app = Application.builder().token(BOT_TOKEN if BOT_TOKEN is not None else "").build()
        telegram_app.add_handler(CommandHandler("start", start))
        telegram_app.add_handler(CommandHandler("add", add_cmd))
        telegram_app.add_handler(MessageHandler(filters.TEXT, catch_links))  # Sin restricción de grupos
        await telegram_app.initialize()
        await telegram_app.start()
        print("✅ Bot inicializado correctamente")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación FastAPI."""
    # Startup - Inicialización
    await init_telegram_app()
    print("🚀 FastAPI iniciado con bot de Telegram")
    
    yield  # Aquí la aplicación está activa
    
    # Shutdown - Limpieza
    global telegram_app
    if telegram_app:
        await telegram_app.stop()
        await telegram_app.shutdown()
        print("✅ Bot cerrado correctamente")
    print("🛑 FastAPI cerrado")

# Crear la aplicación FastAPI con lifespan
app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    """Endpoint webhook para recibir eventos desde Telegram."""
    global telegram_app
    
    # Guard clause: verificar inicialización
    if not telegram_app:
        await init_telegram_app()
    
    try:
        json_update = await request.json()
        if telegram_app:
            update = Update.de_json(json_update, telegram_app.bot)
            await telegram_app.process_update(update)
            return Response(content="ok", status_code=200)
        else:
            return Response(content="Error: Telegram app is not initialized", status_code=500)
    except Exception as e:
        print(f"❌ Error webhook: {str(e)}")
        return Response(content=f"Error: {str(e)}", status_code=400)

@app.get("/")
async def root():
    """Endpoint de salud para verificar que el servicio está activo."""
    return {"status": "Bot activo", "webhook": "/webhook"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
    print(f"✅ FastAPI iniciado en http://localhost:{port}")