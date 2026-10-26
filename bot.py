import asyncio
import html as htmllib
import json
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from telegram import BotCommand, Update
from telegram.error import RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])  # only this Telegram user can use the bot
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID") or None  # default parent folder
LOCAL_API_URL = os.getenv("LOCAL_API_URL") or None  # e.g. http://localhost:8081 (files up to 2 GB)
WORK_DIR = Path(os.getenv("WORK_DIR", "work"))
SITE_URL = os.getenv("SITE_URL") or None  # e.g. https://yoursite.com/r2admin.php
SITE_PIN = os.getenv("SITE_PIN") or None  # admin PIN (keep it only in .env)
TOKEN_FILE = "token.json"
# --- speed tuning (all optional, set in .env) ---
PARALLEL_FILES = max(1, int(os.getenv("PARALLEL_FILES", str(max(2, min(4, os.cpu_count() or 2))))))  # files downloaded/compressed at the same time
SITE_PARALLEL = max(1, int(os.getenv("SITE_PARALLEL", "6")))  # parts of a website upload sent at the same time
PART_RETRIES = max(0, int(os.getenv("PART_RETRIES", "3")))  # retries of ONE failed part (not the whole file)
MP3_LEVEL = min(9, max(0, int(os.getenv("MP3_LEVEL", "7"))))  # ffmpeg mp3 effort: 0 best/slowest ... 9 fastest
SETTINGS_FILE = Path("settings.json")
SCOPES = ["https://www.googleapis.com/auth/drive"]

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO
)
log = logging.getLogger("zipbot")

# ---------- State ----------
queues: dict[int, asyncio.Queue] = {}  # one job queue per user: files are handled one by one
pending: dict[int, int] = {}  # files received but not finished yet
queue_order: dict[int, list] = {}  # queued (not yet uploaded) files in the order they were sent
_chat_locks: dict[int, asyncio.Lock] = {}
worker_tasks: dict[int, asyncio.Task] = {}  # the task processing each user's files
cmd_tasks: dict[int, asyncio.Task] = {}  # running /upload or /zip job per user
current_job: dict[int, bool] = {}  # is a file being processed right now?
STATE = {"gen": 0}  # /stop increases this; running ffmpeg / uploads / zipping notice and abort


class Stopped(Exception):
    pass


# ---------- Retry ----------
RETRIES = int(os.getenv("RETRIES", "2"))  # extra attempts after the first one (2 = 3 tries in total)
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "3"))  # seconds; grows with each attempt (3s, 6s, ...)


def retry_sync(func, *args, cancelled=lambda: False, label="", **kwargs):
    """Call func(*args). On failure retry up to RETRIES more times. /stop is never retried."""
    for attempt in range(RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Stopped:
            raise
        except Exception as e:
            if attempt >= RETRIES or cancelled():
                raise
            log.warning("%s failed (attempt %d/%d): %s - retrying", label or func.__name__, attempt + 1, RETRIES + 1, e)
            reset_drive_service()  # a stale connection is a common cause: rebuild it
            time.sleep(RETRY_DELAY * (attempt + 1))


async def retry_async(coro_fn, *args, label="", **kwargs):
    """Same as retry_sync for coroutines."""
    for attempt in range(RETRIES + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except (Stopped, asyncio.CancelledError):
            raise
        except Exception as e:
            if attempt >= RETRIES:
                raise
            log.warning("%s failed (attempt %d/%d): %s - retrying", label or "task", attempt + 1, RETRIES + 1, e)
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))


def make_check():
    """Returns a function that becomes True as soon as /stop is used."""
    gen = STATE["gen"]
    return lambda: STATE["gen"] != gen

_bg_tasks: set = set()


def spawn(coro):
    """Run a coroutine in the background (keeps a reference so it isn't garbage collected)."""
    t = asyncio.get_running_loop().create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


# ---------- Settings (remembered between restarts) ----------
def load_settings() -> dict:
    try:
        return json.loads(SETTINGS_FILE.read_text())
    except Exception:
        return {}


def save_settings(data: dict):
    SETTINGS_FILE.write_text(json.dumps(data, indent=2))


def get_user_settings(uid: int) -> dict:
    return load_settings().get(str(uid), {})


def set_user_setting(uid: int, **kwargs):
    data = load_settings()
    data.setdefault(str(uid), {}).update(kwargs)
    save_settings(data)


# ---------- Google Drive ----------
_creds = None
_service = None
_drive_lock = threading.Lock()


def _load_creds():
    global _creds
    if _creds is None:
        if not os.path.exists(TOKEN_FILE):
            raise RuntimeError("token.json not found. Run `python auth.py` first.")
        _creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not _creds.valid:
        if _creds.refresh_token:
            _creds.refresh(Request())
            Path(TOKEN_FILE).write_text(_creds.to_json())
        else:
            raise RuntimeError("Drive credentials invalid. Re-run `python auth.py`.")
    return _creds


def drive_service(fresh: bool = False):
    """The upload service is reused between files (faster). fresh=True builds a separate one."""
    global _service
    with _drive_lock:
        creds = _load_creds()
        if fresh:
            return build("drive", "v3", credentials=creds, cache_discovery=False)
        if _service is None:
            _service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return _service


def reset_drive_service():
    global _service
    with _drive_lock:
        _service = None


def get_or_create_folder(path: str) -> str:
    """Find or create a folder (supports nested like 'Work/Reports'). Returns its ID."""
    service = drive_service(fresh=True)
    parent = DRIVE_FOLDER_ID or "root"
    for part in [p.strip() for p in path.split("/") if p.strip()]:
        escaped = part.replace("\\", "\\\\").replace("'", "\\'")
        q = (
            f"name='{escaped}' and mimeType='application/vnd.google-apps.folder' "
            f"and '{parent}' in parents and trashed=false"
        )
        res = service.files().list(q=q, fields="files(id)", pageSize=1).execute()
        if res["files"]:
            parent = res["files"][0]["id"]
        else:
            meta = {
                "name": part,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent],
            }
            parent = service.files().create(body=meta, fields="id").execute()["id"]
    return parent


def upload_to_drive(path: Path, folder_id: str | None, cancelled=lambda: False) -> str:
    service = drive_service()
    metadata = {"name": path.name}
    if folder_id:
        metadata["parents"] = [folder_id]
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    big = path.stat().st_size > 5 * 1024 * 1024
    # small files: one single request (fastest); big files: resumable in 32 MB chunks
    media = MediaFileUpload(
        str(path), mimetype=mime, resumable=big, chunksize=32 * 1024 * 1024 if big else -1
    )
    request = service.files().create(body=metadata, media_body=media, fields="id, webViewLink")
    if cancelled():
        raise Stopped()
    if not big:
        return request.execute(num_retries=3)["webViewLink"]
    response = None
    while response is None:
        if cancelled():  # checked between chunks, so a stop takes effect within seconds
            raise Stopped()
        _, response = request.next_chunk(num_retries=3)
    return response["webViewLink"]


# ---------- Website (r2admin.php) ----------
SITE_AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".webm", ".flac"}
_site = {"session": None, "csrf": None}
_site_lock = threading.Lock()
SITE_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}


def _csrf_from(page: str):
    m = re.search(r'name="csrf"\s+value="([^"]+)"', page)
    return m.group(1) if m else None


def _is_login(page: str) -> bool:
    return 'name="pin"' in page or 'name="login_pin"' in page


def site_login() -> str:
    """Log in with the PIN through the site's admin.php login. Returns the dashboard HTML."""
    if not SITE_URL or not SITE_PIN:
        raise RuntimeError("SITE_URL / SITE_PIN are not set in .env")
    sess = requests.Session()
    sess.headers.update(SITE_HEADERS)
    r = sess.get(SITE_URL, timeout=30)
    r.raise_for_status()
    page = r.text
    if _is_login(page):
        login_url = urljoin(SITE_URL, "admin.php")
        lp = sess.get(login_url, timeout=30).text
        csrf = _csrf_from(lp) or _csrf_from(page)
        if not csrf:
            raise RuntimeError("Could not find the login token on the site page")
        sess.post(login_url, data={"csrf": csrf, "action": "login", "pin": SITE_PIN, "login_pin": SITE_PIN}, timeout=30)
        page = sess.get(SITE_URL, timeout=30).text
        if _is_login(page):
            # the page may still show a form: ask the upload API whether we are logged in
            try:
                chk = sess.post(urljoin(SITE_URL, "r2_upload_api.php"),
                                json={"action": "ping", "csrf": _csrf_from(page)}, timeout=30)
                logged_in = chk.status_code not in (401, 403)
            except Exception:
                logged_in = False
            if not logged_in:
                raise RuntimeError("Website login failed - check SITE_PIN in .env")
    _site["session"], _site["csrf"] = sess, _csrf_from(page)
    return page


def site_page() -> str:
    """Fetch the dashboard (logs in first if needed)."""
    if _site["session"] is None:
        return site_login()
    page = _site["session"].get(SITE_URL, timeout=30).text
    if _is_login(page):
        return site_login()
    _site["csrf"] = _csrf_from(page) or _site["csrf"]
    return page


def site_stories() -> list:
    with _site_lock:
        page = site_page()
    # accepts both 'ID 6' (r2admin.php) and 'Story ID: 6' (admin.php dashboard)
    found = re.findall(r'<div class="story-title"[^>]*>(.*?)</div>\s*<div class="small"[^>]*>\s*(?:Story\s+)?ID:?\s*(\d+)', page, flags=re.S)
    if not found and not re.search(r'name="action"\s+value="logout"', page):
        # not the dashboard at all (login page, host protection page, error page...)
        title = re.search(r'<title>(.*?)</title>', page, flags=re.S | re.I)
        log.warning("Site page is not the dashboard. Title: %s | start: %r", title.group(1).strip() if title else None, page[:200])
        _site["session"] = None
        raise RuntimeError("The site did not return the admin dashboard (login failed or the host blocked the bot). Check SITE_URL / SITE_PIN, then see the bot log.")
    return [{"id": int(i), "title": htmllib.unescape(re.sub(r'<[^>]+>', '', t)).strip()} for t, i in found]


def site_add_story(title: str) -> dict:
    with _site_lock:
        site_page()
        r = _site["session"].post(
            SITE_URL,
            data={"csrf": _site["csrf"], "action": "add_story", "title": title, "description": ""},
            timeout=60,
        )
        # the site answers with a redirect to a page that shows a green/red notice
        m = re.search(r'<div class="notice( error)?">\s*(.*?)\s*</div>', r.text, flags=re.S)
        if m and m.group(1):
            raise RuntimeError(htmllib.unescape(m.group(2)).strip())
    stories = site_stories()
    want = title.strip().casefold()
    hits = [st for st in stories if st["title"].strip().casefold() == want]
    if hits:
        return max(hits, key=lambda x: x["id"])  # newest = highest ID
    raise RuntimeError("Story was not created (the site did not list it afterwards)")


def site_api(payload: dict) -> dict:
    with _site_lock:  # short calls only; the big R2 transfers run outside this lock
        return _site_api_locked(payload)


def _site_api_locked(payload: dict) -> dict:
    url = urljoin(SITE_URL, "r2_upload_api.php")
    err = "unknown error"
    for attempt in (1, 2):
        if _site["session"] is None:
            site_login()
        r = _site["session"].post(url, json={**payload, "csrf": _site["csrf"]}, timeout=(20, 180))
        try:
            data = r.json()
        except Exception:
            data = None
        if data and data.get("ok"):
            return data
        err = (data or {}).get("error") or f"HTTP {r.status_code}"
        if attempt == 1 and (r.status_code in (401, 403) or re.search(r"log ?in|csrf|token|session|expired", str(err), re.I)):
            _site["session"] = None  # session ended: log in again and retry once
            continue
        break
    raise RuntimeError(f"Website: {err}")


_part_pool = ThreadPoolExecutor(max_workers=SITE_PARALLEL, thread_name_prefix="r2part")
_tls = threading.local()


def _put_session() -> requests.Session:
    """One keep-alive HTTP session per worker thread (faster than a new connection per part)."""
    sess = getattr(_tls, "sess", None)
    if sess is None:
        sess = _tls.sess = requests.Session()
    return sess


def _upload_part(path: Path, upload_id: str, n: int, part_size: int, mime: str, cancelled) -> dict:
    """Read part n from the file and PUT it to R2. A failed part is retried on its own."""
    last = None
    for attempt in range(PART_RETRIES + 1):
        if cancelled():
            raise Stopped()
        try:
            signed = site_api({"action": "sign_part", "uploadId": upload_id, "partNumber": n})
            with open(path, "rb") as f:
                f.seek((n - 1) * part_size)
                chunk = f.read(part_size)
            r = _put_session().put(signed["url"], data=chunk, headers={"Content-Type": mime}, timeout=(20, 1800))
            if not r.ok:
                raise RuntimeError(f"R2 upload failed with HTTP {r.status_code}")
            etag = r.headers.get("ETag")
            if not etag:
                raise RuntimeError("R2 did not return an ETag (bucket CORS ExposeHeaders?)")
            return {"partNumber": n, "etag": etag}
        except Stopped:
            raise
        except Exception as e:
            last = e
            if attempt < PART_RETRIES:
                log.warning("part %d of %s failed (%s) - retrying", n, path.name, e)
                time.sleep(1 + 2 * attempt)
    raise last


def upload_to_site(path: Path, story_id, cancelled=lambda: False) -> str:
    """Upload through the site's own API (initiate -> sign_part -> PUT to R2 -> complete), parts in parallel."""
    if path.suffix.lower() not in SITE_AUDIO_EXTS:
        raise RuntimeError("the website only accepts audio: " + ", ".join(sorted(e[1:] for e in SITE_AUDIO_EXTS)))
    size = path.stat().st_size
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    if cancelled():
        raise Stopped()
    started = site_api({
        "action": "initiate", "storyId": str(story_id),
        "fileName": path.name, "fileSize": size, "mimeType": mime,
    })
    upload_id = started["uploadId"]
    part_size, total = int(started["partSize"]), int(started["totalParts"])
    futures = []
    try:
        futures = [
            _part_pool.submit(_upload_part, path, upload_id, n, part_size, mime, cancelled)
            for n in range(1, total + 1)
        ]
        parts = [fu.result() for fu in futures]  # in part order
        site_api({"action": "complete", "uploadId": upload_id, "parts": parts})
    except BaseException:
        for fu in futures:
            fu.cancel()
        try:
            site_api({"action": "abort", "uploadId": upload_id})  # don't leave half uploads in R2
        except Exception:
            pass
        raise
    return "saved on the website"


def do_upload(path: Path, uid: int, cancelled=lambda: False) -> str:
    """Send one file to the chosen destination, retrying failed attempts."""
    return retry_sync(_do_upload_once, path, uid, cancelled, cancelled=cancelled, label=f"upload {path.name}")


def _do_upload_once(path: Path, uid: int, cancelled=lambda: False) -> str:
    st = get_user_settings(uid)
    if st.get("dest") == "site":
        if not st.get("story_id"):
            raise RuntimeError("No story selected. Use /stories then /story <id>, or /newstory Title")
        return upload_to_site(path, st["story_id"], cancelled)
    return upload_to_drive(path, target_folder_id(uid), cancelled)


def target_label(uid: int) -> str:
    st = get_user_settings(uid)
    if st.get("dest") == "site":
        return f"website story: {st.get('story_title') or 'not selected'}"
    return target_folder_name(uid)


def target_folder_id(uid: int) -> str | None:
    return get_user_settings(uid).get("folder_id") or DRIVE_FOLDER_ID


def target_folder_name(uid: int) -> str:
    return get_user_settings(uid).get("folder") or "My Drive (root)"


# ---------- Helpers ----------
def user_dir(user_id: int) -> Path:
    d = WORK_DIR / str(user_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def safe_name(name: str) -> str:
    return re.sub(r"[^\w.\- ]", "_", name).strip() or "file"


def unique_path(folder: Path, name: str) -> Path:
    """Pick a free file name and reserve it."""
    folder.mkdir(parents=True, exist_ok=True)
    p = folder / safe_name(name)
    stem, suffix, i = p.stem, p.suffix, 1
    while True:
        try:
            with open(p, "x"):
                pass
            return p
        except FileExistsError:
            p = folder / f"{stem}_{i}{suffix}"
            i += 1


def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id != OWNER_ID:
            if update.message:
                await update.message.reply_text("Sorry, this bot is private.")
            return
        return await func(update, context)

    return wrapper


def human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


async def say(msg, text):
    """Reply without hitting Telegram's flood limit."""
    lock = _chat_locks.setdefault(msg.chat_id, asyncio.Lock())
    async with lock:
        for _ in range(5):
            try:
                return await msg.reply_text(text)
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except Exception:
                log.exception("reply failed")
                return None
    return None


def natural_key(name: str):
    """Sort like a person: file2 before file10."""
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", name)]


def number_label(st: dict, n: int) -> str:
    prefix = st.get("number_prefix", "Ep")
    return f"{prefix} {n:03d}" if prefix else f"{n:03d}"


def apply_number(uid: int, path: Path) -> Path:
    """Rename 'talk.mp3' to 'Ep 001 - talk.mp3' (or 'Ep 001.mp3') so Drive's name sort keeps the order."""
    st = get_user_settings(uid)
    if not st.get("number"):
        return path
    n = int(st.get("next_number", 1))
    label = number_label(st, n)
    new_name = f"{label} - {path.name}" if st.get("number_keepname", True) else f"{label}{path.suffix}"
    try:
        new = unique_path(path.parent, new_name)
        os.replace(path, new)  # replaces the empty reserved placeholder
    except Exception:
        log.exception("numbering failed")
        return path
    set_user_setting(uid, next_number=n + 1)
    return new


def ordered_files(uid: int) -> list:
    """Queued files: in the order they were sent (default) or sorted by name (/sort name)."""
    folder = user_dir(uid)
    listed = [p for p in queue_order.get(uid, []) if p.exists()]
    queue_order[uid] = listed
    known = set(listed)
    rest = sorted(
        (f for f in folder.iterdir() if f.is_file() and f not in known),
        key=lambda f: (f.stat().st_mtime, f.name),
    )
    files = rest + listed
    if get_user_settings(uid).get("sort") == "name":
        files = sorted(files, key=lambda f: natural_key(f.name))
    return files


# ---------- Audio compression ----------
AUDIO_EXTS = {".mp3", ".wav", ".flac", ".m4a", ".aac", ".ogg", ".oga", ".opus", ".wma", ".aiff", ".aif", ".amr"}
NO_RECOMPRESS = AUDIO_EXTS | {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mkv", ".mov", ".zip", ".rar", ".7z", ".gz", ".pdf"}


def is_audio(path: Path) -> bool:
    return path.suffix.lower() in AUDIO_EXTS


def compress_audio(src: Path, bitrate: int, mono: bool, cancelled=lambda: False):
    """Re-encode to MP3. Returns (final_path, old_size, new_size). Keeps the original if no smaller."""
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg is not installed (run: sudo apt install -y ffmpeg)")
    old = src.stat().st_size
    tmp = src.with_name(src.stem + ".compressing.mp3")
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error", "-i", str(src),
        "-vn", "-map_metadata", "0", "-codec:a", "libmp3lame",
        "-compression_level", str(MP3_LEVEL),  # higher = faster encoding
        "-b:a", f"{bitrate}k",
    ]
    if mono:
        cmd += ["-ac", "1"]
    cmd.append(str(tmp))
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    while True:
        try:
            _, err = proc.communicate(timeout=0.3)
            break
        except subprocess.TimeoutExpired:
            if cancelled():  # /stop: kill ffmpeg right away
                proc.kill()
                proc.communicate()
                tmp.unlink(missing_ok=True)
                raise Stopped()
    if proc.returncode != 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError((err or "").strip()[-300:] or "ffmpeg failed")
    if cancelled():
        tmp.unlink(missing_ok=True)
        raise Stopped()
    new = tmp.stat().st_size
    if new >= old:  # already small/efficient: keep the original
        tmp.unlink(missing_ok=True)
        return src, old, old
    src.unlink()
    final = unique_path(src.parent, src.with_suffix(".mp3").name)
    tmp.rename(final)
    return final, old, new


# ---------- One-by-one file processing ----------
def get_queue(uid: int) -> asyncio.Queue:
    q = queues.get(uid)
    if q is None:
        q = asyncio.Queue()
        queues[uid] = q
        worker_tasks[uid] = spawn(worker(uid, q))
    return q


file_tasks: dict[int, set] = {}  # files being processed right now, per user


async def worker(uid: int, q: asyncio.Queue):
    """Download + compress up to PARALLEL_FILES files at once. Numbering, queueing and
    uploading still happen in the order the files were sent (see the gate in _process_file)."""
    sem = asyncio.Semaphore(PARALLEL_FILES)
    active = file_tasks.setdefault(uid, set())
    prev_gate = None

    async def run_one(msg, bot, gate_prev, gate_self):
        try:
            await process_file(msg, bot, uid, gate_prev)
        except Stopped:
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("processing failed")
        finally:
            gate_self.set()  # let the next file continue, whatever happened here
            active.discard(asyncio.current_task())
            if queues.get(uid) is q:  # not replaced by /stop
                pending[uid] = max(0, pending.get(uid, 1) - 1)
                current_job[uid] = bool(active)
            q.task_done()
            sem.release()

    while True:
        await sem.acquire()
        msg, bot = await q.get()
        current_job[uid] = True
        gate = asyncio.Event()
        task = asyncio.create_task(run_one(msg, bot, prev_gate, gate))
        active.add(task)
        prev_gate = gate


async def wait_idle(uid: int, msg=None):
    """Wait until every file already sent has been processed."""
    n = pending.get(uid, 0)
    if n and msg:
        await say(msg, f"Waiting for {n} file(s) still being processed...")
    await get_queue(uid).join()


async def process_file(msg, bot, uid: int, gate_prev=None):
    ctx = {"dest": None, "committed": False}
    try:
        await _process_file(msg, bot, uid, ctx, gate_prev)
    except asyncio.CancelledError:
        d = ctx["dest"]
        if d is not None and not ctx["committed"]:
            d.unlink(missing_ok=True)  # remove the half-downloaded / half-processed file
        raise


async def _process_file(msg, bot, uid: int, ctx: dict, gate_prev=None):
    cancelled = make_check()
    if msg.document:
        tg_file_id, name = msg.document.file_id, msg.document.file_name or "document"
    elif msg.photo:
        p = msg.photo[-1]
        tg_file_id, name = p.file_id, f"photo_{p.file_unique_id}.jpg"
    elif msg.video:
        tg_file_id = msg.video.file_id
        name = msg.video.file_name or f"video_{msg.video.file_unique_id}.mp4"
    elif msg.audio:
        tg_file_id = msg.audio.file_id
        name = msg.audio.file_name or f"audio_{msg.audio.file_unique_id}.mp3"
    elif msg.voice:
        tg_file_id, name = msg.voice.file_id, f"voice_{msg.voice.file_unique_id}.ogg"
    else:
        return

    async def busy(action="upload_document"):
        try:
            await bot.send_chat_action(msg.chat_id, action)
        except Exception:
            pass

    dest = unique_path(user_dir(uid), name)
    ctx["dest"] = dest
    async def _download():
        await busy()
        tg_file = await bot.get_file(
            tg_file_id, read_timeout=3600, write_timeout=3600, connect_timeout=60
        )
        local = Path(tg_file.file_path) if tg_file.file_path else None
        if LOCAL_API_URL and local and local.is_absolute() and local.exists():
            await asyncio.to_thread(shutil.move, str(local), str(dest))
        else:
            await tg_file.download_to_drive(custom_path=dest)

    try:
        await retry_async(_download, label=f"download {name}")
    except Exception as e:
        log.exception("download failed")
        dest.unlink(missing_ok=True)
        note = "" if LOCAL_API_URL else "\n(Telegram's default bot limit is 20 MB. Run bigfiles.sh to allow up to 2 GB.)"
        await say(msg, f"Could not download '{name}' after {RETRIES + 1} attempts: {e}{note}")
        return

    info = ""
    st = get_user_settings(uid)
    if st.get("compress") and is_audio(dest):
        try:
            await busy()
            dest, old, new = await asyncio.to_thread(
                compress_audio, dest, st.get("bitrate", 64), st.get("mono", False), cancelled
            )
            ctx["dest"] = dest
            info = (
                f"\nCompressed {human_size(old)} -> {human_size(new)}"
                if new < old
                else "\n(Already small, original kept)"
            )
        except Exception as e:
            log.exception("compression failed")
            info = f"\nCompression failed, original kept: {e}"

    if gate_prev is not None:
        await gate_prev.wait()  # the previous file is done: now number / upload in the order sent
    st = get_user_settings(uid)
    dest = apply_number(uid, dest)
    ctx["dest"] = dest

    if st.get("auto"):
        try:
            await busy()
            link = await asyncio.to_thread(do_upload, dest, uid, cancelled)
            dest.unlink(missing_ok=True)
            ctx["committed"] = True
            await say(msg, f"Uploaded {dest.name} to {target_label(uid)}{info}\n{link}")
        except Exception as e:
            log.exception("auto upload failed")
            queue_order.setdefault(uid, []).append(dest)
            ctx["committed"] = True
            await say(msg, f"Upload failed after {RETRIES + 1} attempts: {e}\nFile kept in queue; try /upload.")
        return

    queue_order.setdefault(uid, []).append(dest)
    ctx["committed"] = True
    count = len(ordered_files(uid))
    await say(
        msg,
        f"Added {dest.name} ({human_size(dest.stat().st_size)}). Queue: {count} file(s).{info}",
    )


# ---------- Handlers ----------
HELP_TEXT = """HOW IT WORKS
Send me files (documents, photos, videos, audio). I upload them to your Google Drive, one by one, in the order you sent them.

QUICK START
1) /folder My Folder  - choose the Drive folder
2) /auto on  - every file goes to Drive right away
   or /auto off, send files, then /upload (separate files) or /zip name (one zip)

WEBSITE (audiosfm.online)
/dest site|drive - send files to the website or to Google Drive
/stories - list the stories on the website
/story <id or title> - choose which story gets the episodes
/newstory Title - create a story and select it
(audio only: mp3 m4a aac wav ogg opus webm flac. Zip is Drive-only)

UPLOADING
/folder Name - Drive folder (created if missing, 'A/B' for nested). /folder root = My Drive
/auto on|off - on: upload each file instantly. off: collect files in a queue first
/upload - upload all queued files separately
/zip [name] - zip queued files and upload the zip

QUEUE
/list - show queued files, in order
/clear - delete all queued files
/stop - FORCE STOP: cancel the current file, drop waiting files (queued files are kept)

AUDIO COMPRESSION
/compress on|off - shrink audio to MP3 before upload
/bitrate 64 - quality in kbps, 16-320 (32 voice, 64 small, 128 good)
/mono on|off - mono audio, about half the size

NAMING AND ORDER
/number on|off - name files Ep 001, Ep 002... so Drive keeps the order
/number prefix Ep - text before the number ('none' for plain 001)
/number name on|off - keep the original name or only the number
/number reset - next file is 001 again. /number 25 starts at 25
/sort sent|name - queue order: as sent, or by file name

OTHER
/help - this message
/start - short welcome

TIPS
- Several files are downloaded/compressed at the same time; numbering and upload keep the order you sent them.
- Send photos as 'File' to keep full quality.
- Telegram limits bot downloads to 20 MB unless the big-files setup is installed (up to 2 GB).
- Before a new numbered batch: /number reset"""


def settings_summary(uid: int) -> str:
    st = get_user_settings(uid)
    onoff = lambda v: "ON" if v else "OFF"
    fmt = number_label(st, int(st.get("next_number", 1)))
    fmt = fmt + (" - name" if st.get("number_keepname", True) else "")
    return (
        "YOUR SETTINGS NOW\n"
        f"Destination: {target_label(uid)}\n"
        f"Auto-upload: {onoff(st.get('auto'))}\n"
        f"Audio compression: {onoff(st.get('compress'))} ({st.get('bitrate', 64)} kbps, mono {onoff(st.get('mono'))})\n"
        f"Numbering: {onoff(st.get('number'))} (next: {fmt})\n"
        f"Queue order: {'by name' if st.get('sort') == 'name' else 'as sent'}"
    )


@owner_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hi! Send me files and I will upload them to your Google Drive.\n\n"
        "Send /help to see every command and how to use them."
    )


@owner_only
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await update.message.reply_text(HELP_TEXT)
    await update.message.reply_text(settings_summary(uid))


@owner_only
async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    uid = update.effective_user.id
    pending[uid] = pending.get(uid, 0) + 1
    get_queue(uid).put_nowait((msg, context.bot))  # processed one by one, in order


@owner_only
async def set_folder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not context.args:
        await update.message.reply_text(
            f"Current folder: {target_folder_name(uid)}\n"
            "Change it with: /folder My Folder Name\n"
            "Reset to root with: /folder root"
        )
        return
    name = " ".join(context.args).strip()
    if name.lower() == "root":
        set_user_setting(uid, folder=None, folder_id=None)
        await update.message.reply_text("Folder reset to My Drive root.")
        return
    status = await update.message.reply_text(f"Looking for folder '{name}'...")
    try:
        folder_id = await asyncio.to_thread(get_or_create_folder, name)
        set_user_setting(uid, folder=name, folder_id=folder_id)
        await status.edit_text(f"Folder set: {name}\nAll uploads will go there.")
    except Exception as e:
        log.exception("folder failed")
        await status.edit_text(f"Could not set folder: {e}")


@owner_only
async def set_auto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    arg = context.args[0].lower() if context.args else ""
    if arg not in ("on", "off"):
        state = "ON" if get_user_settings(uid).get("auto") else "OFF"
        await update.message.reply_text(f"Auto-upload is {state}. Use /auto on or /auto off.")
        return
    set_user_setting(uid, auto=(arg == "on"))
    if arg == "on":
        await update.message.reply_text(
            f"Auto-upload ON. Every file you send goes straight to: {target_label(uid)}"
        )
    else:
        await update.message.reply_text("Auto-upload OFF. Files are queued until /upload or /zip.")


@owner_only
async def set_compress(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    arg = context.args[0].lower() if context.args else ""
    if arg in ("on", "off"):
        set_user_setting(uid, compress=(arg == "on"))
    st = get_user_settings(uid)
    await update.message.reply_text(
        f"Audio compression: {'ON' if st.get('compress') else 'OFF'}\n"
        f"Bitrate: {st.get('bitrate', 64)} kbps | Mono: {'yes' if st.get('mono') else 'no'}\n"
        "Change with /compress on|off, /bitrate 64, /mono on|off"
    )


@owner_only
async def set_bitrate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    try:
        kbps = int(context.args[0])
        assert 16 <= kbps <= 320
    except Exception:
        await update.message.reply_text(
            "Use a number from 16 to 320, e.g. /bitrate 64\n"
            "32 = voice/podcast, 64 = small music, 128 = good, 192+ = high quality."
        )
        return
    set_user_setting(uid, bitrate=kbps)
    await update.message.reply_text(f"Audio bitrate set to {kbps} kbps.")


@owner_only
async def set_mono(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    arg = context.args[0].lower() if context.args else ""
    if arg not in ("on", "off"):
        await update.message.reply_text("Use /mono on or /mono off")
        return
    set_user_setting(uid, mono=(arg == "on"))
    await update.message.reply_text(f"Mono audio: {arg.upper()}")


@owner_only
async def list_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    files = ordered_files(uid)
    if not files:
        await update.message.reply_text(f"Queue is empty.\nDestination: {target_label(uid)}")
        return
    total = sum(f.stat().st_size for f in files)
    lines = [f"{i}. {f.name} ({human_size(f.stat().st_size)})" for i, f in enumerate(files, 1)]
    lines.append(f"\nTotal: {human_size(total)}\nDestination: {target_label(uid)}")
    await update.message.reply_text("\n".join(lines))


@owner_only
async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    shutil.rmtree(user_dir(uid), ignore_errors=True)
    queue_order.pop(uid, None)
    await update.message.reply_text("Queue cleared.")


async def _upload_job(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cancelled = make_check()
    await wait_idle(uid, update.message)
    files = ordered_files(uid)
    if not files:
        await say(update.message, "No files queued. Send me some files first.")
        return

    status = await say(
        update.message, f"Uploading {len(files)} file(s) to {target_label(uid)}..."
    )
    done, failed = 0, []
    try:
      for i, f in enumerate(files, 1):  # one by one, in the order they were sent
        try:
            await asyncio.to_thread(do_upload, f, uid, cancelled)
            f.unlink(missing_ok=True)
            done += 1
        except Stopped:
            raise asyncio.CancelledError()
        except Exception as e:
            log.exception("upload failed for %s", f.name)
            failed.append(f"{f.name} (after {RETRIES + 1} attempts): {e}")
        if status and i < len(files):
            try:
                await status.edit_text(f"Uploading... {i}/{len(files)}  ({f.name})")
            except Exception:
                pass
    except asyncio.CancelledError:
        if status:
            try:
                await status.edit_text(f"Stopped after {done}/{len(files)} file(s).")
            except Exception:
                pass
        raise

    text = f"Uploaded {done}/{len(files)} file(s) to {target_label(uid)}."
    if failed:
        text += "\n\nFailed (kept in queue):\n" + "\n".join(failed)
    if status:
        try:
            await status.edit_text(text)
            return
        except Exception:
            pass
    await say(update.message, text)


def make_zip(files: list, zip_path: Path, cancelled=lambda: False):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            if cancelled():
                raise Stopped()
            # already-compressed formats are stored as is (much faster, same size)
            kind = zipfile.ZIP_STORED if f.suffix.lower() in NO_RECOMPRESS else zipfile.ZIP_DEFLATED
            zf.write(f, arcname=f.name, compress_type=kind)


async def _zip_job(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cancelled = make_check()
    if get_user_settings(uid).get("dest") == "site":
        await say(update.message, "Zip is only for Google Drive. The website takes audio files one by one: use /upload, or /dest drive first.")
        return
    await wait_idle(uid, update.message)
    folder = user_dir(uid)
    files = ordered_files(uid)
    if not files:
        await say(update.message, "No files queued. Send me some files first.")
        return

    name = safe_name(" ".join(context.args)) if context.args else "archive"
    if not name.lower().endswith(".zip"):
        name += ".zip"

    zip_dir = WORK_DIR / f"_zips_{uid}"
    zip_dir.mkdir(parents=True, exist_ok=True)
    zip_path = zip_dir / name

    status = await say(update.message, f"Zipping {len(files)} file(s)...")
    try:
        await asyncio.to_thread(make_zip, files, zip_path, cancelled)
        if status:
            await status.edit_text(f"Zipped ({human_size(zip_path.stat().st_size)}). Uploading to Drive...")
        link = await asyncio.to_thread(
            retry_sync, upload_to_drive, zip_path, target_folder_id(uid), cancelled,
            cancelled=cancelled, label=f"upload {zip_path.name}",
        )
        text = f"Uploaded: {name} to {target_folder_name(uid)}\n{link}"
        if status:
            await status.edit_text(text)
        else:
            await say(update.message, text)
        shutil.rmtree(folder, ignore_errors=True)
        queue_order.pop(uid, None)
    except (Stopped, asyncio.CancelledError):
        if status:
            try:
                await status.edit_text("Zip/upload stopped.")
            except Exception:
                pass
        raise
    except Exception as e:
        log.exception("zip/upload failed")
        await say(update.message, f"Failed: {e}\nYour queued files are kept; try /zip again.")
    finally:
        shutil.rmtree(zip_dir, ignore_errors=True)


async def _guard(job, update, context):
    try:
        await job(update, context)
    except (Stopped, asyncio.CancelledError):
        pass
    except Exception:
        log.exception("job failed")


async def _run_exclusive(update: Update, context: ContextTypes.DEFAULT_TYPE, job):
    uid = update.effective_user.id
    t = cmd_tasks.get(uid)
    if t and not t.done():
        await update.message.reply_text("An upload/zip is already running. Send /stop to cancel it.")
        return
    cmd_tasks[uid] = spawn(_guard(job, update, context))


@owner_only
async def upload_direct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_exclusive(update, context, _upload_job)


@owner_only
async def zip_and_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_exclusive(update, context, _zip_job)


@owner_only
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force stop: abort the current file (download/ffmpeg/upload), drop waiting files."""
    uid = update.effective_user.id
    STATE["gen"] += 1  # running ffmpeg / Drive uploads / zipping abort within seconds
    busy_now = current_job.get(uid, False)

    t = cmd_tasks.pop(uid, None)
    cmd_stopped = bool(t and not t.done())
    if cmd_stopped:
        t.cancel()

    dropped = 0
    q = queues.pop(uid, None)
    if q is not None:
        while True:
            try:
                q.get_nowait()
                q.task_done()
                dropped += 1
            except asyncio.QueueEmpty:
                break
    w = worker_tasks.pop(uid, None)
    if w is not None and not w.done():
        w.cancel()
    for ft in list(file_tasks.pop(uid, set())):
        ft.cancel()
    pending[uid] = 0
    current_job[uid] = False

    parts = []
    if busy_now:
        parts.append("the file being processed")
    if dropped:
        parts.append(f"{dropped} waiting file(s)")
    if cmd_stopped:
        parts.append("the running /upload or /zip")
    text = ("Stopped: " + ", ".join(parts) + ".") if parts else "Nothing was running."
    text += "\nFiles already in the queue are kept (/clear deletes them)."
    await update.message.reply_text(text)


@owner_only
async def set_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    args = context.args or []
    arg = args[0].lower() if args else ""

    def example(st):
        n = int(st.get("next_number", 1))
        label = number_label(st, n)
        return f"{label} - talk.mp3" if st.get("number_keepname", True) else f"{label}.mp3"

    if arg == "on":
        set_user_setting(uid, number=True, next_number=1)
        st = get_user_settings(uid)
        await update.message.reply_text(
            f"Numbering ON, starting at 001. Next file: {example(st)}\n"
            "Change the look with /number prefix Ep  and  /number name on|off"
        )
    elif arg == "off":
        set_user_setting(uid, number=False)
        await update.message.reply_text("Numbering OFF.")
    elif arg == "reset":
        set_user_setting(uid, next_number=1)
        await update.message.reply_text(f"Counter reset. Next file: {example(get_user_settings(uid))}")
    elif arg == "prefix":
        raw = " ".join(args[1:]).strip()
        if not raw:
            await update.message.reply_text("Use /number prefix Ep   (or /number prefix none for plain 001)")
            return
        prefix = "" if raw.lower() == "none" else safe_name(raw)
        set_user_setting(uid, number_prefix=prefix)
        await update.message.reply_text(f"Prefix set. Next file: {example(get_user_settings(uid))}")
    elif arg == "name":
        val = args[1].lower() if len(args) > 1 else ""
        if val not in ("on", "off"):
            await update.message.reply_text(
                "/number name on  = keep the original name: 'Ep 001 - talk.mp3'\n"
                "/number name off = only the number: 'Ep 001.mp3'"
            )
            return
        set_user_setting(uid, number_keepname=(val == "on"))
        await update.message.reply_text(f"Next file: {example(get_user_settings(uid))}")
    elif arg.isdigit():
        set_user_setting(uid, next_number=int(arg))
        await update.message.reply_text(f"Next file: {example(get_user_settings(uid))}")
    else:
        st = get_user_settings(uid)
        await update.message.reply_text(
            f"Numbering: {'ON' if st.get('number') else 'OFF'}\n"
            f"Next file: {example(st)}\n\n"
            "/number on | off | reset | 25 (start from 25)\n"
            "/number prefix Ep   (text before the number, or 'none')\n"
            "/number name on|off (keep the original file name or not)"
        )


@owner_only
async def set_sort(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    arg = context.args[0].lower() if context.args else ""
    if arg in ("sent", "name"):
        set_user_setting(uid, sort=arg)
        await update.message.reply_text(
            "Queue order: sorted by file NAME (natural: 2 before 10)." if arg == "name"
            else "Queue order: the order you SENT the files."
        )
    else:
        cur = "name" if get_user_settings(uid).get("sort") == "name" else "sent"
        await update.message.reply_text(
            f"Queue order is: {cur}\nUse /sort sent (order you sent) or /sort name (by file name).\n"
            "Applies to /list, /upload and /zip. Auto mode always uploads in the order received."
        )


async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("help", "All commands and instructions"),
            BotCommand("dest", "Destination: site or drive"),
            BotCommand("stories", "List website stories"),
            BotCommand("story", "Choose the website story"),
            BotCommand("newstory", "Create a website story"),
            BotCommand("folder", "Set the Drive folder"),
            BotCommand("auto", "Upload files instantly on/off"),
            BotCommand("upload", "Upload queued files"),
            BotCommand("zip", "Zip queued files and upload"),
            BotCommand("list", "Show the queue"),
            BotCommand("stop", "Force stop"),
            BotCommand("clear", "Delete queued files"),
            BotCommand("compress", "Audio compression on/off"),
            BotCommand("bitrate", "Audio quality in kbps"),
            BotCommand("mono", "Mono audio on/off"),
            BotCommand("number", "Number files Ep 001, Ep 002..."),
            BotCommand("sort", "Queue order: sent or name"),
        ])
    except Exception:
        log.exception("could not set the command menu")


@owner_only
async def set_dest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    arg = context.args[0].lower() if context.args else ""
    if arg == "site":
        if not SITE_URL or not SITE_PIN:
            await update.message.reply_text("Set SITE_URL and SITE_PIN in .env on the VPS first, then restart the bot.")
            return
        set_user_setting(uid, dest="site")
        st = get_user_settings(uid)
        extra = "" if st.get("story_id") else "\nNext: /stories to see your stories, then /story <id> (or /newstory Title)."
        await update.message.reply_text(f"Destination: website.{extra}")
    elif arg == "drive":
        set_user_setting(uid, dest="drive")
        await update.message.reply_text(f"Destination: Google Drive ({target_folder_name(uid)}).")
    else:
        await update.message.reply_text(
            f"Destination now: {target_label(uid)}\nUse /dest site or /dest drive."
        )


@owner_only
async def list_stories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status = await update.message.reply_text("Loading stories from the website...")
    try:
        stories = await asyncio.to_thread(site_stories)
    except Exception as e:
        log.exception("stories failed")
        await status.edit_text(f"Could not load stories: {e}")
        return
    if not stories:
        await status.edit_text("No stories yet. Create one with /newstory Title")
        return
    lines = [f"{st['id']}: {st['title']}" for st in stories]
    await status.edit_text("Stories (use /story <id>):\n" + "\n".join(lines))


@owner_only
async def pick_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    query = " ".join(context.args).strip()
    if not query:
        st = get_user_settings(uid)
        await update.message.reply_text(
            f"Current story: {st.get('story_title') or 'none'}\nUse /story <id or part of the title>. See /stories."
        )
        return
    try:
        stories = await asyncio.to_thread(site_stories)
    except Exception as e:
        await update.message.reply_text(f"Could not load stories: {e}")
        return
    match = [x for x in stories if str(x["id"]) == query] or [x for x in stories if query.lower() in x["title"].lower()]
    if len(match) != 1:
        names = "\n".join(f"{x['id']}: {x['title']}" for x in match) if match else "No match."
        await update.message.reply_text(f"{'Several matches, be more specific:' if match else ''}\n{names}".strip())
        return
    set_user_setting(uid, story_id=match[0]["id"], story_title=match[0]["title"])
    await update.message.reply_text(f"Story selected: {match[0]['title']} (ID {match[0]['id']})")


@owner_only
async def new_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    title = " ".join(context.args).strip()
    if not title:
        await update.message.reply_text("Use /newstory Title of the story")
        return
    status = await update.message.reply_text(f"Creating story '{title}'...")
    try:
        story = await asyncio.to_thread(site_add_story, title)
    except Exception as e:
        log.exception("newstory failed")
        await status.edit_text(f"Could not create the story: {e}")
        return
    set_user_setting(uid, story_id=story["id"], story_title=story["title"])
    await status.edit_text(f"Story created and selected: {story['title']} (ID {story['id']})")


def main():
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    builder = Application.builder().token(BOT_TOKEN).post_init(post_init)
    if LOCAL_API_URL:
        builder = (
            builder.base_url(f"{LOCAL_API_URL}/bot")
            .base_file_url(f"{LOCAL_API_URL}/file/bot")
            .local_mode(True)
        )
        log.info("Using local Bot API server at %s", LOCAL_API_URL)
    app = builder.build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("dest", set_dest))
    app.add_handler(CommandHandler("stories", list_stories))
    app.add_handler(CommandHandler("story", pick_story))
    app.add_handler(CommandHandler("newstory", new_story))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("folder", set_folder))
    app.add_handler(CommandHandler("auto", set_auto))
    app.add_handler(CommandHandler("compress", set_compress))
    app.add_handler(CommandHandler("bitrate", set_bitrate))
    app.add_handler(CommandHandler("mono", set_mono))
    app.add_handler(CommandHandler("upload", upload_direct))
    app.add_handler(CommandHandler("zip", zip_and_upload))
    app.add_handler(CommandHandler(["stop", "forcestop"], stop))
    app.add_handler(CommandHandler("number", set_number))
    app.add_handler(CommandHandler("sort", set_sort))
    app.add_handler(CommandHandler("list", list_files))
    app.add_handler(CommandHandler("clear", clear))
    media = filters.Document.ALL | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE
    app.add_handler(MessageHandler(media, receive_file))
    log.info("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
