"""
TikTok + Shazam API  ·  FastAPI  ·  listo para Render

Flujo:
  texto o link de TikTok
    -> extrae info  (API tikwm  +  yt-dlp como respaldo)
    -> reconoce la música con Shazam (shazamio)
    -> entrega la canción (yt-dlp: YouTube -> SoundCloud -> preview de Shazam)
    -> TODO se borra: al descargar cada archivo, con DELETE o por tiempo (TTL)
"""
import asyncio
import os
import re
import secrets
import shutil
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import httpx
import yt_dlp
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from shazamio import Shazam
from starlette.background import BackgroundTask

# ───────────────────── Config (variables de entorno en Render) ─────────────────────
API_KEY = os.getenv("API_KEY", "").strip()                  # si está vacía, la API es pública
TTL = int(os.getenv("FILE_TTL_SECONDS", "600"))             # segundos que vive un job sin descargarse
MAX_MB = int(os.getenv("MAX_DOWNLOAD_MB", "80"))            # tope por archivo
PARALLEL = int(os.getenv("MAX_PARALLEL_JOBS", "2"))         # jobs a la vez (plan free = poca RAM)
COOKIES_SRC = os.getenv("YTDLP_COOKIES_FILE", "")           # opcional: cookies.txt de YouTube
TMP_ROOT = Path(os.getenv("TMP_DIR", "/tmp/tsapi"))
COOKIES = Path("/tmp/yt_cookies.txt")

UA = ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Mobile Safari/537.36")
TIKWM = "https://www.tikwm.com/api/"
TIKTOK_RE = re.compile(r"https?://(?:[\w-]+\.)*tiktok\.com/\S+", re.I)
EXT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/webp": ".webp", "image/png": ".png",
    "video/mp4": ".mp4", "audio/mpeg": ".mp3", "audio/mp4": ".m4a", "audio/x-m4a": ".m4a",
    "audio/aac": ".aac",
}

JOBS: dict[str, dict[str, Any]] = {}     # job_id -> {"t": creado, "files": {archivo: nombre_descarga}}
sem = asyncio.Semaphore(PARALLEL)
shazam = Shazam()


# ───────────────────────────── Limpieza ─────────────────────────────
def rm_job(jid: str) -> None:
    JOBS.pop(jid, None)
    shutil.rmtree(TMP_ROOT / jid, ignore_errors=True)


def consume(jid: str, name: str) -> None:
    """Se ejecuta DESPUÉS de enviar el archivo: lo borra y, si el job quedó vacío, borra el job."""
    job = JOBS.get(jid)
    if not job:
        return
    (TMP_ROOT / jid / name).unlink(missing_ok=True)
    job["files"].pop(name, None)
    if not job["files"]:
        rm_job(jid)


async def janitor() -> None:
    """Cada 30 s borra jobs vencidos y carpetas huérfanas."""
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for jid, job in list(JOBS.items()):
            if now - job["t"] > TTL:
                rm_job(jid)
        if TMP_ROOT.exists():
            for d in TMP_ROOT.iterdir():
                if d.name not in JOBS:
                    shutil.rmtree(d, ignore_errors=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    shutil.rmtree(TMP_ROOT, ignore_errors=True)
    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    if COOKIES_SRC and os.path.isfile(COOKIES_SRC):      # copia editable (yt-dlp reescribe cookies)
        shutil.copy(COOKIES_SRC, COOKIES)
    task = asyncio.create_task(janitor())
    yield
    task.cancel()
    shutil.rmtree(TMP_ROOT, ignore_errors=True)


app = FastAPI(title="TikTok + Shazam API", version="1.0.0", lifespan=lifespan)
app.add_middleware(                      # permite llamar la API desde un HTML/web (CORS)
    CORSMiddleware,
    allow_origins=[o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")],
    allow_methods=["*"], allow_headers=["*"],
)


async def auth(x_api_key: Optional[str] = Header(None), api_key: Optional[str] = Query(None)) -> None:
    if API_KEY and not secrets.compare_digest(x_api_key or api_key or "", API_KEY):
        raise HTTPException(401, "API key inválida o faltante (header x-api-key)")


# ───────────────────────────── Utilidades ─────────────────────────────
def abs_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    return "https://www.tikwm.com" + u if u.startswith("/") else u


def public(d: dict) -> dict:
    return {k: v for k, v in d.items() if not k.startswith("_")}


def slim(v: dict) -> dict:
    return {
        "id": v["id"], "url": v["url"], "description": v["description"],
        "thumbnail_url": v["thumbnail_url"], "author": v["author"]["username"], "stats": v["stats"],
    }


def safe_name(s: Optional[str]) -> str:
    return re.sub(r'[\\/:*?"<>|\r\n]+', "", s or "").strip()[:80] or "audio"


async def fetch_to(url: str, stem: Path, default_ext: str) -> Path:
    """Descarga `url` a `stem` + extensión (según content-type) con tope de tamaño."""
    limit = MAX_MB * 1024 * 1024
    dest: Optional[Path] = None
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=60,
                                     headers={"User-Agent": UA, "Referer": "https://www.tiktok.com/"}) as c:
            async with c.stream("GET", url) as r:
                r.raise_for_status()
                ctype = r.headers.get("content-type", "").split(";")[0].strip().lower()
                dest = stem.with_name(stem.name + EXT.get(ctype, default_ext))
                size = 0
                with dest.open("wb") as f:
                    async for chunk in r.aiter_bytes(65536):
                        size += len(chunk)
                        if size > limit:
                            raise ValueError(f"Archivo mayor a {MAX_MB} MB")
                        f.write(chunk)
        return dest
    except Exception:
        if dest:
            dest.unlink(missing_ok=True)
        raise


# ───────────────────────────── TikTok: info ─────────────────────────────
async def tikwm(path: str, data: dict) -> dict:
    last = ""
    for _ in range(3):
        async with httpx.AsyncClient(timeout=30, headers={"User-Agent": UA}) as c:
            r = await c.post(TIKWM + path, data=data)
        j = r.json()
        if j.get("code") == 0:
            return j["data"]
        last = str(j.get("msg") or "")
        if "limit" in last.lower():          # tikwm free: 1 petición por segundo
            await asyncio.sleep(1.5)
            continue
        break
    raise RuntimeError(last or "tikwm sin respuesta")


def norm_tikwm(d: dict) -> dict:
    a = d.get("author") or {}
    mi = d.get("music_info") or {}
    vid = str(d.get("id") or d.get("video_id") or "")
    user = a.get("unique_id") or ""
    title = d.get("title") or ""
    return {
        "id": vid,
        "url": f"https://www.tiktok.com/@{user}/video/{vid}" if user and vid else None,
        "description": title,
        "hashtags": re.findall(r"#(\w+)", title),
        "duration": d.get("duration"),
        "created_at": d.get("create_time"),
        "region": d.get("region"),
        "author": {
            "id": a.get("id"), "username": user, "nickname": a.get("nickname"),
            "avatar": abs_url(a.get("avatar")),
        },
        "stats": {
            "plays": d.get("play_count"), "likes": d.get("digg_count"),
            "comments": d.get("comment_count"), "shares": d.get("share_count"),
            "saves": d.get("collect_count"), "downloads": d.get("download_count"),
        },
        "thumbnail_url": abs_url(d.get("origin_cover") or d.get("cover")),
        "tiktok_sound": {
            "title": mi.get("title"), "author": mi.get("author"), "album": mi.get("album"),
            "original": mi.get("original"), "duration": mi.get("duration"),
            "cover": abs_url(mi.get("cover")),
        },
        "images": [abs_url(i) for i in (d.get("images") or [])],      # posts de fotos
        "_video_url": abs_url(d.get("hdplay") or d.get("play")),
        "_music_url": abs_url(d.get("music") or mi.get("play")),
    }


def ytdlp_info(url: str) -> dict:
    """Respaldo con yt-dlp si tikwm falla."""
    opts = {"quiet": True, "no_warnings": True, "skip_download": True,
            "noplaylist": True, "socket_timeout": 20}
    with yt_dlp.YoutubeDL(opts) as ydl:
        i = ydl.extract_info(url, download=False)
    text = i.get("description") or i.get("title") or ""
    return {
        "id": str(i.get("id")), "url": i.get("webpage_url") or url, "description": text,
        "hashtags": re.findall(r"#(\w+)", text),
        "duration": i.get("duration"), "created_at": i.get("timestamp"), "region": None,
        "author": {"id": i.get("uploader_id"), "username": i.get("uploader"),
                   "nickname": i.get("channel") or i.get("uploader"), "avatar": None},
        "stats": {"plays": i.get("view_count"), "likes": i.get("like_count"),
                  "comments": i.get("comment_count"), "shares": i.get("repost_count"),
                  "saves": None, "downloads": None},
        "thumbnail_url": i.get("thumbnail"),
        "tiktok_sound": {"title": i.get("track"), "author": i.get("artist"), "album": i.get("album"),
                         "original": None, "duration": None, "cover": None},
        "images": [], "_video_url": None, "_music_url": None,
    }


async def get_info(url: str) -> dict:
    try:
        info = norm_tikwm(await tikwm("", {"url": url, "hd": 1}))
        info["url"] = info["url"] or url
        return info
    except Exception as e1:
        try:
            return await asyncio.to_thread(ytdlp_info, url)
        except Exception as e2:
            raise HTTPException(502, f"No pude extraer ese TikTok (tikwm: {e1} | yt-dlp: {str(e2)[:160]})")


async def tikwm_search(q: str, n: int) -> list[dict]:
    try:
        d = await tikwm("feed/search", {"keywords": q, "count": n, "cursor": 0, "HD": 1})
    except Exception as e:
        raise HTTPException(502, f"Búsqueda en TikTok falló: {e}")
    return [norm_tikwm(v) for v in (d.get("videos") or [])]


# ───────────────────────────── yt-dlp: descargas ─────────────────────────────
def ytdlp_download(target: str, out_dir: Path, base: str, audio: bool) -> Path:
    opts: dict[str, Any] = {
        "quiet": True, "no_warnings": True, "noplaylist": True,
        "socket_timeout": 20, "retries": 2,
        "outtmpl": str(out_dir / f"{base}.%(ext)s"),
        "max_filesize": MAX_MB * 1024 * 1024,
    }
    if audio:
        opts["format"] = "bestaudio/best"
        opts["match_filter"] = yt_dlp.utils.match_filter_func("duration < 720")
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio",
                                   "preferredcodec": "mp3", "preferredquality": "192"}]
    else:
        opts["format"] = "best[ext=mp4]/best"
    if COOKIES.is_file():
        opts["cookiefile"] = str(COOKIES)
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([target])
    files = sorted(out_dir.glob(f"{base}.*"))
    if not files:
        raise RuntimeError("yt-dlp no generó archivo")
    return files[0]


# ───────────────────────────── Shazam ─────────────────────────────
async def make_sample(src: Path, dst: Path, start: int, length: int = 30) -> None:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-loglevel", "error", "-ss", str(start), "-i", str(src),
        "-t", str(length), "-vn", "-ac", "1", "-ar", "44100", "-c:a", "pcm_s16le", str(dst),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    _, err = await asyncio.wait_for(proc.communicate(), 60)
    if proc.returncode != 0 or not dst.exists():
        raise RuntimeError(err.decode(errors="ignore")[:200])


async def recognize(src: Path, duration: Optional[float], work: Path) -> Optional[dict]:
    """Reconoce con Shazam. Si el audio es largo prueba una segunda ventana."""
    starts = [0] if not duration or duration <= 45 else [0, 20]
    for i, s in enumerate(starts):
        wav = work / f"_sample{i}.wav"
        try:
            await make_sample(src, wav, s)
            out = await asyncio.wait_for(shazam.recognize(str(wav)), 45)
        except Exception:
            out = None
        finally:
            wav.unlink(missing_ok=True)
        if out and out.get("track"):
            return out["track"]
    return None


def parse_track(t: dict) -> dict:
    meta: dict[str, Any] = {}
    for sec in t.get("sections") or []:
        if sec.get("type") == "SONG":
            for m in sec.get("metadata") or []:
                meta[(m.get("title") or "").lower()] = m.get("text")
    preview = None
    for act in (t.get("hub") or {}).get("actions") or []:
        if act.get("type") == "uri" and act.get("uri"):
            preview = act["uri"]
    img = t.get("images") or {}
    return {
        "title": t.get("title"), "artist": t.get("subtitle"),
        "album": meta.get("album"), "label": meta.get("label"), "released": meta.get("released"),
        "genre": (t.get("genres") or {}).get("primary"),
        "isrc": t.get("isrc"), "shazam_id": t.get("key"),
        "cover": img.get("coverarthq") or img.get("coverart"),
        "shazam_url": t.get("url"),
        "_preview": preview,
    }


async def shazam_search(q: str, limit: int) -> list[dict]:
    try:
        res = await asyncio.wait_for(shazam.search_track(query=q, limit=limit), 30)
    except Exception as e:
        raise HTTPException(502, f"Shazam no respondió: {e}")
    hits = ((res or {}).get("tracks") or {}).get("hits") or []
    return [parse_track(h["track"]) for h in hits if h.get("track")]


async def deliver_song(song: dict, work: Path) -> tuple[Optional[Path], Optional[str]]:
    """Canción completa: YouTube -> SoundCloud -> preview de 30 s de Shazam."""
    q = f"{song.get('artist') or ''} - {song.get('title') or ''}".strip(" -")
    for label, target in (("youtube", f"ytsearch1:{q} audio"), ("soundcloud", f"scsearch1:{q}")):
        try:
            return await asyncio.to_thread(ytdlp_download, target, work, "song", True), label
        except Exception:
            for f in work.glob("song.*"):
                f.unlink(missing_ok=True)
    if song.get("_preview"):
        try:
            return await fetch_to(song["_preview"], work / "song_preview", ".m4a"), "shazam_preview_30s"
        except Exception:
            pass
    return None, None


# ───────────────────────────── Pipeline ─────────────────────────────
class ExtractIn(BaseModel):
    query: str                  # link de TikTok o texto
    mode: str = "tiktok"        # texto -> "tiktok" (busca videos) | "song" (busca canción por título/letra)
    pick: int = 0               # qué resultado de la búsqueda usar
    results: int = 5            # cuántos resultados listar
    thumbnail: bool = True      # entregar miniatura
    video: bool = False         # entregar el video (sin marca de agua si se puede)
    tiktok_audio: bool = False  # entregar el audio original del TikTok
    recognize: bool = True      # reconocer música con Shazam
    song: bool = True           # entregar la canción completa reconocida


async def pipeline(p: ExtractIn, jid: str, work: Path) -> tuple[dict, dict]:
    q = p.query.strip()
    out: dict[str, Any] = {"source": {"query": q}}
    info: Optional[dict] = None
    song: Optional[dict] = None
    found_by: Optional[str] = None
    audio_path: Optional[Path] = None
    video_path: Optional[Path] = None
    keep: dict[str, str] = {}      # archivo -> nombre al descargar
    roles: dict[str, str] = {}     # rol -> archivo

    def add(role: str, path: Path, display: str) -> None:
        keep[path.name] = display
        roles[role] = path.name

    m = TIKTOK_RE.search(q)
    if m:
        url = m.group(0).rstrip(").,;!?\"'")
        out["source"].update(type="link", url=url)
        info = await get_info(url)
    elif p.mode == "song":
        out["source"]["type"] = "song_search"
        found = await shazam_search(q, p.results)
        if not found:
            raise HTTPException(404, "Shazam no encontró canciones con ese texto")
        out["search_results"] = [public(s) for s in found]
        song, found_by = found[min(p.pick, len(found) - 1)], "text"
    else:
        out["source"]["type"] = "tiktok_search"
        vids = await tikwm_search(q, p.results)
        if not vids:
            raise HTTPException(404, "TikTok no devolvió resultados para ese texto")
        out["search_results"] = [slim(v) for v in vids]
        info = vids[min(p.pick, len(vids) - 1)]

    async def get_video() -> Path:
        nonlocal video_path
        if video_path is None:
            if info and info.get("_video_url"):
                video_path = await fetch_to(info["_video_url"], work / "video", ".mp4")
            else:
                video_path = await asyncio.to_thread(ytdlp_download, info["url"], work, "video", False)
        return video_path

    # ── Info de TikTok + miniatura + reconocimiento ──
    if info:
        out["video"] = public(info)
        if p.thumbnail and info.get("thumbnail_url"):
            try:
                t = await fetch_to(info["thumbnail_url"], work / "thumbnail", ".jpg")
                add("thumbnail", t, t.name)
            except Exception:
                pass

        dur = (info.get("tiktok_sound") or {}).get("duration") or info.get("duration")
        if info.get("_music_url") and (p.recognize or p.tiktok_audio):
            try:
                audio_path = await fetch_to(info["_music_url"], work / "tiktok_audio", ".mp3")
            except Exception:
                audio_path = None

        if p.recognize:
            track = await recognize(audio_path, dur, work) if audio_path else None
            if not track and not info.get("images"):       # reintenta con el audio mezclado del video
                try:
                    track = await recognize(await get_video(), info.get("duration"), work)
                except Exception:
                    track = None
            if track:
                song, found_by = parse_track(track), "audio"

        if p.tiktok_audio and audio_path:
            add("tiktok_audio", audio_path, f"tiktok_sound_{info['id']}{audio_path.suffix}")
        if p.video:
            try:
                v = await get_video()
                add("video", v, f"tiktok_{info['id']}{v.suffix}")
            except Exception as e:
                out["video"]["video_error"] = str(e)[:160]

    # ── Canción: miniatura (modo song) + entrega ──
    if song:
        out["song"] = {"found": True, "found_by": found_by, **public(song)}
        if not info and p.thumbnail and song.get("cover"):
            try:
                t = await fetch_to(song["cover"], work / "thumbnail", ".jpg")
                add("thumbnail", t, t.name)
            except Exception:
                pass
        if p.song:
            path, via = await deliver_song(song, work)
            if not path and audio_path and audio_path.exists():
                path, via = audio_path, "tiktok_sound"
            if path:
                add("song", path, f"{safe_name(song.get('artist'))} - {safe_name(song.get('title'))}{path.suffix}")
            out["song"]["audio_source"] = via
    elif p.recognize:
        out["song"] = {"found": False}

    # ── Limpieza: solo queda lo que se va a entregar ──
    for f in work.iterdir():
        if f.name not in keep:
            f.unlink(missing_ok=True)
    JOBS[jid]["files"] = keep
    return out, roles


async def handle(p: ExtractIn, request: Request) -> dict:
    if p.mode not in ("tiktok", "song"):
        raise HTTPException(400, "mode debe ser 'tiktok' o 'song'")
    if len(p.query.strip()) < 2:
        raise HTTPException(400, "query vacío")
    p.pick, p.results = max(0, p.pick), max(1, min(p.results, 20))

    base = (os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")
            or str(request.base_url)).rstrip("/")
    jid = uuid.uuid4().hex
    work = TMP_ROOT / jid
    JOBS[jid] = {"t": time.time(), "files": {}}
    work.mkdir(parents=True, exist_ok=True)
    try:
        async with sem:
            data, roles = await asyncio.wait_for(pipeline(p, jid, work), timeout=240)
    except HTTPException:
        rm_job(jid)
        raise
    except asyncio.TimeoutError:
        rm_job(jid)
        raise HTTPException(504, "Tiempo agotado procesando la petición")
    except Exception as e:
        rm_job(jid)
        raise HTTPException(502, f"Error procesando: {str(e)[:200]}")

    data["ok"] = True
    data["files"] = {role: f"{base}/files/{jid}/{name}" for role, name in roles.items()}
    if roles:
        JOBS[jid]["t"] = time.time()          # el TTL cuenta desde que se entregan los links
        data["job_id"] = jid
        data["expires_in_seconds"] = TTL
        data["cleanup"] = (f"Cada archivo se borra al descargarlo. Si no lo descargas, se borra solo en {TTL}s. "
                           f"También: DELETE /api/job/{jid}")
    else:
        rm_job(jid)                           # nada que entregar -> nada que limpiar
        data["cleanup"] = "Sin archivos: no queda nada en el servidor."
    return data


# ───────────────────────────── Rutas ─────────────────────────────
@app.get("/")
async def root():
    return {
        "name": "TikTok + Shazam API",
        "docs": "/docs",
        "extract": "GET|POST /api/extract",
        "download": "GET /files/{job_id}/{archivo}  (se borra al descargar; ?keep=1 para no borrar)",
        "delete": "DELETE /api/job/{job_id}",
    }


@app.get("/health")
async def health():
    return {"ok": True, "jobs_activos": len(JOBS)}


@app.post("/api/extract", dependencies=[Depends(auth)])
async def extract_post(body: ExtractIn, request: Request):
    return await handle(body, request)


@app.get("/api/extract", dependencies=[Depends(auth)])
async def extract_get(
    request: Request,
    q: str = Query(..., min_length=2, description="Link de TikTok o texto"),
    mode: str = "tiktok", pick: int = 0, results: int = 5,
    thumbnail: bool = True, video: bool = False, tiktok_audio: bool = False,
    recognize: bool = True, song: bool = True,
):
    body = ExtractIn(query=q, mode=mode, pick=pick, results=results, thumbnail=thumbnail,
                     video=video, tiktok_audio=tiktok_audio, recognize=recognize, song=song)
    return await handle(body, request)


@app.get("/files/{jid}/{name}")
async def get_file(jid: str, name: str, keep: bool = False):
    job = JOBS.get(jid)
    if not job or name not in job["files"] or not (TMP_ROOT / jid / name).is_file():
        raise HTTPException(404, "Archivo inexistente, ya entregado o vencido")
    return FileResponse(TMP_ROOT / jid / name, filename=job["files"][name],
                        background=None if keep else BackgroundTask(consume, jid, name))


@app.delete("/api/job/{jid}")
async def delete_job(jid: str):
    if jid not in JOBS:
        raise HTTPException(404, "Job inexistente o ya limpiado")
    rm_job(jid)
    return {"ok": True, "deleted": jid}
