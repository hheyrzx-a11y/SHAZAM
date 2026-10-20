"""TikTok Music Intel API (FastAPI) - para Render.

Entrada: link de TikTok o texto (letra de canción).
Salida: stats del video, miniatura, canción reconocida (Shazam), letra y audio.
Todo se borra del servidor después de entregarse (o a los JOB_TTL_SECONDS).
"""
import asyncio
import os
import re
import shutil
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import yt_dlp
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from shazamio import Shazam

API_KEY = os.getenv("API_KEY", "")  # opcional: si lo pones, se exige en cada petición
JOB_TTL = int(os.getenv("JOB_TTL_SECONDS", "600"))
MAX_VIDEO_SEC = int(os.getenv("MAX_VIDEO_SECONDS", "600"))
MAX_SONG_SEC = 900
ROOT = Path("/tmp/jobs")

TIKTOK_RE = re.compile(r"https?://(?:[\w-]+\.)?tiktok\.com/\S+", re.I)
JOBS: dict = {}
SEM = asyncio.Semaphore(2)
shazam = Shazam()


# ----------------------------------------------------------------- limpieza
def drop(jid: str):
    job = JOBS.pop(jid, None)
    if job:
        t = job.get("task")
        if t and not t.done():
            t.cancel()
    shutil.rmtree(ROOT / jid, ignore_errors=True)


async def delivered(jid: str, kind: str):
    job = JOBS.get(jid)
    if not job:
        return
    job["pending"].discard(kind)
    if not job["pending"]:
        drop(jid)


async def janitor():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for jid, j in list(JOBS.items()):
            if now - j["created"] > JOB_TTL:
                drop(jid)


@asynccontextmanager
async def lifespan(_app):
    shutil.rmtree(ROOT, ignore_errors=True)
    ROOT.mkdir(parents=True, exist_ok=True)
    task = asyncio.create_task(janitor())
    yield
    task.cancel()


app = FastAPI(title="TikTok Music Intel API", lifespan=lifespan)


def auth(x_api_key: str = Header(default=""), key: str = ""):
    if API_KEY and API_KEY not in (x_api_key, key):
        raise HTTPException(401, "API key inválida")


# ------------------------------------------------------------------ helpers
def ydl_info(target: str) -> dict:
    opts = {"quiet": True, "no_warnings": True, "skip_download": True, "noplaylist": True}
    with yt_dlp.YoutubeDL(opts) as y:
        return y.extract_info(target, download=False)


def ydl_audio(target: str, out_base: Path, max_sec: int):
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "format": "bestaudio/best",
        "outtmpl": str(out_base) + ".%(ext)s",
        "match_filter": yt_dlp.utils.match_filter_func(f"duration <? {max_sec}"),
        "postprocessors": [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "128"}
        ],
    }
    with yt_dlp.YoutubeDL(opts) as y:
        y.download([target])
    p = Path(str(out_base) + ".mp3")
    return p if p.exists() else None


async def fetch_file(http: httpx.AsyncClient, url: str, dest: Path) -> bool:
    try:
        async with http.stream("GET", url) as r:
            if r.status_code != 200:
                return False
            with open(dest, "wb") as f:
                async for chunk in r.aiter_bytes():
                    f.write(chunk)
        return dest.exists() and dest.stat().st_size > 0
    except Exception:
        return False


def parse_track(t):
    if not t:
        return None
    meta = {}
    for s in t.get("sections") or []:
        for m in s.get("metadata") or []:
            meta[(m.get("title") or "").lower()] = m.get("text")
    img = t.get("images") or {}
    return {
        "title": t.get("title"),
        "artist": t.get("subtitle"),
        "album": meta.get("album"),
        "label": meta.get("label"),
        "released": meta.get("released"),
        "genre": (t.get("genres") or {}).get("primary"),
        "cover": img.get("coverarthq") or img.get("coverart"),
        "shazam_url": t.get("url"),
        "shazam_id": t.get("key"),
        "source": "shazam",
    }


def build_video(info: dict) -> dict:
    v, l, c, s = (info.get(k) or 0 for k in ("view_count", "like_count", "comment_count", "repost_count"))
    desc = info.get("description") or ""
    return {
        "id": info.get("id"),
        "url": info.get("webpage_url"),
        "description": desc,
        "hashtags": re.findall(r"#(\w+)", desc),
        "author": info.get("uploader") or info.get("creator"),
        "author_id": info.get("uploader_id") or info.get("channel"),
        "duration_sec": info.get("duration"),
        "upload_date": info.get("upload_date"),
        "stats": {
            "views": v,
            "likes": l,
            "comments": c,
            "shares": s,
            "engagement_rate_pct": round((l + c + s) / v * 100, 2) if v else None,
        },
        "thumbnail_source": info.get("thumbnail"),
        "tiktok_music": {
            "track": info.get("track"),
            "artist": info.get("artist") or ", ".join(info.get("artists") or []) or None,
        },
    }


async def identify(clip: Path, d: Path):
    sample = d / "sample.mp3"
    try:
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(clip), "-t", "30", "-c", "copy", str(sample),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await p.wait()
    except Exception:
        pass
    target = sample if sample.exists() else clip
    try:
        fn = getattr(shazam, "recognize", None) or shazam.recognize_song
        out = await asyncio.wait_for(fn(str(target)), 60)
        return parse_track(out.get("track"))
    except Exception:
        return None


async def search_song(q: str):
    try:  # 1) buscador de Shazam
        r = await shazam.search_track(query=q, limit=5)
        hits = (r.get("tracks") or {}).get("hits") or []
        if hits:
            s = parse_track(hits[0].get("track"))
            if s:
                return s
    except Exception:
        pass
    try:  # 2) respaldo: búsqueda en YouTube con la letra
        info = await asyncio.to_thread(ydl_info, f"ytsearch1:{q} lyrics")
        e = (info.get("entries") or [info])[0]
        if e:
            return {
                "title": e.get("track") or e.get("title"),
                "artist": e.get("artist") or e.get("uploader"),
                "source": "youtube_search",
            }
    except Exception:
        pass
    return None


async def lyrics_for(http: httpx.AsyncClient, song: dict):
    title, artist = song.get("title"), song.get("artist") or ""
    if not title:
        return None
    try:
        r = await http.get("https://lrclib.net/api/get", params={"artist_name": artist, "track_name": title})
        if r.status_code == 200 and r.json().get("plainLyrics"):
            return r.json()["plainLyrics"]
        r = await http.get("https://lrclib.net/api/search", params={"q": f"{artist} {title}".strip()})
        if r.status_code == 200:
            for it in r.json():
                if it.get("plainLyrics"):
                    return it["plainLyrics"]
    except Exception:
        pass
    return None


async def deezer_preview(http: httpx.AsyncClient, song: dict, dest: Path):
    q = f'artist:"{song.get("artist") or ""}" track:"{song["title"]}"'
    r = await http.get("https://api.deezer.com/search", params={"q": q, "limit": 1})
    data = r.json().get("data") or []
    if data and data[0].get("preview") and await fetch_file(http, data[0]["preview"], dest):
        return dest
    return None


async def get_music(http, song, clip, d: Path):
    if song and song.get("title"):
        q = f'{song.get("artist") or ""} {song["title"]}'.strip()
        try:  # canción completa (yt-dlp, YouTube)
            p = await asyncio.to_thread(ydl_audio, f"ytsearch1:{q} audio", d / "full", MAX_SONG_SEC)
            if p:
                return p, "full_song_youtube"
        except Exception:
            pass
        try:  # preview de 30 s (Deezer, gratis)
            p = await deezer_preview(http, song, d / "deezer.mp3")
            if p:
                return p, "deezer_preview_30s"
        except Exception:
            pass
    if clip:  # audio original del TikTok
        return clip, "tiktok_clip"
    return None, None


# ----------------------------------------------------------------- pipeline
async def run(job: dict, query: str, want_audio: bool, want_lyrics: bool):
    d: Path = job["dir"]
    try:
        async with SEM:
            headers = {"User-Agent": "Mozilla/5.0"}
            async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as http:
                m = TIKTOK_RE.search(query)
                result = {"input": {"type": "tiktok_link" if m else "text", "query": query}}
                clip, song = None, None

                if m:
                    url = m.group(0)
                    job["step"] = "extrayendo info del video"
                    info = await asyncio.to_thread(ydl_info, url)
                    if (info.get("duration") or 0) > MAX_VIDEO_SEC:
                        raise ValueError("El video es demasiado largo")
                    result["video"] = build_video(info)

                    if info.get("thumbnail"):
                        job["step"] = "descargando miniatura"
                        if await fetch_file(http, info["thumbnail"], d / "thumbnail.jpg"):
                            job["files"]["thumbnail"] = d / "thumbnail.jpg"

                    job["step"] = "extrayendo audio"
                    clip = await asyncio.to_thread(ydl_audio, url, d / "clip", MAX_VIDEO_SEC)
                    if clip:
                        job["step"] = "reconociendo con Shazam"
                        song = await identify(clip, d)
                    if not song and info.get("track"):
                        song = {
                            "title": info["track"],
                            "artist": info.get("artist") or ", ".join(info.get("artists") or []),
                            "source": "tiktok_metadata",
                        }
                else:
                    job["step"] = "buscando canción por letra"
                    song = await search_song(query)
                    if not song:
                        raise ValueError("No encontré ninguna canción con esa letra")

                if song and want_lyrics:
                    job["step"] = "buscando letra"
                    song["lyrics"] = await lyrics_for(http, song)
                result["song"] = song

                audio_src = None
                if want_audio:
                    job["step"] = "obteniendo música"
                    ap, audio_src = await get_music(http, song, clip, d)
                    if ap:
                        final = d / "audio.mp3"
                        if ap != final:
                            shutil.move(str(ap), str(final))
                        job["files"]["audio"] = final

                for f in d.iterdir():  # borra temporales
                    if f.name not in ("thumbnail.jpg", "audio.mp3"):
                        f.unlink(missing_ok=True)

                jid = job["id"]
                result["downloads"] = {
                    k: f"/files/{jid}/{k}" for k in job["files"]
                }
                if audio_src:
                    result["downloads"]["audio_source"] = audio_src
                job["pending"] = set(job["files"].keys())
                job["result"] = result
                job["status"] = "done"
                job["step"] = "listo"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)[:300]


# ---------------------------------------------------------------- endpoints
class Req(BaseModel):
    query: str  # link de TikTok o letra de canción
    audio: bool = True  # entregar el audio (mp3)
    lyrics: bool = True  # buscar la letra


@app.get("/")
async def root():
    return {
        "name": "TikTok Music Intel API",
        "usage": {
            "1": "POST /analyze {\"query\": \"link de TikTok o letra\"} -> job_id",
            "2": "GET /status/{job_id} -> progreso y resultado",
            "3": "GET /files/{job_id}/audio y /files/{job_id}/thumbnail",
        },
    }


@app.post("/analyze", dependencies=[Depends(auth)])
async def analyze(req: Req):
    q = req.query.strip()
    if not q:
        raise HTTPException(400, "query vacío")
    if len(JOBS) >= 20:
        raise HTTPException(429, "Servidor ocupado, intenta en un momento")
    jid = uuid.uuid4().hex[:12]
    d = ROOT / jid
    d.mkdir(parents=True, exist_ok=True)
    job = {
        "id": jid, "dir": d, "created": time.time(), "status": "working",
        "step": "en cola", "files": {}, "pending": set(), "result": None, "error": None,
    }
    JOBS[jid] = job
    job["task"] = asyncio.create_task(run(job, q, req.audio, req.lyrics))
    return {"job_id": jid, "status_url": f"/status/{jid}"}


@app.get("/status/{jid}", dependencies=[Depends(auth)])
async def status(jid: str, bg: BackgroundTasks):
    job = JOBS.get(jid)
    if not job:
        raise HTTPException(404, "Job no existe (ya entregado, borrado o expirado)")
    out = {"job_id": jid, "status": job["status"], "step": job["step"]}
    if job["status"] == "done":
        out["result"] = job["result"]
        if not job["pending"]:
            bg.add_task(drop, jid)
    elif job["status"] == "error":
        out["error"] = job["error"]
        bg.add_task(drop, jid)
    return out


@app.get("/files/{jid}/{kind}", dependencies=[Depends(auth)])
async def get_file(jid: str, kind: str, bg: BackgroundTasks):
    job = JOBS.get(jid)
    if not job:
        raise HTTPException(404, "Job no existe")
    path = job["files"].get(kind)
    if job["status"] != "done" or not path or not Path(path).exists():
        raise HTTPException(404, "Archivo no disponible")
    bg.add_task(delivered, jid, kind)
    song = (job["result"] or {}).get("song") or {}
    base = re.sub(r'[\\/:*?"<>|]', "", f'{song.get("artist") or ""} - {song.get("title") or jid}').strip(" -")
    if kind == "audio":
        return FileResponse(path, media_type="audio/mpeg", filename=f"{base}.mp3", background=bg)
    return FileResponse(path, media_type="image/jpeg", filename=f"{jid}.jpg", background=bg)


@app.delete("/job/{jid}", dependencies=[Depends(auth)])
async def delete_job(jid: str):
    drop(jid)
    return {"deleted": jid}
