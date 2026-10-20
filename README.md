# TikTok + Shazam API (Render)

Recibe un **link de TikTok** o un **texto**, devuelve info/estadísticas/miniatura, reconoce la música con **Shazam**, entrega la canción y **borra todo** después.

## Qué usa
| Paso | Librería / API (gratis) |
|---|---|
| Info, stats, miniatura, búsqueda por texto | API **tikwm** (respaldo: **yt-dlp**) |
| Reconocer la música | **shazamio** |
| Entregar la canción | **yt-dlp** → YouTube → SoundCloud → preview 30 s de Shazam |
| Audio / recortes | **ffmpeg** |

## Subir a Render
1. Sube estos 4 archivos a un repo de GitHub: `main.py`, `requirements.txt`, `Dockerfile`, `render.yaml`.
2. Render → **New → Blueprint** (usa `render.yaml`) o **New → Web Service** → repo → Runtime **Docker** → plan **Free**.
3. Entra al servicio → **Environment** → copia el valor de `API_KEY` (si lo creaste manual, agrega tú una variable `API_KEY` con la clave que quieras).
4. Cuando diga *Live*, abre `https://TU-APP.onrender.com/docs` para probar desde el navegador.

### Variables opcionales
| Variable | Default | Para qué |
|---|---|---|
| `API_KEY` | vacía | Si la pones, toda petición necesita el header `x-api-key`. Si la dejas vacía, la API es pública. |
| `FILE_TTL_SECONDS` | 600 | Segundos antes de borrar un job que nadie descargó. |
| `MAX_DOWNLOAD_MB` | 80 | Tope por archivo. |
| `MAX_PARALLEL_JOBS` | 2 | Jobs a la vez (plan free tiene 512 MB RAM). |
| `YTDLP_COOKIES_FILE` | – | Ruta de un `cookies.txt` (Render → Secret Files, p. ej. `/etc/secrets/cookies.txt`) por si YouTube bloquea la IP. |

## Cómo pedirle cosas

**Por link de TikTok**
```bash
curl -H "x-api-key: TU_CLAVE" \
  "https://TU-APP.onrender.com/api/extract?q=https://vm.tiktok.com/XXXXXXX/"
```

**Por texto** (busca videos en TikTok y usa el resultado `pick`, por defecto el primero)
```bash
curl -H "x-api-key: TU_CLAVE" \
  "https://TU-APP.onrender.com/api/extract?q=goku%20edit&results=5&pick=0"
```

**Por letra / nombre de canción** (busca en Shazam, sin pasar por TikTok)
```bash
curl -H "x-api-key: TU_CLAVE" \
  "https://TU-APP.onrender.com/api/extract?q=nombre%20o%20letra&mode=song"
```

**POST (JSON)**
```bash
curl -X POST "https://TU-APP.onrender.com/api/extract" \
  -H "x-api-key: TU_CLAVE" -H "Content-Type: application/json" \
  -d '{"query":"https://vm.tiktok.com/XXXXXXX/","video":true,"tiktok_audio":true}'
```

### Parámetros
| Parámetro | Default | Qué hace |
|---|---|---|
| `q` / `query` | – | Link de TikTok o texto. |
| `mode` | `tiktok` | Con texto: `tiktok` busca videos, `song` busca canciones en Shazam. |
| `pick` / `results` | 0 / 5 | Qué resultado usar / cuántos listar (máx. 20). |
| `thumbnail` | true | Entrega la miniatura. |
| `video` | false | Entrega el video (sin marca de agua si se puede). |
| `tiktok_audio` | false | Entrega el audio original del TikTok. |
| `recognize` | true | Reconoce la música con Shazam. |
| `song` | true | Entrega la canción completa reconocida. |

### Respuesta (ejemplo)
```json
{
  "ok": true,
  "source": {"type": "link", "query": "...", "url": "..."},
  "video": {
    "id": "7300000000000000000",
    "url": "https://www.tiktok.com/@user/video/7300000000000000000",
    "description": "texto #fyp #anime",
    "hashtags": ["fyp", "anime"],
    "duration": 15,
    "created_at": 1700000000,
    "author": {"id": "...", "username": "user", "nickname": "User", "avatar": "..."},
    "stats": {"plays": 120000, "likes": 9000, "comments": 150, "shares": 300, "saves": 800, "downloads": 40},
    "thumbnail_url": "https://...",
    "tiktok_sound": {"title": "original sound", "author": "user", "original": true, "duration": 15}
  },
  "song": {
    "found": true, "found_by": "audio",
    "title": "Título", "artist": "Artista", "album": "Álbum", "label": "Sello",
    "released": "2023", "genre": "Pop", "isrc": "...", "cover": "https://...", "shazam_url": "https://...",
    "audio_source": "youtube"
  },
  "files": {
    "thumbnail": "https://TU-APP.onrender.com/files/<job_id>/thumbnail.jpg",
    "song": "https://TU-APP.onrender.com/files/<job_id>/song.mp3"
  },
  "job_id": "<job_id>",
  "expires_in_seconds": 600,
  "cleanup": "Cada archivo se borra al descargarlo..."
}
```
`audio_source` puede ser `youtube`, `soundcloud`, `shazam_preview_30s` (solo 30 s) o `tiktok_sound`. Si la música no se reconoce: `"song": {"found": false}`.

### Ejemplos de código
**Python**
```python
import requests
BASE, KEY = "https://TU-APP.onrender.com", "TU_CLAVE"
h = {"x-api-key": KEY}
r = requests.get(f"{BASE}/api/extract", params={"q": "https://vm.tiktok.com/XXXX/"}, headers=h, timeout=300).json()
print(r["video"]["stats"], r["song"])
for role, url in r["files"].items():          # cada descarga borra el archivo del servidor
    open(f"{role}.bin", "wb").write(requests.get(url).content)
```

**JavaScript**
```js
const BASE = "https://TU-APP.onrender.com", KEY = "TU_CLAVE";
const r = await (await fetch(`${BASE}/api/extract?q=${encodeURIComponent(link)}`, {headers: {"x-api-key": KEY}})).json();
console.log(r.video.stats, r.song, r.files);
```

## Limpieza (qué se borra y cuándo)
- **Al descargar** un archivo desde `/files/<job_id>/<archivo>`, se borra. Cuando se descargó el último, se borra el job entero.
- **A mano:** `DELETE /api/job/<job_id>`.
- **Automático:** si nadie descarga, el job se borra a los `FILE_TTL_SECONDS`.
- Los archivos temporales (recortes para Shazam, audio/video que no pediste) se borran antes de responder.
- Si algo falla a medias, el job se borra al instante.
- Para previsualizar sin borrar, agrega `?keep=1` al link del archivo (se borra igual por TTL).

## Ojo
- **Plan free de Render:** se duerme tras ~15 min sin uso; la primera petición tarda ~1 min en despertar. El disco es temporal (justo lo que queremos).
- **tikwm** es gratis pero limita a ~1 petición por segundo; la API reintenta sola.
- **YouTube** a veces bloquea IPs de servidores. Si falla, cae a SoundCloud y luego al preview de 30 s; para arreglarlo usa `YTDLP_COOKIES_FILE`.
- **Mantenimiento:** TikTok y YouTube cambian seguido. Si algo deja de funcionar, en Render usa **Manual Deploy → Clear build cache & deploy** para instalar el `yt-dlp` más nuevo.
- Usa la canción completa solo para uso personal; respeta los derechos de autor.
