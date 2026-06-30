"""
from __future__ import annotations
import random
AI Voice Agent Server for paid ad-supported phone calls.
Routes: Twilio Voice → WebSocket → [Deepgram STT] → [LLM + Ad Matching] → [TTS] → Twilio
"""
import os
import re
import json
import uuid
import time
import hmac
import hashlib as _hashlib
import logging
import html
import asyncio
import threading
import audioop
import base64 as _b64
try:
    import miniaudio
except Exception:
    miniaudio = None
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from collections import defaultdict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Header
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse, Response
from pydantic import BaseModel

import httpx as _http
import websockets


async def ws_connect(url: str, headers: Optional[Dict[str, str]] = None, **kwargs):
    """Open a websockets client connection across library versions.

    websockets renamed the header kwarg over time: legacy/new builds expose
    either ``additional_headers`` or ``extra_headers``, and the top-level
    ``websockets.connect`` in some 1x releases forwards unknown kwargs straight
    into asyncio's ``create_connection`` (raising TypeError). Try the modern
    name first, fall back to the legacy name, then fall back to no headers.
    """
    if headers:
        try:
            return await websockets.connect(url, additional_headers=headers, **kwargs)
        except TypeError:
            pass
        try:
            return await websockets.connect(url, extra_headers=headers, **kwargs)
        except TypeError:
            pass
    return await websockets.connect(url, **kwargs)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("aivoice")

app = FastAPI(title="AI Voice Agent", version="1.0.0")

# ─── AD INVENTORY ─────────────────────────────────────────────────────────────
AD_DB: List[Dict[str, Any]] = []

SEGMENT_TAGS: Dict[str, List[str]] = {}

play_counts: Dict[str, int] = defaultdict(int)
total_revenue: float = 0.0
impression_log: List[Dict[str, Any]] = []

# ─── PERSISTENCE (SQLite) ─────────────────────────────────────────────────────
# Durable storage for ads + impressions so play counts/revenue survive restarts.
# On Render: attach a persistent disk and set DATABASE_PATH=/var/data/aivoice.db
# Locally it defaults to ./data/aivoice.db.
import sqlite3

DB_PATH = os.environ.get(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "aivoice.db"),
)
_db_lock = threading.RLock()
_db: Optional[sqlite3.Connection] = None


def db_conn() -> sqlite3.Connection:
    global _db
    if _db is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _db = sqlite3.connect(DB_PATH, check_same_thread=False)
        _db.row_factory = sqlite3.Row
        _db.execute("PRAGMA journal_mode=WAL;")
    return _db

def db_save_setting(key: str, value: Any):
    with _db_lock:
        c = db_conn()
        c.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        c.commit()

def db_load_settings() -> dict:
    with _db_lock:
        c = db_conn()
        rows = c.execute("SELECT key, value FROM settings").fetchall()
        out = {}
        for r in rows:
            try:
                out[r["key"]] = int(r["value"])
            except:
                out[r["key"]] = r["value"]
        return out


def db_init():
    """Create tables, seed ads on first run, then hydrate in-memory state."""
    with _db_lock:
        c = db_conn()
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS ads (
                id TEXT PRIMARY KEY, sponsor TEXT, industry TEXT, keywords TEXT,
                script TEXT, cta TEXT, bid_cpm REAL, daily_cap INTEGER,
                weight REAL, active INTEGER, variants TEXT
            );
            CREATE TABLE IF NOT EXISTS impressions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ad_id TEXT, session_id TEXT,
                caller_id TEXT, sponsor TEXT, bid_cpm REAL, revenue_usd REAL,
                ts REAL, date TEXT
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT, caller_id TEXT, ts REAL
            );
            """
        )
        c.commit()

        # Migration: add variants column to pre-existing DBs that lack it.
        cols = {row["name"] for row in c.execute("PRAGMA table_info(ads)").fetchall()}
        if "variants" not in cols:
            c.execute("ALTER TABLE ads ADD COLUMN variants TEXT")
            c.commit()
            log.info("[db] migrated: added ads.variants column")

        # --- migration: add placement column to ads if missing ---
        try:
            c.execute("ALTER TABLE ads ADD COLUMN placement TEXT DEFAULT 'none'")
            c.commit()
        except Exception:
            pass
        # NOTE: we no longer auto-seed from the hardcoded AD_DB default list.
        # The SQLite DB is the single source of truth for ads.
        # If the table is empty, no ads will play — the admin must add them via /admin.
        # (Previous behavior re-seeded default ads after every disk wipe,
        # which made it look like "ghost ads" were playing without admin control.)
        # One-time cleanup: remove any legacy built-in seed ads that an older
        # build may have persisted to the DB, so they can never play or reappear.
        _seed_ids = ("ad_001", "ad_002", "ad_003", "ad_004", "ad_005", "ad_006")
        _purged = c.execute("DELETE FROM ads WHERE id IN (?,?,?,?,?,?)", _seed_ids).rowcount
        if _purged:
            c.commit()
            log.info(f"[db] purged {_purged} legacy seed ad(s) from DB")
        n = c.execute("SELECT COUNT(*) AS n FROM ads").fetchone()["n"]
        if n == 0:
            log.info("[db] ads table is empty — no ads loaded until added via /admin")
        else:
            # Load ads from DB as the source of truth.
            rows = c.execute("SELECT * FROM ads").fetchall()
            AD_DB.clear()
            for r in rows:
                AD_DB.append({
                    "placement": (r["placement"] if "placement" in r.keys() else "none"),
                    "id": r["id"], "sponsor": r["sponsor"], "industry": r["industry"],
                    "keywords": json.loads(r["keywords"] or "[]"), "script": r["script"],
                    "cta": r["cta"], "bid_cpm": r["bid_cpm"], "daily_cap": r["daily_cap"],
                    "weight": r["weight"], "active": bool(r["active"]),
                    "variants": json.loads(r["variants"] or "[]") if "variants" in r.keys() else [],
                })
            log.info(f"[db] loaded {len(AD_DB)} ads from {DB_PATH}")

        # Hydrate play counts + revenue + recent impression log from history.
        global total_revenue
        for row in c.execute(
            "SELECT ad_id, COUNT(*) AS n FROM impressions GROUP BY ad_id"
        ).fetchall():
            play_counts[row["ad_id"]] = row["n"]
        rev = c.execute("SELECT COALESCE(SUM(revenue_usd),0) AS s FROM impressions").fetchone()["s"]
        total_revenue = float(rev or 0.0)
        for row in c.execute(
            "SELECT * FROM impressions ORDER BY id DESC LIMIT 100"
        ).fetchall():
            impression_log.append({
                "ad_id": row["ad_id"], "session_id": row["session_id"],
                "caller_id": row["caller_id"], "sponsor": row["sponsor"],
                "bid_cpm": row["bid_cpm"], "revenue_usd": row["revenue_usd"],
                "ts": row["ts"], "date": row["date"],
            })
        impression_log.reverse()  # keep chronological order
        log.info(
            f"[db] hydrated plays={sum(play_counts.values())} "
            f"revenue=${total_revenue:.2f} impressions={len(impression_log)}"
        )

        # Load persisted frequency settings from DB (overrides env defaults)
        try:
            saved = db_load_settings()
            for k, v in saved.items():
                if k in FREQUENCY:
                    FREQUENCY[k] = int(v)
            log.info(f"[db] loaded frequency overrides: {saved}")
        except Exception as e:
            log.warning(f"[db] frequency load skipped: {e}")


def _db_upsert_ad(c: sqlite3.Connection, ad: Dict[str, Any]):
    c.execute(
        """INSERT INTO ads (id,sponsor,industry,keywords,script,cta,bid_cpm,daily_cap,weight,active,variants,placement)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET sponsor=excluded.sponsor,industry=excluded.industry,
             keywords=excluded.keywords,script=excluded.script,cta=excluded.cta,
             bid_cpm=excluded.bid_cpm,daily_cap=excluded.daily_cap,weight=excluded.weight,
             active=excluded.active,variants=excluded.variants,placement=excluded.placement""",
        (
            ad["id"], ad.get("sponsor"), ad.get("industry"),
            json.dumps(ad.get("keywords", [])), ad.get("script"), ad.get("cta"),
            ad.get("bid_cpm", 0.0), ad.get("daily_cap", 100),
            ad.get("weight", 1.0), 1 if ad.get("active", True) else 0,
            json.dumps(ad.get("variants", [])),
            ad.get("placement", "none"),
        ),
    )


def db_save_ad(ad: Dict[str, Any]):
    with _db_lock:
        c = db_conn()
        _db_upsert_ad(c, ad)
        c.commit()


def db_delete_ad(ad_id: str):
    with _db_lock:
        c = db_conn()
        c.execute("DELETE FROM ads WHERE id=?", (ad_id,))
        c.commit()


def record_call(session_id: str, caller_id: Optional[str]):
    """Log one inbound call for time-window analytics."""
    try:
        with _db_lock:
            c = db_conn()
            c.execute(
                "INSERT INTO calls (session_id, caller_id, ts) VALUES (?,?,?)",
                (session_id, caller_id, time.time()),
            )
            c.commit()
    except Exception as e:
        log.warning(f"[calls] record failed: {e}")


def call_counts() -> Dict[str, int]:
    """Number of inbound calls within rolling time windows."""
    now = time.time()
    windows = {"h24": 86400, "d3": 259200, "d7": 604800, "d14": 1209600, "d30": 2592000}
    out = {}
    try:
        with _db_lock:
            c = db_conn()
            for k, secs in windows.items():
                out[k] = c.execute("SELECT COUNT(*) FROM calls WHERE ts >= ?", (now - secs,)).fetchone()[0]
    except Exception as e:
        log.warning(f"[calls] count failed: {e}")
        out = {k: 0 for k in windows}
    return out


def db_insert_impression(record: Dict[str, Any]):
    with _db_lock:
        c = db_conn()
        c.execute(
            """INSERT INTO impressions (ad_id,session_id,caller_id,sponsor,bid_cpm,revenue_usd,ts,date)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                record.get("ad_id"), record.get("session_id"), record.get("caller_id"),
                record.get("sponsor"), record.get("bid_cpm"), record.get("revenue_usd"),
                record.get("ts"), record.get("date"),
            ),
        )
        c.commit()


# Admin dashboard auth — set ADMIN_TOKEN in Render to a long random string.
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")


def check_admin(token: Optional[str]):
    """Raise 401 unless the provided token matches ADMIN_TOKEN.
    If ADMIN_TOKEN is unset, admin endpoints are locked (deny-by-default)."""
    if not ADMIN_TOKEN:
        raise HTTPException(503, "Admin disabled: set ADMIN_TOKEN env var")
    if not token or not hmac.compare_digest(token, ADMIN_TOKEN):
        raise HTTPException(401, "Unauthorized")


# ─── BILLING WEBHOOK / AD-SERVER INTEGRATION ──────────────────────────────────

BILLING_ENABLED = os.environ.get("BILLING_WEBHOOK_ENABLED", "0") == "1"
BILLING_URL = os.environ.get("BILLING_WEBHOOK_URL")
BILLING_SECRET = os.environ.get("BILLING_WEBHOOK_SECRET")


def sign_payload(payload: bytes) -> str:
    if not BILLING_SECRET:
        return ""
    return hmac.new(
        BILLING_SECRET.encode("utf-8"), payload, _hashlib.sha256
    ).hexdigest()


def fire_billing_webhook(record: Dict[str, Any]):
    if not (BILLING_ENABLED and BILLING_URL):
        return
    try:
        payload = json.dumps({
            "event": "ad_impression",
            "data": record,
        })
        headers = {"Content-Type": "application/json"}
        sig = sign_payload(payload.encode("utf-8"))
        if sig:
            headers["X-Signature-SHA256"] = sig

        def _send() -> None:
            try:
                with _http.Client(timeout=5) as client:
                    client.post(BILLING_URL, content=payload.encode("utf-8"), headers=headers)
            except Exception as exc:
                log.warning(f"[billing] webhook failed: {exc}")

        threading.Thread(target=_send, daemon=True).start()
    except Exception as exc:
        log.warning(f"[billing] enqueue failed: {exc}")


# ─── SESSION STATE ─────────────────────────────────────────────────────────────

@dataclass
class Session:
    session_id: str
    caller_id: Optional[str] = None
    caller_name: Optional[str] = None
    segment: List[str] = field(default_factory=list)
    transcript: List[Dict] = field(default_factory=list)
    topic_extract: Dict[str, float] = field(default_factory=dict)
    ads_played: List[str] = field(default_factory=list)
    last_ad_at: float = 0.0
    ad_play_times: List[float] = field(default_factory=list)  # ad play timestamps for rolling-window frequency
    created_at: float = field(default_factory=time.time)
    ended_at: Optional[float] = None
    metadata: Dict = field(default_factory=dict)
    dg_ws: Any = field(default=None)
    audio_buffer: bytearray = field(default_factory=bytearray)
    last_voice_at: float = 0.0
    in_speech: bool = False
    is_processing: bool = False
    is_speaking: bool = False          # True while agent TTS is playing out
    last_heard_at: float = 0.0         # last time we detected caller voice
    call_control_id: Optional[str] = None
    hung_up: bool = False

    async def ensure_dg_ws(self) -> Any:
        if self.dg_ws is None or getattr(self.dg_ws, "closed", False):
            url = (
                "wss://api.deepgram.com/v1/listen?"
                "encoding=mulaw&sample_rate=8000&channels=1&language=en-US&punctuate=true&interim_results=true"
            )
            headers = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}
            self.dg_ws = await ws_connect(url, headers=headers, close_timeout=5)
        return self.dg_ws


sessions: Dict[str, Session] = {}


# ─── AD ENGINE ────────────────────────────────────────────────────────────────

def classify_industry(transcript: List[Dict]) -> Dict[str, float]:
    texts = " ".join(m.get("text", "") for m in transcript[-8:]).lower()
    scores: Dict[str, float] = defaultdict(float)
    industry_keywords = {
        "technology": ["software", "app", "code", "api", "cloud", "server", "website", "computer", "ai", "data"],
        "home_services": ["electric", "plumb", "home", "house", "repair", "energy", "solar", "bill"],
        "health": ["health", "fitness", "doctor", "sleep", "exercise", "tracker", "weight"],
        "travel": ["travel", "flight", "trip", "vacation", "hotel", "book", "airport", "destination"],
        "legal": ["law", "legal", "lawyer", "contract", "rights", "dispute", "court", "sue"],
    }
    for industry, kwlist in industry_keywords.items():
        for kw in kwlist:
            if kw in texts:
                scores[industry] += 1.0 / (1 + max(0, 5 - len(kwlist)))
    total = sum(scores.values()) or 1.0
    return {k: v / total for k, v in scores.items()}


def select_ad(session: Session) -> Optional[Dict[str, Any]]:
    """Select the best ad for this moment using configurable frequency controls.

    Frequency rules (all per individual call / phone time):
    - AD_MIN_INTERVAL_SECONDS: minimum time since the last ad in this call.
    - AD_MAX_ADS / AD_WINDOW_SECONDS: maximum number of ads allowed in any rolling
      window of AD_WINDOW_SECONDS (e.g. max 2 ads in the last 10 minutes of call time).
    - Each ad also respects its own daily_cap (global across all callers).
    - Same ad + variants are allowed to repeat (no per-caller blacklist).

    This gives fine-grained control over ad density without hard-coding numbers.
    """
    now = time.time()
    min_int = FREQUENCY.get("min_interval_seconds", 90)
    max_ads = FREQUENCY.get("max_ads", 2)
    window = FREQUENCY.get("window_seconds", 600)
    force_after = FREQUENCY.get("force_min_ads_after_seconds", 300)
    min_target = FREQUENCY.get("min_ads_target", 1)

    # 1. Enforce minimum spacing between ads in this call
    if session.last_ad_at > 0 and (now - session.last_ad_at) < min_int:
        return None

    # 2. Enforce maximum ads in a rolling time window (based on call/phone time)
    if max_ads > 0 and window > 0:
        window_start = now - window
        recent_ads = [t for t in session.ad_play_times if t >= window_start]
        if len(recent_ads) >= max_ads:
            return None

    # 3. Force minimum ads after X minutes of call time
    # If the call has been going long enough and we haven't hit the minimum yet,
    # we relax the relevance threshold so an ad is more likely to be selected.
    call_age = now - getattr(session, 'created_at', now)
    force_min_mode = False
    if call_age > force_after and len(session.ad_play_times) < min_target:
        force_min_mode = True

    context = session.topic_extract or classify_industry(session.transcript)

    candidates = []
    for ad in AD_DB:
        if ad.get("placement", "none") != "none":
            continue  # placement ads play at their trigger, not by keyword
        if not ad["active"]:
            continue
        if play_counts[ad["id"]] >= ad["daily_cap"]:
            continue

        texts = " ".join(m.get("text", "") for m in session.transcript[-6:]).lower()
        keyword_hits = sum(1 for kw in ad["keywords"] if kw.lower() in texts)
        relevance = min(1.0, 0.6 + 0.1 * keyword_hits) if keyword_hits > 0 else 0.15
        if ad["industry"] in context:
            relevance = min(1.0, relevance + (0.3 if keyword_hits > 0 else 0.15) * context[ad["industry"]])
        score = ad["bid_cpm"] * ad["weight"] * max(0.3, relevance)
        candidates.append({"ad": ad, "score": score, "relevance": relevance})

    if not candidates:
        return None
    min_relevance = 0.1 if force_min_mode else 0.35
    # Only ads that actually matched (keyword/industry relevance) are eligible.
    # Among those the highest bid x relevance wins, so a high-bid ad with no
    # keyword match can no longer shadow and block a genuinely matched ad.
    eligible = [c for c in candidates if c["relevance"] >= min_relevance]
    if not eligible:
        if force_min_mode:
            log.info("[ad] forcing minimum ad due to call duration (relaxed relevance)")
            eligible = candidates
        else:
            return None
    eligible.sort(key=lambda x: x["score"], reverse=True)
    best = eligible[0]

    log.info(
        f"[ad] selected {best['ad']['id']} sponsor={best['ad']['sponsor']} "
        f"score={best['score']:.2f} relevance={best['relevance']:.2f} "
        f"(window_ads={len([t for t in session.ad_play_times if t >= now - window])} force_min={force_min_mode})"
    )
    return best["ad"]


def record_play(ad_id: str, session_id: str, caller_id: Optional[str]):
    play_counts[ad_id] += 1
    ad = next((a for a in AD_DB if a["id"] == ad_id), None)
    rev = ad["bid_cpm"] / 1000.0 if ad else 0.0
    global total_revenue
    total_revenue += rev
    record = {
        "ad_id": ad_id,
        "session_id": session_id,
        "caller_id": caller_id,
        "sponsor": ad["sponsor"] if ad else None,
        "bid_cpm": ad["bid_cpm"] if ad else None,
        "revenue_usd": rev,
        "ts": time.time(),
        "date": datetime.now(timezone.utc).isoformat(),
    }
    impression_log.append(record)
    db_insert_impression(record)
    log.info(
        f"[ad] play #{play_counts[ad_id]} sponsor={record['sponsor']} "
        f"revenue +${rev:.4f} day=${total_revenue:.2f}"
    )
    fire_billing_webhook(record)


def pick_ad_script(ad: Dict[str, Any], session: Session) -> str:
    """Choose the best script for this ad based on conversation keywords.
    If the ad has variants, pick the variant whose keywords best match recent
    speech; otherwise fall back to the ad's base script."""
    variants = ad.get("variants") or []
    if not variants:
        return ad["script"]
    texts = " ".join(m.get("text", "") for m in session.transcript[-6:]).lower()
    best_script = ad["script"]
    best_hits = 0
    for v in variants:
        kws = v.get("keywords", []) or []
        hits = sum(1 for kw in kws if kw.lower() in texts)
        if hits > best_hits:
            best_hits = hits
            best_script = v.get("script") or ad["script"]
    return best_script


_placement_rotation: Dict[str, int] = {}


_start_variant_rotation: Dict[str, int] = {}


def pick_start_script(ad):
    pool = [v for v in ad.get("variants", []) if v.get("start_enabled", True) and v.get("script")]
    if not pool:
        return ad.get("script", "")
    i = _start_variant_rotation.get(ad["id"], 0) % len(pool)
    _start_variant_rotation[ad["id"]] = i + 1
    return pool[i].get("script", "")


def pick_placement_ad(placement: str) -> Optional[Dict[str, Any]]:
    pool = [a for a in AD_DB if a.get("active", True) and a.get("placement", "none") == placement]
    if not pool:
        return None
    pool.sort(key=lambda a: a.get("id", ""))
    i = _placement_rotation.get(placement, 0) % len(pool)
    _placement_rotation[placement] = i + 1
    return pool[i]


def play_placement_ad_lines(session: Session, placement: str) -> Optional[str]:
    ad = pick_placement_ad(placement)
    if not ad:
        return None
    now = time.time()
    session.ads_played.append(ad["id"])
    session.last_ad_at = now
    session.ad_play_times.append(now)
    record_play(ad["id"], session.session_id, session.caller_id)
    session.metadata["pending_ad_id"] = ad["id"]
    if placement == "start":
        return pick_start_script(ad)
    return pick_ad_script({**ad, "variants": [v for v in ad.get("variants", []) if v.get("other_enabled", True)]}, session)


def maybe_inject_ad(session: Session) -> Optional[str]:
    ad = select_ad(session)
    if ad:
        now = time.time()
        session.ads_played.append(ad["id"])
        session.last_ad_at = now
        session.ad_play_times.append(now)   # for rolling window frequency control
        record_play(ad["id"], session.session_id, session.caller_id)
        session.metadata["pending_ad_id"] = ad["id"]
        return pick_ad_script({**ad, "variants": [v for v in ad.get("variants", []) if v.get("other_enabled", True)]}, session)
    return None

def build_post_ad_bridge(last_user_text: str) -> str:
    """After an ad plays, create a short, natural bridge that re-engages the user
    with what they were originally asking. Helps continue search conversations
    smoothly instead of dead air after the sponsor message.
    """
    if not last_user_text:
        return "Sorry about that quick break — what else can I help you with?"

    q = last_user_text.strip()
    # Keep it short and spoken
    options = [
        f"Sorry for the interruption. You were asking about {q[:70]} — what specifically were you looking for?",
        "Anyway, going back to what you asked — did that help, or do you need more details?",
        f"Back to your question. You mentioned {q[:60]}. Anything else on that?",
        "Alright, back to you — what were you trying to find out?",
    ]
    return random.choice(options)


# ─── REAL-TIME NEWS FEED (NewsAPI.org free tier) ───────────────────────────────

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
NEWSAPI_BASE = "https://newsapi.org/v2"

NEWS_TOPIC_QUERY: Dict[str, str] = {
    "technology": "technology OR AI OR software",
    "home_services": "home improvement OR energy OR utilities",
    "health": "health OR fitness OR wellness",
    "travel": "travel OR airlines OR tourism",
    "legal": "law OR legal OR regulation",
    "finance": "finance OR markets OR economy",
    "education": "education OR learning OR schools",
}

_news_cache: List[Dict[str, str]] = []
_news_cache_expires: float = 0.0
_NEWS_TTL = 900


def _best_topic_query(topic_extract: Dict[str, float]) -> Optional[str]:
    top = sorted(topic_extract.items(), key=lambda x: x[1], reverse=True)
    for industry, _ in top:
        q = NEWS_TOPIC_QUERY.get(industry)
        if q:
            return q
    return None


async def _fetch_news(query: str, limit: int = 5) -> List[Dict[str, str]]:
    if not NEWS_API_KEY:
        return []

    now = time.time()
    global _news_cache, _news_cache_expires
    if _news_cache and now < _news_cache_expires:
        ranked = sorted(
            _news_cache,
            key=lambda a: int(
                any(tok in (a.get("title", "") + " " + a.get("description", "")).lower()
                    for tok in query.lower().split(" OR "))
            ),
            reverse=True,
        )
        return ranked[:limit]

    try:
        url = f"{NEWSAPI_BASE}/everything"
        params = {
            "q": query,
            "sortBy": "publishedAt",
            "pageSize": 8,
            "language": "en",
            "apiKey": NEWS_API_KEY,
        }
        async with _http.AsyncClient(timeout=8) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()

        articles = data.get("articles", [])
        out = []
        for art in articles:
            title = art.get("title", "")
            source = art.get("source", {}).get("name", "the news")
            if not title:
                continue
            title = html.unescape(title)
            title = re.sub(r"\s+", " ", title).strip()
            if len(title) > 90:
                title = title[:87] + "..."
            out.append({
                "title": title,
                "source": source,
                "publishedAt": art.get("publishedAt", ""),
            })

        _news_cache = out
        _news_cache_expires = now + _NEWS_TTL
        return out[:limit]

    except Exception as exc:
        log.warning(f"[news] fetch error: {exc}")
        return []


async def get_contextual_event(session: Session) -> Optional[str]:
    call_events = sum(1 for t in session.transcript if t.get("role") == "system")
    if call_events >= 3:
        return None

    assists = sum(1 for t in session.transcript if t.get("role") == "assistant")
    if assists == 0 or assists % 9 != 0:
        return None

    if session.transcript:
        first_ts = session.transcript[0].get("ts", session.created_at)
        if time.time() - first_ts < 30:
            return None

    query = _best_topic_query(session.topic_extract or classify_industry(session.transcript))
    if not query:
        return None

    articles = await _fetch_news(query)
    if not articles:
        return None

    idx = abs(hash(session.session_id)) % len(articles)
    art = articles[idx]
    return f"Quick update from {art['source']}. {art['title']}."


# ─── SYSTEM PROMPT ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a warm, helpful voice on a phone call. You assist callers with whatever they need.
Identity rules (important):
- You do NOT have a name and you are NOT a branded product. If asked who or what you are, deflect warmly: say you're "just here to help" or "a helpful voice on the line" and steer back to helping. Never volunteer a name.
- Never say you are an AI, a bot, a language model, a virtual assistant, or software. Do not describe how you work or what you're "trained" on. Simply help, naturally, like a knowledgeable person would.
- If pressed hard on whether you're a real person, stay light and non-committal ("I'm just here to help you out") — do not claim to be a specific human, and do not launch into an AI disclosure.
Style rules:
- Keep answers short and spoken-friendly: 1-3 sentences unless more detail is asked for.
- Conversational and natural, never robotic. No "as an AI", no canned phrases.
- When unsure, say "I'm not sure, but let me help you look into it."
- Output only what you'd say aloud. No markdown, no bullet lists, no parentheses, no emojis.
- If asked to be transferred, say you can't connect calls but can stay on the line and help.
- Brief news snippets may be injected; acknowledge briefly if relevant or move on.

CRITICAL ACCURACY RULE — NEVER HALLUCINATE CONTACT INFO:
- You must NEVER invent, guess, or make up any phone number, address, zip code, business hours, website, or contact detail.
- Only use exact information that was explicitly provided to you in the current conversation (from search results or the caller).
- If the caller asks for a phone number, address, hours, or specific business contact and you do not have the exact verified details right in front of you, you MUST say something like:
  "I don't have that exact information right now" or "I'm not finding the current phone number for that — do you have another way I can help?"
- It is better to admit you don't have the info than to give a wrong number or address. Wrong contact details are harmful.
- For other facts (weather, news, scores, prices, general knowledge), you can use reasonable information, but for anything that sounds like a phone, address, or specific local business contact, be extremely strict.

LOOKUP & CLARIFICATION BEHAVIOR (key to good user experience and accuracy):
- Use your own reasoning and knowledge first to understand exactly what the user is really asking for.
- For current, specific, or verifiable info (phone numbers, addresses, hours, weather, sports scores, stock prices, news, current prices, business or product details):
  - Do NOT guess or make up an answer.
  - Instead, output EXACTLY one of these two special lines and NOTHING ELSE:
    [NEED_SEARCH: the best specific search query to find the exact info]
    Example: user asks "phone for the pizza place" → [NEED_SEARCH: Joe's Pizza Columbus Ohio phone number]
    [CLARIFY: one short natural spoken question to confirm the exact thing or location before searching]
    Example: [CLARIFY: Is that the Joe's Pizza downtown or the one on Main Street?]
- This lets you "search your knowledge base" first to interpret the request properly and either get better search results or ask the user to confirm so we don't waste a search or give wrong info.
- Only after the system runs the search (or the user clarifies) will you receive the live facts and produce the final spoken answer.
- STRONGLY PREFER [NEED_SEARCH]. Use [CLARIFY] only as a true last resort when you cannot form ANY reasonable search query. Make your best assumption and search rather than asking. Never ask more than one clarifying question in a call — after one, pick the most likely interpretation and search immediately.
- For normal conversational questions or stable knowledge (history, definitions, "how are you", general how-to), just answer naturally and directly. Never use the special tags for those.
"""


def build_messages(session: Session) -> List[Dict]:
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    for turn in session.transcript[-12:]:
        role = "user" if turn["role"] == "user" else "assistant"
        msgs.append({"role": role, "content": turn["text"]})
    return msgs


# ─── LLM + AD TTS PIPELINE ────────────────────────────────────────────────────

import httpx

DEEPGRAM_API_KEY = os.environ.get("DEEPGRAM_API_KEY", "")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE = os.environ.get("LLM_BASE_URL", "https://api.together.xyz/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo")

TTS_PROVIDER = os.environ.get("TTS_PROVIDER", "cartesia")  # cartesia | elevenlabs | openai | deepgram
SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")  # serper.dev — Google results for accurate phone/address lookups
TELNYX_API_KEY = os.environ.get("TELNYX_API_KEY", "")  # needed to hang up calls server-side
# Hang up if the caller is silent for this many seconds while the agent is just listening.
SILENCE_HANGUP_SEC = float(os.environ.get("SILENCE_HANGUP_SEC", "10"))
CARTESIA_API_KEY = os.environ.get("CARTESIA_API_KEY", "")
CARTESIA_VOICE = os.environ.get("CARTESIA_VOICE", "")  # main assistant voice; empty => auto-resolve from account
CARTESIA_AD_VOICE = os.environ.get("CARTESIA_AD_VOICE", "")  # voice for ad reads; empty => falls back to main voice
AD_BREAK_CUE = os.environ.get("AD_BREAK_CUE", "And now, a quick word from our sponsor.")

AD_CUE_PATH = os.path.join(os.environ.get("DATA_DIR", "/var/data"), "ad_cue.ulaw")
AD_CUE_ULAW: Optional[bytes] = None
AD_OUTRO_DIR = os.environ.get("DATA_DIR", "/var/data")
AD_OUTRO_ULAW: Dict[str, bytes] = {}


def _ad_outro_path(ad_id):
    safe = re.sub(r"[^A-Za-z0-9_-]", "", ad_id or "")
    return os.path.join(AD_OUTRO_DIR, "ad_outro_" + safe + ".ulaw")


def load_ad_outros():
    import glob
    AD_OUTRO_ULAW.clear()
    try:
        for p in glob.glob(os.path.join(AD_OUTRO_DIR, "ad_outro_*.ulaw")):
            base = os.path.basename(p)[len("ad_outro_"):-len(".ulaw")]
            with open(p, "rb") as f:
                AD_OUTRO_ULAW[base] = f.read()
        log.info(f"[cue] loaded {len(AD_OUTRO_ULAW)} ad outro(s)")
    except Exception as e:
        log.warning(f"[cue] outro load failed: {e}")


def _decode_audio_to_ulaw(data: bytes) -> bytes:
    """Decode MP3/WAV/etc bytes to 8kHz mono mu-law (telephone format)."""
    if miniaudio is None:
        raise RuntimeError("audio decoder (miniaudio) not available")
    dec = miniaudio.decode(data, output_format=miniaudio.SampleFormat.SIGNED16, nchannels=1, sample_rate=8000)
    return audioop.lin2ulaw(dec.samples.tobytes(), 2)


def load_ad_cue() -> None:
    global AD_CUE_ULAW
    try:
        with open(AD_CUE_PATH, "rb") as f:
            AD_CUE_ULAW = f.read()
        log.info(f"[cue] loaded ad cue ({len(AD_CUE_ULAW)} bytes mu-law)")
    except FileNotFoundError:
        AD_CUE_ULAW = None
    except Exception as e:
        AD_CUE_ULAW = None
        log.warning(f"[cue] load failed: {e}")


def save_ad_cue(ulaw: bytes) -> None:
    global AD_CUE_ULAW
    os.makedirs(os.path.dirname(AD_CUE_PATH), exist_ok=True)
    with open(AD_CUE_PATH, "wb") as f:
        f.write(ulaw)
    AD_CUE_ULAW = ulaw


def clear_ad_cue() -> None:
    global AD_CUE_ULAW
    AD_CUE_ULAW = None
    try:
        os.remove(AD_CUE_PATH)
    except FileNotFoundError:
        pass

# Ad frequency controls (per call / phone time)
# These let you control how many ads are played relative to call duration.
# Runtime frequency settings (can be overridden from /admin dashboard)
# Loaded from DB on startup, falling back to env vars
FREQUENCY = {
    "min_interval_seconds": int(os.environ.get("AD_MIN_INTERVAL_SECONDS", "90")),
    "max_ads": int(os.environ.get("AD_MAX_ADS", "2")),
    "window_seconds": int(os.environ.get("AD_WINDOW_SECONDS", "600")),
    # Minimum ads enforcement
    "force_min_ads_after_seconds": int(os.environ.get("AD_FORCE_MIN_AFTER_SECONDS", "300")),  # 5 minutes
    "min_ads_target": int(os.environ.get("AD_MIN_ADS_TARGET", "1")),
}
CARTESIA_MODEL = os.environ.get("CARTESIA_MODEL", "sonic-2")
_resolved_voice: Optional[str] = None


def resolve_cartesia_voice() -> str:
    """Return a valid Cartesia voice ID. Uses CARTESIA_VOICE if set, else
    fetches the first English voice from the account and caches it."""
    global _resolved_voice
    if CARTESIA_VOICE:
        return CARTESIA_VOICE
    if _resolved_voice:
        return _resolved_voice
    for ver in ("2024-11-13", "2024-06-10"):
        try:
            r = http.get(
                "https://api.cartesia.ai/voices/",
                headers={"Cartesia-Version": ver, "X-API-Key": CARTESIA_API_KEY},
                timeout=10,
            )
            if r.status_code != 200:
                continue
            data = r.json()
            items = data if isinstance(data, list) else data.get("data", [])
            # prefer an English voice, else take the first available
            en = [v for v in items if str(v.get("language", "")).startswith("en")]
            pick = (en or items)
            if pick:
                _resolved_voice = pick[0]["id"]
                log.info(f"[tts:cartesia] auto-resolved voice id={_resolved_voice} name={pick[0].get('name')}")
                return _resolved_voice
        except Exception as exc:
            log.warning(f"[tts:cartesia] voice resolve failed (ver={ver}): {exc}")
    # last-resort fallback (a commonly-available Cartesia public voice)
    return "a0e99841-438c-4a64-b679-ae501e7d6091"
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE = os.environ.get("ELEVENLABS_VOICE", "Rachel")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_TTS_VOICE = os.environ.get("OPENAI_TTS_VOICE", "nova")

http = httpx.Client(timeout=30)


async def telnyx_hangup(call_control_id: str):
    """Hang up a Telnyx call via the Call Control API.
    Uses empty body; logs full response on non-success (common 422 means call
    already ended or invalid state — not always fatal).
    """
    if not (TELNYX_API_KEY and call_control_id):
        return
    try:
        def _do_hangup():
            return http.post(
                f"https://api.telnyx.com/v2/calls/{call_control_id}/actions/hangup",
                headers={"Authorization": f"Bearer {TELNYX_API_KEY}", "Content-Type": "application/json"},
                json={},
                timeout=10,
            )
        r = await asyncio.to_thread(_do_hangup)
        if r.status_code in (200, 204):
            log.info(f"[telnyx] hangup success cc={call_control_id[:18]}")
        else:
            # 422 is common if the call already ended naturally — not a hard failure
            log.warning(f"[telnyx] hangup cc={call_control_id[:18]} status={r.status_code} body={r.text[:300]!r}")
    except Exception as exc:
        log.warning(f"[telnyx] hangup failed: {exc}")


# Improved web search trigger logic (cheap heuristics, no extra LLM cost).
# We only pay for Serper on questions that actually need current data.
MUST_SEARCH = [
    "phone number", "number for", "address for", "hours for", "open today", "is .* open",
    "directions to", "near me", "closest", "nearest",
    "weather", "forecast", "temperature", "raining", "snow", "stock", "stocks",
    "stock price", "score", "scores", "who won", "game today", "live score", "how are the .* doing",
    "news", "latest news", "breaking", "what happened", "update on",
    "current price", "how much does", "price of", "in stock", "where to buy", "cost of",
    "part number", "specs for", "tell me about the", "latest on",
]
HISTORICAL_SKIP = ["who was", "when did", "what was", "in 17", "in 18", "in 19", "in 20",
                   "died in", "lived in", "built in", "abraham lincoln", "george washington",
                   "ancient", "history of", "last century"]
CURRENT_MARKERS = ["today", "right now", "currently", "latest", "this week", "live", "as of now"]

def _refine_search_query(user_text: str, transcript=None) -> str:
    """Turn a spoken query into a better Google search string.
    Uses recent conversation context for follow-ups (e.g. "their hours?" after mentioning a business).
    This dramatically improves result quality without extra cost.
    """
    q = user_text.strip()
    if not transcript:
        return q[:180]

    # Grab last 1-2 user turns for context
    recent = []
    for m in transcript[-3:]:
        if m.get("role") == "user":
            recent.append(m.get("text", ""))
    if not recent:
        return q[:180]

    context = " ".join(recent[-2:])[-160:]
    # Simple heuristic: if the current question is short/follow-up like, prepend context
    short_followups = ("their ", "its ", "the ", "that ", "those ", "what about", "how about", "and the", "also")
    if len(q) < 45 or any(q.lower().startswith(s) for s in short_followups):
        combined = f"{context} {q}".strip()
        # Avoid repeating the same business name
        return combined[:220]
    return q[:180]


def needs_web_lookup(text: str, transcript=None):
    """Smart trigger: search only for things that need fresh data.
    Keeps cost low by skipping historical facts and pure conversation.
    EXTRA AGGRESSIVE on anything that could lead to phone/address hallucination."""
    if not text: return False
    t = text.lower().strip()

    # Conversational chit-chat — never search
    chit_chat = ("how are you", "how's it going", "what's up", "how you doing",
                 "tell me a joke", "what's your name", "who are you")
    if any(c in t for c in chit_chat):
        return False

    hist = any(h in t for h in HISTORICAL_SKIP)

    # Very strong early block for historical questions (Abraham Lincoln style)
    # These should never trigger expensive/current-data search.
    historical_where = ("where did", "where was", "where does he live", "where did he", "where she lived")
    if any(hq in t for hq in historical_where) or hist:
        if any(h in t for h in ("lincoln", "washington", "president", "died", "lived", "built", "ancient", "history")) or hist:
            return False

    # Business / local / contact lookups — ALWAYS search (this is the most important accuracy case)
    contact_patterns = ("phone", "number for", "the number", "call ", "address", "hours", "open", 
                        "directions", "near me", "closest", "nearest", "located", "contact", 
                        "zip code", "area code")
    if any(x in t for x in contact_patterns):
        return True

    # Explicit "where is" / "where's" for current locations/businesses only
    if ("where is" in t or "where's" in t or "where can i" in t or "where to find" in t):
        if not hist:
            return True

    # Hard categories the user specified (news, sports, weather, stocks, product info)
    if any(p in t for p in MUST_SEARCH):
        return True

    # Current-time language, but only if not clearly historical
    has_now = any(m in t for m in CURRENT_MARKERS)
    if has_now and not hist:
        return True

    # "what is / who is" style questions:
    # Only search if it smells like current/business info.
    if any(b in t for b in ("what is", "who is", "how much", "price", "what are")):
        if hist:
            return False
        business_flavor = ("price", "cost", "open", "store", "company", "stock", "news", "game", "weather", "today", "now", "business", "restaurant", "store", "shop")
        if any(w in t for w in business_flavor):
            return True
        return False

    # Follow-up context
    if transcript:
        recent = " ".join(m.get("text","") for m in transcript[-3:] if m.get("role")=="user").lower()
        if any(m in recent for m in CURRENT_MARKERS) and not hist:
            return True
    return False

async def web_lookup(user_text: str, transcript=None) -> str:
    """Query Serper.dev (Google) and return clean factual text the LLM can read aloud.
    Uses a refined search query built from user_text + recent transcript context for much better results on follow-ups.
    Prioritises knowledge graph (phone/address/hours), answer box, places, then organic.
    """
    if not SERPER_API_KEY:
        return ""

    # Build a much better search query using context
    search_q = _refine_search_query(user_text, transcript)
    log.info(f"[serper] refined query: {search_q[:80]!r}")

    try:
        r = await asyncio.to_thread(
            http.post,
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": search_q, "num": 8},
        )
    except Exception as exc:
        log.warning(f"[serper] request failed: {exc}")
        return ""
    if r.status_code != 200:
        log.warning(f"[serper] HTTP {r.status_code}: {r.text[:200]!r}")
        return ""

    try:
        data = r.json()
    except Exception:
        return ""

    parts: List[str] = []

    # 1. Knowledge graph — the gold for businesses (verified phone/address/hours).
    kg = data.get("knowledgeGraph") or {}
    if kg:
        title = kg.get("title", "")
        attrs = kg.get("attributes", {}) or {}
        if title:
            parts.append(f"Name: {title}")
        if kg.get("type"):
            parts.append(f"Type: {kg['type']}")
        if attrs.get("Address") or kg.get("address"):
            parts.append(f"Address: {attrs.get('Address') or kg.get('address')}")
        if attrs.get("Phone") or kg.get("phone"):
            parts.append(f"Phone: {attrs.get('Phone') or kg.get('phone')}")
        if attrs.get("Hours") or kg.get("hours"):
            parts.append(f"Hours: {attrs.get('Hours') or kg.get('hours')}")
        if kg.get("website"):
            parts.append(f"Website: {kg['website']}")
        for k, v in attrs.items():
            if k not in ("Address", "Phone", "Hours") and len(parts) < 10:
                parts.append(f"{k}: {v}")

    # 2. Answer box — direct answers (weather, conversions, quick facts).
    ab = data.get("answerBox") or {}
    if ab:
        for key in ("answer", "snippet", "title"):
            if ab.get(key):
                parts.append(str(ab[key]))
                break

    # 3. Map pack / places — local business results with phone+address.
    for place in (data.get("places") or [])[:3]:
        seg = place.get("title", "")
        if place.get("address"):
            seg += f" — {place['address']}"
        if place.get("phoneNumber"):
            seg += f" — {place['phoneNumber']}"
        if seg:
            parts.append(seg)

    # 4. Top organic results as fallback context.
    if not parts:
        for o in (data.get("organic") or [])[:3]:
            seg = o.get("title", "")
            if o.get("snippet"):
                seg += f": {o['snippet']}"
            if seg:
                parts.append(seg)

    result = "\n".join(p for p in parts if p).strip()
    if not result:
        return ""
    # Add a small header so the LLM knows this came from live search
    return f"Live search results for: {search_q}\n{result}"[:1400]


async def call_llm(session: Session, user_text: str) -> tuple:
    session.transcript.append({"role": "user", "text": user_text, "ts": time.time()})
    session.topic_extract = classify_industry(session.transcript)

    messages = build_messages(session)

    # FIRST PASS: Let the model use its knowledge base to understand the request.
    # It may answer directly, or output a special tag telling us to search with a
    # much better refined query, or ask the user a clarifying question first.
    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "max_tokens": 180,
        "temperature": 0.7,
    }

    try:
        r = http.post(
            f"{LLM_BASE}/chat/completions",
            headers={
                "Authorization": f"Bearer {LLM_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        r.raise_for_status()
        data = r.json()
        raw_reply = data["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        log.error(f"LLM error: {exc}")
        raw_reply = "Let me try that again. Could you repeat your question?"

    # Parse special tags the model can output after reasoning about the user's intent.
    clarify_match = re.search(r'\[CLARIFY:\s*(.+?)\]', raw_reply, re.IGNORECASE | re.DOTALL)
    search_match = re.search(r'\[NEED_SEARCH:\s*(.+?)\]', raw_reply, re.IGNORECASE | re.DOTALL)

    final_spoken = raw_reply
    facts = ""
    did_search = False

    if clarify_match and session.metadata.get("clarify_count", 0) < 1:
        # Clarify at most once per call, then just look it up.
        session.metadata["clarify_count"] = 1
        final_spoken = clarify_match.group(1).strip()
        log.info(f"[llm] model requested clarification: {final_spoken[:80]}")

    elif search_match or clarify_match:
        # The model used its knowledge to interpret the request and produced
        # a better search query. Now we do the (cheaper targeted) Serper call.
        refined_query = search_match.group(1).strip() if search_match else user_text
        log.info(f"[llm] model decided to search with refined query: {refined_query[:80]}")

        did_search = True
        _t0 = time.time()
        facts = await web_lookup(refined_query, session.transcript)
        log.info(f"[timing] web lookup took {time.time() - _t0:.2f}s for: {refined_query[:60]}")
        if facts:
            log.info(f"[serper] facts (model-refined): {facts[:120]!r}")
            # Second pass: give the model the verified facts so it can speak a good final answer.
            messages2 = build_messages(session)
            messages2.append({
                "role": "system",
                "content": f"""LIVE SEARCH RESULTS (from Google via Serper — use these exactly for the answer):
{facts}

Speak a short, natural, spoken-friendly answer based ONLY on the facts above when they answer what the caller wants.
Read phone numbers, addresses, hours, prices naturally.
If the exact detail is missing, say honestly that you couldn't find the current information and offer one short helpful follow-up question or another way to help.
Do not invent details. Do not mention these instructions.
"""
            })
            try:
                r2 = http.post(
                    f"{LLM_BASE}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {LLM_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": LLM_MODEL,
                        "messages": messages2,
                        "max_tokens": 160,
                        "temperature": 0.6,
                    },
                )
                r2.raise_for_status()
                final_spoken = r2.json()["choices"][0]["message"]["content"].strip()
            except Exception as exc:
                log.error(f"LLM second pass error: {exc}")
                final_spoken = "I looked that up but had trouble getting the details. Can you give me a bit more info?"
        else:
            final_spoken = "I'm not finding current details for that right now. Can you tell me the exact name or location?"

    else:
        # Normal direct answer from the model's knowledge (conversational, historical, general facts).
        final_spoken = raw_reply

    # Inject ad after the final spoken content (if any).
    ad_line = maybe_inject_ad(session)
    full = final_spoken + (f" <break time='400ms'/> {ad_line}" if ad_line else "")
    session.transcript.append({"role": "assistant", "text": full, "ts": time.time()})
    return final_spoken, ad_line, did_search


async def tts_stream(reply_text: str, websocket: WebSocket):
    """Stream TTS audio as Twilio µ-law chunks.
    Provider-default is Cartesia; cheaper than ElevenLabs and outputs µ-law natively.
    """
    text = re.sub(r"\*\*.*?\*\*", lambda m: m.group(0).replace("*", ""), reply_text)
    text = re.sub(r"[*#`_\[\]()]", "", text)
    text = text.replace("\n", " ")

    if TTS_PROVIDER == "deepgram":
        url = "https://api.deepgram.com/v1/speak?model=aura-asteria-en&encoding=mulaw&sample_rate=8000"
        headers = {
            "Authorization": f"Token {DEEPGRAM_API_KEY}",
            "Content-Type": "application/json",
        }
        with http.stream("POST", url, headers=headers, json={"text": text}) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes():
                await websocket.send_bytes(chunk)

    elif TTS_PROVIDER == "elevenlabs":
        url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE}/stream"
        headers = {"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"}
        payload = {
            "text": text,
            "model_id": "eleven_turbo_v2_5",
            "output_format": "pcm_8000",
        }
        with http.stream("POST", url, headers=headers, json=payload) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes():
                await websocket.send_bytes(chunk)

    else:  # cartesia default — REST (POST /tts/bytes), matches Telnyx path
        url = "https://api.cartesia.ai/tts/bytes"
        headers = {
            "Cartesia-Version": "2024-11-13",
            "X-API-Key": CARTESIA_API_KEY,
            "Content-Type": "application/json",
        }
        payload = {
            "model_id": CARTESIA_MODEL,
            "transcript": text,
            "voice": {"mode": "id", "id": resolve_cartesia_voice()},
            "output_format": {
                "container": "raw",
                "encoding": "pcm_mulaw",
                "sample_rate": 8000,
            },
            "language": "en",
        }
        with http.stream("POST", url, headers=headers, json=payload) as r:
            if r.status_code != 200:
                log.warning(f"[tts:cartesia] HTTP {r.status_code}: {r.read()[:400]!r}")
                return
            for chunk in r.iter_bytes():
                await websocket.send_bytes(chunk)

    await websocket.send_json({"type": "audio_done"})


# ─── TWILIO WEBHOOK ROUTES ────────────────────────────────────────────────────

@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return JSONResponse({
        "status": "ok",
        "sessions_active": len(sessions),
        "total_revenue_today": round(total_revenue, 3),
        "total_plays": sum(play_counts.values()),
        "impressions_logged": len(impression_log),
    })


@app.get("/admin/ads")
async def list_ads(x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    return {
        "ads": AD_DB,
        "play_counts": dict(play_counts),
        "total_revenue_usd": round(total_revenue, 4),
        "billing_connected": bool(BILLING_ENABLED and BILLING_URL),
    }


@app.post("/twilio/voice")
async def twilio_voice_webhook():
    twiml = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="alice">Connecting you to your assistant.</Say>
  <Connect>
    <Stream url="{ws_url}" contentType="audio/x-mulaw;rate=8000" />
  </Connect>
</Response>""".format(ws_url=os.environ.get("PUBLIC_WS_URL", "wss://your-app.onrender.com/ws"))

    return PlainTextResponse(twiml, media_type="application/xml")


@app.api_route("/telnyx/voice", methods=["GET", "POST"])
async def telnyx_voice_webhook():
    """Return Telnyx TeXML that opens a bidirectional µ-law WebSocket stream."""
    try:
        ws_url = os.environ.get("PUBLIC_WSS_URL") or os.environ.get("PUBLIC_WS_URL", "wss://your-app.onrender.com")
        # Force wss scheme for WebSocket
        if ws_url.startswith("https://"):
            ws_url = "wss://" + ws_url[len("https://"):]
        # Normalize to the dedicated Telnyx WebSocket route /telnyx/ws
        if ws_url.endswith("/telnyx/ws"):
            pass
        elif ws_url.endswith("/ws"):
            ws_url = ws_url[: -len("/ws")].rstrip("/") + "/telnyx/ws"
        else:
            ws_url = ws_url.rstrip("/") + "/telnyx/ws"

        texml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="{ws_url}" bidirectionalMode="rtp" contentType="audio/x-mulaw;rate=8000" />
  </Connect>
  <Pause length="40"/>
</Response>"""
        return PlainTextResponse(texml, media_type="application/xml")
    except Exception as exc:
        log.exception(f"[telnyx] error building voice webhook TeXML: {exc}")
        # Return a minimal valid response so Telnyx doesn't get a giant HTML error page
        fallback = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="alice">Sorry, we're having trouble right now. Please try again later.</Say>
</Response>"""
        return PlainTextResponse(fallback, media_type="application/xml")


@app.websocket("/telnyx/ws")
async def telnyx_websocket_endpoint(websocket: WebSocket):
    """Telnyx media streaming over WebSocket with bidirectional RTP.
    Inbound audio from the caller arrives as raw µ-law bytes.
    Outbound audio to the caller is pumped back as raw µ-law bytes.
    """
    await websocket.accept()
    session_id = str(uuid.uuid4())
    session = Session(session_id=session_id)
    sessions[session_id] = session
    log.info(f"[telnyx] ws connected session={session_id}")

    stream_id: Optional[str] = None
    call_control_id: Optional[str] = None
    caller_num: Optional[str] = None
    media_buffer = bytearray()
    awaiting_start = True
    _b64 = __import__("base64")

    async def _send_outbound(text: str, voice_id: Optional[str] = None, raw_ulaw: Optional[bytes] = None, prefix_ulaw: Optional[bytes] = None, suffix_ulaw: Optional[bytes] = None):
        nonlocal stream_id
        if not stream_id:
            return
        # Collect µ-law audio from Cartesia, then send it back to Telnyx as
        # JSON "media" events with base64 payload, chunked into 160-byte
        # (~20ms @ 8kHz PCMU) frames. Telnyx expects this JSON format even in
        # bidirectional mode — raw WebSocket bytes are NOT played.
        outbound_buffer = bytearray()
        if raw_ulaw is not None:
            outbound_buffer.extend(raw_ulaw)
        else:
            if prefix_ulaw:
                outbound_buffer.extend(prefix_ulaw)
            async for chunk in _cartesia_ulaw_stream(text, voice_id=voice_id):
                outbound_buffer.extend(chunk)
            if suffix_ulaw:
                outbound_buffer.extend(suffix_ulaw)
        if not outbound_buffer:
            return
        session.is_speaking = True
        try:
            frame = 160
            for i in range(0, len(outbound_buffer), frame):
                payload = bytes(outbound_buffer[i:i + frame])
                b64 = _b64.b64encode(payload).decode("ascii")
                await websocket.send_text(json.dumps({
                    "event": "media",
                    "stream_id": stream_id,
                    "media": {"payload": b64},
                }))
                # pace slightly so Telnyx's jitter buffer plays smoothly
                await asyncio.sleep(0.018)
        finally:
            session.is_speaking = False
            # Reset the silence clock so the caller gets the full window to respond
            # AFTER the agent finishes speaking — not counted from before.
            session.last_heard_at = time.time()

    async def _silence_watchdog():
        """Hang up if the caller is silent for SILENCE_HANGUP_SEC while the agent
        is idle (not speaking, not processing). Gives them the full window to reply."""
        if SILENCE_HANGUP_SEC <= 0:
            return
        try:
            while not session.hung_up:
                await asyncio.sleep(1.0)
                if awaiting_start or not session.last_heard_at:
                    continue
                if session.is_speaking or session.is_processing:
                    continue
                idle = time.time() - session.last_heard_at
                if idle >= SILENCE_HANGUP_SEC:
                    log.info(f"[telnyx] silence {idle:.0f}s — hanging up session={session_id}")
                    session.hung_up = True
                    if session.call_control_id:
                        await telnyx_hangup(session.call_control_id)
                    try:
                        await websocket.close()
                    except Exception:
                        pass
                    return
        except Exception as exc:
            log.warning(f"[telnyx] watchdog error: {exc}")

    try:
        while True:
            raw = await websocket.receive()

            if "text" in raw:
                msg = json.loads(raw["text"])
                event = msg.get("event")

                if event == "connected":
                    log.info(f"[telnyx] connected session={session_id}")

                elif event == "start":
                    awaiting_start = False
                    stream_id = msg.get("stream_id") or msg.get("start", {}).get("stream_id") or msg.get("streamId")
                    call_control_id = msg.get("call_control_id") or msg.get("start", {}).get("call_control_id")
                    caller_num = msg.get("from") or msg.get("start", {}).get("from")
                    session.caller_id = caller_num or ""
                    session.call_control_id = call_control_id
                    session.last_heard_at = time.time()
                    session.segment = SEGMENT_TAGS.get(session.caller_id, [])
                    mf = msg.get("start", {}).get("media_format", {})
                    encoding = mf.get("encoding", "PCMU")
                    sample_rate = mf.get("sample_rate", 8000)
                    session.metadata["telnyx_encoding"] = encoding
                    session.metadata["telnyx_sample_rate"] = sample_rate
                    log.info(
                        f"[telnyx] stream start stream_id={stream_id} "
                        f"call_control_id={call_control_id} caller={caller_num} "
                        f"encoding={encoding} rate={sample_rate}"
                    )
                    # Log this inbound call (for call-count analytics).
                    record_call(session.session_id, caller_num)
                    # Start the silence watchdog (hangs up after N seconds idle).
                    asyncio.create_task(_silence_watchdog())
                    # Greet the caller immediately so the call doesn't open silent.
                    greeting = os.environ.get(
                        "GREETING",
                        "Hi! Thanks for calling. How can I help you today?",
                    )
                    session.transcript.append({"role": "assistant", "text": greeting, "ts": time.time()})
                    async def _intro():
                        start_ad = play_placement_ad_lines(session, "start")
                        if start_ad:
                            await _send_outbound(start_ad, (CARTESIA_AD_VOICE or None), suffix_ulaw=AD_OUTRO_ULAW.get(session.metadata.get("pending_ad_id")))
                        await _send_outbound(greeting)
                    asyncio.create_task(_intro())

                elif event == "stop":
                    log.info(f"[telnyx] stream stopped stream_id={stream_id}")
                    break

                elif event == "media":
                    if awaiting_start or not stream_id:
                        continue
                    if msg.get("media", {}).get("track") not in (None, "inbound"):
                        continue
                    payload = msg.get("media", {}).get("payload")
                    if not payload:
                        continue
                    chunk = _b64.b64decode(payload)
                    media_buffer.extend(chunk)

                    if len(media_buffer) >= 240:
                        to_send, media_buffer = bytes(media_buffer[:240]), media_buffer[240:]
                        await process_speech(to_send, session, websocket, stream_id or "", send_fn=_send_outbound)

            elif "bytes" in raw:
                # In bidirectional RTP mode, inbound audio arrives as raw bytes.
                if awaiting_start or not stream_id:
                    continue
                media_buffer.extend(raw["bytes"])
                if len(media_buffer) >= 240:
                    to_send, media_buffer = bytes(media_buffer[:240]), media_buffer[240:]
                    await process_speech(to_send, session, websocket, stream_id or "", send_fn=_send_outbound)

    except WebSocketDisconnect:
        log.info(f"[telnyx] disconnected session={session_id}")
    except Exception as exc:
        log.error(f"[telnyx] error session={session_id} exc={exc}")
    finally:
        session.ended_at = time.time()
        duration = (session.ended_at - session.created_at) if session.ended_at and session.created_at else 0
        session_dur_tag = f"dur={duration:.1f}s"
        log.info(
            f"[session] ended id={session_id} {session_dur_tag} "
            f"transcript_turns={len(session.transcript)} ads={session.ads_played}"
        )


async def _cartesia_ulaw_stream(text: str, voice_id: Optional[str] = None) -> Any:
    """Yield µ-law bytes from Cartesia REST (POST /v1/tts/bytes).
    voice_id overrides the default voice (used for ad reads)."""
    cleaned = re.sub(r"\*\*.*?\*\*", lambda m: m.group(0).replace("*", ""), text)
    cleaned = re.sub(r"[*#`_\[\]()]", "", cleaned).replace("\n", " ")
    url = "https://api.cartesia.ai/tts/bytes"
    headers = {
        "Cartesia-Version": "2024-11-13",
        "X-API-Key": CARTESIA_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "model_id": CARTESIA_MODEL,
        "transcript": cleaned,
        "voice": {"mode": "id", "id": voice_id or resolve_cartesia_voice()},
        "output_format": {"container": "raw", "encoding": "pcm_mulaw", "sample_rate": 8000},
        "language": "en",
    }
    with http.stream("POST", url, headers=headers, json=payload) as r:
        if r.status_code != 200:
            body = r.read()
            log.warning(f"[tts:cartesia] HTTP {r.status_code}: {body[:400]!r}")
            return
        for chunk in r.iter_bytes():
            yield chunk


# Keep Twilio/WebSocket handler and remaining server code.
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = str(uuid.uuid4())
    session = Session(session_id=session_id)
    sessions[session_id] = session
    log.info(f"[ws] connected session={session_id}")

    last_stream_sid: Optional[str] = None
    media_buffer = bytearray()

    try:
        while True:
            raw = await websocket.receive()

            if "text" in raw:
                msg = json.loads(raw["text"])
                event_type = msg.get("event")

                if event_type == "connected":
                    log.info(f"[ws] connected={msg}")

                elif event_type == "start":
                    last_stream_sid = msg.get("streamSid")
                    caller_num = msg.get("customParameters", {}).get("From", "")
                    session.caller_id = caller_num
                    session.segment = SEGMENT_TAGS.get(caller_num, [])
                    log.info(f"[ws] stream started streamSid={last_stream_sid} caller={caller_num}")

                elif event_type == "media":
                    if not last_stream_sid:
                        continue
                    chunk = __import__("base64").b64decode(msg.get("media", {}).get("payload", ""))
                    media_buffer.extend(chunk)

                    if len(media_buffer) >= 240:
                        to_send, media_buffer = bytes(media_buffer[:240]), media_buffer[240:]
                        await process_speech(to_send, session, websocket, last_stream_sid)

                elif event_type == "stop":
                    media_buffer.clear()
                    log.info(f"[ws] stream stopped streamSid={last_stream_sid}")

            elif "bytes" in raw:
                pass  # silence frames from Twilio

    except WebSocketDisconnect:
        log.info(f"[ws] disconnected session={session_id}")
    except Exception as exc:
        log.error(f"[ws] error session={session_id} exc={exc}")
    finally:
        session.ended_at = time.time()
        duration = (session.ended_at - session.created_at) if session.ended_at and session.created_at else 0
        session_dur_tag = f"dur={duration:.1f}s"
        log.info(
            f"[session] ended id={session_id} {session_dur_tag} "
            f"transcript_turns={len(session.transcript)} ads={session.ads_played}"
        )


async def deepgram_transcribe(audio_ulaw: bytes) -> str:
    """Transcribe a complete µ-law (8kHz) utterance via Deepgram REST /v1/listen.
    Reliable replacement for the streaming WebSocket which times out (1011)."""
    if not audio_ulaw:
        return ""
    url = (
        "https://api.deepgram.com/v1/listen"
        "?encoding=mulaw&sample_rate=8000&channels=1&model=nova-2&punctuate=true&language=en-US"
    )
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
        "Content-Type": "audio/mulaw",
    }
    try:
        r = await asyncio.to_thread(
            http.post, url, headers=headers, content=audio_ulaw
        )
    except Exception as exc:
        log.warning(f"[ASR] deepgram request failed: {exc}")
        return ""
    if r.status_code != 200:
        log.warning(f"[ASR] deepgram HTTP {r.status_code}: {r.text[:200]!r}")
        return ""
    try:
        data = r.json()
        alt = (
            data.get("results", {})
            .get("channels", [{}])[0]
            .get("alternatives", [{}])[0]
        )
        return (alt.get("transcript") or "").strip()
    except Exception as exc:
        log.warning(f"[ASR] deepgram parse failed: {exc}")
        return ""


async def process_speech(chunk: bytes, session: Session, ws: WebSocket, stream_sid: str, send_fn=None):
    """Buffer caller audio; on end-of-utterance (silence gap) transcribe via
    Deepgram REST, then run LLM + TTS reply. Replaces the streaming WS path."""
    now = time.time()
    # Measure loudness of this µ-law frame (convert to linear PCM first).
    try:
        pcm = audioop.ulaw2lin(chunk, 2)
        rms = audioop.rms(pcm, 2)
    except Exception:
        rms = 0

    SILENCE_RMS = 250          # below this = "silence"
    MIN_UTTERANCE_BYTES = 4000  # ~0.5s of 8kHz µ-law before we bother transcribing
    SILENCE_GAP = 0.7          # seconds of silence that ends an utterance

    session.audio_buffer.extend(chunk)

    if rms >= SILENCE_RMS:
        session.last_voice_at = now
        session.last_heard_at = now
        session.in_speech = True
        return

    # We're in a silent frame. If we were speaking and enough silence has passed, flush.
    if not session.in_speech:
        # cap buffer growth during long pre-speech silence
        if len(session.audio_buffer) > 16000:
            session.audio_buffer = bytearray(session.audio_buffer[-8000:])
        return

    if (now - session.last_voice_at) < SILENCE_GAP:
        return
    if len(session.audio_buffer) < MIN_UTTERANCE_BYTES:
        session.in_speech = False
        session.audio_buffer = bytearray()
        return

    # End of utterance — grab and reset the buffer.
    utterance = bytes(session.audio_buffer)
    session.audio_buffer = bytearray()
    session.in_speech = False

    # Don't start a second pipeline while one is mid-flight (avoids overlap).
    if session.is_processing:
        return

    session.is_processing = True
    asyncio.create_task(_handle_utterance(utterance, session, ws, stream_sid, send_fn))


async def _handle_utterance(utterance: bytes, session: Session, ws: WebSocket, stream_sid: str, send_fn=None):
    """Transcribe a complete utterance, then run LLM + TTS. Runs as a background
    task so the WebSocket receive loop keeps reading inbound audio."""
    try:
        transcript = await deepgram_transcribe(utterance)
        if not transcript:
            return

        log.info(f"[ASR] transcript={transcript}")

        # Contextual news event injection (NewsAPI)
        event = await get_contextual_event(session)
        if event:
            if send_fn:
                await send_fn(event)
            else:
                await send_tts(ws, stream_sid, event)
            session.transcript.append({"role": "system", "text": event, "ts": time.time()})

        # LLM reply (returns spoken reply + optional ad line, played separately)
        reply, ad_line, did_search = await call_llm(session, transcript)

        # Send audio back to the caller — reply in the main voice, ad in the ad voice.
        if send_fn:
            # Post-web-search ad: play BEFORE the answer so the caller must hear it.
            if did_search:
                ps_ad = play_placement_ad_lines(session, "post_search")
                if ps_ad:
                    await send_fn(ps_ad, (CARTESIA_AD_VOICE or None), prefix_ulaw=(AD_CUE_ULAW or None), suffix_ulaw=AD_OUTRO_ULAW.get(session.metadata.get("pending_ad_id")))
            if reply:
                await send_fn(reply)

            if ad_line:
                # Pre-ad cue so the caller knows a sponsor message is coming

                # The actual ad script (separate voice)
                await send_fn(ad_line, (CARTESIA_AD_VOICE or None), prefix_ulaw=(AD_CUE_ULAW or None), suffix_ulaw=AD_OUTRO_ULAW.get(session.metadata.get("pending_ad_id")))

                # Post-ad bridge: re-engage with the original question to keep
                # momentum, especially useful for search / factual conversations.
                bridge = build_post_ad_bridge(transcript)
                if bridge:
                    await send_fn(bridge)  # back to main voice

        else:
            # Legacy / Twilio path (no separate voice support here)
            if reply:
                await send_tts(ws, stream_sid, reply)
            if ad_line:
                await send_tts(ws, stream_sid, ad_line)
                bridge = build_post_ad_bridge(transcript)
                if bridge:
                    await send_tts(ws, stream_sid, bridge)

        if transcript.lower().strip() in {"goodbye", "bye", "stop", "end call", "hang up"}:
            if not send_fn:
                await ws.send_json({"type": "hangup"})
    except Exception as exc:
        log.warning(f"[ASR] utterance handling failed: {exc}")
    finally:
        session.is_processing = False


async def send_tts(ws: WebSocket, stream_sid: str, text: str):
    await ws.send_json({
        "type": "info",
        "streamSid": stream_sid,
        "text": text,
    })
    await tts_stream(text, ws)


# ─── ADMIN API ─────────────────────────────────────────────────────────────────

class AdVariant(BaseModel):
    keywords: List[str] = []
    script: str = ""
    start_enabled: bool = True
    other_enabled: bool = True


class AdPayload(BaseModel):
    sponsor: str
    industry: str
    keywords: List[str]
    script: str
    bid_cpm: float
    daily_cap: int = 100
    weight: float = 1.0
    variants: List[AdVariant] = []
    placement: str = "none"


class AdEdit(BaseModel):
    """All fields optional — only provided fields are updated."""
    sponsor: Optional[str] = None
    industry: Optional[str] = None
    keywords: Optional[List[str]] = None
    script: Optional[str] = None
    bid_cpm: Optional[float] = None
    daily_cap: Optional[int] = None
    weight: Optional[float] = None
    active: Optional[bool] = None
    variants: Optional[List[AdVariant]] = None
    placement: Optional[str] = None


@app.post("/admin/ads")
async def create_ad(payload: AdPayload, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    ad_id = f"ad_{uuid.uuid4().hex[:6]}"
    new_ad = {
        "id": ad_id,
        **payload.model_dump(),
        "cta": "",
        "active": True,
    }
    AD_DB.append(new_ad)
    db_save_ad(new_ad)
    return {"ok": True, "ad": new_ad}


@app.put("/admin/ads/{ad_id}")
@app.patch("/admin/ads/{ad_id}")
async def edit_ad(ad_id: str, payload: AdEdit, x_admin_token: Optional[str] = Header(default=None)):
    """Edit any field of an existing ad, including keyword-specific variants."""
    check_admin(x_admin_token)
    ad = next((a for a in AD_DB if a["id"] == ad_id), None)
    if not ad:
        raise HTTPException(404, "Ad not found")
    updates = payload.model_dump(exclude_none=True)
    ad.update(updates)
    db_save_ad(ad)
    return {"ok": True, "ad": ad}


@app.post("/admin/ads/{ad_id}/toggle")
async def toggle_ad(ad_id: str, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    ad = next((a for a in AD_DB if a["id"] == ad_id), None)
    if not ad:
        raise HTTPException(404, "Ad not found")
    ad["active"] = not ad["active"]
    db_save_ad(ad)
    return {"ok": True, "ad": ad}


@app.delete("/admin/ads/{ad_id}")
async def delete_ad(ad_id: str, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    global AD_DB
    AD_DB = [a for a in AD_DB if a["id"] != ad_id]
    play_counts.pop(ad_id, None)
    db_delete_ad(ad_id)
    return {"ok": True}


@app.get("/admin/stats")
async def admin_stats(x_admin_token: Optional[str] = Header(default=None)):
    """Per-ad analytics: how many times each ad was heard + revenue + recent plays."""
    check_admin(x_admin_token)
    per_ad = []
    for ad in AD_DB:
        plays = play_counts.get(ad["id"], 0)
        per_ad.append({
            "id": ad["id"],
            "sponsor": ad["sponsor"],
            "industry": ad["industry"],
            "active": ad.get("active", True),
            "bid_cpm": ad["bid_cpm"],
            "daily_cap": ad.get("daily_cap", 100),
            "weight": ad.get("weight", 1.0),
            "keywords": ad.get("keywords", []),
            "script": ad.get("script", ""),
            "variants": ad.get("variants", []),
            "placement": ad.get("placement", "none"),
            "outro_set": ad["id"] in AD_OUTRO_ULAW,
            "plays": plays,
            "revenue_usd": round(plays * ad["bid_cpm"] / 1000.0, 4),
        })
    per_ad.sort(key=lambda x: x["plays"], reverse=True)
    return {
        "totals": {
            "total_plays": sum(play_counts.values()),
            "total_revenue_usd": round(total_revenue, 4),
            "active_ads": sum(1 for a in AD_DB if a.get("active", True)),
            "total_ads": len(AD_DB),
            "sessions_active": len(sessions),
            "impressions_logged": len(impression_log),
            "calls": call_counts(),
        },
        "ads": per_ad,
        "recent_impressions": list(reversed(impression_log[-25:])),
    }

@app.get("/admin/settings")
async def get_settings(x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    return {
        "frequency": dict(FREQUENCY),
        "source": "db+env"
    }

@app.put("/admin/settings")
async def update_settings(payload: dict, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    allowed = {"min_interval_seconds", "max_ads", "window_seconds", "force_min_ads_after_seconds", "min_ads_target"}
    updated = {}
    for k, v in payload.items():
        if k in allowed:
            try:
                val = int(v)
                db_save_setting(k, val)
                updated[k] = val
            except Exception as e:
                log.warning(f"[settings] bad value for {k}: {v} ({e})")
    # Reload from DB into the live FREQUENCY dict so ad logic sees the change immediately
    try:
        saved = db_load_settings()
        for k, v in saved.items():
            if k in FREQUENCY:
                FREQUENCY[k] = int(v)
    except Exception as e:
        log.warning(f"[settings] reload failed: {e}")
    log.info(f"[settings] updated: {updated}  current FREQUENCY={FREQUENCY}")
    return {"updated": updated, "current": dict(FREQUENCY)}



@app.post("/admin/ad-cue")
async def upload_ad_cue(payload: dict, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    b64 = (payload.get("mp3_b64") or payload.get("b64") or "").strip()
    if "," in b64 and b64[:5].lower() == "data:":
        b64 = b64.split(",", 1)[1]
    b64 = re.sub(r"\s+", "", b64).replace("-", "+").replace("_", "/")
    if len(b64) % 4:
        b64 += "=" * (4 - len(b64) % 4)
    try:
        raw = _b64.b64decode(b64)
    except Exception as e:
        raise HTTPException(400, f"invalid base64 audio: {e}")
    if not raw:
        raise HTTPException(400, "empty audio")
    try:
        ulaw = _decode_audio_to_ulaw(raw)
    except Exception as e:
        raise HTTPException(400, f"could not decode audio: {e}")
    save_ad_cue(ulaw)
    log.info(f"[cue] uploaded ad cue ({len(ulaw)} bytes)")
    return {"ok": True, "bytes": len(ulaw), "seconds": round(len(ulaw) / 8000.0, 2)}


@app.get("/admin/ad-cue")
async def get_ad_cue(x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    n = len(AD_CUE_ULAW) if AD_CUE_ULAW else 0
    return {"set": n > 0, "bytes": n, "seconds": round(n / 8000.0, 2)}


@app.delete("/admin/ad-cue")
async def remove_ad_cue(x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    clear_ad_cue()
    return {"ok": True}


@app.post("/admin/ads/{ad_id}/outro")
async def upload_ad_outro(ad_id: str, payload: dict, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    b64 = (payload.get("mp3_b64") or payload.get("b64") or "").strip()
    if "," in b64 and b64[:5].lower() == "data:":
        b64 = b64.split(",", 1)[1]
    b64 = re.sub(r"\s+", "", b64).replace("-", "+").replace("_", "/")
    if len(b64) % 4:
        b64 += "=" * (4 - len(b64) % 4)
    try:
        raw = _b64.b64decode(b64)
    except Exception as e:
        raise HTTPException(400, f"invalid base64 audio: {e}")
    try:
        ulaw = _decode_audio_to_ulaw(raw)
    except Exception as e:
        raise HTTPException(400, f"could not decode audio: {e}")
    os.makedirs(AD_OUTRO_DIR, exist_ok=True)
    with open(_ad_outro_path(ad_id), "wb") as f:
        f.write(ulaw)
    AD_OUTRO_ULAW[ad_id] = ulaw
    return {"ok": True, "seconds": round(len(ulaw) / 8000.0, 2)}


@app.delete("/admin/ads/{ad_id}/outro")
async def delete_ad_outro(ad_id: str, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    AD_OUTRO_ULAW.pop(ad_id, None)
    try:
        os.remove(_ad_outro_path(ad_id))
    except FileNotFoundError:
        pass
    return {"ok": True}


@app.get("/admin/billing")
async def admin_billing(x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    companies: Dict[str, Dict[str, Any]] = {}
    cpm_by_sponsor: Dict[str, float] = {}
    for ad in AD_DB:
        sp = (ad.get("sponsor") or "").strip() or "(unknown)"
        companies.setdefault(sp, {})
        cpm_by_sponsor.setdefault(sp, float(ad.get("bid_cpm") or 0.0))
    try:
        with _db_lock:
            c = db_conn()
            rows = c.execute("SELECT sponsor, ts FROM impressions").fetchall()
        for r in rows:
            sp = (r["sponsor"] or "").strip() or "(unknown)"
            try:
                ym = time.strftime("%Y-%m", time.gmtime(float(r["ts"] or 0)))
            except Exception:
                ym = "unknown"
            m = companies.setdefault(sp, {})
            m[ym] = m.get(ym, 0) + 1
    except Exception as e:
        log.warning(f"[billing] aggregate failed: {e}")
    out = []
    for sp, months in companies.items():
        out.append({
            "sponsor": sp,
            "cpm": cpm_by_sponsor.get(sp, 0.0),
            "months": months,
            "total_plays": sum(months.values()),
        })
    out.sort(key=lambda x: x["sponsor"].lower())
    return {"companies": out}


@app.post("/admin/company-cpm")
async def set_company_cpm(payload: dict, x_admin_token: Optional[str] = Header(default=None)):
    check_admin(x_admin_token)
    sponsor = (payload.get("sponsor") or "").strip() or "(unknown)"
    try:
        cpm = float(payload.get("cpm"))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid cpm")
    if cpm < 0:
        raise HTTPException(status_code=400, detail="cpm must be >= 0")
    updated = 0
    for ad in AD_DB:
        sp = (ad.get("sponsor") or "").strip() or "(unknown)"
        if sp == sponsor:
            ad["bid_cpm"] = cpm
            db_save_ad(ad)
            updated += 1
    return {"ok": True, "sponsor": sponsor, "cpm": cpm, "updated": updated}


@app.get("/icon-192.png")
async def icon_192():
    return Response(content=ICON_192, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/icon-512.png")
async def icon_512():
    return Response(content=ICON_512, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/icon.svg")
async def icon_svg():
    return Response(content=ICON_SVG, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/sw.js")
async def service_worker():
    return Response(content=SW_JS, media_type="application/javascript", headers={"Cache-Control": "no-cache"})


@app.get("/admin", response_class=HTMLResponse)
@app.get("/admin/", response_class=HTMLResponse)
async def admin_dashboard():
    """Single-page dashboard. Prompts for the admin token, then renders live
    stats and an ad editor. The token is sent as the X-Admin-Token header on
    every API call (never embedded in the served HTML)."""
    return HTMLResponse(ADMIN_HTML)



# ─── PWA SUPPORT (installable as app) ──────────────────────────────────────────
@app.get("/manifest.json")
async def app_manifest():
    return JSONResponse({
        "name": "Ad Console",
        "short_name": "Ad Console",
        "description": "Manage ads, placement, frequency and intro/outro sounds",
        "start_url": "/admin",
        "display": "standalone",
        "background_color": "#10141a",
        "theme_color": "#10141a",
        "icons": [
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "maskable"},
            {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }, media_type="application/manifest+json")


@app.get("/manifest.json")
async def pwa_manifest():
    return {
        "name": "AI Voice Agent Admin",
        "short_name": "VoiceAdmin",
        "start_url": "/admin",
        "display": "standalone",
        "background_color": "#0f172a",
        "theme_color": "#0ea5e9",
        "icons": [{
            "src": "data:image/svg+xml;base64,data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTkyIiBoZWlnaHQ9IjE5MiIgdmlld0JveD0iMCAwIDE5MiAxOTIiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PHJlY3Qgd2lkdGg9IjE5MiIgaGVpZ2h0PSIxOTIiIHJ4PSI0MCIgZmlsbD0iIzBlYTUxMyIvPjxjaXJjbGUgY3g9IjY0IiBjeT0iOTYiIHI9IjMyIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjE2IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48cGF0aCBkPSJNODAgNjRjMCAwIDMyLTI0IDQ4LTI0czMyIDI0IDMyIDI0IiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjE2IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48cGF0aCBkPSJNMTI4IDk2YzAgMTcuNjgtMTQuMzIgMzItMzIgMzJzLTMyLTE0LjMyLTMyLTMyIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjE2IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48L3N2Zz4=",
            "sizes": "192x192",
            "type": "image/svg+xml"
        }]
    }


@app.get("/manifest.json")
async def pwa_manifest():
    return {
        "name": "AI Voice Agent Admin",
        "short_name": "VoiceAdmin",
        "start_url": "/admin",
        "display": "standalone",
        "background_color": "#0f172a",
        "theme_color": "#0ea5e9",
        "icons": [{
            "src": "data:image/svg+xml;base64,data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTkyIiBoZWlnaHQ9IjE5MiIgdmlld0JveD0iMCAwIDE5MiAxOTIiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PHJlY3Qgd2lkdGg9IjE5MiIgaGVpZ2h0PSIxOTIiIHJ4PSI0MCIgZmlsbD0iIzBlYTUxMyIvPjxjaXJjbGUgY3g9IjY0IiBjeT0iOTYiIHI9IjMyIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjE2IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48cGF0aCBkPSJNODAgNjRjMCAwIDMyLTI0IDQ4LTI0czMyIDI0IDMyIDI0IiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjE2IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48cGF0aCBkPSJNMTI4IDk2YzAgMTcuNjgtMTQuMzIgMzItMzIgMzJzLTMyLTE0LjMyLTMyLTMyIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjE2IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48L3N2Zz4=",
            "sizes": "192x192",
            "type": "image/svg+xml"
        }]
    }


_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _read_text(name, fallback=""):
    try:
        with open(os.path.join(_BASE_DIR, name), "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return fallback


def _read_bytes(name):
    try:
        with open(os.path.join(_BASE_DIR, name), "rb") as f:
            return f.read()
    except Exception:
        return b""


ADMIN_HTML = _read_text("admin_dashboard.html", "<!doctype html><meta charset=utf-8><body style='font-family:sans-serif;padding:40px;background:#10141a;color:#fff'><h2>Ad Console</h2><p>Dashboard file missing.</p></body>")
ICON_192 = _read_bytes("icon-192.png")
ICON_512 = _read_bytes("icon-512.png")
ICON_SVG = "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><rect width='64' height='64' rx='14' fill='#10141a'/><rect x='8' y='27' width='5' height='10' rx='2.5' fill='#34d7f0'/><rect x='18' y='20' width='5' height='24' rx='2.5' fill='#34d7f0'/><rect x='28' y='12' width='5' height='40' rx='2.5' fill='#2dd4bf'/><rect x='38' y='20' width='5' height='24' rx='2.5' fill='#2dd4bf'/><rect x='48' y='27' width='5' height='10' rx='2.5' fill='#2dd4bf'/></svg>"
SW_JS = "self.addEventListener('install', function(e){ self.skipWaiting(); }); self.addEventListener('activate', function(e){ self.clients.claim(); }); self.addEventListener('fetch', function(e){});"

@app.on_event("startup")
async def _on_startup():
    try:
        db_init()
        load_ad_cue()
        load_ad_outros()
    except Exception as exc:
        log.error(f"[db] init failed: {exc} — running with in-memory state only")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
