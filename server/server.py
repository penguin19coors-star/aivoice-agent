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
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from collections import defaultdict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Header
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
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
AD_DB: List[Dict[str, Any]] = [
    {
        "id": "ad_001",
        "sponsor": "TechLaunch SaaS",
        "industry": "technology",
        "keywords": ["software", "app", "cloud", "ai", "automation", "startup"],
        "script": "By the way, TechLaunch just released an AI automation tool that lets small teams ship apps 10 times faster. Check them out at techlaunch dot A I.",
        "cta": "Visit techlaunch.ai",
        "bid_cpm": 12.50,
        "daily_cap": 500,
        "weight": 1.0,
        "active": True,
    },
    {
        "id": "ad_002",
        "sponsor": "GreenPower Energy",
        "industry": "home_services",
        "keywords": ["electric", "power", "solar", "home", "energy", "bill", "utility"],
        "script": "Quick tip from GreenPower Energy: switching to solar can cut your electric bill by 60 percent. Ask about their free home assessment.",
        "cta": "Call GreenPower for a free quote",
        "bid_cpm": 9.75,
        "daily_cap": 300,
        "weight": 1.2,
        "active": True,
    },
    {
        "id": "ad_003",
        "sponsor": "FitTrack Rings",
        "industry": "health",
        "keywords": ["health", "fitness", "tracker", "wearable", "exercise", "sleep", "steps", "healthy"],
        "script": "If you're tracking goals, FitTrack's new smart ring monitors sleep, steps, and recovery all day. Now available at fittrack dot com.",
        "cta": "Shop FitTrack.com",
        "bid_cpm": 8.50,
        "daily_cap": 200,
        "weight": 0.9,
        "active": True,
    },
    {
        "id": "ad_004",
        "sponsor": "CloudVPS",
        "industry": "technology",
        "keywords": ["server", "cloud", "hosting", "vps", "deploy", "infrastructure", "api", "dev"],
        "script": "For developers, CloudVPS has bare-metal instances starting at five dollars a month with 99.99 percent uptime. Promo code AI Agent.",
        "cta": "CloudVPS .dev",
        "bid_cpm": 7.00,
        "daily_cap": 100,
        "weight": 1.5,
        "active": True,
    },
    {
        "id": "ad_005",
        "sponsor": "CheapFlights",
        "industry": "travel",
        "keywords": ["travel", "flight", "trip", "vacation", "hotel", "book", "airport", "destination"],
        "script": "Planning a trip? CheapFlights compares 500 airlines to find you under market fares, with price drop alerts for free.",
        "cta": "Download CheapFlights app",
        "bid_cpm": 6.25,
        "daily_cap": 150,
        "weight": 1.0,
        "active": True,
    },
    {
        "id": "ad_006",
        "sponsor": "LegalEase",
        "industry": "legal",
        "keywords": ["law", "legal", "lawyer", "contract", "rights", "dispute", "court", "sue"],
        "script": "Have a small legal question? LegalEase connects you with licensed attorneys in minutes for a flat 29 dollar consult. No firm required.",
        "cta": "LegalEase dot co",
        "bid_cpm": 14.00,
        "daily_cap": 80,
        "weight": 2.0,
        "active": True,
    },
]

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
_db_lock = threading.Lock()
_db: Optional[sqlite3.Connection] = None


def db_conn() -> sqlite3.Connection:
    global _db
    if _db is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _db = sqlite3.connect(DB_PATH, check_same_thread=False)
        _db.row_factory = sqlite3.Row
        _db.execute("PRAGMA journal_mode=WAL;")
    return _db


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
            """
        )
        c.commit()

        # Migration: add variants column to pre-existing DBs that lack it.
        cols = {row["name"] for row in c.execute("PRAGMA table_info(ads)").fetchall()}
        if "variants" not in cols:
            c.execute("ALTER TABLE ads ADD COLUMN variants TEXT")
            c.commit()
            log.info("[db] migrated: added ads.variants column")

        # NOTE: we no longer auto-seed from the hardcoded AD_DB default list.
        # The SQLite DB is the single source of truth for ads.
        # If the table is empty, no ads will play — the admin must add them via /admin.
        # (Previous behavior re-seeded default ads after every disk wipe,
        # which made it look like "ghost ads" were playing without admin control.)
        n = c.execute("SELECT COUNT(*) AS n FROM ads").fetchone()["n"]
        if n == 0:
            log.info("[db] ads table is empty — no ads loaded until added via /admin")
        else:
            # Load ads from DB as the source of truth.
            rows = c.execute("SELECT * FROM ads").fetchall()
            AD_DB.clear()
            for r in rows:
                AD_DB.append({
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


def _db_upsert_ad(c: sqlite3.Connection, ad: Dict[str, Any]):
    c.execute(
        """INSERT INTO ads (id,sponsor,industry,keywords,script,cta,bid_cpm,daily_cap,weight,active,variants)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET sponsor=excluded.sponsor,industry=excluded.industry,
             keywords=excluded.keywords,script=excluded.script,cta=excluded.cta,
             bid_cpm=excluded.bid_cpm,daily_cap=excluded.daily_cap,weight=excluded.weight,
             active=excluded.active,variants=excluded.variants""",
        (
            ad["id"], ad.get("sponsor"), ad.get("industry"),
            json.dumps(ad.get("keywords", [])), ad.get("script"), ad.get("cta"),
            ad.get("bid_cpm", 0.0), ad.get("daily_cap", 100),
            ad.get("weight", 1.0), 1 if ad.get("active", True) else 0,
            json.dumps(ad.get("variants", [])),
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
    now = time.time()
    if now - session.last_ad_at < 90:
        return None
    already_played = set(session.ads_played)
    context = session.topic_extract or classify_industry(session.transcript)

    candidates = []
    for ad in AD_DB:
        if not ad["active"]:
            continue
        if ad["id"] in already_played:
            continue
        if play_counts[ad["id"]] >= ad["daily_cap"]:
            continue

        texts = " ".join(m.get("text", "") for m in session.transcript[-6:]).lower()
        keyword_hits = sum(1 for kw in ad["keywords"] if kw.lower() in texts)
        relevance = 0.2 + 0.6 * min(1.0, keyword_hits / max(1, len(ad["keywords"]) * 0.4))
        if ad["industry"] in context:
            relevance += 0.3 * context[ad["industry"]]
        score = ad["bid_cpm"] * ad["weight"] * max(0.3, relevance)
        candidates.append({"ad": ad, "score": score, "relevance": relevance})

    if not candidates:
        return None
    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]
    if best["relevance"] < 0.35:
        return None

    log.info(
        f"[ad] selected {best['ad']['id']} sponsor={best['ad']['sponsor']} "
        f"score={best['score']:.2f} relevance={best['relevance']:.2f}"
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


def maybe_inject_ad(session: Session) -> Optional[str]:
    ad = select_ad(session)
    if ad:
        session.ads_played.append(ad["id"])
        session.last_ad_at = time.time()
        record_play(ad["id"], session.session_id, session.caller_id)
        return pick_ad_script(ad, session)
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
                json={"client_state": "silence_timeout"},
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

    # Live web lookup for factual questions (phone, address, hours, weather…).
    facts = ""
    if needs_web_lookup(user_text, session.transcript):
        facts = await web_lookup(user_text, session.transcript)
        if facts:
            log.info(f"[serper] facts for '{user_text[:40]}': {facts[:120]!r}")
            messages.append({
                "role": "system",
                "content": f"""LIVE SEARCH RESULTS (from Google via Serper — use these exactly):
{facts}

HOW TO USE THESE RESULTS:
- Base your answer ONLY on the facts above when they are relevant. Read phone numbers, addresses, hours, prices, and scores exactly as shown.
- If the specific detail the caller wants (phone, address, current price, hours, score, etc.) is not in the results, say clearly that you couldn't find the exact/current information and offer to help another way or ask one short clarifying question (e.g. "Is that the one downtown or on Main Street?").
- Never invent or guess numbers, addresses, or business details.
- For non-contact facts (weather, news, general info), you can combine the search results with your general knowledge if it helps, but prioritize the live data.
- Speak the information naturally. Example good response when data is missing: "I'm not finding the current phone number for that in my search — do you have the name of the specific location?"
"""
            })

    # Hard safety: if we decided a web lookup was needed for contact info
    # but got nothing back, force the model to admit it instead of hallucinating.
    if needs_web_lookup(user_text, session.transcript) and not facts:
        messages.append({
            "role": "system",
            "content": """IMPORTANT: A web search was attempted for this query but no useful verified results came back.
This is likely a request for current phone, address, hours, price, score, or business info.
You MUST NOT invent any of those details.
Tell the caller honestly that you couldn't find the exact information right now.
You may ask ONE short, helpful clarifying question to narrow it down (e.g. "Which location are you thinking of?" or "Do you have the city or street name?").
Then offer to help with something else. Good example: "I'm not finding current details for that in my search. Which specific one are you looking for — the downtown spot or another location?"
"""
        })

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
        reply = data["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        log.error(f"LLM error: {exc}")
        reply = "Let me try that again. Could you repeat your question?"

    ad_line = maybe_inject_ad(session)
    full = reply + (f" <break time='400ms'/> {ad_line}" if ad_line else "")
    session.transcript.append({"role": "assistant", "text": full, "ts": time.time()})
    return reply, ad_line


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

    async def _send_outbound(text: str, voice_id: Optional[str] = None):
        nonlocal stream_id
        if not stream_id:
            return
        # Collect µ-law audio from Cartesia, then send it back to Telnyx as
        # JSON "media" events with base64 payload, chunked into 160-byte
        # (~20ms @ 8kHz PCMU) frames. Telnyx expects this JSON format even in
        # bidirectional mode — raw WebSocket bytes are NOT played.
        outbound_buffer = bytearray()
        async for chunk in _cartesia_ulaw_stream(text, voice_id=voice_id):
            outbound_buffer.extend(chunk)
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
                    # Start the silence watchdog (hangs up after N seconds idle).
                    asyncio.create_task(_silence_watchdog())
                    # Greet the caller immediately so the call doesn't open silent.
                    greeting = os.environ.get(
                        "GREETING",
                        "Hi! Thanks for calling. How can I help you today?",
                    )
                    session.transcript.append({"role": "assistant", "text": greeting, "ts": time.time()})
                    asyncio.create_task(_send_outbound(greeting))

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
        reply, ad_line = await call_llm(session, transcript)

        # Send audio back to the caller — reply in the main voice, ad in the ad voice.
        if send_fn:
            if reply:
                await send_fn(reply)

            if ad_line:
                # Pre-ad cue so the caller knows a sponsor message is coming
                cue = AD_BREAK_CUE
                await send_fn(cue, (CARTESIA_AD_VOICE or None))

                # The actual ad script (separate voice)
                await send_fn(ad_line, (CARTESIA_AD_VOICE or None))

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
                cue = AD_BREAK_CUE
                await send_tts(ws, stream_sid, cue)
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


class AdPayload(BaseModel):
    sponsor: str
    industry: str
    keywords: List[str]
    script: str
    bid_cpm: float
    daily_cap: int = 100
    weight: float = 1.0
    variants: List[AdVariant] = []


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
        },
        "ads": per_ad,
        "recent_impressions": list(reversed(impression_log[-25:])),
    }


@app.get("/admin", response_class=HTMLResponse)
@app.get("/admin/", response_class=HTMLResponse)
async def admin_dashboard():
    """Single-page dashboard. Prompts for the admin token, then renders live
    stats and an ad editor. The token is sent as the X-Admin-Token header on
    every API call (never embedded in the served HTML)."""
    return HTMLResponse(ADMIN_HTML)


ADMIN_HTML = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AI Voice Agent — Ad Dashboard</title>
<style>
:root{--bg:#0b0e14;--card:#151a23;--line:#222b3a;--txt:#e6edf3;--mut:#8b98a9;--acc:#4f9cf9;--good:#3fb950;--warn:#d29922;}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--txt)}
header{padding:18px 24px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:16px;flex-wrap:wrap}
h1{font-size:18px;margin:0;font-weight:600}
.wrap{padding:24px;max-width:1100px;margin:0 auto}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin-bottom:24px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px}
.card .k{color:var(--mut);font-size:12px;text-transform:uppercase;letter-spacing:.04em}
.card .v{font-size:26px;font-weight:700;margin-top:6px}
.card .v.good{color:var(--good)}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:12px;overflow:hidden}
th,td{padding:11px 14px;text-align:left;border-bottom:1px solid var(--line);font-size:14px}
th{color:var(--mut);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.04em}
tr:last-child td{border-bottom:none}
.pill{display:inline-block;padding:2px 9px;border-radius:99px;font-size:12px;font-weight:600}
.pill.on{background:rgba(63,185,80,.15);color:var(--good)}
.pill.off{background:rgba(210,153,34,.15);color:var(--warn)}
button{background:var(--acc);color:#fff;border:0;border-radius:8px;padding:8px 14px;font-weight:600;cursor:pointer;font-size:14px}
button.ghost{background:transparent;border:1px solid var(--line);color:var(--txt)}
button.danger{background:transparent;border:1px solid #5a2330;color:#f85149}
button:hover{opacity:.9}
h2{font-size:15px;margin:28px 0 12px;color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
input,textarea{width:100%;background:#0d1117;border:1px solid var(--line);border-radius:8px;color:var(--txt);padding:9px 11px;font:inherit}
label{display:block;font-size:12px;color:var(--mut);margin:10px 0 4px}
.row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.gate{max-width:380px;margin:80px auto;text-align:center}
.muted{color:var(--mut);font-size:13px}
.err{color:#f85149;font-size:13px;margin-top:8px;min-height:18px}
.actions{display:flex;gap:8px;flex-wrap:wrap}
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.6);display:none;align-items:flex-start;justify-content:center;overflow:auto;z-index:50}
.modal-bg.show{display:flex}
.modal{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:22px;max-width:640px;width:92%;margin:48px 0}
.modal h3{margin:0 0 4px;font-size:17px}
.variant{border:1px solid var(--line);border-radius:10px;padding:12px;margin-top:10px;position:relative}
.variant .vrm{position:absolute;top:8px;right:8px}
.small{font-size:12px;padding:5px 10px}
</style></head><body>
<div id="gate" class="gate">
  <h1>🔐 Ad Dashboard</h1>
  <p class="muted">Enter your admin token to continue.</p>
  <input id="tok" type="password" placeholder="ADMIN_TOKEN" autofocus>
  <div style="margin-top:12px"><button onclick="login()">Unlock</button></div>
  <div id="gerr" class="err"></div>
</div>

<div id="app" style="display:none">
<header>
  <h1>📞 AI Voice Agent</h1>
  <span class="muted" id="sub">Ad performance dashboard</span>
  <span style="flex:1"></span>
  <button class="ghost" onclick="load()">↻ Refresh</button>
  <button class="ghost" onclick="logout()">Lock</button>
</header>
<div class="wrap">
  <div class="cards" id="cards"></div>

  <h2>Ads — times heard &amp; revenue</h2>
  <table id="adtbl"><thead><tr>
    <th>Sponsor</th><th>Industry</th><th>Status</th><th>Times Heard</th><th>CPM</th><th>Revenue</th><th>Variants</th><th></th>
  </tr></thead><tbody></tbody></table>

  <h2>Add a new ad</h2>
  <div class="card">
    <div class="row">
      <div><label>Sponsor name</label><input id="f_sponsor" placeholder="Acme Co."></div>
      <div><label>Industry</label><input id="f_industry" placeholder="technology / health / travel / legal / home_services"></div>
    </div>
    <label>Keywords (comma-separated — trigger the ad when caller mentions these)</label>
    <input id="f_keywords" placeholder="software, app, cloud, automation">
    <label>Script (what the agent says aloud — spell URLs like 'acme dot com')</label>
    <textarea id="f_script" rows="2" placeholder="Quick tip from Acme: ..."></textarea>
    <div class="row">
      <div><label>Bid CPM (USD per 1000 plays)</label><input id="f_cpm" type="number" value="10.0" step="0.5"></div>
      <div><label>Daily cap (max plays/day)</label><input id="f_cap" type="number" value="500"></div>
    </div>
    <div style="margin-top:14px"><button onclick="addAd()">+ Create Ad</button> <span id="aerr" class="err"></span></div>
  </div>

  <h2>Recent plays</h2>
  <table id="imptbl"><thead><tr><th>When</th><th>Sponsor</th><th>Caller</th><th>Revenue</th></tr></thead><tbody></tbody></table>
</div>
</div>

<!-- Edit modal -->
<div id="modalbg" class="modal-bg" onclick="if(event.target===this)closeEdit()">
  <div class="modal">
    <h3>Edit ad</h3>
    <div class="muted" id="e_id" style="margin-bottom:8px"></div>
    <div class="row">
      <div><label>Sponsor</label><input id="e_sponsor"></div>
      <div><label>Industry</label><input id="e_industry"></div>
    </div>
    <label>Default keywords (comma-separated)</label>
    <input id="e_keywords">
    <label>Default script</label>
    <textarea id="e_script" rows="2"></textarea>
    <div class="row">
      <div><label>Bid CPM</label><input id="e_cpm" type="number" step="0.5"></div>
      <div><label>Daily cap</label><input id="e_cap" type="number"></div>
    </div>

    <h2 style="margin-top:18px">Keyword variants <span class="muted" style="text-transform:none">— different scripts for different keywords</span></h2>
    <div class="muted" style="margin-bottom:6px">When the caller's words match a variant's keywords, that script plays instead of the default.</div>
    <div id="e_variants"></div>
    <button class="ghost small" style="margin-top:8px" onclick="addVariantRow()">+ Add variant</button>

    <div style="margin-top:18px;display:flex;gap:10px;align-items:center">
      <button onclick="saveEdit()">Save changes</button>
      <button class="ghost" onclick="closeEdit()">Cancel</button>
      <span id="eerr" class="err"></span>
    </div>
  </div>
</div>

<script>
let TOK = sessionStorage.getItem("adm_tok") || "";
let ADS = {};  // id -> ad object, cached from last load
function hdr(){return {"X-Admin-Token":TOK,"Content-Type":"application/json"};}
async function api(path,opts){opts=opts||{};opts.headers=hdr();const r=await fetch(path,opts);if(r.status===401)throw new Error("Unauthorized");if(!r.ok)throw new Error("HTTP "+r.status);return r.json();}
function login(){TOK=document.getElementById("tok").value.trim();sessionStorage.setItem("adm_tok",TOK);load();}
function logout(){TOK="";sessionStorage.removeItem("adm_tok");document.getElementById("app").style.display="none";document.getElementById("gate").style.display="block";}
function money(n){return "$"+Number(n).toFixed(n<1?4:2);}
function esc(s){return (s==null?"":String(s)).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}[c]));}
async function load(){
  try{
    const d=await api("/admin/stats");
    document.getElementById("gate").style.display="none";
    document.getElementById("app").style.display="block";
    document.getElementById("gerr").textContent="";
    const t=d.totals;
    document.getElementById("cards").innerHTML=[
      ["Total plays",t.total_plays],
      ["Revenue (run)","<span class='good'>"+money(t.total_revenue_usd)+"</span>"],
      ["Active ads",t.active_ads+" / "+t.total_ads],
      ["Live calls",t.sessions_active],
      ["Impressions",t.impressions_logged],
    ].map(([k,v])=>`<div class='card'><div class='k'>${k}</div><div class='v'>${v}</div></div>`).join("");
    ADS={};
    d.ads.forEach(a=>ADS[a.id]=a);
    document.querySelector("#adtbl tbody").innerHTML=d.ads.map(a=>`<tr>
      <td>${esc(a.sponsor)}</td><td>${esc(a.industry)}</td>
      <td><span class='pill ${a.active?"on":"off"}'>${a.active?"Active":"Paused"}</span></td>
      <td><b>${a.plays}</b></td><td>${money(a.bid_cpm)}</td><td class='good'>${money(a.revenue_usd)}</td>
      <td>${(a.variants&&a.variants.length)?a.variants.length:"—"}</td>
      <td class='actions'>
        <button class='ghost small' onclick="openEdit('${a.id}')">Edit</button>
        <button class='ghost small' onclick="toggleAd('${a.id}')">${a.active?"Pause":"Resume"}</button>
        <button class='danger small' onclick="delAd('${a.id}')">Delete</button>
      </td></tr>`).join("") || "<tr><td colspan=8 class='muted'>No ads yet.</td></tr>";
    document.querySelector("#imptbl tbody").innerHTML=(d.recent_impressions||[]).map(i=>`<tr>
      <td class='muted'>${new Date(i.ts*1000).toLocaleString()}</td>
      <td>${esc(i.sponsor)}</td><td>${esc(i.caller_id||"—")}</td><td class='good'>${money(i.revenue_usd)}</td>
      </tr>`).join("") || "<tr><td colspan=4 class='muted'>No plays yet.</td></tr>";
  }catch(e){
    if(e.message==="Unauthorized"){document.getElementById("gerr").textContent="Wrong token.";logout();}
    else document.getElementById("gerr").textContent=e.message;
  }
}
async function toggleAd(id){await api("/admin/ads/"+id+"/toggle",{method:"POST"});load();}
async function delAd(id){if(confirm("Delete this ad?")){await api("/admin/ads/"+id,{method:"DELETE"});load();}}
async function addAd(){
  const body={
    sponsor:document.getElementById("f_sponsor").value.trim(),
    industry:document.getElementById("f_industry").value.trim(),
    keywords:document.getElementById("f_keywords").value.split(",").map(s=>s.trim()).filter(Boolean),
    script:document.getElementById("f_script").value.trim(),
    bid_cpm:parseFloat(document.getElementById("f_cpm").value)||0,
    daily_cap:parseInt(document.getElementById("f_cap").value)||500,
  };
  if(!body.sponsor||!body.script){document.getElementById("aerr").textContent="Sponsor and script required.";return;}
  try{await api("/admin/ads",{method:"POST",body:JSON.stringify(body)});
    ["f_sponsor","f_industry","f_keywords","f_script"].forEach(i=>document.getElementById(i).value="");
    document.getElementById("aerr").textContent="";load();
  }catch(e){document.getElementById("aerr").textContent=e.message;}
}

// ---- Edit modal + variants ----
let EDIT_ID=null;
function variantRowHTML(kw,script){
  return `<div class="variant">
    <button class="danger small vrm" onclick="this.parentNode.remove()">✕</button>
    <label>Trigger keywords (comma-separated)</label>
    <input class="v_kw" value="${esc((kw||[]).join(', '))}">
    <label>Script for these keywords</label>
    <textarea class="v_script" rows="2">${esc(script||"")}</textarea>
  </div>`;
}
function addVariantRow(kw,script){
  document.getElementById("e_variants").insertAdjacentHTML("beforeend",variantRowHTML(kw,script));
}
function openEdit(id){
  const a=ADS[id]; if(!a)return;
  EDIT_ID=id;
  document.getElementById("e_id").textContent="ID: "+id;
  document.getElementById("e_sponsor").value=a.sponsor||"";
  document.getElementById("e_industry").value=a.industry||"";
  document.getElementById("e_keywords").value=(a.keywords||[]).join(", ");
  document.getElementById("e_script").value=a.script||"";
  document.getElementById("e_cpm").value=a.bid_cpm||0;
  document.getElementById("e_cap").value=a.daily_cap||500;
  document.getElementById("e_variants").innerHTML="";
  (a.variants||[]).forEach(v=>addVariantRow(v.keywords,v.script));
  document.getElementById("eerr").textContent="";
  document.getElementById("modalbg").classList.add("show");
}
function closeEdit(){document.getElementById("modalbg").classList.remove("show");EDIT_ID=null;}
async function saveEdit(){
  if(!EDIT_ID)return;
  const variants=[...document.querySelectorAll("#e_variants .variant")].map(v=>({
    keywords:v.querySelector(".v_kw").value.split(",").map(s=>s.trim()).filter(Boolean),
    script:v.querySelector(".v_script").value.trim(),
  })).filter(v=>v.script);
  const body={
    sponsor:document.getElementById("e_sponsor").value.trim(),
    industry:document.getElementById("e_industry").value.trim(),
    keywords:document.getElementById("e_keywords").value.split(",").map(s=>s.trim()).filter(Boolean),
    script:document.getElementById("e_script").value.trim(),
    bid_cpm:parseFloat(document.getElementById("e_cpm").value)||0,
    daily_cap:parseInt(document.getElementById("e_cap").value)||500,
    variants:variants,
  };
  try{await api("/admin/ads/"+EDIT_ID,{method:"PUT",body:JSON.stringify(body)});
    closeEdit();load();
  }catch(e){document.getElementById("eerr").textContent=e.message;}
}
if(TOK) load();
</script>
</body></html>"""


@app.on_event("startup")
async def _on_startup():
    try:
        db_init()
    except Exception as exc:
        log.error(f"[db] init failed: {exc} — running with in-memory state only")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
