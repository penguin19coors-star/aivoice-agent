"""
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
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from collections import defaultdict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from pydantic import BaseModel

import httpx as _http
import websockets

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

    async def ensure_dg_ws(self) -> Any:
        if self.dg_ws is None or getattr(self.dg_ws, "closed", False):
            url = (
                "wss://api.deepgram.com/v1/listen?"
                "encoding=mulaw&sample_rate=8000&channels=1&language=en-US&punctuate=true&interim_results=true"
            )
            headers = {
                "Authorization": f"Token {DEEPGRAM_API_KEY}",
                "Content-Type": "application/json",
            }
            self.dg_ws = await websockets.connect(url, additional_headers=headers, close_timeout=5)
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
    log.info(
        f"[ad] play #{play_counts[ad_id]} sponsor={record['sponsor']} "
        f"revenue +${rev:.4f} day=${total_revenue:.2f}"
    )
    fire_billing_webhook(record)


def maybe_inject_ad(session: Session) -> Optional[str]:
    ad = select_ad(session)
    if ad:
        session.ads_played.append(ad["id"])
        session.last_ad_at = time.time()
        record_play(ad["id"], session.session_id, session.caller_id)
        return ad["script"]
    return None


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

SYSTEM_PROMPT = """You are a friendly, natural-sounding phone assistant.
Rules:
- Keep answers short, spoken-friendly: 1-3 sentences max unless asked for more detail.
- Use a conversational tone — not a chatbot.
- Never start with chatbots phrases like 'as an ai language model'.
- Occasionally reference ads placed by the system as natural advice.
- When unsure, say 'I don't know, but let me help you look into it.'
- Output only what you'd say aloud. No markdown, no bullet lists, no parentheses.
- If the user asks to be transferred, say you can't connect calls but can stay on the line.
- The system occasionally injects short news snippets; acknowledge briefly if relevant or ignore safely.
- Target: ~120 words per response.
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
CARTESIA_API_KEY = os.environ.get("CARTESIA_API_KEY", "")
CARTESIA_VOICE = os.environ.get("CARTESIA_VOICE", "a79a1ab6-270d-4b3e-b14e-35e35dc18dbb")
CARTESIA_MODEL = os.environ.get("CARTESIA_MODEL", "sonic-english")
ELEVENLABS_API_KEY = os.environ.get("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE = os.environ.get("ELEVENLABS_VOICE", "Rachel")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_TTS_VOICE = os.environ.get("OPENAI_TTS_VOICE", "nova")

http = httpx.Client(timeout=30)


async def call_llm(session: Session, user_text: str) -> str:
    session.transcript.append({"role": "user", "text": user_text, "ts": time.time()})
    session.topic_extract = classify_industry(session.transcript)

    messages = build_messages(session)
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
    if ad_line:
        reply += f" <break time='400ms'/> {ad_line}"

    session.transcript.append({"role": "assistant", "text": reply, "ts": time.time()})
    return reply


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

    else:  # cartesia default
        url = "wss://api.cartesia.ai/tts/websocket"
        headers = {"Cartesia-Version": "2024-06-10"}
        payload = {
            "model_id": CARTESIA_MODEL,
            "transcript": text,
            "voice": {"mode": "id", "id": CARTESIA_VOICE},
            "output_format": {
                "encoding": "mulaw",
                "sample_rate": 8000,
            },
            "language": "en",
        }
        async with websockets.connect(
            url, additional_headers=headers, close_timeout=5
        ) as ws:
            await ws.send(json.dumps(payload))
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=8)
                except asyncio.TimeoutError:
                    break
                if isinstance(msg, bytes):
                    await websocket.send_bytes(msg)
                else:
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue
                    if data.get("event") == "done":
                        break
                    if data.get("error"):
                        log.warning(f"[tts:cartesia] {data.get('error')}")
                        break

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
async def list_ads():
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


@app.post("/telnyx/voice")
async def telnyx_voice_webhook():
    """Return Telnyx TeXML that opens a bidirectional µ-law WebSocket stream."""
    ws_url = os.environ.get("PUBLIC_WSS_URL") or os.environ.get("PUBLIC_WS_URL", "wss://your-app.onrender.com/telnyx/ws")
    # Strip trailing /ws if the env has the generic ws path so we can append /telnyx/ws
    if ws_url.endswith("/ws"):
        ws_url = ws_url[:-3]
    if not ws_url.endswith("/telnyx/ws"):
        ws_url = ws_url.rstrip("/") + "/telnyx/ws"

    texml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="{ws_url}" bidirectionalMode="rtp" contentType="audio/x-mulaw;rate=8000" />
  </Connect>
  <Pause length="40"/>
</Response>"""
    return PlainTextResponse(texml, media_type="application/xml")


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

    async def _send_outbound(text: str):
        nonlocal stream_id
        if not stream_id:
            return
        # Produce µ-law and send raw bytes back to Telnyx.
        outbound_buffer = bytearray()
        async for chunk in _cartesia_ulaw_stream(text):
            outbound_buffer.extend(chunk)
        if outbound_buffer:
            await websocket.send_bytes(bytes(outbound_buffer))

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
                    chunk = __import__("base64").b64decode(payload)
                    media_buffer.extend(chunk)

                    if len(media_buffer) >= 240:
                        to_send, media_buffer = bytes(media_buffer[:240]), media_buffer[240:]
                        await process_speech(to_send, session, websocket, stream_id or "")

            elif "bytes" in raw:
                # In bidirectional RTP mode, inbound audio arrives as raw bytes.
                if awaiting_start or not stream_id:
                    continue
                media_buffer.extend(raw["bytes"])
                if len(media_buffer) >= 240:
                    to_send, media_buffer = bytes(media_buffer[:240]), media_buffer[240:]
                    await process_speech(to_send, session, websocket, stream_id or "")

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


async def _cartesia_ulaw_stream(text: str) -> Any:
    """Yield µ-law bytes from Cartesia for outbound audio."""
    cleaned = re.sub(r"\*\*.*?\*\*", lambda m: m.group(0).replace("*", ""), text)
    cleaned = re.sub(r"[*#`_\[\]()]", "", cleaned).replace("\n", " ")
    url = "wss://api.cartesia.ai/tts/websocket"
    headers = {"Cartesia-Version": "2024-06-10"}
    payload = {
        "model_id": CARTESIA_MODEL,
        "transcript": cleaned,
        "voice": {"mode": "id", "id": CARTESIA_VOICE},
        "output_format": {"encoding": "mulaw", "sample_rate": 8000},
        "language": "en",
    }
    async with websockets.connect(url, additional_headers=headers, close_timeout=5) as ws:
        await ws.send(json.dumps(payload))
        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=8)
            except asyncio.TimeoutError:
                break
            if isinstance(msg, bytes):
                yield msg
            else:
                try:
                    data = json.loads(msg)
                except Exception:
                    continue
                if data.get("event") == "done":
                    break
                if data.get("error"):
                    log.warning(f"[tts:cartesia] {data.get('error')}")
                    break


# Keep Twilio/WebSocket handler and remaining server code.@app.websocket("/ws")
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


async def process_speech(chunk: bytes, session: Session, ws: WebSocket, stream_sid: str):
    """Send chunk to a persistent Deepgram Live socket and await the final transcript."""
    try:
        dg = await session.ensure_dg_ws()
        await dg.send(chunk)
    except Exception as exc:
        log.warning(f"[ASR] send error: {exc}; socket may be stale")
        try:
            session.dg_ws = None
            dg = await session.ensure_dg_ws()
            await dg.send(chunk)
        except Exception as exc2:
            log.warning(f"[ASR] reconnect failed: {exc2}")
            return

    try:
        raw_msg = await asyncio.wait_for(session.dg_ws.recv(), timeout=4)
    except asyncio.TimeoutError:
        return

    try:
        data = json.loads(raw_msg)
    except Exception:
        return

    if not data.get("is_final"):
        return

    alt = data.get("channel", {}).get("alternatives", [{}])[0]
    transcript = (alt.get("transcript") or "").strip()
    if not transcript:
        return
    if alt.get("no_speech") or transcript.startswith(" "):
        return

    log.info(f"[ASR] transcript={transcript}")

    # Contextual news event injection (NewsAPI)
    event = await get_contextual_event(session)
    if event:
        await send_tts(ws, stream_sid, event)
        session.transcript.append({"role": "system", "text": event, "ts": time.time()})

    # LLM reply
    reply = await call_llm(session, transcript)

    # Send audio + metadata hint to Twilio
    await send_tts(ws, stream_sid, reply)

    if transcript.lower().strip() in {"goodbye", "bye", "stop", "end call", "hang up"}:
        await ws.send_json({"type": "hangup"})


async def send_tts(ws: WebSocket, stream_sid: str, text: str):
    await ws.send_json({
        "type": "info",
        "streamSid": stream_sid,
        "text": text,
    })
    await tts_stream(text, ws)


# ─── ADMIN API ─────────────────────────────────────────────────────────────────

class AdPayload(BaseModel):
    sponsor: str
    industry: str
    keywords: List[str]
    script: str
    bid_cpm: float
    daily_cap: int = 100
    weight: float = 1.0


@app.post("/admin/ads")
async def create_ad(payload: AdPayload):
    ad_id = f"ad_{uuid.uuid4().hex[:6]}"
    new_ad = {
        "id": ad_id,
        **payload.model_dump(),
        "active": True,
    }
    AD_DB.append(new_ad)
    return {"ok": True, "ad": new_ad}


@app.post("/admin/ads/{ad_id}/toggle")
async def toggle_ad(ad_id: str):
    ad = next((a for a in AD_DB if a["id"] == ad_id), None)
    if not ad:
        raise HTTPException(404, "Ad not found")
    ad["active"] = not ad["active"]
    return {"ok": True, "ad": ad}


@app.delete("/admin/ads/{ad_id}")
async def delete_ad(ad_id: str):
    global AD_DB
    AD_DB = [a for a in AD_DB if a["id"] != ad_id]
    play_counts.pop(ad_id, None)
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
