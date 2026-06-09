import re

with open("server.py") as f:
    content = f.read()

old_block = '''_LOOKUP_TRIGGERS = (
    "phone number", "number for", "address", "where is", "located", "location",
    "hours", "open", "close", "closing", "directions", "zip code", "area code",
    "how much", "price", "cost", "weather", "near me", "nearest", "closest",
    "what time", "when does", "contact", "website", "reviews", "rating",
    "who is", "what is", "how do i", "look up", "find me", "search for",
)

def needs_web_lookup(text: str) -> bool:
    """Heuristic: does this question need live/factual data we shouldn't guess at?"""
    t = text.lower()
    return any(trig in t for trig in _LOOKUP_TRIGGERS)
'''

new_block = '''# Improved web search trigger logic.
# Goal: only call Serper (costs money) when the question is about
# time-sensitive, current, or location-specific facts that the model
# cannot know accurately from training data.
# Historical / general-knowledge / conversational questions are answered
# directly by the LLM to stay cheap and fast.

MUST_SEARCH = [
    # Contact / local business (critical for accuracy)
    "phone number", "number for", "what is the number", "address for", "hours for",
    "is .* open", "closing time", "open today", "directions to", "near me",
    "closest", "nearest", "zip code", "area code",
    # Real-time current data categories (news, sports, weather, stocks, product info)
    "weather", "forecast", "temperature", "raining", "snowing", "rain today",
    "stock", "stocks", "stock price", "market", "dow jones", "nasdaq",
    "score", "scores", "who won", "final score", "live score", "game today",
    "news", "latest news", "breaking news", "what happened", "today's news",
    "current price", "how much does", "price of", "in stock", "where to buy",
    "part number", "specs for",
]

HISTORICAL_SKIP = [
    "who was", "when did", "what was", "in 17", "in 18", "in 19", "in 20",
    "died in", "lived in", "built in", "invented", "discovered", "ancient",
    "history of", "during the", "last century", "was the president of",
    "abraham lincoln", "george washington",
]

CURRENT_TIME_MARKERS = [
    "today", "right now", "currently", "as of now", "latest", "this week",
    "this morning", "tonight", "live", "right this", "as of today",
]

def needs_web_lookup(text: str, transcript: Optional[List[Dict]] = None) -> bool:
    """Return True if we should hit Serper before answering.

    Cheap string heuristics (no extra LLM calls):
    - Business contact/local info (phone, address, hours, near me) → always search
    - Current events categories the user asked for (news, sports, weather, stocks, product info)
    - "today / now / latest / live" language (unless clearly historical)
    - Skip historical facts ("who was Abraham Lincoln", past dates, etc.)
    - Conversational and static general knowledge left to the model.
    """
    if not text:
        return False
    t = text.lower().strip()

    # 1. Business / location / contact info — always verify
    business = ("phone", "address", "hours", "open", "directions", "near me",
                "closest", "nearest", "zip", "area code", "contact")
    if any(b in t for b in business):
        return True

    # 2. Hard must-search categories
    for pat in MUST_SEARCH:
        if pat in t:
            return True

    # 3. Current-time language
    has_current = any(m in t for m in CURRENT_TIME_MARKERS)
    is_historical = any(h in t for h in HISTORICAL_SKIP)
    if has_current and not is_historical:
        return True

    # 4. Broad starters only if not historical
    broad = ("what is", "who is", "how much", "price", "cost of", "where is")
    if any(b in t for b in broad):
        if is_historical:
            return False
        return True

    # 5. Recent context for follow-ups
    if transcript:
        recent = " ".join(m.get("text", "") for m in transcript[-4:] if m.get("role") == "user").lower()
        if any(m in recent for m in CURRENT_TIME_MARKERS) and not is_historical:
            return True

    return False
'''

if old_block in content:
    content = content.replace(old_block, new_block)
    with open("server.py", "w") as f:
        f.write(content)
    print("SUCCESS: replaced trigger logic")
else:
    print("OLD BLOCK NOT FOUND EXACTLY")
    # Try a looser replacement for the function only
    pattern = r'_LOOKUP_TRIGGERS = \([\s\S]*?return any\(trig in t for trig in _LOOKUP_TRIGGERS\)'
    if re.search(pattern, content):
        content = re.sub(pattern, new_block.strip(), content, flags=re.DOTALL)
        with open("server.py", "w") as f:
            f.write(content)
        print("SUCCESS: fuzzy replaced trigger logic")
    else:
        print("FAILED to find old code")
'