#!/usr/bin/env python3
"""
Thailand Plastic Impact Monitor — RSS fetcher

Runs server-side (GitHub Actions, or locally). Pulls Google News RSS feeds for
Thailand-local, regional APAC, and global upstream supply-chain signals. Parses,
classifies, dedupes, and writes the result to ``data/signals.json`` for the
static dashboard to consume.

No paid APIs. No API keys. Standard library only — no external deps required.
"""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import html
import json
import os
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Iterable

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(REPO_ROOT, "data", "signals.json")

USER_AGENT = (
    "Mozilla/5.0 (compatible; ThailandPlasticImpactMonitor/3.0; "
    "+https://github.com/) Python-urllib"
)
FETCH_TIMEOUT_SEC = 15
MAX_WORKERS = 6
MAX_ITEMS_PER_FEED = 30  # cap to keep JSON small

# ---------------------------------------------------------------------------
# Keyword buckets
# ---------------------------------------------------------------------------

TH_LOCAL_TH = [
    "ปิโตรเคมี ราคาสูงขึ้น",
    "เม็ดพลาสติก ราคาสูงขึ้น",
    "วัตถุดิบพลาสติก ขาดแคลน",
    "วัตถุดิบพลาสติก ราคาสูงขึ้น",
    "แนฟทา ราคาสูงขึ้น",
    "ห่วงโซ่อุปทาน พลาสติก สะดุด",
    "อุตสาหกรรมปิโตรเคมี ชะลอตัว",
]

TH_LOCAL_EN = [
    "Thailand petrochemical price pressure",
    "Thailand plastic resin price surge",
    "Thailand naphtha price pressure",
    "Thailand plastic raw material shortage",
    "Thailand resin supply disruption",
]

REGIONAL_EN = [
    "Asia naphtha supply pressure",
    "Asia petrochemical supply disruption",
    "Asia resin price surge",
    "Southeast Asia plastic resin shortage",
    "ASEAN petrochemical price pressure",
    "APAC polymer supply disruption",
]

GLOBAL_EN = [
    "naphtha price surge",
    "petrochemical supply disruption",
    "plastic resin shortage",
    "polystyrene shortage",
    "polypropylene shortage",
    "plastic feedstock price pressure",
    "polymer supply disruption",
]

DIRECT_LAB_EN = [
    "sterile plasticware shortage",
    "laboratory consumables shortage",
    "petri dish shortage",
]

DIRECT_LAB_TH = [
    "จานเพาะเชื้อ ขาดแคลน",
    "อุปกรณ์ห้องแล็บ ขาดแคลน",
]


def build_query_plan() -> list[dict]:
    """Build the master list of (keyword, lang, bucket) triples to fetch."""

    plan: list[dict] = []
    for k in TH_LOCAL_TH:
        plan.append({"keyword": k, "lang": "th", "bucket": "local"})
    for k in TH_LOCAL_EN:
        plan.append({"keyword": k, "lang": "en-th", "bucket": "local"})
    for k in REGIONAL_EN:
        plan.append({"keyword": k, "lang": "en", "bucket": "regional"})
    for k in GLOBAL_EN:
        plan.append({"keyword": k, "lang": "en", "bucket": "global"})
    for k in DIRECT_LAB_EN:
        plan.append({"keyword": k, "lang": "en", "bucket": "direct"})
    for k in DIRECT_LAB_TH:
        plan.append({"keyword": k, "lang": "th", "bucket": "direct"})
    return plan


# ---------------------------------------------------------------------------
# Classification vocabulary
# ---------------------------------------------------------------------------

SUPPLY_TERMS = [
    "shortage", "shortages", "delay", "delayed", "lead time",
    "supply disruption", "supply issue", "supply constraint",
    "price surge", "prices surge", "price pressure", "price increase",
    "prices rise", "price rises", "prices jump", "prices climb",
    "prices soar", "soaring price", "price hike", "price hikes",
    "tight supply", "tightness", "tighten",
    "production cut", "plant shutdown", "plant closure", "plant outage",
    "restructuring", "uncertainty", "volatility", "curtailment", "outage",
    "force majeure", "disrupt", "disruption",
    "rising costs", "rising prices", "rising price",
    # Thai
    "ขาดแคลน", "ล่าช้า", "ส่งมอบล่าช้า", "ราคาสูงขึ้น", "ราคาพุ่ง",
    "สะดุด", "ชะงัก", "ตึงตัว", "ปิดโรงงาน", "ลดกำลังการผลิต",
    "ความไม่แน่นอน", "ชะลอตัว", "ปรับโครงสร้าง",
]

DIRECT_LAB_TERMS = [
    "petri dish", "agar plate", "sterile plasticware",
    "laboratory consumables", "lab consumables",
    "culture plate", "microplate", "pipette tip", "centrifuge tube",
    "จานเพาะเชื้อ", "อุปกรณ์ห้องแล็บ", "วัสดุสิ้นเปลืองห้องแล็บ",
    "พลาสติกห้องแล็บ", "อาหารเลี้ยงเชื้อ",
]

PLASTIC_TERMS = [
    "plastic resin", "plastic pellet", "polyethylene", "polypropylene",
    "polystyrene", "pet resin", "hdpe", "ldpe", "pp resin", "pe resin",
    "plastic raw material", "polymer",
    "เม็ดพลาสติก", "พลาสติก", "โพลีเอทิลีน", "โพลีโพรพิลีน",
    "โพลิเมอร์", "วัตถุดิบพลาสติก",
]

PETRO_TERMS = [
    "petrochemical", "naphtha", "ethylene", "propylene", "feedstock",
    "olefin", "aromatics", "cracker", "steam cracker", "refinery",
    "benzene", "butadiene", "styrene",
    "ปิโตรเคมี", "แนฟทา", "เอทิลีน", "โพรพิลีน",
    "อุตสาหกรรมปิโตรเคมี", "โรงกลั่น", "สไตรีน", "บิวทาไดอีน",
]

THAILAND_MENTION = [
    "thailand", "thai", "bangkok", "ไทย", "ประเทศไทย", "กรุงเทพ",
    "ptt", "scg", "ivl", "indorama", "irpc", "gc plc",
    "ptt global chemical", "map ta phut", "แหลมฉบัง", "ระยอง", "rayong",
]

APAC_MENTION = [
    "asia", "asian", "asia-pacific", "asia pacific", "apac",
    "southeast asia", "south-east asia", "asean", "indochina",
    "china", "japan", "korea", "singapore", "malaysia",
    "vietnam", "indonesia", "philippines", "taiwan", "india", "hong kong",
]

EXCLUSION_PATTERNS = [
    re.compile(r"\bplastic waste\b", re.IGNORECASE),
    re.compile(r"\brecycl(?:ing|ed|able|e)\b", re.IGNORECASE),
    re.compile(r"\bocean plastic", re.IGNORECASE),
    re.compile(r"\bbeach cleanup\b", re.IGNORECASE),
    re.compile(r"\bmicroplastic", re.IGNORECASE),
    re.compile(r"\bplastic pollution\b", re.IGNORECASE),
    re.compile(r"\blab[- ]?grown meat\b", re.IGNORECASE),
    re.compile(r"\bbrain cell", re.IGNORECASE),
    re.compile(r"\borganoid", re.IGNORECASE),
    re.compile(r"\bcovid testing demand\b", re.IGNORECASE),
    re.compile(r"\bdisease outbreak\b", re.IGNORECASE),
    re.compile(r"\bschool experiment", re.IGNORECASE),
    re.compile(r"\bstudent project", re.IGNORECASE),
    re.compile(r"\bscience fair", re.IGNORECASE),
    re.compile(r"\bacademic research only\b", re.IGNORECASE),
    re.compile(r"ขยะพลาสติก"),
    re.compile(r"รีไซเคิล"),
    re.compile(r"มลพิษ.*พลาสติก"),
    re.compile(r"มลพิษทะเล"),
    re.compile(r"การเลือกตั้ง"),
    re.compile(r"ประท้วง"),
]


# ---------------------------------------------------------------------------
# RSS URL builder
# ---------------------------------------------------------------------------

def build_rss_url(keyword: str, lang: str) -> str:
    """Build a Google News RSS search URL for the given keyword + lang code."""

    encoded = urllib.parse.quote(keyword)
    if lang == "th":
        return (
            f"https://news.google.com/rss/search?q={encoded}"
            f"&hl=th&gl=TH&ceid=TH:th"
        )
    if lang == "en-th":
        return (
            f"https://news.google.com/rss/search?q={encoded}"
            f"&hl=en-TH&gl=TH&ceid=TH:en"
        )
    return (
        f"https://news.google.com/rss/search?q={encoded}"
        f"&hl=en&gl=US&ceid=US:en"
    )


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_url(url: str) -> str:
    """GET a URL with a real User-Agent and a hard timeout."""

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SEC) as resp:
        raw = resp.read()
    # Google News RSS is UTF-8; fall back gracefully on edge cases
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# RSS parsing
# ---------------------------------------------------------------------------

def _text(elem: ET.Element | None) -> str:
    if elem is None or elem.text is None:
        return ""
    return html.unescape(elem.text.strip())


def parse_rss(xml_text: str) -> list[dict]:
    """Parse RSS XML into a list of item dicts."""

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    items: list[dict] = []
    # Google News RSS: rss/channel/item
    for item in root.iter("item"):
        title = _text(item.find("title"))
        link_raw = _text(item.find("link"))
        description = _text(item.find("description"))
        pub_date = _text(item.find("pubDate"))
        source_el = item.find("source")
        source = _text(source_el) if source_el is not None else ""

        if not title:
            continue

        items.append({
            "title": title,
            "link_raw": link_raw,
            "description": description,
            "pubDate": pub_date,
            "source": source,
        })
    return items


# ---------------------------------------------------------------------------
# Link extraction (mirrors the v2 browser logic, identical rules)
# ---------------------------------------------------------------------------

GOOGLE_NEWS_ARTICLE_RE = re.compile(
    r"^https?://news\.google\.com/(rss/)?articles/", re.IGNORECASE
)
GOOGLE_NEWS_READ_RE = re.compile(
    r"^https?://news\.google\.com/read/", re.IGNORECASE
)
GOOGLE_NEWS_RSS_FEED_RE = re.compile(
    r"^https?://news\.google\.com/rss/(search|topics|headlines)",
    re.IGNORECASE,
)
GOOGLE_NEWS_SEARCH_RE = re.compile(
    r"news\.google\.com/search", re.IGNORECASE
)

HREF_RE = re.compile(r"""href=["']([^"']+)["']""", re.IGNORECASE)


def is_google_news_article(url: str) -> bool:
    if not url:
        return False
    return bool(
        GOOGLE_NEWS_ARTICLE_RE.match(url) or GOOGLE_NEWS_READ_RE.match(url)
    )


def is_google_news_rss_feed(url: str) -> bool:
    if not url:
        return False
    if GOOGLE_NEWS_RSS_FEED_RE.match(url):
        return True
    return "/rss/search?q=" in url or url.startswith("https://news.google.com/rss?")


def is_publisher_homepage(url: str) -> bool:
    """Reject bare publisher homepages — we want article-level URLs."""

    if not url:
        return True
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError:
        return True
    path = (parsed.path or "").rstrip("/")
    return path == ""


def extract_href_from_description(description: str) -> str | None:
    if not description:
        return None
    decoded = html.unescape(description)
    for match in HREF_RE.finditer(decoded):
        url = match.group(1)
        if not url:
            continue
        if is_google_news_rss_feed(url):
            continue
        if is_publisher_homepage(url):
            continue
        if GOOGLE_NEWS_SEARCH_RE.search(url):
            continue
        if is_google_news_article(url):
            return url
        try:
            parsed = urllib.parse.urlparse(url)
        except ValueError:
            continue
        if parsed.path and len(parsed.path) > 1 and not is_publisher_homepage(url):
            return url
    return None


def resolve_article_url(item: dict) -> tuple[str | None, str]:
    """Pick the best article URL for an item, mirroring v2 rules.

    Returns (url, source_label).
    """

    link_raw = item.get("link_raw") or ""
    if (
        link_raw
        and is_google_news_article(link_raw)
        and not is_google_news_rss_feed(link_raw)
    ):
        return link_raw, "item.link (Google News article)"

    from_desc = extract_href_from_description(item.get("description") or "")
    if from_desc:
        return from_desc, "item.description href"

    if (
        link_raw
        and not is_google_news_rss_feed(link_raw)
        and not is_publisher_homepage(link_raw)
    ):
        return link_raw, "item.link (fallback)"

    return None, "none"


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

THAI_RE = re.compile(r"[\u0E00-\u0E7F]")


def detect_language(text: str) -> str:
    if not text:
        return "en"
    return "th" if THAI_RE.search(text) else "en"


def find_matches(text: str, terms: Iterable[str]) -> list[str]:
    lower = text.lower()
    return [t for t in terms if t.lower() in lower]


def has_any(text: str, terms: Iterable[str]) -> bool:
    lower = text.lower()
    return any(t.lower() in lower for t in terms)


def is_excluded(text: str) -> bool:
    return any(p.search(text) for p in EXCLUSION_PATTERNS)


HTML_TAG_RE = re.compile(r"<[^>]+>")


def classify(item: dict) -> dict:
    """Apply Thailand-relevance + risk classification."""

    haystack = f"{item.get('title', '')} {item.get('description', '')}"
    clean_text = HTML_TAG_RE.sub(" ", haystack)
    lang = detect_language(item.get("title", ""))

    if is_excluded(clean_text):
        return {
            "type": "filtered", "typeLabel": "Filtered",
            "relevance": "filtered", "relevanceLabel": "Filtered",
            "risk": "low", "lang": lang,
            "supplyTerms": [], "domainTerms": [],
            "reason": "Excluded by rule",
        }

    supply_terms = find_matches(clean_text, SUPPLY_TERMS)
    direct_terms = find_matches(clean_text, DIRECT_LAB_TERMS)
    plastic_terms = find_matches(clean_text, PLASTIC_TERMS)
    petro_terms = find_matches(clean_text, PETRO_TERMS)

    has_domain = bool(direct_terms or plastic_terms or petro_terms)
    has_supply = bool(supply_terms)

    if not has_domain:
        return {
            "type": "filtered", "typeLabel": "Filtered",
            "relevance": "filtered", "relevanceLabel": "Filtered",
            "risk": "low", "lang": lang,
            "supplyTerms": supply_terms, "domainTerms": [],
            "reason": "No domain term",
        }

    if direct_terms:
        return {
            "type": "direct",
            "typeLabel": "Direct Lab Consumables Signal",
            "relevance": "direct",
            "relevanceLabel": "Direct Lab Consumables",
            "risk": "high" if has_supply else "med",
            "lang": lang,
            "supplyTerms": supply_terms,
            "domainTerms": direct_terms + plastic_terms + petro_terms,
        }

    is_thailand = has_any(clean_text, THAILAND_MENTION)
    is_apac = has_any(clean_text, APAC_MENTION)

    if is_thailand:
        relevance, relevance_label = "local", "Local Thailand Signal"
    elif is_apac:
        relevance, relevance_label = "regional", "Regional Thailand Impact"
    else:
        relevance, relevance_label = "global", "Global Thailand Impact"

    if relevance == "local":
        risk = "high" if has_supply else "med"
    elif has_supply and (plastic_terms or petro_terms):
        risk = "high"
    elif has_supply:
        risk = "med"
    else:
        risk = "low"

    type_label = (
        "Thailand Local Signal" if relevance == "local"
        else "Thailand Impact Signal"
    )
    type_ = "local" if relevance == "local" else "impact"

    return {
        "type": type_, "typeLabel": type_label,
        "relevance": relevance, "relevanceLabel": relevance_label,
        "risk": risk, "lang": lang,
        "supplyTerms": supply_terms,
        "domainTerms": plastic_terms + petro_terms,
    }


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------

ZW_RE = re.compile(r"[\u200b\u200c\u200d]")
QUOTE_RE = re.compile(r"""[\u201c\u201d"'`\u2019]""")
WS_RE = re.compile(r"\s+")


def normalize_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = WS_RE.sub(" ", s)
    s = ZW_RE.sub("", s)
    s = QUOTE_RE.sub("", s)
    return s


def dedupe(items: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for it in items:
        key = normalize_title(it.get("title", ""))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


# ---------------------------------------------------------------------------
# Worker: fetch one feed
# ---------------------------------------------------------------------------

def fetch_feed(query: dict) -> dict:
    """Fetch one RSS feed, parse items, return a report dict."""

    url = build_rss_url(query["keyword"], query["lang"])
    try:
        xml_text = fetch_url(url)
        parsed = parse_rss(xml_text)[:MAX_ITEMS_PER_FEED]
        items: list[dict] = []
        for p in parsed:
            url_resolved, src_label = resolve_article_url(p)
            items.append({
                "title": p["title"],
                "description": p["description"],
                "pubDate": p["pubDate"],
                "source": p["source"],
                "queryKeyword": query["keyword"],
                "queryLang": query["lang"],
                "queryBucket": query["bucket"],
                "resolvedUrl": url_resolved,
                "resolvedSource": src_label,
            })
        return {
            "ok": True,
            "keyword": query["keyword"],
            "lang": query["lang"],
            "bucket": query["bucket"],
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # pragma: no cover — network errors are runtime
        return {
            "ok": False,
            "keyword": query["keyword"],
            "lang": query["lang"],
            "bucket": query["bucket"],
            "error": f"{type(exc).__name__}: {exc}",
            "items": [],
        }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> int:
    plan = build_query_plan()
    print(f"[fetch_rss] {len(plan)} queries planned", file=sys.stderr)

    raw_items: list[dict] = []
    feed_report: list[dict] = []
    success_count = 0
    fail_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for result in pool.map(fetch_feed, plan):
            if result["ok"]:
                success_count += 1
                raw_items.extend(result["items"])
                feed_report.append({
                    "keyword": result["keyword"],
                    "lang": result["lang"],
                    "bucket": result["bucket"],
                    "ok": True,
                    "count": result["count"],
                })
                print(
                    f"[fetch_rss]  OK   [{result['bucket']:8}] "
                    f"{result['keyword']!r}  -> {result['count']} items",
                    file=sys.stderr,
                )
            else:
                fail_count += 1
                feed_report.append({
                    "keyword": result["keyword"],
                    "lang": result["lang"],
                    "bucket": result["bucket"],
                    "ok": False,
                    "error": result["error"],
                })
                print(
                    f"[fetch_rss]  FAIL [{result['bucket']:8}] "
                    f"{result['keyword']!r}  -> {result['error']}",
                    file=sys.stderr,
                )

    unique_items = dedupe(raw_items)
    classified: list[dict] = []
    for it in unique_items:
        cls = classify(it)
        merged = {**it, **cls}
        classified.append(merged)

    visible_count = sum(1 for it in classified if it["type"] != "filtered")
    print(
        f"[fetch_rss] {success_count}/{len(plan)} feeds OK, "
        f"{fail_count} failed. {len(classified)} unique items, "
        f"{visible_count} active signals.",
        file=sys.stderr,
    )

    out = {
        "schemaVersion": 1,
        "generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "summary": {
            "queriesPlanned": len(plan),
            "feedsOk": success_count,
            "feedsFailed": fail_count,
            "uniqueItems": len(classified),
            "activeSignals": visible_count,
        },
        "feedReport": feed_report,
        "items": classified,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2)
    print(f"[fetch_rss] Wrote {OUTPUT_PATH}", file=sys.stderr)

    # Don't fail the workflow on partial results — we want stale-but-present
    # data over a missing file. Only fail if literally nothing came back.
    if success_count == 0 and fail_count > 0:
        print("[fetch_rss] All feeds failed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
