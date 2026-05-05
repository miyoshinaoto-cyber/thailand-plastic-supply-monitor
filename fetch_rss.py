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
    "Mozilla/5.0 (compatible; ThailandPlasticImpactMonitor/5.0; "
    "+https://github.com/) Python-urllib"
)
FETCH_TIMEOUT_SEC = 15
MAX_WORKERS = 6
MAX_ITEMS_PER_FEED = 30  # cap to keep JSON small

SCHEMA_VERSION = 2  # bumped for scoring/tier fields

# Similarity dedup: keep only one of any two items whose normalized titles
# overlap above this Jaccard threshold AND whose pubDates are within 1 day.
# 0.65 catches synonym-swap near-duplicates ("X worsens" vs "X deepens",
# "as Middle East tensions escalate" vs "amid Middle East tensions") that
# tend to score 0.66-0.69 token-overlap.
SIMILARITY_THRESHOLD = 0.65

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
    "supply chain disruption", "shipping disruption", "freight disruption",
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
    "ห่วงโซ่อุปทาน", "ค่าขนส่ง", "โรงงานปิด",
]

# Lab consumable terms — used for the BOTH-required direct-lab gate
DIRECT_LAB_TERMS = [
    "petri dish", "agar plate", "sterile plasticware",
    "laboratory consumables", "lab consumables",
    "culture plate", "microplate", "pipette tip", "centrifuge tube",
    "จานเพาะเชื้อ", "อุปกรณ์ห้องแล็บ", "วัสดุสิ้นเปลืองห้องแล็บ",
    "พลาสติกห้องแล็บ", "อาหารเลี้ยงเชื้อ",
]

# Supply-constraint terms specific to the Direct-Lab BOTH-required gate.
# Must be a real shortage/delay/backorder, not just "uncertainty".
LAB_SUPPLY_CONSTRAINT_TERMS = [
    "shortage", "shortages", "delay", "delayed", "lead time",
    "backorder", "back order", "back-order",
    "unavailable", "out of stock",
    "supply disruption", "supply issue", "supply constraint",
    "ขาดแคลน", "ล่าช้า", "ส่งมอบล่าช้า", "สะดุด",
]

PLASTIC_TERMS = [
    "plastic resin", "plastic pellet", "polyethylene", "polypropylene",
    "polystyrene", "pet resin", "hdpe", "ldpe", "pp resin", "pe resin",
    "plastic raw material", "plastic feedstock", "polymer",
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

# Geopolitical / oil / logistics terms — these count as supply-chain-domain
# because they upstream-affect Thailand's plastic feedstock costs even when
# no plastic/petrochemical word appears in the headline.
MIDEAST_OIL_TERMS = [
    "middle east", "iran", "iranian", "strait of hormuz", "hormuz",
    "red sea", "houthi", "saudi arabia", "saudi", "uae", "qatar",
    "oil price", "oil prices", "crude oil", "crude price", "crude prices",
    "brent crude", "wti crude",
    "ตะวันออกกลาง", "อิหร่าน", "ช่องแคบฮอร์มุซ", "น้ำมันดิบ",
]

LOGISTICS_TERMS = [
    "shipping disruption", "freight disruption", "supply chain disruption",
    "container shortage", "port congestion", "shipping rates",
    "freight rates", "ocean freight",
    "ห่วงโซ่อุปทาน", "ค่าขนส่ง", "โรงงานปิด",
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
    # Metaphorical "petri dish" usage — must be filtered, even if the article
    # mentions the literal term "petri dish". These take priority because
    # otherwise lots of innovation/policy articles slip through.
    re.compile(r"\bpetri dish for\b", re.IGNORECASE),
    re.compile(r"\ba petri dish for\b", re.IGNORECASE),
    re.compile(r"\bas a petri dish\b", re.IGNORECASE),
    re.compile(r"\bpetri dish of\b", re.IGNORECASE),
    re.compile(r"\bpolitical petri dish\b", re.IGNORECASE),
    re.compile(r"\beconomic petri dish\b", re.IGNORECASE),
    re.compile(r"\bsocial petri dish\b", re.IGNORECASE),

    # Existing waste/recycling/research-only exclusions
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

# "Soft" exclusion — innovation / policy / general-science framing.
# These do NOT auto-filter; they only filter when the article LACKS strong
# supply-chain terms (naphtha, resin shortage, petrochemical disruption,
# shipping disruption, etc.). This protects e.g. a "petrochemical industry
# innovation" article that does mention real supply pressure.
SOFT_EXCLUSION_PATTERNS = [
    re.compile(r"\binnovation\b", re.IGNORECASE),
    re.compile(r"\bfoundation\b", re.IGNORECASE),
    re.compile(r"\btomorrow'?s solutions?\b", re.IGNORECASE),
    re.compile(r"\bfuture solutions?\b", re.IGNORECASE),
    re.compile(r"\bresearch funding\b", re.IGNORECASE),
    re.compile(r"\bpolicy forum\b", re.IGNORECASE),
    re.compile(r"\bsocial experiment\b", re.IGNORECASE),
    re.compile(r"\bscience policy\b", re.IGNORECASE),
    re.compile(r"\bstartup ecosystem\b", re.IGNORECASE),
    re.compile(r"\bclimate solutions?\b", re.IGNORECASE),
    re.compile(r"\bpublic health metaphor\b", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Scoring · Tier · Source quality · Impact summary
# ---------------------------------------------------------------------------

# Tier 1 — Direct supply risk: upstream materials, plant disruption, feedstock,
# and Mideast/oil-corridor risks (these affect naphtha and shipping costs into
# Asia at the source level, so we treat them as direct supply risk).
TIER1_TERMS = [
    "naphtha", "petrochemical", "polymer", "polystyrene", "polypropylene",
    "polyethylene", "plastic resin", "plastic pellet", "plastic raw material",
    "plastic feedstock", "feedstock", "ethylene", "propylene", "olefin",
    "aromatics", "cracker", "steam cracker", "refinery", "benzene",
    "butadiene", "styrene", "plant shutdown", "plant closure", "plant outage",
    "production cut", "force majeure",
    # Mideast / oil corridor — direct upstream risk for Asian naphtha
    "middle east", "iran", "strait of hormuz", "hormuz", "red sea",
    "houthi", "oil price", "oil prices", "crude oil",
    "ปิโตรเคมี", "แนฟทา", "เม็ดพลาสติก", "วัตถุดิบพลาสติก",
    "โพลีเอทิลีน", "โพลีโพรพิลีน", "โพลิเมอร์",
    "อุตสาหกรรมปิโตรเคมี", "โรงกลั่น", "ปิดโรงงาน", "ลดกำลังการผลิต",
    "ตะวันออกกลาง", "อิหร่าน", "ช่องแคบฮอร์มุซ", "น้ำมันดิบ",
]

# Tier 2 — Supply chain / logistics
TIER2_TERMS = [
    "shipping disruption", "freight disruption", "supply chain disruption",
    "shipping rates", "freight rates", "ocean freight",
    "container shortage", "port congestion",
    "ห่วงโซ่อุปทาน", "ค่าขนส่ง",
]

# Tier 3 — Indirect / market price impact (fallback for Active items that
# don't hit Tier 1 or Tier 2 vocab).
TIER3_HINTS = [
    "price increase", "price hike", "price hikes", "rising prices",
    "rising price", "inflation",
    "ราคาสูงขึ้น", "ราคาพุ่ง", "เงินเฟ้อ",
]

UPSTREAM_MATERIAL_TERMS = [
    "naphtha", "petrochemical", "resin", "polymer", "polystyrene",
    "polypropylene", "polyethylene", "feedstock",
    # Direct Lab Consumables are the upstream FROM A LAB-PROCUREMENT ANGLE,
    # so they earn the same +40 as petrochemical materials.
    "petri dish", "agar plate", "sterile plasticware",
    "laboratory consumables", "lab consumables",
    "ปิโตรเคมี", "แนฟทา", "เม็ดพลาสติก", "วัตถุดิบพลาสติก",
    "โพลิเมอร์", "โพลีเอทิลีน", "โพลีโพรพิลีน",
    "จานเพาะเชื้อ", "อุปกรณ์ห้องแล็บ", "วัสดุสิ้นเปลืองห้องแล็บ",
]

SUPPLY_DISRUPTION_TERMS = [
    "shortage", "shortages", "disruption", "disrupt", "disrupted",
    "shutdown", "closure", "outage", "constraint", "constraints",
    "delay", "delayed", "force majeure", "production cut",
    # Price-pressure signals are a form of supply pressure for our purposes —
    # without them, naphtha/resin "price surge" headlines miss the +30 even
    # though they're squarely the kind of signal we want surfaced.
    "price surge", "prices surge", "price pressure", "prices jump",
    "prices climb", "prices soar", "soaring price", "price hike",
    "price hikes", "tight supply", "tightness",
    "ขาดแคลน", "ล่าช้า", "ส่งมอบล่าช้า", "สะดุด", "ปิดโรงงาน",
    "ลดกำลังการผลิต", "ราคาสูงขึ้น", "ราคาพุ่ง", "ตึงตัว",
]

LOGISTICS_SCORING_TERMS = TIER2_TERMS

# Penalty: consumer-price-only signal. Hits -20 unless an upstream driver is
# also present in the same text.
CONSUMER_PRICE_TERMS = [
    "bottled water", "food price", "food prices", "grocery", "groceries",
    "household goods", "consumer goods", "consumer price",
    "pump price", "retail price", "supermarket price",
    "ราคาน้ำดื่ม", "ราคาอาหาร", "สินค้าอุปโภค", "ราคาขายปลีก",
    "ปั๊มน้ำมัน",
]

GENERIC_ECONOMY_TERMS = [
    "gdp growth", "stock market", "stocks rise", "stocks fall",
    "currency", "exchange rate", "central bank", "monetary policy",
    "interest rate", "trade balance", "consumer confidence",
    "ตลาดหุ้น", "อัตราดอกเบี้ย", "อัตราแลกเปลี่ยน", "ดัชนีหุ้น",
]

# Source-quality classification --------------------------------------------

EXCLUDED_SOURCE_DOMAINS = {
    "facebook.com", "m.facebook.com", "web.facebook.com", "fb.com",
    "twitter.com", "x.com",
    "tiktok.com", "instagram.com",
    "reddit.com", "pantip.com",
    "youtube.com", "youtu.be",
}
EXCLUDED_SOURCE_NAMES_LOWER = {
    "facebook", "twitter", "tiktok", "instagram", "reddit", "pantip",
    "youtube",
}

PREFERRED_SOURCE_HINTS = [
    # Thailand
    "bangkokbiznews", "bangkokpost", "thestandard", "nationthailand",
    "prachachat", "thaipublica", "thairath", "matichon", "krungthep",
    # Global / regional business
    "reuters", "nikkei", "bloomberg", "scmp", "ft.com", "wsj.com",
    "icis.com", "platts", "spglobal", "argusmedia", "chemanalyst",
    "chemical-news", "channelnewsasia", "straitstimes",
]


def _matches_any(text_lower: str, terms: Iterable[str]) -> bool:
    return any(t.lower() in text_lower for t in terms)


def assign_tier(text: str, classification: dict) -> tuple[int, str]:
    if classification.get("type") == "filtered":
        return 0, "—"
    # Direct Lab Consumables are inherently Tier 1 — these are the most
    # actionable signals for our use case regardless of which TIER vocab hits.
    if classification.get("relevance") == "direct":
        return 1, "Tier 1 · Direct Supply Risk"
    lower = text.lower()
    if _matches_any(lower, TIER1_TERMS):
        return 1, "Tier 1 · Direct Supply Risk"
    if _matches_any(lower, TIER2_TERMS):
        return 2, "Tier 2 · Supply Chain / Logistics"
    return 3, "Tier 3 · Market Impact"


def compute_score(
    text: str, classification: dict, source_quality: str
) -> tuple[int, list[str]]:
    """Return (score, breakdown_lines). Score clamped to 0..100."""

    if classification.get("type") == "filtered":
        return 0, []

    lower = text.lower()
    breakdown: list[str] = []
    score = 0

    if _matches_any(lower, UPSTREAM_MATERIAL_TERMS):
        score += 40
        breakdown.append("+40 upstream material term")

    if _matches_any(lower, SUPPLY_DISRUPTION_TERMS):
        score += 30
        breakdown.append("+30 supply disruption term")

    if classification.get("relevance") == "local" or classification.get("lang") == "th":
        score += 20
        breakdown.append("+20 Thailand relevance")

    if _matches_any(lower, LOGISTICS_SCORING_TERMS):
        score += 10
        breakdown.append("+10 logistics term")

    has_consumer_price = _matches_any(lower, CONSUMER_PRICE_TERMS)
    has_upstream = _matches_any(lower, UPSTREAM_MATERIAL_TERMS)
    has_oil = any(t in lower for t in (
        "oil price", "crude oil", "middle east", "iran", "strait of hormuz",
        "ตะวันออกกลาง", "อิหร่าน", "น้ำมันดิบ",
    ))
    if has_consumer_price and not (has_upstream or has_oil):
        score -= 20
        breakdown.append("-20 consumer price without upstream context")

    if _matches_any(lower, GENERIC_ECONOMY_TERMS):
        score -= 30
        breakdown.append("-30 generic economy article")

    has_supply = _matches_any(lower, SUPPLY_DISRUPTION_TERMS)
    if not has_upstream and not has_supply:
        score -= 40
        breakdown.append("-40 weak / unclear relevance")

    if source_quality == "preferred":
        score += 5
        breakdown.append("+5 preferred source")
    elif source_quality == "low":
        score -= 15
        breakdown.append("-15 low-quality source")

    score = max(0, min(100, score))
    return score, breakdown


def score_band(score: int) -> str:
    if score >= 70:
        return "HIGH"
    if score >= 40:
        return "MEDIUM"
    return "LOW"


def classify_source_quality(item: dict) -> tuple[str, str]:
    """Return (label, reason). label ∈ {'preferred','neutral','low'}."""

    # Multiple URL signals to check — Google News often gives a redirect URL
    # in <link>, so the publisher's actual domain comes from <source url=...>.
    urls_to_check = [
        (item.get("resolvedUrl") or "").lower(),
        (item.get("sourceUrl") or "").lower(),
        (item.get("link_raw") or "").lower(),
    ]
    source_name = (item.get("source") or "").lower()

    for url in urls_to_check:
        if not url:
            continue
        for bad_domain in EXCLUDED_SOURCE_DOMAINS:
            if bad_domain in url:
                return "low", f"Excluded domain: {bad_domain}"

    for bad_name in EXCLUDED_SOURCE_NAMES_LOWER:
        if re.search(rf"\b{re.escape(bad_name)}\b", source_name):
            return "low", f"Excluded source: {bad_name}"

    for url in urls_to_check:
        if not url:
            continue
        for good in PREFERRED_SOURCE_HINTS:
            if good in url:
                return "preferred", f"Preferred source: {good}"
    for good in PREFERRED_SOURCE_HINTS:
        if good in source_name:
            return "preferred", f"Preferred source: {good}"

    return "neutral", "Source not in preferred or excluded lists"


def is_low_quality_source(item: dict) -> tuple[bool, str | None]:
    quality, reason = classify_source_quality(item)
    if quality == "low":
        return True, reason
    return False, None


def build_impact_summary(item: dict, classification: dict, tier: int) -> str:
    """One-line, factual potential-impact narrative for a card."""

    if classification.get("type") == "filtered":
        return ""

    text = f"{item.get('title','')} {item.get('description','')}".lower()

    has_naphtha = "naphtha" in text or "แนฟทา" in text
    has_resin = any(t in text for t in (
        "resin", "polymer", "polystyrene", "polypropylene", "polyethylene",
        "เม็ดพลาสติก", "วัตถุดิบพลาสติก", "โพลิเมอร์",
    ))
    has_petrochem = "petrochemical" in text or "ปิโตรเคมี" in text
    has_mideast = any(t in text for t in (
        "middle east", "iran", "strait of hormuz", "red sea", "houthi",
        "ตะวันออกกลาง", "อิหร่าน", "ช่องแคบฮอร์มุซ",
    ))
    has_shipping = any(t in text for t in (
        "shipping", "freight", "container", "port congestion", "ค่าขนส่ง",
    ))
    has_shutdown = any(t in text for t in (
        "shutdown", "closure", "force majeure", "outage", "ปิดโรงงาน",
    ))
    has_lab = any(t in text for t in (
        "petri dish", "agar plate", "sterile plasticware",
        "laboratory consumables", "lab consumables",
        "จานเพาะเชื้อ", "อุปกรณ์ห้องแล็บ",
    ))

    if classification.get("relevance") == "direct" or has_lab:
        return (
            "Lab consumables supply pressure → potential Thailand R&D and "
            "QC operations impact."
        )
    if has_mideast and (has_naphtha or has_petrochem or has_resin):
        return (
            "Middle East / oil corridor disruption affecting upstream "
            "feedstock → naphtha and petrochemical cost pressure into "
            "Thailand resin pricing."
        )
    if has_mideast:
        return (
            "Middle East / oil corridor risk → upstream feedstock and "
            "shipping cost pressure for Thailand petrochemical chain."
        )
    if has_shutdown and (has_resin or has_petrochem):
        return (
            "Petrochemical / resin plant disruption → tighter regional "
            "supply, potential price pressure on Thailand polymer buyers."
        )
    if has_naphtha:
        return (
            "Naphtha price / supply pressure → direct cost pressure on "
            "Thailand petrochemical and downstream plastic resin."
        )
    if has_resin:
        return (
            "Resin / polymer supply signal → potential downstream plastic "
            "cost pressure for Thailand manufacturers."
        )
    if has_petrochem:
        return (
            "Petrochemical industry signal → upstream cost / availability "
            "implication for Thailand plastic chain."
        )
    if has_shipping or tier == 2:
        return (
            "Logistics / freight disruption → import lead times and landed "
            "cost risk for Thailand plastic and lab consumables."
        )

    return (
        "Indirect market signal → monitor for cost pass-through into "
        "Thailand plastic supply chain."
    )


def enrich(item: dict, classification: dict) -> dict:
    """Bolt on score, tier, source-quality, impact summary fields."""

    haystack = HTML_TAG_RE.sub(
        " ", f"{item.get('title','')} {item.get('description','')}"
    )

    quality, quality_reason = classify_source_quality(item)
    tier_int, tier_label = assign_tier(haystack, classification)
    score, breakdown = compute_score(haystack, classification, quality)
    band = score_band(score)
    summary = build_impact_summary(item, classification, tier_int)

    return {
        "score": score,
        "scoreBand": band,
        "scoreBreakdown": breakdown,
        "tier": tier_int,
        "tierLabel": tier_label,
        "sourceQuality": quality,
        "sourceQualityReason": quality_reason,
        "impactSummary": summary,
    }


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
        # Google News also sets <source url="https://publisher.com">; that is
        # the publisher's homepage and is the most reliable signal of which
        # outlet the article is from.
        source_url = (
            source_el.get("url", "")
            if source_el is not None and source_el.get("url")
            else ""
        )

        if not title:
            continue

        items.append({
            "title": title,
            "link_raw": link_raw,
            "description": description,
            "pubDate": pub_date,
            "source": source,
            "sourceUrl": source_url,
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


def find_first_match(text: str, patterns: Iterable[re.Pattern]) -> str | None:
    """Return the matched substring of the first pattern that hits, or None."""

    for pat in patterns:
        m = pat.search(text)
        if m:
            return m.group(0)
    return None


def is_excluded(text: str) -> tuple[bool, str | None]:
    """Hard exclusion: returns (excluded, matched_pattern_string).

    Replaces the old boolean-only is_excluded so we can emit a filteredReason.
    """

    for p in EXCLUSION_PATTERNS:
        m = p.search(text)
        if m:
            return True, m.group(0)
    return False, None


HTML_TAG_RE = re.compile(r"<[^>]+>")


def classify(item: dict) -> dict:
    """Apply Thailand-relevance + risk classification with explicit reasons.

    Output shape (in addition to existing fields) adds:
      - relevanceReason: short string explaining why an item was kept
      - filteredReason:  short string explaining why a filtered item was dropped
      - matchedSupplyChainTerms: the supply-chain-domain hits used to gate it
    """

    haystack = f"{item.get('title', '')} {item.get('description', '')}"
    clean_text = HTML_TAG_RE.sub(" ", haystack)
    lang = detect_language(item.get("title", ""))

    def filtered(reason: str, **extra) -> dict:
        out = {
            "type": "filtered", "typeLabel": "Filtered",
            "relevance": "filtered", "relevanceLabel": "Filtered",
            "risk": "low", "lang": lang,
            "supplyTerms": [], "domainTerms": [],
            "matchedSupplyChainTerms": [],
            "relevanceReason": "",
            "filteredReason": reason,
        }
        out.update(extra)
        return out

    # ---- Gate 1: hard exclusions (metaphors, recycling, etc.)
    excluded, matched_excl = is_excluded(clean_text)
    if excluded:
        # Differentiate metaphorical petri-dish hits from generic exclusions
        if "petri dish" in (matched_excl or "").lower() or "petri" in (matched_excl or "").lower():
            return filtered(f"Metaphorical petri dish: '{matched_excl}'")
        return filtered(f"Excluded by rule: '{matched_excl}'")

    # ---- Gather term hits we'll use throughout
    supply_terms = find_matches(clean_text, SUPPLY_TERMS)
    direct_lab_terms = find_matches(clean_text, DIRECT_LAB_TERMS)
    plastic_terms = find_matches(clean_text, PLASTIC_TERMS)
    petro_terms = find_matches(clean_text, PETRO_TERMS)
    mideast_terms = find_matches(clean_text, MIDEAST_OIL_TERMS)
    logistics_terms = find_matches(clean_text, LOGISTICS_TERMS)
    lab_supply_terms = find_matches(clean_text, LAB_SUPPLY_CONSTRAINT_TERMS)

    has_supply = bool(supply_terms)

    # ---- Gate 2: Direct Lab Consumables — needs BOTH lab term AND supply
    # constraint term. A lab term alone (e.g. metaphor that slipped past the
    # excluder, or a "petri dish was used in a study" item) is not enough.
    if direct_lab_terms:
        if lab_supply_terms:
            return {
                "type": "direct",
                "typeLabel": "Direct Lab Consumables Signal",
                "relevance": "direct",
                "relevanceLabel": "Direct Lab Consumables",
                "risk": "high",
                "lang": lang,
                "supplyTerms": supply_terms,
                "domainTerms": direct_lab_terms + plastic_terms + petro_terms,
                "matchedSupplyChainTerms": direct_lab_terms,
                "relevanceReason": (
                    f"Lab consumable [{', '.join(direct_lab_terms[:3])}] + "
                    f"supply constraint [{', '.join(lab_supply_terms[:3])}]"
                ),
                "filteredReason": "",
            }
        # Lab term but no supply constraint — drop. This is what catches
        # "petri dish was used in research" and similar non-supply mentions.
        return filtered(
            "Lab consumable mentioned but no supply constraint term "
            f"(found lab terms: {', '.join(direct_lab_terms[:3])})"
        )

    # ---- Gate 3: must include at least one supply-chain domain term.
    # The bar is: petrochemical / plastic / Mideast-oil / logistics. This is
    # broader than v3 — Iran and Strait of Hormuz alone are enough, even
    # without a "plastic" word, because they upstream-affect Thailand.
    supply_chain_domain_terms = (
        plastic_terms + petro_terms + mideast_terms + logistics_terms
    )
    if not supply_chain_domain_terms:
        return filtered(
            "No supply-chain domain term "
            "(needs petrochemical/plastic/Mideast-oil/logistics vocab)"
        )

    # ---- Gate 4: soft exclusion. If the article reads like an innovation /
    # foundation / policy piece AND it has no real supply-side language,
    # drop it even if a feedstock keyword sneaks into the body. This is what
    # catches "Europe: a petri dish for tomorrow's solutions" style framings
    # that escape the hard excluder.
    soft_match = find_first_match(clean_text, SOFT_EXCLUSION_PATTERNS)
    if soft_match and not has_supply:
        return filtered(
            f"General science/policy framing ('{soft_match}') without supply pressure"
        )

    # ---- Geographic relevance
    is_thailand = has_any(clean_text, THAILAND_MENTION)
    is_apac = has_any(clean_text, APAC_MENTION)

    if is_thailand:
        relevance, relevance_label = "local", "Local Thailand Signal"
    elif is_apac:
        relevance, relevance_label = "regional", "Regional Thailand Impact"
    else:
        relevance, relevance_label = "global", "Global Thailand Impact"

    # ---- Risk
    if relevance == "local":
        risk = "high" if has_supply else "med"
    elif has_supply and (plastic_terms or petro_terms or mideast_terms or logistics_terms):
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

    # Build a compact human-readable reason for the card
    domain_summary_parts: list[str] = []
    if mideast_terms:
        domain_summary_parts.append(f"Mideast/oil [{', '.join(mideast_terms[:2])}]")
    if petro_terms:
        domain_summary_parts.append(f"petro [{', '.join(petro_terms[:2])}]")
    if plastic_terms:
        domain_summary_parts.append(f"plastic [{', '.join(plastic_terms[:2])}]")
    if logistics_terms:
        domain_summary_parts.append(f"logistics [{', '.join(logistics_terms[:2])}]")
    domain_summary = "; ".join(domain_summary_parts) or "domain match"
    supply_summary = (
        f" + supply [{', '.join(supply_terms[:3])}]"
        if supply_terms else " (no supply term — low risk)"
    )

    return {
        "type": type_, "typeLabel": type_label,
        "relevance": relevance, "relevanceLabel": relevance_label,
        "risk": risk, "lang": lang,
        "supplyTerms": supply_terms,
        "domainTerms": plastic_terms + petro_terms + mideast_terms + logistics_terms,
        "matchedSupplyChainTerms": supply_chain_domain_terms,
        "relevanceReason": f"{domain_summary}{supply_summary}",
        "filteredReason": "",
    }


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------

ZW_RE = re.compile(r"[\u200b\u200c\u200d]")
QUOTE_RE = re.compile(r"""[\u201c\u201d"'`\u2019]""")
WS_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^\w\s\u0E00-\u0E7F]")


def normalize_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = WS_RE.sub(" ", s)
    s = ZW_RE.sub("", s)
    s = QUOTE_RE.sub("", s)
    return s


def dedupe(items: list[dict]) -> list[dict]:
    """Exact-match dedupe by normalized title. Preserves first occurrence."""

    seen: set[str] = set()
    out: list[dict] = []
    for it in items:
        key = normalize_title(it.get("title", ""))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


# Word-set similarity dedup ------------------------------------------------

def _title_token_set(title: str) -> set[str]:
    """Lowercase, strip punctuation, split into a token set for Jaccard."""

    s = normalize_title(title)
    s = PUNCT_RE.sub(" ", s)
    tokens = [t for t in s.split() if len(t) > 1]
    return set(tokens)


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _parse_pubdate(s: str) -> dt.datetime | None:
    """Best-effort RFC 2822 parser; tolerates blank/garbage."""

    if not s:
        return None
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(s)
        if d is None:
            return None
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d
    except (TypeError, ValueError):
        return None


def similarity_dedupe(
    items: list[dict],
    threshold: float = SIMILARITY_THRESHOLD,
    days_window: int = 1,
) -> tuple[list[dict], int]:
    """Drop near-duplicates: same date ±days_window AND title Jaccard ≥ threshold.

    Among any cluster of similar items, keep the one with the highest score.
    Returns (kept_items, dropped_count).
    """

    # Pre-compute token sets and parsed dates so we don't redo work in the loop
    enriched: list[tuple[dict, set[str], dt.datetime | None]] = []
    for it in items:
        toks = _title_token_set(it.get("title", ""))
        d = _parse_pubdate(it.get("pubDate", ""))
        enriched.append((it, toks, d))

    # Greedy clustering: sort by score desc so the first kept in each cluster
    # is the strongest candidate.
    enriched.sort(
        key=lambda triple: (triple[0].get("score", 0), len(triple[1])),
        reverse=True,
    )

    kept: list[tuple[dict, set[str], dt.datetime | None]] = []
    dropped = 0

    for cand, cand_toks, cand_date in enriched:
        is_dup = False
        for kept_item, kept_toks, kept_date in kept:
            # Date proximity (or both missing dates — be lenient there)
            if cand_date and kept_date:
                delta_days = abs((cand_date - kept_date).total_seconds()) / 86400.0
                if delta_days > days_window:
                    continue
            # Title similarity
            if _jaccard(cand_toks, kept_toks) >= threshold:
                is_dup = True
                break

        if is_dup:
            dropped += 1
        else:
            kept.append((cand, cand_toks, cand_date))

    # Restore original publish-time ordering for downstream sort stability
    return [t[0] for t in kept], dropped


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
                "sourceUrl": p.get("sourceUrl", ""),
                "link_raw": p.get("link_raw", ""),
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

    # Title-exact dedupe first (cheap)
    unique_items = dedupe(raw_items)

    # Drop low-quality sources (Facebook, social, forums) before classifying.
    # We track how many were dropped for the summary.
    after_source_filter: list[dict] = []
    dropped_low_quality = 0
    for it in unique_items:
        is_low, _reason = is_low_quality_source(it)
        if is_low:
            dropped_low_quality += 1
            continue
        after_source_filter.append(it)

    # Classify + enrich (score, tier, source quality, impact summary)
    classified: list[dict] = []
    for it in after_source_filter:
        cls = classify(it)
        ext = enrich(it, cls)
        classified.append({**it, **cls, **ext})

    # Similarity dedup — uses the score field, so must come AFTER enrich()
    deduped, similarity_dropped = similarity_dedupe(classified)

    visible_count = sum(1 for it in deduped if it["type"] != "filtered")
    high_count = sum(
        1 for it in deduped
        if it["type"] != "filtered" and it.get("scoreBand") == "HIGH"
    )
    medium_count = sum(
        1 for it in deduped
        if it["type"] != "filtered" and it.get("scoreBand") == "MEDIUM"
    )
    print(
        f"[fetch_rss] {success_count}/{len(plan)} feeds OK, "
        f"{fail_count} failed. {len(deduped)} unique items "
        f"(dropped {dropped_low_quality} low-quality, "
        f"{similarity_dropped} near-duplicates), "
        f"{visible_count} active "
        f"({high_count} high, {medium_count} medium).",
        file=sys.stderr,
    )

    out = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "summary": {
            "queriesPlanned": len(plan),
            "feedsOk": success_count,
            "feedsFailed": fail_count,
            "uniqueItems": len(deduped),
            "activeSignals": visible_count,
            "highSignals": high_count,
            "mediumSignals": medium_count,
            "droppedLowQuality": dropped_low_quality,
            "droppedNearDuplicates": similarity_dropped,
        },
        "feedReport": feed_report,
        "items": deduped,
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
