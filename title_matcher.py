# title_matcher.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import re

import praw
from rapidfuzz import fuzz

# Szersza lista „pustych” słów, które nie wnoszą informacji o dramie
STOPWORDS = {
    "title","link","pls","please","help","drama","name",
    "find","looking","for","this","that","the","a","an",
    "anyone","can","need","someone","pls","plz","me","my",
}

_CJK_OR_LATIN_RE = re.compile(r"[^0-9A-Za-z\u4e00-\u9FFF\s]+")
_MULTISPACE_RE = re.compile(r"\s+")
_EMOJI_OR_SYMBOLS_RE = re.compile(r"[\u2600-\u27BF\U0001F300-\U0001FAFF]")

def normalize_title(text: str) -> str:
    """Lowercase, usuń emoji/symbole, zostaw łacińskie + CJK, usuń stopwordy, znormalizuj spacje."""
    t = (text or "").lower()
    t = _EMOJI_OR_SYMBOLS_RE.sub(" ", t)
    t = _CJK_OR_LATIN_RE.sub(" ", t)
    t = _MULTISPACE_RE.sub(" ", t).strip()
    toks = [w for w in t.split() if w and w not in STOPWORDS]
    return " ".join(toks)

def token_stats(norm_text: str) -> dict:
    has_cjk = bool(re.search(r"[\u4e00-\u9FFF]", norm_text))
    toks = [w for w in (norm_text.split() if norm_text else []) if w]
    return {"tokens": toks, "n": len(toks), "has_cjk": has_cjk, "chars": len(norm_text)}

def is_uninformative(stats: dict) -> bool:
    # bardzo krótki i bez CJK → nieinformatywny
    return (stats["n"] < 2) and (not stats["has_cjk"]) and (stats["chars"] < 8)

def safe_score(a_norm: str, b_norm: str, a_stats: dict, b_stats: dict) -> int:
    """
    Bezpieczne liczenie podobieństwa:
    - jeśli któryś tytuł ma ≤2 tokeny → cap wynik na 85 (żeby nie wchodził w progi 'certain'),
    - w pozostałych wypadkach bierz maksimum z kilku metryk.
    """
    if a_stats["n"] <= 2 or b_stats["n"] <= 2:
        s1 = fuzz.ratio(a_norm, b_norm)
        s2 = fuzz.partial_ratio(a_norm, b_norm)
        return min(max(s1, s2), 85)
    return max(
        fuzz.ratio(a_norm, b_norm),
        fuzz.token_sort_ratio(a_norm, b_norm),
        fuzz.token_set_ratio(a_norm, b_norm),
    )

def normalize_flair(text: Optional[str]) -> str:
    if not text:
        return ""
    t = _EMOJI_OR_SYMBOLS_RE.sub(" ", text)
    t = re.sub(r"[^\w\s&]+", " ", t)
    t = t.strip().lower()
    t = _MULTISPACE_RE.sub(" ", t)
    return t

def _subreddit(cfg: Dict[str, Any]):
    praw_site = (cfg.get("reddit", {}) or {}).get("praw_site") or "Cleanup_Bot"
    sub_name  = (cfg.get("reddit", {}) or {}).get("subreddit") or "CShortDramas"
    r = praw.Reddit(praw_site)
    return r.subreddit(sub_name)

def _fetch_pool(cfg: Dict[str, Any], limit_per_flair: int, compare_flairs: List[str]) -> Tuple[List[Any], Dict[str, int]]:
    s = _subreddit(cfg)
    want = {normalize_flair(x) for x in (compare_flairs or [])}
    per_flair_count: Dict[str, int] = {normalize_flair(x): 0 for x in (compare_flairs or [])}
    pool = []
    # prosto: weź spory bufor z /new, przefiltruj po flarach
    for p in s.new(limit=limit_per_flair * max(1, len(want))):
        f_norm = normalize_flair(getattr(p, "link_flair_text", None))
        if f_norm in want:
            pool.append(p)
            per_flair_count[f_norm] = per_flair_count.get(f_norm, 0) + 1
    return pool, per_flair_count

def _post_to_candidate(p, author_name_query: str) -> Dict[str, Any]:
    flair = getattr(p, "link_flair_text", None) or ""
    author = getattr(p.author, "name", "[deleted]") if p.author else "[deleted]"
    rel = "same_author" if author.lower() == (author_name_query or "").lower() else "different_author"
    return {
        "post_id": p.id,
        "title_raw": p.title or "",
        "permalink": f"https://www.reddit.com{p.permalink}",
        "created_utc": int(p.created_utc),
        "flair": flair,
        "author": f"u/{author}",
        "relation": rel,
    }

def match_title(
    *,
    title_raw: str,
    author_name: str,
    config: Dict[str, Any],
    max_candidates: int = 10,
    fetch_limit_per_flair: int = 200,
    exclude_post_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    exclude = set(exclude_post_ids or [])
    thr_certain = int((config.get("decision", {}) or {}).get("title_threshold_auto", 93))
    thr_border  = int((config.get("decision", {}) or {}).get("title_threshold_border", 85))

    q_norm = normalize_title(title_raw)
    q_stats = token_stats(q_norm)

    compare_flairs = (config.get("flairs", {}) or {}).get("compare_against") or ["Link Request", "Found & Shared", "Request Complete"]
    pool, per_flair = _fetch_pool(config, fetch_limit_per_flair, compare_flairs)

    candidates: List[Dict[str, Any]] = []
    for p in pool:
        if getattr(p, "id", None) in exclude:
            continue
        c = _post_to_candidate(p, author_name_query=author_name)
        c_norm = normalize_title(c["title_raw"])
        c_stats = token_stats(c_norm)
        if is_uninformative(q_stats) or is_uninformative(c_stats):
            continue
        score = safe_score(q_norm, c_norm, q_stats, c_stats)
        # UWAGA: exact TYLKO przy identycznych normach (nigdy na „score==100”)
        match_type = "exact" if (q_norm and q_norm == c_norm) else "fuzzy"
        certainty = "certain" if score >= thr_certain else ("borderline" if score >= thr_border else "low")
        c.update({
            "title_norm": c_norm,
            "norm_token_count": c_stats["n"],
            "score": int(score),
            "match_type": match_type,
            "certainty": certainty,
        })
        candidates.append(c)

    candidates.sort(key=lambda x: (x["score"], x["created_utc"]), reverse=True)
    best = candidates[0] if candidates else None

    report = {
        "title_norm": q_norm,
        "thresholds": {"certain": thr_certain, "borderline": thr_border},
        "pool_size": len(pool),
        "fetched_per_flair": per_flair,
        "candidates": candidates[:max_candidates],
        "best": best,
        "pool_ids": [getattr(p, "id", None) for p in pool if getattr(p, "id", None)],
    }
    return report

