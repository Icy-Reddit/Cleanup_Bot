# decision_engine.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import re

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _cfg_get(d: Dict[str, Any], path: List[str], default: Any):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _token_stats(norm_text: str) -> dict:
    has_cjk = bool(re.search(r"[\u4e00-\u9FFF]", norm_text or ""))
    toks = [w for w in ((norm_text or "").split()) if w]
    return {"n": len(toks), "has_cjk": has_cjk, "chars": len(norm_text or "")}

# Komentarze – fallback (mogą być nadpisane w config.yaml)
REPEATED_COMMENT_FALLBACK = (
    "This link was already requested, you may have overlooked it. Please scroll in link requests or try keywords or title in the search bar. "
    "You may get lucky and it’s already found — or you click follow when you find the earlier post and we’re all fine.\n"
    "How to Use the Search Bar in Our Subreddit: "
    "https://www.reddit.com/r/CShortDramas/comments/1ig6a8j/how_to_use_the_search_bar_in_our_subreddit/"
)

MISSING_TITLE_FALLBACK = (
    "Your post has been removed because it doesn't include the drama name or a short description in the header. "
    "This is required to keep the subreddit organized and help others find and fulfill similar requests.\n"
    "If the name is visible on the poster, just add it to the header.\n"
    "If there’s no name, please add a short description instead. Think of what caught your attention, add the genre, storyline, actor’s name, or a brief summary of what you saw in the ad.\n"
    "Why do we ask this? Because search terms like “link” or “Do you know the title?” aren’t helpful for others looking for the same drama."
)

# ---- subset/short-title guard helpers ----
_CONTENT_STOP = {
    "the","a","an","to","of","for","and","or","with","in","on","at","by",
    "my","your","our","their","his","her","him","me","you","we","they",
    "is","are","was","were","be","been","being","do","does","did","done",
    "this","that","these","those","from","as","but","if","then","so","not",
}

def _split_tokens(s: str) -> List[str]:
    return [t for t in (s or "").split() if t]

def _content_tokens(s: str) -> List[str]:
    return [t for t in _split_tokens(s) if t not in _CONTENT_STOP]

def _coverage(a_norm: str, b_norm: str):
    """Pokrycie na treściowych tokenach: (pokrycie_query, pokrycie_kandydata, overlap, |qa|, |qb|)"""
    a = set(_content_tokens(a_norm))
    b = set(_content_tokens(b_norm))
    if not a or not b:
        return 0.0, 0.0, 0, 0, 0
    overlap = len(a & b)
    cov_a = overlap / max(1, len(a))
    cov_b = overlap / max(1, len(b))
    return cov_a, cov_b, overlap, len(a), len(b)

def decide(
    *,
    title_validation: Dict[str, Any],
    title_report: Optional[Dict[str, Any]],
    poster_report: Optional[Dict[str, Any]],
    context: Dict[str, Any],
    config: Dict[str, Any],
) -> Dict[str, Any]:

    thr_title_auto   = int(_cfg_get(config, ["decision", "title_threshold_auto"], 93))
    thr_title_border = int(_cfg_get(config, ["decision", "title_threshold_border"], 85))

    # parametry guardów (możesz ustawić w config.yaml)
    short_title_max_tokens = int(_cfg_get(config, ["decision","short_title_max_tokens"], 3))
    min_cov_query_for_auto = float(_cfg_get(config, ["decision","min_cov_query_for_auto"], 0.90))
    min_cov_cand_for_auto  = float(_cfg_get(config, ["decision","min_cov_cand_for_auto"], 0.50))

    repeated_tmpl = _cfg_get(config, ["comments", "repeated_request_template"], REPEATED_COMMENT_FALLBACK)
    missing_tmpl  = _cfg_get(config, ["comments", "missing_title_template"], MISSING_TITLE_FALLBACK)

    ev_title = {"type": "none", "score": None, "certainty": "none", "relation": "unknown", "candidate": None}
    ev_postr = {"status": "NO_REPORT", "distance": None, "relation": "unknown", "candidate": None}
    links: List[str] = []

    # 1) Brak tytułu → auto-remove
    if title_validation.get("status") == "MISSING":
        rr = "Lack of Drama Name or Description in Title"
        return {
            "when": _now_utc_iso(),
            "action": "AUTO_REMOVE",
            "category": "MISSING",
            "removal_reason": rr,
            "removal_comment": missing_tmpl,
            "evidence": {"title_match": ev_title, "poster_match": ev_postr},
            "links": links,
        }

    # 2) Wyciągnij najlepszy tytułowy kandydat
    best_t = (title_report or {}).get("best") if title_report else None
    if best_t:
        ev_title["type"] = "exact" if best_t.get("match_type") == "exact" else "fuzzy"
        ev_title["score"] = int(best_t.get("score", 0))
        ev_title["certainty"] = best_t.get("certainty", "low")
        ev_title["relation"] = best_t.get("relation", "unknown")
        ev_title["candidate"] = best_t
        if best_t.get("permalink"):
            links.append(best_t["permalink"])

    # 3) Guard: nie rób auto-remove na podstawie „pustych” tytułów
    title_auto_allowed = True
    if ev_title["candidate"]:
        cand_norm = ev_title["candidate"].get("title_norm") or ""
        cand_stats = _token_stats(cand_norm)
        if (cand_stats["n"] < 2) and (not cand_stats["has_cjk"]):
            title_auto_allowed = False

    # 4) Subset/short-title guard dla different_author
    q_norm   = (title_report or {}).get("title_norm", "") if title_report else ""
    cand_norm = (best_t or {}).get("title_norm", "") if best_t else ""
    cov_q, cov_c, ov, qn, cn = _coverage(q_norm, cand_norm)

    subsety = (
        ev_title["candidate"] is not None
        and ev_title["relation"] == "different_author"
        and ev_title["type"] != "exact"  # exact → tylko identyczne normy
        and (
            qn <= short_title_max_tokens       # bardzo krótki tytuł zapytania
            or cov_q < min_cov_query_for_auto  # zapytanie nie jest niemal w całości pokryte
            or cov_c < min_cov_cand_for_auto   # kandydat ma dużo „nadmiaru” treści
        )
    )

    # 5) Decyzje progowe
    if ev_title["score"] is not None and ev_title["score"] >= thr_title_auto and title_auto_allowed:
        if ev_title["relation"] == "same_author":
            return _mk_decision("DUPLICATE", "Duplicate Post", None, ev_title, ev_postr, links)
        if ev_title["relation"] == "different_author":
            if subsety:
                return _mk_queue("AMBIGUOUS", "High score but subset/short-title match; needs review.", ev_title, ev_postr, links)
            return _mk_decision("REPEATED", "Repeated Request", repeated_tmpl, ev_title, ev_postr, links)

    if ev_title["score"] is not None and thr_title_border <= ev_title["score"] < thr_title_auto:
        return _mk_queue("AMBIGUOUS", "Title match borderline without strong poster confirmation.", ev_title, ev_postr, links)

    # Brak sygnału
    return {
        "when": _now_utc_iso(),
        "action": "NO_ACTION",
        "category": "NO_SIGNAL",
        "removal_reason": None,
        "removal_comment": None,
        "evidence": {"title_match": ev_title, "poster_match": ev_postr},
        "links": links,
    }

def _mk_decision(category: str, rr: str, comment: str | None, ev_title, ev_postr, links):
    return {
        "when": _now_utc_iso(),
        "action": "AUTO_REMOVE",
        "category": category,
        "removal_reason": rr,
        "removal_comment": comment,
        "evidence": {"title_match": ev_title, "poster_match": ev_postr},
        "links": links,
    }

def _mk_queue(category: str, reason: str, ev_title, ev_postr, links):
    return {
        "when": _now_utc_iso(),
        "action": "MOD_QUEUE",
        "category": category,
        "removal_reason": reason,
        "removal_comment": None,
        "evidence": {"title_match": ev_title, "poster_match": ev_postr},
        "links": links,
    }

def pretty_print_decision(report: Dict[str, Any]) -> None:
    print("=============== DECISION ENGINE ===============")
    print(f"When: {report.get('when')}")
    print(f"Action: {report.get('action')} | Category: {report.get('category')}")
    print(f"Removal Reason: {report.get('removal_reason')}")
    if report.get("removal_comment"):
        print("Removal Comment:")
        print(report["removal_comment"])
    ev = report.get("evidence", {})
    tm = ev.get("title_match", {})
    pm = ev.get("poster_match", {})
    print("\n-- Title Match --")
    print(f"type={tm.get('type')} | score={tm.get('score')} | certainty={tm.get('certainty')} | relation={tm.get('relation')}")
    if tm.get("candidate") and isinstance(tm["candidate"], dict):
        c = tm["candidate"]
        print(f"  title='{c.get('title_raw')}' | flair={c.get('flair')} | author={c.get('author')}")
        print(f"  link={c.get('permalink')}")
    print("\n-- Poster Match --")
    print(f"status={pm.get('status')} | distance={pm.get('distance')} | relation={pm.get('relation')}")
    if pm.get("candidate") and isinstance(pm["candidate"], dict):
        c = pm["candidate"]
        print(f"  title='{c.get('title')}' | flair={c.get('flair')} | author={c.get('author')}")
        print(f"  link={c.get('permalink')}")
    links = report.get("links") or []
    if links:
        print("\nLinks:")
        for u in links:
            print(f"- {u}")
    print("===============================================")
