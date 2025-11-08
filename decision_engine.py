# decision_engine.py
# Decision Engine for r/CShortDramas Titlematch
# Zwraca decyzję: AUTO_REMOVE | MOD_QUEUE | NO_ACTION
# Kategorie: DUPLICATE, REPEATED, AMBIGUOUS, NO_SIGNAL
from __future__ import annotations
from typing import Dict, Any, List, Optional

# ----------------------------- Helpers -----------------------------

def _is_title_certain(score: int, auto_threshold: int = 93) -> bool:
    try:
        s = int(score)
    except Exception:
        s = 0
    return s >= int(auto_threshold)

def _fix_reddit_link(link: str) -> str:
    if not link:
        return link
    l = link.strip()
    if l.startswith("/r/"):
        return "https://www.reddit.com" + l
    return l

def _links_from_title(title_report: Optional[Dict[str, Any]]) -> List[str]:
    if not title_report:
        return []
    links: List[str] = []
    link = title_report.get("link") or title_report.get("best_link")
    if isinstance(link, str):
        links.append(_fix_reddit_link(link))
    cand = title_report.get("candidates") or title_report.get("list") or []
    if isinstance(cand, list):
        for c in cand:
            if isinstance(c, dict):
                l = c.get("link")
                if isinstance(l, str):
                    links.append(_fix_reddit_link(l))
    # deduplikacja
    seen = set()
    uniq = []
    for l in links:
        if l not in seen:
            uniq.append(l)
            seen.add(l)
    return uniq[:10]

def _tokenize(s: str) -> List[str]:
    if not s:
        return []
    return [t for t in s.replace("/", " ").replace("_", " ").split() if t]

def _informative_overlap(a: str, b: str) -> float:
    aset = {t.lower() for t in _tokenize(a) if len(t) >= 4}
    bset = {t.lower() for t in _tokenize(b) if len(t) >= 4}
    if not aset or not bset:
        return 0.0
    return len(aset & bset) / max(1, len(aset | bset))

# ----------------------------- Main API -----------------------------

def decide(
    *,
    flair: str = "",
    source: str = "",
    title_report: Optional[Dict[str, Any]] = None,
    poster_report: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,  # ignorowane, ale akceptowane
    **_ignored,                                # toleruj inne nadmiarowe pola
) -> Dict[str, Any]:
    """
    Główna funkcja Decision Engine (odporna na brak 'flair' i 'source').
    """
    cfg = config or {}
    dcfg = (cfg.get("decision") or {})
    auto_t = int(dcfg.get("auto_threshold", 93))

    t = title_report or {}
    t_type = (t.get("type") or t.get("match_type") or "").lower()     # 'exact' | 'normalized_exact' | 'fuzzy' | ...
    t_score = int(t.get("score", 0))
    t_rel = (t.get("relation") or "").lower()                         # 'same_author' | 'different_author' | 'unknown'
    t_title_a = t.get("title_a") or t.get("title_left") or t.get("title_src") or ""
    t_title_b = t.get("title_b") or t.get("title_right") or t.get("title_dst") or t.get("title") or ""
    t_evd = t

    p = poster_report or {}
    p_status = (p.get("status") or "NO_REPORT").upper()               # 'NO_REPORT' | 'POSSIBLE' | 'CERTAIN'
    p_evd = p

    links = _links_from_title(t)

    # ------------------ Reguły DUPLICATE / REPEATED ------------------

    # SAME AUTHOR:
    # - auto-remove tylko dla exact/normalized_exact z progiem pewności
    #   lub gdy poster==CERTAIN
    # - w pozostałych: gdy overlap bardzo niski -> NO_ACTION, inaczej -> MOD_QUEUE
    if t_rel == "same_author":
        cond_title_strict = (t_type in ("exact", "normalized_exact")) and _is_title_certain(t_score, auto_t)
        cond_poster_strict = (p_status == "CERTAIN")
        if cond_title_strict or cond_poster_strict:
            return {
                "action": "AUTO_REMOVE",
                "category": "DUPLICATE",
                "reason": "Duplicate: same author and either normalized/exact title match is certain or poster is CERTAIN.",
                "removal_reason": "Duplicate Post",
                "removal_comment": None,
                "evidence": {"title_match": t_evd, "poster_match": p_evd},
                "links": links,
            }
        else:
            overlap = _informative_overlap(t_title_a, t_title_b)
            if overlap < 0.25:
                return {
                    "action": "NO_ACTION",
                    "category": "NO_SIGNAL",
                    "reason": "Same author but low informative overlap; skipping auto action.",
                    "removal_reason": None,
                    "removal_comment": None,
                    "evidence": {"title_match": t_evd, "poster_match": p_evd},
                    "links": links,
                }
            return {
                "action": "MOD_QUEUE",
                "category": "AMBIGUOUS",
                "reason": "Same author but not exact/normalized_exact title match; needs mod review.",
                "removal_reason": None,
                "removal_comment": None,
                "evidence": {"title_match": t_evd, "poster_match": p_evd},
                "links": links,
            }

    # DIFFERENT AUTHOR → REPEATED (strict tytuł lub poster CERTAIN)
    if t_rel == "different_author":
        cond_title_strict = (t_type in ("exact", "normalized_exact")) and _is_title_certain(t_score, auto_t)
        cond_poster_strict = (p_status == "CERTAIN")
        if cond_title_strict or cond_poster_strict:
            return {
                "action": "AUTO_REMOVE",
                "category": "REPEATED",
                "reason": "Repeated request: different author and either normalized/exact title match is certain or poster is CERTAIN.",
                "removal_reason": "Repeated Request",
                "removal_comment": None,
                "evidence": {"title_match": t_evd, "poster_match": p_evd},
                "links": links,
            }

    # UNKNOWN relacja albo brak silnych warunków auto-remove
    return {
        "action": "NO_ACTION",
        "category": "NO_SIGNAL",
        "reason": "No strong signals from title and poster.",
        "removal_reason": None,
        "removal_comment": None,
        "evidence": {"title_match": t_evd, "poster_match": p_evd},
        "links": links,
    }

# ----------------------------- Convenience wrapper -----------------------------

def run_decision_engine(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Przyjmij jeden słownik i przekaż dalej (odporne na dodatkowe klucze).
    """
    # Pozwól decide zignorować nadmiarowe pola (np. 'context')
    return decide(**payload)
