# decision_engine.py
# Decision Engine for r/CShortDramas Titlematch
# - bierze raport z matchera tytułu i (opcjonalnie) postera
# - zwraca decyzję: AUTO_REMOVE | MOD_QUEUE | NO_ACTION
# - kategorie: DUPLICATE, REPEATED, AMBIGUOUS, NO_SIGNAL (oraz inne, jeśli wywołujący chce)
#
# UWAGA: walidacja braków tytułu / generic (MISSING) powinna być wykonana w title_validator
#        i obsłużona wcześniej. Ten moduł rozstrzyga głównie duplicate/repeated/ambiguous.

from __future__ import annotations
from typing import Dict, Any, List, Optional


# ----------------------------- Helpers -----------------------------

def _is_title_certain(score: int, auto_threshold: int = 93) -> bool:
    """
    Konserwatywnie traktujemy 'pewność' dopasowania tytułu.
    Jeżeli wywołujący przekazuje dodatkowe info (np. 'certainty'), można to połączyć z progiem.
    """
    try:
        s = int(score)
    except Exception:
        s = 0
    return s >= int(auto_threshold)


def _links_from_title(title_report: Optional[Dict[str, Any]]) -> List[str]:
    """
    Wyciągnij link(i) kandydatów z raportu matchera tytułu, jeśli są.
    """
    if not title_report:
        return []
    links: List[str] = []
    # pojedynczy najlepszy:
    link = title_report.get("link") or title_report.get("best_link")
    if isinstance(link, str):
        links.append(_fix_reddit_link(link))
    # lista kandydatów:
    cand = title_report.get("candidates") or title_report.get("list") or []
    if isinstance(cand, list):
        for c in cand:
            if isinstance(c, dict):
                l = c.get("link")
                if isinstance(l, str):
                    links.append(_fix_reddit_link(l))
    # deduplikacja z zachowaniem kolejności
    seen = set()
    uniq = []
    for l in links:
        if l not in seen:
            uniq.append(l)
            seen.add(l)
    return uniq[:10]


def _fix_reddit_link(link: str) -> str:
    """
    Ujednolica linki w kierunku pełnego URL, jeśli przekazany jest skrót.
    """
    if not link:
        return link
    l = link.strip()
    if l.startswith("/r/"):
        return "https://www.reddit.com" + l
    return l


def _tokenize(s: str) -> List[str]:
    if not s:
        return []
    return [t for t in s.replace("/", " ").replace("_", " ").split() if t]


def _informative_overlap(a: str, b: str) -> float:
    """
    Ostrożna metryka nakładania się informacyjnych tokenów (>=4 znaki).
    """
    aset = {t.lower() for t in _tokenize(a) if len(t) >= 4}
    bset = {t.lower() for t in _tokenize(b) if len(t) >= 4}
    if not aset or not bset:
        return 0.0
    return len(aset & bset) / max(1, len(aset | bset))


# ----------------------------- Main API -----------------------------

def decide(
    *,
    flair: str,
    source: str,
    title_report: Optional[Dict[str, Any]],
    poster_report: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,   # <— nowy, ignorowany
    **_ignored,                                 # <— toleruj inne nadmiarowe pola
) -> Dict[str, Any]:
    """
    Główna funkcja Decison Engine.

    Parametry:
      flair         – tekst flaira posta (np. "📌 Link Request")
      source        – z jakiego źródła pochodzi wpis ("new" / "modqueue")
      title_report  – wynik matchera tytułu (słownik z polami: type, score, certainty, relation, title, flair, link...)
      poster_report – wynik matchera postera (opcjonalny; pola: status in {"NO_REPORT","POSSIBLE","CERTAIN"}, distance, relation)
      config        – konfiguracja (może zawierać decision.auto_threshold dla tytułu)

    Zwracany słownik ma klucze:
      action, category, reason, removal_reason, removal_comment, evidence, links
    """
    cfg = config or {}
    dcfg = (cfg.get("decision") or {})
    auto_t = int(dcfg.get("auto_threshold", 93))

    # Tytuł – bezpiecznie odczytujemy właściwości
    t = title_report or {}
    t_type = (t.get("type") or t.get("match_type") or "").lower()  # 'exact' | 'normalized_exact' | 'fuzzy' | ...
    t_score = int(t.get("score", 0))
    t_cert = (t.get("certainty") or "").lower()  # 'certain' | 'borderline' | 'low' | ''
    t_rel = (t.get("relation") or "").lower()    # 'same_author' | 'different_author' | 'unknown'
    t_title_a = t.get("title_a") or t.get("title_left") or t.get("title_src") or ""
    t_title_b = t.get("title_b") or t.get("title_right") or t.get("title_dst") or t.get("title") or ""
    t_evd = t

    # Poster – jeśli jest
    p = poster_report or {}
    p_status = (p.get("status") or "NO_REPORT").upper()  # 'NO_REPORT' | 'POSSIBLE' | 'CERTAIN'
    p_evd = p

    # Linki do pokazania w logu
    links = _links_from_title(t)

    # ------------------ Reguły DUPLICATE / REPEATED ------------------

    # 2) Duplicate / Repeated (title certain or poster CERTAIN)
    # same_author / different_author / unknown

    # SAME AUTHOR:
    # - auto-remove tylko dla exact/normalized_exact z pewnością (lub gdy poster==CERTAIN),
    # - fuzzy-100 i podobne przypadki -> do obejrzenia (MOD_QUEUE).
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
            # zbyt ryzykowne, by usuwać w ciemno
            # dodatkowy bezpiecznik (opcjonalny): jeśli overlap skrajnie niski, NO_ACTION; inaczej MOD_QUEUE
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

    # DIFFERENT AUTHOR:
    # - repeated request: exact/normalized_exact (pewne) lub poster==CERTAIN -> auto-remove
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

    # UNKNOWN relacja albo nie spełniliśmy warunków auto-remove -> brak silnego sygnału z tej warstwy
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
    Opcjonalny wrapper, jeśli wywołujący przekazuje jeden słownik:
      payload = {
        "flair": str,
        "source": str,
        "title_report": dict|None,
        "poster_report": dict|None,
        "config": dict|None,
      }
    """
    return decide(
        flair=payload.get("flair", ""),
        source=payload.get("source", ""),
        title_report=payload.get("title_report"),
        poster_report=payload.get("poster_report"),
        config=payload.get("config"),
        context=payload.get("context"),   # <— dopisane
    )
