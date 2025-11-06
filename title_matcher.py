# title_matcher.py
# r/CShortDramas — Title Matcher (text only)
#
# - Robust to call from recent_scan_live.py adapters:
#   * match_title_for_post(post, config=None, **kwargs)
#   * match_title(title_raw=..., author_name=..., subreddit=..., reddit=..., config=None, **kwargs)
# - CJK-safe normalization + normalized-exact short-circuit
# - Fuzzy on normalized strings (rapidfuzz.token_set_ratio)
# - Candidate pool from recent subreddit posts with flairs:
#     📌 Link Request, 🔗 Found & Shared, ✅ Request Complete
# - Thresholds read from config.yaml (fallbacks if missing)
# - Returns a stable report: {"best": {...} or None, "top": [...], "pool_ids": [...]}

from __future__ import annotations

import unicodedata
import re
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    # minimal fallback (very rarely used; strongly recommend rapidfuzz)
    def _ratio(a: str, b: str) -> int:
        # simple normalized Levenshtein-ish ratio placeholder
        if not a and not b:
            return 100
        if not a or not b:
            return 0
        # naive token-set overlap
        sa, sb = set(a.split()), set(b.split())
        if not sa or not sb:
            return 0
        return int(100 * len(sa & sb) / max(1, len(sa | sb)))
    class fuzz:
        token_set_ratio = staticmethod(_ratio)

# ---------- Config helpers ----------

_DEFAULTS = {
    "decision": {
        "title_threshold_auto": 93,
        "title_threshold_border": 85,
        "time_window_days": 14,
    }
}

FLAIRS_DEFAULT = [
    "📌 Link Request",
    "🔗 Found & Shared",
    "✅ Request Complete",
]

def _get(cfg: Optional[dict], path: str, default: Any) -> Any:
    cur = cfg or {}
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _thresholds(cfg: Optional[dict]) -> Tuple[int, int]:
    auto_t = int(_get(cfg, "decision.title_threshold_auto", _DEFAULTS["decision"]["title_threshold_auto"]))
    border_t = int(_get(cfg, "decision.title_threshold_border", _DEFAULTS["decision"]["title_threshold_border"]))
    return auto_t, border_t

def _time_window_days(cfg: Optional[dict]) -> int:
    return int(_get(cfg, "decision.time_window_days", _DEFAULTS["decision"]["time_window_days"]))

def _flairs(cfg: Optional[dict]) -> List[str]:
    # Możesz dodać do config.yaml sekcję matcher.flairs jeśli chcesz
    fl = _get(cfg, "matcher.flairs", None)
    if isinstance(fl, list) and fl:
        return fl
    return FLAIRS_DEFAULT[:]

# ---------- Normalization (CJK-safe) ----------

_PUNCT_CATS = {"Pc", "Pd", "Pe", "Pf", "Pi", "Po", "Ps"}

def _normalize_title(s: str) -> str:
    """
    NFKC + casefold → drop Unicode punctuation → collapse whitespace.
    Leaves CJK letters/digits intact.
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).casefold()
    s = "".join(ch if unicodedata.category(ch) not in _PUNCT_CATS else " " for ch in s)
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s

# ---------- Candidate building ----------

def _utc_now() -> float:
    return time.time()

def _fetch_recent_candidates(
    reddit: Any,
    subreddit_name: str,
    window_days: int,
    limit_per_source: int = 2500,  # podniesione wg Twojej decyzji
    flairs: Optional[List[str]] = None,
    exclude_post_id: Optional[str] = None,
    exclude_post_url: Optional[str] = None,
) -> List[Any]:
    """
    Build a recent candidate pool from subreddit, filtered by time window and flair.
    Uses /new() + mod queque  — najprostsze i wystarczające (modqueue pobierasz gdzie indziej).
    """
    flairs = flairs or _flairs(None)
    out: List[Any] = []
    try:
        sub = reddit.subreddit(subreddit_name)
        now = _utc_now()
        min_ts = now - window_days * 86400
        
        # źródło 1
        for s in sub.new(limit=limit_per_source):
            try:
                # time filter
                if getattr(s, "created_utc", 0.0) < min_ts:
                    continue
                # flair filter (string match on link_flair_text)
                lf = getattr(s, "link_flair_text", None) or ""
                if lf not in flairs:
                    continue
                # exclude current post
                if exclude_post_id and getattr(s, "id", None) == exclude_post_id:
                    continue
                if exclude_post_url and getattr(s, "permalink", None) == exclude_post_url:
                    continue
                out.append(s)
            except Exception:
                continue
        
        # --- źródło 2: Mod Queue (dodatkowe kandydaty) ---
        seen_ids = {getattr(s, "id", None) for s in out}
        try:
            for s in sub.mod.modqueue(limit=limit_per_source):
                try:
                    # time filter
                    if getattr(s, "created_utc", 0.0) < min_ts:
                        continue
                    # flair filter (string match on link_flair_text)
                    lf = getattr(s, "link_flair_text", None) or ""
                    if lf not in flairs:
                        continue
                    # exclude current post
                    if exclude_post_id and getattr(s, "id", None) == exclude_post_id:
                        continue
                    if exclude_post_url and getattr(s, "permalink", None) == exclude_post_url:
                        continue
                    sid = getattr(s, "id", None)
                    if sid and sid in seen_ids:
                        continue
                    out.append(s)
                    if sid:
                        seen_ids.add(sid)
                except Exception:
                    continue
        except Exception:
            pass
    except Exception:
        # reddit or network error — return whatever we have (likely empty)
        return out
    return out

# ---------- Scoring ----------

def _relation(author_a: Optional[str], author_b: Optional[str]) -> str:
    if not author_a or not author_b:
        return "unknown"
    return "same_author" if author_a.casefold() == author_b.casefold() else "different_author"

APP_NAMES = ("shortmax", "shortwave", "dramabox", "kalos")

# zbuduj część regexu z listy powyżej
_APP_ALT = r"(?:%s)" % "|".join(APP_NAMES)

def _strip_app_context(s: str) -> str:
    """
    Usuwa kontekst typu 'on/in/via <APP>' oraz nawiasowe/końcowe wstawki z nazwą aplikacji.
    Pracuje na już znormalizowanym tekście (lower/casefold + bez interpunkcji nadmiarowej).
    """
    if not s:
        return s
    # 'on/in/via/from/at APP'
    s2 = re.sub(rf"\b(?:on|in|via|from|at)\s+{_APP_ALT}\b", " ", s, flags=re.I)
    # nawiasy/końcówki: '(APP)', '- APP', ', APP', ': APP' itp. na końcu lub prawie końcu
    s2 = re.sub(rrf"(?:[\(\[\-,:]\s*{_APP_ALT}\s*[\)\]])\s*$", " ", s2, flags=re.I)
    s2 = re.sub(rrf"\s{_APP_ALT}\s*$", " ", s2, flags=re.I)
    # porządkowanie spacji
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2

_SEG_SEP = re.compile(r"\s*(?:/|\||\baka\b|\bor\b)\s*", flags=re.I)

def _segment_variants(s: str) -> list[str]:
    """
    Dzieli znormalizowany tytuł na segmenty aliasów (np. 'A / B' → ['a', 'b']),
    przycina i składa spacje. Zwraca co najmniej 1 element (gdy brak separatorów).
    """
    if not s:
        return []
    parts = [p.strip() for p in _SEG_SEP.split(s) if p and p.strip()]
    if not parts:
        return [s.strip()]
    return parts

def _score_pair(q_norm: str, c_norm: str) -> Tuple[int, str]:
    """
    Returns (score, match_type). match_type in {"normalized_exact", "fuzzy"}.
    """
    if q_norm and c_norm and q_norm == c_norm:
        return 100, "normalized_exact"

    # exact po zdjęciu kontekstu aplikacji (Shortmax/Shortwave/Dramabox/Kalos)
    q_alt = _strip_app_context(q_norm)
    c_alt = _strip_app_context(c_norm)
    if q_alt and c_alt and q_alt == c_alt:
        return 100, "normalized_exact"

    # NOWE: exact, jeśli jedna strona równa któremuś segmentowi drugiej (A / B, A | B, A or B, A aka B)
    q_segs = _segment_variants(q_norm)
    c_segs = _segment_variants(c_norm)
    if q_segs and c_segs:
        # q == któryś segment c
        if any(q_norm == seg for seg in c_segs):
            return 100, "normalized_exact"
        # c == któryś segment q
        if any(c_norm == seg for seg in q_segs):
            return 100, "normalized_exact"
        # po zdjęciu kontekstu APP
        q_segs_alt = [_strip_app_context(seg) for seg in q_segs]
        c_segs_alt = [_strip_app_context(seg) for seg in c_segs]
        if any(q_alt == seg for seg in c_segs_alt if seg) or any(c_alt == seg for seg in q_segs_alt if seg):
            return 100, "normalized_exact"

    # w pozostałych wypadkach fuzzy
    return int(fuzz.token_set_ratio(q_norm, c_norm)), "fuzzy"

def _certainty(score: int, auto_t: int, border_t: int) -> str:
    if score >= auto_t:
        return "certain"
    if score >= border_t:
        return "borderline"
    return "low"

# ---------- Report builders ----------

def _candidate_info(s: Any) -> Dict[str, Any]:
    author_name = None
    try:
        author_name = getattr(getattr(s, "author", None), "name", None)
    except Exception:
        author_name = None
    return {
        "title": getattr(s, "title", None),
        "permalink": getattr(s, "permalink", None),
        "flair": getattr(s, "link_flair_text", None),
        "author": f"u/{author_name}" if author_name else None,
    }

def _make_entry(
    score: int,
    certainty: str,
    rel: str,
    match_type: str,
    cand: Any,
) -> Dict[str, Any]:
    ent = {
        "score": int(score),
        "certainty": certainty,
        "relation": rel,
        "type": match_type,
        "candidate": _candidate_info(cand),
    }
    return ent

def _empty_report() -> Dict[str, Any]:
    return {"best": None, "top": [], "pool_ids": []}

# ---------- Alias extraction from title ----------

_ALIAS_Q_REGEX = re.compile(r"[\"“”]([^\"“”]{3,80})[\"“”]")
_ALIAS_CALLED_REGEX = re.compile(
    r"\b(?:(?:called|titled)\s+|it(?:'s|’s)\s+called\s+)([A-Za-z0-9 \-:'“”\"&]{3,80})",
    flags=re.I,
)

def _extract_title_aliases(title: str) -> List[str]:
    """
    Zwraca listę aliasów z tytułu, aby uruchomić match także dla „czystych” wariantów:
    - z cudzysłowu: "Love Beyond Fate"
    - po słowie kluczowym: called/titled/it's called <ALIAS>
    - po łącznikach:  A or B  /  A / B  /  A | B  /  A aka B
    """
    if not title:
        return []
    txt = title.strip()

    aliases: List[str] = []

    # 1) Cudzysłowy
    for m in _ALIAS_Q_REGEX.finditer(txt):
        aliases.append(m.group(1).strip())

    # 2) called / titled / it's called
    kw = _ALIAS_CALLED_REGEX.search(txt)
    if kw:
        raw = kw.group(1)
        raw = re.split(r"[.;:|/]\s*", raw)[0]
        raw = raw.strip().strip('“”"')
        if 3 <= len(raw) <= 80:
            aliases.append(raw)

    # 3) Łączniki: or / | aka
    #    Przykład: "Use her as a cage or With her as a cage"
    parts = re.split(r"\s*(?:/|\||\baka\b|\bor\b)\s*", txt, flags=re.I)
    for p in parts:
        p = p.strip().strip('“”"')
        if 3 <= len(p) <= 80:
            aliases.append(p)

    # Dedup (case-insensitive) i bez kopiowania pełnego oryginału
    seen = set()
    out: List[str] = []
    orig_key = txt.lower()
    for a in aliases:
        k = a.lower()
        if k == orig_key:
            continue
        if k not in seen:
            seen.add(k)
            out.append(a)
    return out

# ---------- Public API ----------

def match_title_for_post(
    post: Any,
    config: Optional[dict] = None,
    *,
    exclude_post_id: Optional[str] = None,
    exclude_post_url: Optional[str] = None,
    fetch_per_flair: Optional[int] = None,  # kept for forward-compat; not strictly used here
) -> Dict[str, Any]:
    """
    Primary entry (used by our scanner when 'post' obiekt jest dostępny).
    """
    if not post:
        return _empty_report()

    title_raw = getattr(post, "title", "") or ""
    reddit = getattr(post, "_reddit", None)
    subreddit = getattr(getattr(post, "subreddit", None), "display_name", None)
    author_name = getattr(getattr(post, "author", None), "name", None)

    return match_title(
        title_raw=title_raw,
        author_name=author_name,
        subreddit=subreddit,
        reddit=reddit,
        config=config,
        exclude_post_id=exclude_post_id or getattr(post, "id", None),
        exclude_post_url=exclude_post_url or getattr(post, "permalink", None),
    )

def match_title(
    *,
    title_raw: Optional[str] = None,
    author_name: Optional[str] = None,
    subreddit: Optional[str] = None,
    reddit: Optional[Any] = None,
    config: Optional[dict] = None,
    exclude_post_id: Optional[str] = None,
    exclude_post_url: Optional[str] = None,
    flair_in: Optional[str] = None,             # accepted, may be unused
    post_created_utc: Optional[float] = None,   # accepted, may be unused
    fetch_per_flair: Optional[int] = None,      # accepted, may be unused
) -> Dict[str, Any]:
    """
    General entry (adapter-friendly). Works even if some context is missing.
    """
    if not title_raw or not subreddit or not reddit:
        # Not enough info to match — return empty report
        return _empty_report()

    auto_t, border_t = _thresholds(config)
    window_days = _time_window_days(config)
    flairs = _flairs(config)

    # Zbuduj warianty: pełny tytuł + aliasy z cudzysłowu/po 'called/titled'
    title_variants: List[str] = []
    title_variants.append(title_raw)
    for alias in _extract_title_aliases(title_raw):
        if alias.lower() not in [t.lower() for t in title_variants]:
            title_variants.append(alias)

    # Build pool (recent candidates)
    pool = _fetch_recent_candidates(
        reddit=reddit,
        subreddit_name=subreddit,
        window_days=window_days,
        limit_per_source=2500,  # zgodnie z Twoim ustawieniem
        flairs=flairs,
        exclude_post_id=exclude_post_id,
        exclude_post_url=exclude_post_url,
    )

    # Spróbuj dla każdego wariantu i wybierz najlepszy
    global_top_entries: List[Dict[str, Any]] = []
    pool_ids: List[str] = []
    best_entry: Optional[Dict[str, Any]] = None

    for title_q in title_variants:
        query_norm = _normalize_title(title_q)
        if not query_norm:
            continue

        scored: List[Tuple[int, str, Any, str]] = []  # (score, rel, cand, match_type)
        for cand in pool:
            try:
                cand_title = getattr(cand, "title", None) or ""
                cand_norm = _normalize_title(cand_title)
                score, mtype = _score_pair(query_norm, cand_norm)
                rel = _relation(author_name, getattr(getattr(cand, "author", None), "name", None))
                scored.append((int(score), rel, cand, mtype))
            except Exception:
                continue

        if not scored:
            continue

        # sort by score desc, then newest first (if equal)
        scored.sort(key=lambda t: (t[0], getattr(t[2], "created_utc", 0.0)), reverse=True)

        local_top: List[Dict[str, Any]] = []
        for i, (score, rel, cand, mtype) in enumerate(scored):
            certainty = _certainty(score, auto_t, border_t)
            # Bezpiecznik: 'certain' tylko dla normalized_exact (pełna równość po normalizacji)
            if mtype != "normalized_exact" and certainty == "certain":
                certainty = "borderline"

            entry = _make_entry(score, certainty, rel, mtype, cand)
            if i < 3:
                local_top.append(entry)
            if best_entry is None or entry["score"] > best_entry["score"]:
                best_entry = entry

            pid = getattr(cand, "id", None)
            if pid:
                pool_ids.append(pid)

        # dołącz lokalny top (zachowujemy do 3 z każdego wariantu, aby log był informacyjny)
        global_top_entries.extend(local_top)

    # Przytnij globalny top do 3 najlepszych po score (dla czytelności)
    if global_top_entries:
        global_top_entries.sort(key=lambda e: e["score"], reverse=True)
        global_top_entries = global_top_entries[:3]

    if not best_entry:
        return _empty_report()

    return {
        "best": best_entry,
        "top": global_top_entries,
        "pool_ids": pool_ids,
    }
