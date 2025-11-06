# title_matcher.py
# r/CShortDramas — Title Matcher (text only)
#
# Public API:
#   - match_title_for_post(post, config=None, **kwargs) -> dict
#   - match_title(title_raw=..., author_name=..., subreddit=..., reddit=..., config=None, **kwargs) -> dict
#
# Cechy:
# - Normalizacja CJK-safe
# - Short-circuit exact:
#     * identyczne znormalizowane ciągi,
#     * po zdjęciu kontekstu app (shortmax/shortwave/dramabox/kalos),
#     * po usunięciu spacji (np. "stand in" == "standin"),
#     * równość z segmentem (A / B | A or B | A aka B), także po powyższych transformacjach
# - Fuzzy: średnia z 2 najlepszych z {token_set_ratio, token_sort_ratio, ratio}, cap=97
# - Lekka kara dla konfliktujących słów (np. fiancée vs husband/wife)
# - Alias extraction: cudzysłowy, called/titled/it's called, split po / | aka or, oraz
#   ostrożny prefix przed pierwszą kropką lub " - "
# - Candidate pool:
#     * /new + modqueue
#     * filtr flaira odporny na emoji/variation selectors/synonimy
#     * FALLBACK, gdy pool puste: luzuj filtr flaira i dorzuć subreddit.search(...)
#       po 3–5 tokenach z bieżącego tytułu, time_filter=week
#
from __future__ import annotations

import unicodedata
import re
import time
from typing import Any, Dict, List, Optional, Tuple

# ------------- RapidFuzz -------------
try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    def _ratio(a: str, b: str) -> int:
        if not a and not b:
            return 100
        if not a or not b:
            return 0
        sa, sb = set(a.split()), set(b.split())
        if not sa or not sb:
            return 0
        return int(100 * len(sa & sb) / max(1, len(sa | sb)))

    class fuzz:  # type: ignore
        token_set_ratio = staticmethod(_ratio)
        token_sort_ratio = staticmethod(_ratio)
        ratio = staticmethod(_ratio)

# ------------- Konfiguracja -------------
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
    fl = _get(cfg, "matcher.flairs", None)
    if isinstance(fl, list) and fl:
        return fl
    return FLAIRS_DEFAULT[:]

# ------------- Normalizacja tytułu -------------
_PUNCT_CATS = {"Pc", "Pd", "Pe", "Pf", "Pi", "Po", "Ps"}

def _normalize_title(s: str) -> str:
    """
    NFKC + casefold → usuń interpunkcję (zastąp spacją) → zredukuj spacje.
    (CJK pozostaje nietknięte.)
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).casefold()
    s = "".join(ch if unicodedata.category(ch) not in _PUNCT_CATS else " " for ch in s)
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s

# ------------- Flair normalization / check -------------
_FLSynonyms = {
    "📌 link request": {"📌 link request", "link request"},
    "🔗 found & shared": {"🔗 found & shared", "found & shared"},
    "✅ request complete": {"✅ request complete", "request complete"},
}

def _norm_str_cf(s: str) -> str:
    # NFKC + remove format chars (incl. variation selectors) + collapse space + lower
    s = unicodedata.normalize("NFKC", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf")
    s = " ".join(s.split())
    return s.casefold()

def _norm_flair(s: Optional[str]) -> str:
    return _norm_str_cf(s or "")

def _flair_ok(flair: Optional[str], allowed: List[str]) -> bool:
    f = _norm_flair(flair)
    if not f:
        return False
    allow_set = set()
    for a in allowed:
        k = _norm_flair(a)
        allow_set.add(k)
        allow_set |= _FLSynonyms.get(k, set())
    return f in allow_set

# ------------- Budowanie puli kandydatów -------------
def _utc_now() -> float:
    return time.time()

def _pick_query_tokens(title_raw: str, max_tokens: int = 5) -> List[str]:
    """
    Z tytułu bieżącego posta wybierz kilka sensownych tokenów do search().
    Preferuj tokeny ≥4 znaki, bez typowych stopwordów.
    """
    if not title_raw:
        return []
    norm = _normalize_title(title_raw)
    toks = [t for t in norm.split() if t]
    stop = {
        "help", "need", "link", "title", "name", "drama", "short", "series",
        "please", "pls", "plz", "anyone", "someone", "anybody", "looking",
        "where", "watch", "find", "finding", "with", "subs", "eng", "english",
        "the", "a", "an", "and", "or", "of", "for", "to", "in", "on",
    }
    good = [t for t in toks if len(t) >= 4 and t not in stop]
    if not good:
        good = toks[:]
    return good[:max_tokens]

def _fetch_recent_candidates(
    reddit: Any,
    subreddit_name: str,
    window_days: int,
    limit_per_source: int = 2500,
    flairs: Optional[List[str]] = None,
    exclude_post_id: Optional[str] = None,
    exclude_post_url: Optional[str] = None,
    title_raw_for_search: Optional[str] = None,   # NOWE: dla fallback search
) -> List[Any]:
    """
    Zbiera kandydatów z /new + modqueue. Filtruje po flairaCH (odpornie).
    Gdy pool puste → fallback: luzuj flair i dorzuć subreddit.search(...) (time_filter=week).
    """
    flairs = flairs or _flairs(None)
    out: List[Any] = []
    candidates_raw: List[Any] = []

    min_ts = _utc_now() - window_days * 86400

    # 1) Pobierz surowe /new + modqueue (bez filtra flaira, filtrujemy później)
    try:
        sub = reddit.subreddit(subreddit_name)
        for s in sub.new(limit=limit_per_source):
            try:
                if getattr(s, "created_utc", 0.0) >= min_ts:
                    candidates_raw.append(s)
            except Exception:
                continue
        try:
            for s in sub.mod.modqueue(limit=limit_per_source):
                try:
                    if getattr(s, "created_utc", 0.0) >= min_ts:
                        candidates_raw.append(s)
                except Exception:
                    continue
        except Exception:
            # brak uprawnień do modqueue lub błąd — pomiń
            pass
    except Exception:
        candidates_raw = []

    # 2) Filtr flaira — odporny na warianty
    if candidates_raw:
        seen_ids = set()
        for s in candidates_raw:
            sid = getattr(s, "id", None)
            if sid and sid in seen_ids:
                continue
            if exclude_post_id and sid and sid == exclude_post_id:
                continue
            if exclude_post_url and getattr(s, "permalink", None) == exclude_post_url:
                continue
            lf = getattr(s, "link_flair_text", None) or ""
            if _flair_ok(lf, flairs):
                out.append(s)
                if sid:
                    seen_ids.add(sid)

    # 3) FALLBACK: pusto? — luzuj filtr flaira i spróbuj search
    if not out:
        # 3a) luzuj flairy: bierz to, co w candidates_raw, z ograniczeniami
        seen_ids = set()
        for s in candidates_raw:
            sid = getattr(s, "id", None)
            if sid and sid in seen_ids:
                continue
            if exclude_post_id and sid and sid == exclude_post_id:
                continue
            if exclude_post_url and getattr(s, "permalink", None) == exclude_post_url:
                continue
            out.append(s)
            if sid:
                seen_ids.add(sid)

        # 3b) search(...) po kilku tokenach tytułu, time_filter=week
        try:
            if title_raw_for_search:
                toks = _pick_query_tokens(title_raw_for_search, max_tokens=5)
                if toks:
                    query = " ".join(toks)
                    # Uwaga: PRAW search nie ma twardego „since”, więc zawęzimy ręcznie created_utc
                    for s in sub.search(query=query, sort="new", time_filter="week", limit=50):
                        try:
                            if getattr(s, "created_utc", 0.0) < min_ts:
                                continue
                            sid = getattr(s, "id", None)
                            if sid and sid in seen_ids:
                                continue
                            if exclude_post_id and sid and sid == exclude_post_id:
                                continue
                            if exclude_post_url and getattr(s, "permalink", None) == exclude_post_url:
                                continue
                            out.append(s)
                            if sid:
                                seen_ids.add(sid)
                        except Exception:
                            continue
        except Exception:
            # search niedostępny — ignoruj
            pass

    return out

# ------------- Scoring / porównanie tytułów -------------
def _relation(author_a: Optional[str], author_b: Optional[str]) -> str:
    if not author_a or not author_b:
        return "unknown"
    return "same_author" if author_a.casefold() == author_b.casefold() else "different_author"

APP_NAMES = ("shortmax", "shortwave", "dramabox", "kalos")
_APP_ALT = r"(?:%s)" % "|".join(APP_NAMES)

def _strip_app_context(s: str) -> str:
    """
    Usuwa kontekst 'on/in/via/from/at <APP>' oraz końcowe nawiasy/APP z końca.
    Pracuje na już znormalizowanym tekście.
    """
    if not s:
        return s
    s2 = re.sub(rf"\b(?:on|in|via|from|at)\s+{_APP_ALT}\b", " ", s, flags=re.I)
    s2 = re.sub(rf"(?:[\(\[\-,:]\s*{_APP_ALT}\s*[\)\]])\s*$", " ", s2, flags=re.I)
    s2 = re.sub(rf"\s{_APP_ALT}\s*$", " ", s2, flags=re.I)
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2

_SEG_SEP = re.compile(r"\s*(?:/|\||\baka\b|\bor\b)\s*", flags=re.I)

def _segment_variants(s: str) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in _SEG_SEP.split(s) if p and p.strip()]
    if not parts:
        return [s.strip()]
    return parts

# — konfliktujące słowa-klucze (lekka kara)
_CONFLICT_PAIRS = [
    ({"fiancee", "fiancée", "fiance"}, {"husband", "wife"}),
]

def _has_conflict(a: str, b: str) -> bool:
    atoks = set(a.split())
    btoks = set(b.split())
    for left, right in _CONFLICT_PAIRS:
        if (atoks & left) and (btoks & right):
            if not ((atoks & right) and (btoks & left)):
                return True
        if (atoks & right) and (btoks & left):
            if not ((atoks & left) and (btoks & right)):
                return True
    return False

def _score_pair(q_norm: str, c_norm: str) -> Tuple[int, str]:
    """
    Zwraca (score, match_type) — match_type w {"normalized_exact", "fuzzy"}.
    """
    # 1) exact na znormalizowanym tekście
    if q_norm and c_norm and q_norm == c_norm:
        return 100, "normalized_exact"

    # 1a) exact po zdjęciu kontekstu app
    q_alt = _strip_app_context(q_norm)
    c_alt = _strip_app_context(c_norm)
    if q_alt and c_alt and q_alt == c_alt:
        return 100, "normalized_exact"

    # 1b) exact po usunięciu spacji (stand in == standin)
    if q_norm.replace(" ", "") == c_norm.replace(" ", ""):
        return 100, "normalized_exact"
    if q_alt.replace(" ", "") == c_alt.replace(" ", ""):
        return 100, "normalized_exact"

    # 1c) exact po segmentach
    q_segs = _segment_variants(q_norm)
    c_segs = _segment_variants(c_norm)
    if q_segs and c_segs:
        if any(q_norm == seg for seg in c_segs):
            return 100, "normalized_exact"
        if any(c_norm == seg for seg in q_segs):
            return 100, "normalized_exact"
        q_segs_alt = [_strip_app_context(seg) for seg in q_segs]
        c_segs_alt = [_strip_app_context(seg) for seg in c_segs]
        if any(q_alt == seg for seg in c_segs_alt if seg) or any(c_alt == seg for seg in q_segs_alt if seg):
            return 100, "normalized_exact"
        # wariant bez spacji w segmentach
        if any(q_norm.replace(" ", "") == seg.replace(" ", "") for seg in c_segs):
            return 100, "normalized_exact"
        if any(c_norm.replace(" ", "") == seg.replace(" ", "") for seg in q_segs):
            return 100, "normalized_exact"

    # 2) Fuzzy: średnia z dwóch najlepszych (set/sort/ratio)
    s_set = int(fuzz.token_set_ratio(q_norm, c_norm))
    s_sort = int(getattr(fuzz, "token_sort_ratio", fuzz.token_set_ratio)(q_norm, c_norm))
    s_char = int(getattr(fuzz, "ratio", fuzz.token_set_ratio)(q_norm, c_norm))
    trio = sorted([s_set, s_sort, s_char], reverse=True)
    score = int(round((trio[0] + trio[1]) / 2))

    # lekka kara za konfliktujące słowa
    if _has_conflict(q_norm, c_norm):
        score = max(0, score - 5)

    # cap: fuzzy nie zwraca 100
    score = min(score, 97)

    return score, "fuzzy"

def _certainty(score: int, auto_t: int, border_t: int) -> str:
    if score >= auto_t:
        return "certain"
    if score >= border_t:
        return "borderline"
    return "low"

# ------------- Report helpers -------------
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
    return {
        "score": int(score),
        "certainty": certainty,
        "relation": rel,
        "type": match_type,
        "candidate": _candidate_info(cand),
    }

def _empty_report() -> Dict[str, Any]:
    return {"best": None, "top": [], "pool_ids": []}

# ------------- Alias extraction -------------
_ALIAS_Q_REGEX = re.compile(r"[\"“”]([^\"“”]{3,80})[\"“”]")
_ALIAS_CALLED_REGEX = re.compile(
    r"\b(?:(?:called|titled)\s+|it(?:'s|’s)\s+called\s+)([A-Za-z0-9 \-:'“”\"&]{3,80})",
    flags=re.I,
)

_SPLIT_PREFIX = re.compile(r"\s*(?:[.]\s+| \-\s+)", flags=re.UNICODE)

def _extract_title_aliases(title: str) -> List[str]:
    """
    Zwraca aliasy z tytułu:
      - z cudzysłowu,
      - po słowach called/titled/it's called,
      - po łącznikach: / | aka or,
      - prefix przed pierwszą kropką lub " - " (ostrożnie).
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
    parts = re.split(r"\s*(?:/|\||\baka\b|\bor\b)\s*", txt, flags=re.I)
    for p in parts:
        p = p.strip().strip('“”"')
        if 3 <= len(p) <= 80:
            aliases.append(p)

    # 4) Prefix przed kropką lub " - " (np. "Reborn without mercy. FLs name..." → "Reborn without mercy")
    cut = _SPLIT_PREFIX.split(txt, maxsplit=1)
    if cut:
        prefix = cut[0].strip().strip('“”"')
        word_count = len(prefix.split())
        if 2 <= word_count <= 8 and 8 <= len(prefix) <= 80:
            aliases.append(prefix)

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

# ------------- Public API -------------
def match_title_for_post(
    post: Any,
    config: Optional[dict] = None,
    *,
    exclude_post_id: Optional[str] = None,
    exclude_post_url: Optional[str] = None,
    fetch_per_flair: Optional[int] = None,  # kept for fwd-compat; nieużywane tutaj
) -> Dict[str, Any]:
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
    flair_in: Optional[str] = None,             # akceptowane, niewykorzystywane
    post_created_utc: Optional[float] = None,   # akceptowane, niewykorzystywane
    fetch_per_flair: Optional[int] = None,      # akceptowane, niewykorzystywane
) -> Dict[str, Any]:
    if not title_raw or not subreddit or not reddit:
        return _empty_report()

    auto_t, border_t = _thresholds(config)
    window_days = _time_window_days(config)
    flairs = _flairs(config)

    # Warianty tytułu: pełny + aliasy
    title_variants: List[str] = [title_raw]
    for alias in _extract_title_aliases(title_raw):
        if alias.lower() not in [t.lower() for t in title_variants]:
            title_variants.append(alias)

    # Pool kandydatów (z fallbackiem przy pustej puli)
    pool = _fetch_recent_candidates(
        reddit=reddit,
        subreddit_name=subreddit,
        window_days=window_days,
        limit_per_source=2500,
        flairs=flairs,
        exclude_post_id=exclude_post_id,
        exclude_post_url=exclude_post_url,
        title_raw_for_search=title_raw,   # ważne dla fallback search
    )

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

        # sort: score desc, potem nowsze
        scored.sort(key=lambda t: (t[0], getattr(t[2], "created_utc", 0.0)), reverse=True)

        local_top: List[Dict[str, Any]] = []
        for i, (score, rel, cand, mtype) in enumerate(scored):
            certainty = _certainty(score, auto_t, border_t)
            # 'certain' tylko dla normalized_exact — fuzzy podnosimy max do 'borderline'
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

        global_top_entries.extend(local_top)

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
