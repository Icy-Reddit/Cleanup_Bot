# title_validator.py
# Heurystyczna walidacja tytułów dla r/CShortDramas

from __future__ import annotations
import re
import unicodedata
from typing import Dict, List, Set

# ----------------------------- Słowniki / wzorce -----------------------------

GENERIC_STOPWORDS: Set[str] = {
    "need", "needs", "help", "please", "pls", "plz", "anyone", "someone", "anybody",
    "trying", "try", "find", "finding", "look", "looking", "search", "searching",
    "title", "name", "link", "links", "id", "identify", "identification",
    "this", "that", "it", "one", "what", "which",
    "these", "those",
    "drama", "show", "series", "movie", "short", "shorts", "micro", "episode", "episodes",
    "english", "eng", "subs", "subtitle", "subtitles",
    "douyin", "tiktok", "youtube", "yt", "bilibili", "xiaohongshu", "xhs",
    "a", "an", "the", "and", "or", "of", "for", "to", "in", "on", "at", "with",
    "is", "are", "was", "were", "be", "been", "being",
    "my", "your", "their", "his", "her", "our",
    "please,", "please.", "help.", "help,",
}

SUSPECT_HINTS: Set[str] = {
    "help", "title", "link", "looking", "need", "pls", "please", "find", "finding"
}

GENERIC_TITLE_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bneed\s+help\b", re.I),
    re.compile(r"\bhelp\s+me\b", re.I),
    re.compile(r"\bhelp\b.*\bfind\w*\b", re.I),  # help ... find/finds/finding/finde...
    re.compile(r"\bfind(ing)?\b.*\btitle\b", re.I),
    re.compile(r"\b(title|name)\b.*\blink\b", re.I),
    re.compile(r"\blooking\s+for\b", re.I),
    re.compile(r"\bany(one|body)\b.*\bknow\b", re.I),
    re.compile(r"\bdoes\s+anyone\s+know\s+(its|the)\s+name\b", re.I),
    re.compile(r"\banyone\s+know\s+(the\s+)?name\b", re.I),

    re.compile(r"\blooking\s+for\s+(title|link)\b", re.I),

    re.compile(r"\bwhere\s+(can\s+)?(i\s+)?watch\b", re.I),
    re.compile(r"\bwhere\s+to\s+watch\b", re.I),
    re.compile(r"\bwhere\s+(can\s+)?(i\s+)?find\b", re.I),

    re.compile(r"\b(i\s+)?do(?:n'?|’)?t\s+know\s+(the\s+)?(title|name)\b", re.I),
    re.compile(r"\b(i\s+)?do\s+not\s+know\s+(the\s+)?(title|name)\b", re.I),
    re.compile(r"\bunknown\s+(title|name)\b", re.I),

    re.compile(r"\bwhat\s+title\b", re.I),
    re.compile(r"\bwhat\s+is\s+the\s+title\b", re.I),

    # Nietypowe frazy z wcześniejszych hotfixów
    re.compile(r"\bdo\s+anyone\s+know\s+where\s+to\s+find\b", re.I),
    re.compile(r"\bany(one|body)\s+know\s+where\s+(the\s+)?full\s+one\s+is\b", re.I),
    re.compile(r"\bany(one|body)\s+know\s+where\s+to\s+(watch|find)\b", re.I),
    re.compile(r"\bdoes\s+any(one|body)\s+have\s+a\s+link\b", re.I),

    # Wariant z „name in English or the link”
    re.compile(r"\bif\s+some(?:one)?\s+has\s+the\s+name\b", re.I),
    re.compile(r"\bname\s+in\s+english\s+or\s+the\s+link\b", re.I),
    re.compile(r"\b(has|have)\s+the\s+name\s+in\s+english\b", re.I),
]

STRICT_FLAIRS = {"📌 Link Request"}

# ----------------------------- Normalizacja / tokeny -----------------------------

def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def _normalize_text(s: str) -> str:
    s = _nfkc(s or "")
    s = re.sub(r"[^\w\s\-\,\.\u4e00-\u9fff\u3040-\u30ff]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokens(s: str) -> List[str]:
    if not s:
        return []
    return [t for t in s.split() if t]

def _ltoken(t: str) -> str:
    return t.lower()

def _informative_tokens(tokens: List[str]) -> List[str]:
    return [t for t in map(_ltoken, tokens) if t not in GENERIC_STOPWORDS and len(t) >= 2]

# ----------------------------- Heurystyki -----------------------------

def _has_strong_signal(tokens: List[str]) -> bool:
    s = " ".join(tokens)
    if re.search(r"[\u4e00-\u9fff\u3040-\u30ff]", s):
        return True
    if any(re.fullmatch(r"\d{4,}", t) for t in tokens):
        return True
    if any(re.search(r"[A-Za-z]\d|\d[A-Za-z]", t) for t in tokens):
        return True
    informative = _informative_tokens(tokens)
    if sum(1 for t in informative if len(t) >= 4) >= 2:
        return True
    return False

def _token_is_hyphen_title(tok: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]{2,}\-[A-Za-z]{2,}", tok))

def _titlecase_ratio(tokens: List[str]) -> float:
    if not tokens:
        return 0.0
    tc = 0
    for t in tokens:
        if len(t) >= 2 and t[0].isalpha() and t[0].upper() == t[0]:
            tc += 1
    return tc / max(1, len(tokens))

def _has_suspect_word(tokens: List[str]) -> bool:
    tl = [t.lower() for t in tokens]
    if any(t in SUSPECT_HINTS for t in tl):
        return True
    if any(re.fullmatch(r"find\w*", t) for t in tl):
        return True
    for t in tl:
        if re.fullmatch(r"(?:p?l?e?a?se|pls|plz|pleez|llease)", t):
            return True
    return False

def _looks_like_generic_request(s_norm: str) -> bool:
    return any(p.search(s_norm) for p in GENERIC_TITLE_PATTERNS)

# ----------------------------- Inquiry -----------------------------

def is_generic_inquiry(title: str) -> bool:
    title_raw = (title or "").strip()
    if not title_raw:
        return True
    title_norm = _normalize_text(title_raw)
    toks = _tokens(title_norm)
    if not toks:
        return True
    if _has_strong_signal(toks):
        return False
    if _looks_like_generic_request(title_norm):
        return True
    if _has_suspect_word(toks):
        return True
    return False

# ----------------------------- Link Request: generica -----------------------------

def _looks_like_generic_placeholder(title: str) -> bool:
    """
    Wykrywa puste/ogólne tytuły. Wyjątki (NIE-generic): CJK / 4+ cyfry / s02e03 / ep12 / tytuł w cudzysłowie.
    Priorytet: wyraźne wzorce generic > strong_signal.
    """
    if not title:
        return True
    t_raw = title.strip()
    t_norm = _normalize_text(t_raw)
    toks = _tokens(t_norm)

    if not toks:
        return True
    # wcześniejsza reguła: cytat z tytułem → nie generic
    if re.search(r"[\"“][^\"“]{3,}?[\"”]", t_raw):
        return False
    # najpierw sprawdź wyraźne wzorce generic
    if _looks_like_generic_request(t_norm):
        return True
    # dopiero potem "strong signals"
    if _has_strong_signal(toks):
        return False

    informative = _informative_tokens(toks)
    if len(informative) <= 2 and _has_suspect_word(toks):
        return True
    return False

# ----------------------------- Fast-path -----------------------------

_SPLIT_AFTER = re.compile(r"\s*[:;|\-–—\.]\s+")

def _extract_trailing_candidate(title_raw: str) -> str | None:
    if not title_raw:
        return None
    parts = _SPLIT_AFTER.split(title_raw, maxsplit=1)
    if len(parts) < 2:
        return None
    tail = parts[-1].strip().strip('“”"')
    if not tail:
        return None
    tail_norm = _normalize_text(tail)
    toks = _tokens(tail_norm)
    if not toks:
        return None
    word_cnt = len(toks)
    if not (2 <= word_cnt <= 10):
        return None
    if all(_ltoken(t) in GENERIC_STOPWORDS for t in toks):
        return None
    if any(_token_is_hyphen_title(t) for t in toks) or any(len(t) >= 4 for t in toks):
        return tail.strip()
    return None

# nowa funkcja pomocnicza (tuż obok _extract_trailing_candidate)
_INLINE_TITLE = re.compile(
    r"\b(?:it\s+says|it\s+is\s+called|called|titled|aka|aka\s+as|name(?:d)?\s+is)\s+([A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){1,6})",
    re.I
)

def _extract_inline_candidate(title_raw: str) -> str | None:
    """
    Szuka tytułu wbudowanego w zdaniu bez separatora, np.:
      - "It says Love Beyond Fate but I cannot find it"
      - "Named is Hidden Marriage"
      - "It is called The Stand-In"
    Zwraca fragment 'Title Case' 2-7 słów, jeśli wygląda na nazwę.
    """
    if not title_raw:
        return None
    m = _INLINE_TITLE.search(title_raw.strip())
    if m:
        cand = m.group(1).strip().strip('“”"')
        # bardzo zachowawcze sito: 2–7 słów, min. jeden token ≥4 znaki
        toks = cand.split()
        if 2 <= len(toks) <= 7 and any(len(t) >= 4 for t in toks):
            return cand
    return None    

# ----------------------------- Walidator -----------------------------

def validate_title(title: str, flair: str = "", config: Dict = None) -> Dict[str, str]:
    flair = (flair or "").strip()
    title_raw = (title or "").strip()

    if flair == "📌 Link Request":
        # [NOWE] najpierw sprawdź, czy w zdaniu jest „inline title” (np. "It says Love Beyond Fate")
        inline = _extract_inline_candidate(title_raw)
        if inline:
            return {"status": "AMBIGUOUS", "reason": "inline_candidate"}

        if _looks_like_generic_placeholder(title_raw):
            trailing = _extract_trailing_candidate(title_raw)
            if trailing:
                return {"status": "AMBIGUOUS", "reason": "generic_prefix_with_candidate"}
            return {"status": "MISSING", "reason": "generic_placeholder"}

    if not title_raw:
        return {"status": "MISSING", "reason": "empty_title"}

    title_norm = _normalize_text(title_raw)
    toks = _tokens(title_norm)
    if not toks:
        return {"status": "MISSING", "reason": "empty_after_norm"}

    informative = _informative_tokens(toks)

    if flair in STRICT_FLAIRS:
        # wzorzec generic ma priorytet (nie warunkujemy już strong_signal)
        if _looks_like_generic_request(title_norm):
            trailing = _extract_trailing_candidate(title_raw)
            if trailing:
                return {"status": "AMBIGUOUS", "reason": "generic_prefix_with_candidate"}
            return {"status": "MISSING", "reason": "generic_title"}

        words_cnt = len(toks)
        has_hyphen_title = any(_token_is_hyphen_title(t) for t in toks)
        titlecase_ratio = _titlecase_ratio(toks)
        has_suspect = _has_suspect_word(toks)
        has_strong = _has_strong_signal(toks)
        long_informative = [t for t in informative if len(t) >= 4]

        if has_hyphen_title or (words_cnt <= 3 and len(long_informative) >= 1 and titlecase_ratio >= 0.5):
            return {"status": "OK", "reason": "looks_like_title"}

        if not has_strong and len(informative) < 2:
            if not has_suspect and len(long_informative) >= 1:
                return {"status": "AMBIGUOUS", "reason": "short_but_clean"}
            return {"status": "MISSING", "reason": "generic_title"}

        return {"status": "OK", "reason": "title_candidate"}

    if len(informative) == 0 and not _has_strong_signal(toks):
        return {"status": "AMBIGUOUS", "reason": "uninformative"}
    return {"status": "OK", "reason": "title_candidate"}
