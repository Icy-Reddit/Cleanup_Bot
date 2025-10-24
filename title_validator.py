# title_validator.py
# Heurystyczna walidacja tytułów dla r/CShortDramas
# Cel: wychwycić brak nazwy/opisu dramy w 📌 Link Request (np. "Need help finding title or link")

from __future__ import annotations
import re
import unicodedata
from typing import Dict, List, Set

# Słowa nie-niosące informacji (po normalizacji, lower-case)
GENERIC_STOPWORDS: Set[str] = {
    # ogólne prośby/słowa serwisowe
    "need", "needs", "help", "please", "pls", "plz", "anyone", "someone", "anybody",
    "trying", "try", "find", "finding", "look", "looking", "search", "searching",
    "title", "name", "link", "links", "id", "identify", "identification",
    "this", "that", "it", "one", "what", "which",
    # domenowe ogólniki
    "drama", "show", "series", "movie", "short", "shorts", "micro", "episode", "episodes",
    "english", "eng", "subs", "subtitle", "subtitles",
    # platformy / ogólniki
    "douyin", "tiktok", "youtube", "yt", "bilibili", "xiaohongshu", "xhs",
    # spójniki/zaimki itp.
    "a", "an", "the", "and", "or", "of", "for", "to", "in", "on", "at", "with",
    "is", "are", "was", "were", "be", "been", "being",
    "my", "your", "their", "his", "her", "our",
    "please,", "please.", "help.", "help,",  # czasem po znakach
}

# Wyrażenia typu „pusta prośba” – jeśli pasuje i brak innych sygnałów → MISSING
GENERIC_TITLE_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bneed\s+help\b", re.I),
    re.compile(r"\bhelp\s+me\b", re.I),
    re.compile(r"\bhelp\b.*\bfind(ing)?\b", re.I),
    re.compile(r"\bfind(ing)?\b.*\btitle\b", re.I),
    re.compile(r"\b(title|name)\b.*\blink\b", re.I),
    re.compile(r"\blooking\s+for\b", re.I),
    re.compile(r"\bany(one|body)\b.*\bknow\b", re.I),
]

# Flairy, dla których wymagamy faktycznej nazwy/opisu (pełna surowość)
STRICT_FLAIRS = {"📌 Link Request"}

def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def _normalize_text(s: str) -> str:
    s = _nfkc(s or "")
    # Usuwamy nadmiarową interpunkcję (zachowujemy cyfry/litery/CJK)
    s = re.sub(r"[^\w\s\u4e00-\u9fff\u3040-\u30ff]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokens(s: str) -> List[str]:
    if not s:
        return []
    return [t for t in s.lower().split() if t]

def _informative_tokens(tokens: List[str]) -> List[str]:
    return [t for t in tokens if t not in GENERIC_STOPWORDS and len(t) >= 2]

def _has_strong_signal(tokens: List[str]) -> bool:
    """
    Silne sygnały, że tytuł niesie konkretną informację:
    - numer identyfikacyjny / rok / 4+ cyfry (np. 11735),
    - mix liter i cyfr (np. s02e03, ep10),
    - obecność co najmniej 2 sensownych tokenów >=4 znaków po odcięciu stopwordów.
    """
    if any(re.fullmatch(r"\d{4,}", t) for t in tokens):
        return True
    if any(re.search(r"[A-Za-z]\d|\d[A-Za-z]", t) for t in tokens):
        return True
    informative = _informative_tokens(tokens)
    if sum(1 for t in informative if len(t) >= 4) >= 2:
        return True
    return False

def _looks_like_generic_request(raw: str) -> bool:
    return any(p.search(raw) for p in GENERIC_TITLE_PATTERNS)

def validate_title(title: str, flair: str = "", config: Dict = None) -> Dict[str, str]:
    """
    Zwraca dict: {"status": "OK|AMBIGUOUS|MISSING", "reason": "<krótki_powód>"}
    - Dla 📌 Link Request: wymagamy nazwy/opisu; „puste” prośby klasyfikujemy jako MISSING.
    - Dla Inquiry: nieco łagodniej, ale tu i tak robimy tylko walidację (matcher off).
    """
    flair = (flair or "").strip()
    title_raw = (title or "").strip()

    if not title_raw:
        return {"status": "MISSING", "reason": "empty_title"}

    title_norm = _normalize_text(title_raw)
    toks = _tokens(title_norm)
    informative = _informative_tokens(toks)

    # Szybkie ścieżki
    if flair in STRICT_FLAIRS:
        # 1) Ekstremalnie krótkie po usunięciu stopwordów
        if len(informative) < 2 and not _has_strong_signal(toks):
            # Przykłady: "Need help finding title", "Help with link"
            return {"status": "MISSING", "reason": "generic_title"}

        # 2) Dopasowanie do typowych „pustych próśb” bez sygnałów
        if _looks_like_generic_request(title_norm) and not _has_strong_signal(toks):
            return {"status": "MISSING", "reason": "generic_title"}

        # 3) Bardzo krótki tytuł (1 słowo) bez cyfr → mało informacyjny
        if len(toks) <= 2 and not any(ch.isdigit() for ch in title_norm):
            # wyjątek: pojedynczy, mocny wyraz + cyfra/liczba przejdą
            if len(informative) < 1:
                return {"status": "AMBIGUOUS", "reason": "too_short_after_filter"}

    else:
        # Łagodniejsze zasady dla innych flairów (np. Inquiry)
        if len(informative) == 0 and not _has_strong_signal(toks):
            return {"status": "AMBIGUOUS", "reason": "uninformative"}

    # Jeśli przeszło powyższe filtry — wygląda OK
    return {"status": "OK", "reason": "title_candidate"}
