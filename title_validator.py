# title_validator.py
# Heurystyczna walidacja tytułów dla r/CShortDramas
# Cel: wychwycić brak nazwy/opisu w 📌 Link Request (np. "Need help finding title or link"),
#      ale nie karać prawidłowych, krótkich tytułów typu "The stand-in".

from __future__ import annotations
import re
import unicodedata
from typing import Dict, List, Set

# ----------------------------- Słowniki / wzorce -----------------------------

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

# Słowa „podejrzane” w ultra-krótkich tytułach
SUSPECT_HINTS: Set[str] = {
    "help", "title", "link", "looking", "need", "pls", "please", "find", "finding"
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
    re.compile(r"\bdoes\s+anyone\s+know\s+(its|the)\s+name\b", re.I),
    re.compile(r"\banyone\s+know\s+(the\s+)?name\b", re.I),
    
    # Zaktualizowane wzorce dla „looking for”:
    re.compile(r"\blooking\s+for\s+(title|link)\b", re.I),  # Puste zapytania: looking for title/link

    # --- NOWE wzorce: „where … watch/find” ---
    re.compile(r"\bwhere\s+(can\s+)?(i\s+)?watch\b", re.I),   # where (can i) watch
    re.compile(r"\bwhere\s+to\s+watch\b", re.I),              # where to watch
    re.compile(r"\bwhere\s+(can\s+)?(i\s+)?find\b", re.I),    # where (can i) find

    # --- nowe: "don't know the name/title" (również z apostrofem ’) ---
    re.compile(r"\b(i\s+)?do(?:n'?|’)?t\s+know\s+(the\s+)?(title|name)\b", re.I),
    re.compile(r"\b(i\s+)?do\s+not\s+know\s+(the\s+)?(title|name)\b", re.I),
    re.compile(r"\bunknown\s+(title|name)\b", re.I),

]

# Flairy, dla których wymagamy faktycznej nazwy/opisu (pełna surowość)
STRICT_FLAIRS = {"📌 Link Request"}

# ----------------------------- Normalizacja / tokeny -----------------------------

def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def _normalize_text(s: str) -> str:
    s = _nfkc(s or "")
    # Usuwamy nadmiarową interpunkcję (zachowujemy cyfry/litery/CJK i myślnik w środku słowa)
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

# ----------------------------- Heurystyki wykrywania -----------------------------

def _has_strong_signal(tokens: List[str]) -> bool:
    """
    Silne sygnały, że tytuł niesie konkretną informację:
    - znaki CJK (często prawdziwa nazwa),
    - numer identyfikacyjny / rok / 4+ cyfry (np. 11735),
    - mix liter i cyfr (np. s02e03, ep10),
    - co najmniej 2 sensowne tokeny (>=4 znaki) po odcięciu stopwordów.
    """
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
    # np. "Stand-in", "Re-born" — myślnik w rdzeniu, nie prefiks/sufiks
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
    return any(t in SUSPECT_HINTS for t in tl)

def _looks_like_generic_request(s_norm: str) -> bool:
    return any(p.search(s_norm) for p in GENERIC_TITLE_PATTERNS)

# ----------------------------- Inquiry: generica -----------------------------

def is_generic_inquiry(title: str) -> bool:
    """
    True, jeśli tytuł wygląda na „pustą prośbę” (help/title/link/looking for...),
    a jednocześnie nie zawiera silnych sygnałów (np. 11735, CJK, litera+cyfra).
    Używane wyłącznie dla flairów Inquiry.
    """
    title_raw = (title or "").strip()
    if not title_raw:
        return True  # pusty = ewidentnie zły

    title_norm = _normalize_text(title_raw)
    toks = _tokens(title_norm)

    # Jeżeli w ogóle nie ma tokenów po normalizacji, traktujemy jako puste
    if not toks:
        return True

    # Jeśli są „silne sygnały”, nie kwalifikujemy jako 'generic inquiry'
    if _has_strong_signal(toks):
        return False

    # Typowe puste wzorce (need help / help me / looking for / find title / title+link)
    if _looks_like_generic_request(title_norm):
        return True

    # Ultra-krótkie tytuły zawierające słowa podejrzane (help/title/link/please/looking…)
    if _has_suspect_word(toks):
        # brak silnych sygnałów + podejrzane słowo → generica
        return True

    return False

# ----------------------------- Link Request: generica -----------------------------

def _looks_like_generic_placeholder(title: str) -> bool:
    """
    Wykrywa puste/ogólne tytuły typu: 'Does anyone know its name?', 'Please link', 'Need title', itp.
    Zostawia wyjątki (CJK / 4+ cyfry / s02e03 / ep12 / tytuł w cudzysłowie), które sugerują konkretny trop.
    """
    if not title:
        return True

    t_raw = title.strip()
    t_norm = _normalize_text(t_raw)
    toks = _tokens(t_norm)

    # ----- mocne wyjątki (NIE oznaczamy jako generic) -----
    if not toks:
        return True  # puste po normalizacji
    if _has_strong_signal(toks):
        return False
    # tytuł w cudzysłowie (np. "Love Beyond Fate")
    if re.search(r"[\"“][^\"“]{3,}?[\"”]", t_raw):
        return False

    # ----- klasyczne puste wzorce -----
    if _looks_like_generic_request(t_norm):
        return True

    # bardzo mało treści informacyjnej (≤2 tokeny sensowne) + podejrzane słowa
    informative = _informative_tokens(toks)
    if len(informative) <= 2 and _has_suspect_word(toks):
        return True

    return False

# ----------------------------- Walidator główny -----------------------------

def validate_title(title: str, flair: str = "", config: Dict = None) -> Dict[str, str]:
    """
    Zwraca dict: {"status": "OK|AMBIGUOUS|MISSING", "reason": "<krótki_powód>"}.
    Zmiany (kompromis dla krótkich tytułów):
      - Jeśli wygląda jak prawdziwy tytuł (hyphen w rdzeniu albo TitleCase przy ≤3 słowach),
        przepuszczamy jako OK.
      - Jeśli tytuł jest bardzo krótki, ale „czysty” (bez podejrzanych słów) i ma ≥1 sensowny token (≥4 litery),
        to AMBIGUOUS (trafi do MOD_QUEUE), nie MISSING.
      - „Help/link/title/please…” nadal klasyfikują jako MISSING, o ile brak silnych sygnałów.
    """
    flair = (flair or "").strip()
    title_raw = (title or "").strip()

    # 📌 Link Request → najpierw odsień „puste/generic” tytuły
    if flair == "📌 Link Request":
        if _looks_like_generic_placeholder(title_raw):
            return {"status": "MISSING", "reason": "generic_placeholder"}

    # Puste pozycje łapiemy zawsze
    if not title_raw:
        return {"status": "MISSING", "reason": "empty_title"}

    title_norm = _normalize_text(title_raw)
    toks = _tokens(title_norm)
    if not toks:
        return {"status": "MISSING", "reason": "empty_after_norm"}

    informative = _informative_tokens(toks)

    # Sztywne „puste prośby” dla ścisłych flairów, jeśli brak silnych sygnałów
    if flair in STRICT_FLAIRS:
        if _looks_like_generic_request(title_norm) and not _has_strong_signal(toks):
            return {"status": "MISSING", "reason": "generic_title"}

    # Heurystyki ratunkowe dla krótkich tytułów
    words_cnt = len(toks)
    has_hyphen_title = any(_token_is_hyphen_title(t) for t in toks)
    titlecase_ratio = _titlecase_ratio(toks)
    has_suspect = _has_suspect_word(toks)
    has_strong = _has_strong_signal(toks)
    long_informative = [t for t in informative if len(t) >= 4]

    if flair in STRICT_FLAIRS:
        # 1) Wygląda jak tytuł → OK
        #    - token z myślnikiem (stand-in)
        #    - ≤3 słowa, ≥1 sensowny token (≥4 litery) i >=50% TitleCase
        if has_hyphen_title or (words_cnt <= 3 and len(long_informative) >= 1 and titlecase_ratio >= 0.5):
            return {"status": "OK", "reason": "looks_like_title"}

        # 2) Krótkie, ale czyste → AMBIGUOUS (do MOD_QUEUE), o ile brak podejrzanych słów
        if not has_strong and len(informative) < 2:
            if not has_suspect and len(long_informative) >= 1:
                return {"status": "AMBIGUOUS", "reason": "short_but_clean"}
            # 3) Jeśli nadal brak mocnych sygnałów i tytuł jest „pusty” → MISSING
            return {"status": "MISSING", "reason": "generic_title"}

        # 4) W pozostałych przypadkach — OK (bo mamy już dość sygnałów)
        return {"status": "OK", "reason": "title_candidate"}

    else:
        # Łagodniejsze zasady dla innych flairów (np. Inquiry)
        if len(informative) == 0 and not has_strong:
            return {"status": "AMBIGUOUS", "reason": "uninformative"}
        return {"status": "OK", "reason": "title_candidate"}
