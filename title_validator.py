# title_validator.py (patched)
from __future__ import annotations
from typing import Dict, Any, List
import re

_CJK_OR_LATIN_RE = re.compile(r"[^0-9A-Za-z\u4e00-\u9FFF\s]+")
_MULTISPACE_RE = re.compile(r"\s+")
_EMOJI_OR_SYMBOLS_RE = re.compile(r"[\u2600-\u27BF\U0001F300-\U0001FAFF]")

STOPWORDS_VALID = {
    "help","please","pls","plz","find","looking","for","link",
    "title","name","drama","cdrama","movie","series",
    "this","that","the","a","an","anyone","can","someone","need",
    "unknown","idk","dont","don't","know","me","my","to","of"
}

def _norm(text: str) -> str:
    t = (text or "").lower()
    t = _EMOJI_OR_SYMBOLS_RE.sub(" ", t)
    t = _CJK_OR_LATIN_RE.sub(" ", t)
    t = _MULTISPACE_RE.sub(" ", t).strip()
    return t

def _tokens_no_stop(text: str) -> List[str]:
    return [w for w in text.split() if w and w not in STOPWORDS_VALID]

def _has_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9FFF]", text))

def _looks_like_title(original: str) -> bool:
    # 2–6 słów, większość zaczyna się wielką literą albo tytuł jest w cudzysłowie
    if not original:
        return False
    o = original.strip()
    if ('"' in o) or ("“" in o and "”" in o) or ("'" in o):
        return True
    parts = [w for w in re.split(r"\s+", o) if w]
    if 2 <= len(parts) <= 6:
        caps = sum(1 for w in parts if w[:1].isupper())
        return caps >= max(2, len(parts) - 1)  # np. "The Mist of Yun"
    return False

def validate_title(title: str, flair: str, config: Dict[str, Any]) -> Dict[str, Any]:
    t = (title or "").strip()
    t_norm = _norm(t)
    toks_nostop = _tokens_no_stop(t_norm)
    has_cjk = _has_cjk(t_norm)

def _rescue_title_from_url(raw_title: str) -> str | None:
    # znajdź URL, pobierz path, weź ostatni niepusty segment
    # unquote + replace ['_', '-'] -> ' '
    # strip + collapse whitespace
    # jeśli >= 2 słowa zawierające litery -> zwróć string; inaczej None

title = normalize(raw_title)
if looks_empty(title):
    rescued = _rescue_title_from_url(raw_title)
    if rescued and looks_like_title(rescued):
        title = rescued  # użyj „odratowanego” tytułu zamiast oceniać jako MISSING

    
    # NOWE: jeśli wygląda jak krótki, poprawny tytuł — akceptuj
    if _looks_like_title(t):
        return {"status": "OK", "reason": "title_candidate", "notes": ["looks_like_title"]}

    # ZMIANA PROGU: hard MISSING tylko gdy <2 informacyjnych tokenów i brak CJK
    if len(toks_nostop) < 2 and not has_cjk:
        return {"status": "MISSING", "reason": "only_empty_phrases", "notes": ["no informative tokens"]}

    # Heurystyki treści (bez zmian)
    hint_patterns = [
        r"\b(reborn|revenge|ceo|marriage|heiress|pilot|accident|banquet)\b",
        r"\b(actor|actress|ml|fl|male\s+lead|female\s+lead|with\s+actor)\b",
        r"\b(title:)\s*[\u4e00-\u9FFF]+",
        r"[\u4e00-\u9FFF]{2,}",
    ]
    for pat in hint_patterns:
        if re.search(pat, t_norm):
            return {"status": "OK", "reason": "has_actor_or_genre_or_plot", "notes": []}

    # Fallback: po usunięciu stopwordów 2+ tokeny lub obecne CJK → OK
    if len(toks_nostop) >= 2 or has_cjk:
        return {"status": "OK", "reason": "title_candidate", "notes": []}

    return {"status": "AMBIGUOUS", "reason": "short_unclear", "notes": []}
