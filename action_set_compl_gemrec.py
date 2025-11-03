#!/usr/bin/env python3
"""
Ustawianie ✅ Request Complete dla świeżych wątków (ostatnie N dni) po ręcznej akceptacji.

Zasady:
- Źródło: sub.new() filtrowane po czasie (domyślnie --days 3: dziś/wczoraj/przedwczoraj).
- Flair: luźne "request" w tekście flaira, WYKLUCZA "complete".
- Liczą się TYLKO linki z KOMENTARZY nie-botów (selftext ignorujemy).
- Dozwolone domeny wideo: YouTube / youtu.be / Dailymotion / Rumble / Odysee / Facebook.
- Odrzuć: mydramalist.com oraz komentarze ze słowami-kluczami: raw / no subs / unsubbed / similar / different / no soundtrack.
- Komentarz może zawierać TEKST i WIELE linków; bot wybiera pierwszy działający link z dozwolonej domeny.
- Każdy kandydat wymaga potwierdzenia (--interactive) lub globalnego --confirm.

Wymaga: bot jako moderator z uprawnieniem "Flair Posts".
"""

import argparse
import re
import time
from datetime import datetime, timedelta, timezone

import praw
import requests

# --- KONFIG ---
SUBREDDIT = "CShortDramas"
TARGET_FLAIR_TEXT = "✅ Request Complete"

BOT_AUTHORS = {
    "automoderator", "image-sourcery", "image-sourcery-bot", "imagesourcery",
    "remindmebot", "imgurmirrorbot", "linkfixerbot",
}

ELIGIBLE_VIDEO_DOMAINS = {
    "youtube.com", "youtu.be", "dailymotion.com", "rumble.com", "odysee.com",
    "facebook.com", "fb.watch",
}

DISALLOWED_DOMAINS = {"mydramalist.com"}

# --- REGEXY ---

# No-subs / raw / bez dźwięku (rozszerzone o warianty i "no soundtrack")
NO_SUBS_RE = re.compile(
    r"""(?ix)
    \b(
        no\s*(eng(?:lish)?)?\s*subs?           |  # no subs / no english subs
        not\s*subbed                            |
        without\s*(eng(?:lish)?)?\s*subs?       |
        un[-\s]?subbed                           |  # un-subbed / un subbed / unsubbed
        raw(?:\s*only)?                          |
        mandarin\s*only                          |
        chinese\s*only                           |
        cn\s*only                                |
        no\s*english                             |
        doesn.?t\s*have\s*english\s*sub          |
        subs?\s*not\s*available                  |
        no\s*sound(track)?                       |  # no sound / no soundtrack
        no\s*audio                               |
        muted\s*audio                            |
        no\s*translation
    )\b
    """
)

# Trailery / reklamy
TRAILER_AD_RE = re.compile(
    r"\b(trailer|teaser|promo|preview|fragment|ad|reklama|commercial|advertisement)\b",
    re.IGNORECASE
)

# „To nie ten sam / podobny / inna wersja/tytuł” (rozszerzone + standalone 'similar')
SIMILAR_NOT_SAME_RE = re.compile(
    r"""(?ix)
    (?:\bnot\s+(?:the\s+)?same\b.*\b(?:one|drama|movie|series)\b) |
    (?:\bnot\s+(?:this|that)\b.*\b(?:one|drama|movie|series)\b)   |
    (?:\bnot\s+(?:it|the\s+one)\b)                                |
    (?:\ba?different\s+(?:one|drama|movie|series|title|version|cut|edit)\b) |
    (?:\banother\s+(?:one|drama|movie|series|title|version|cut|edit)\b)     |
    (?:\balt(?:ernate|ernative)?\s+version\b)                     |
    (?:\bdifferent\s+title\b)                                     |
    (?:\bsimilar\s+(?:plot|story)\b)                              |
    (?:\bsimilar\b)                                               |  # standalone "similar" dyskwalifikuje
    (?:\bnot\s+(?:the\s+)?same\b.*\bsimilar\b)                    |
    (?:\bversion\b) |
    (?:\bsimilar\b.*\bnot\b.*\bsame\b) |
    """
)

# URL-e w tekście
URL_RE = re.compile(r"https?://[^\s)>\]]+", re.IGNORECASE)

REQ_DELAY = 0.3  # ostrożne tempo sprawdzania linków

# --- POMOCNICZE ---
def flair_is_request(text: str) -> bool:
    """Zwraca True, jeśli flair zawiera 'request', ale nie 'complete'."""
    if not text:
        return False
    t = text.lower()
    return "request" in t and "complete" not in t

def is_bot_comment(c) -> bool:
    """Sprawdza, czy komentarz pochodzi od znanego bota."""
    if not c.author:
        return False
    return (c.author.name or "").lower() in BOT_AUTHORS

def extract_urls(text: str):
    """Zwraca listę wszystkich URL-i w tekście."""
    return URL_RE.findall(text or "")

def domain_of(url: str) -> str:
    """Zwraca domenę (bez www) z adresu URL."""
    try:
        m = re.search(r"https?://([^/]+)", url)
        if not m:
            return ""
        d = m.group(1).lower()
        if d.startswith("www."):
            d = d[4:]
        return d
    except Exception:
        return ""

def comment_disqualifies(text: str) -> bool:
    """True, jeśli komentarz zawiera 'no subs' / 'trailer/ad' / 'not the same/similar/different'."""
    t = (text or "")
    if NO_SUBS_RE.search(t):
        return True
    if TRAILER_AD_RE.search(t):
        return True
    if SIMILAR_NOT_SAME_RE.search(t):
        return True
    return False

def get_reddit():
    return praw.Reddit(site_name="Cleanup_Bot")

def is_active(u: str) -> bool:
    try:
        time.sleep(REQ_DELAY)
        r = requests.head(u, allow_redirects=True, timeout=8)
        if r.status_code in (405, 403):
            r = requests.get(u, allow_redirects=True, timeout=8)
        return r.status_code < 400
    except Exception:
        return False

def get_template_id_for_text(sub, text: str):
    for f in sub.flair.link_templates:
        if (f["text"] or "").strip() == text:
            return f["id"]
    return None

def require_flair_perms(sub, me):
    mods = {m.name.lower(): m for m in sub.moderator()}
    mine = mods.get((me or "").lower())
    if not mine:
        raise SystemExit("❌ Bot nie jest moderatorem tego subreddita.")
    # PRAW nie expose'uje granularnie flag; sprawdzamy na operacji.

def is_removed_or_deleted_comment(c) -> bool:
    """Zwróć True, jeśli komentarz jest usunięty przez usera lub moderatora."""
    body = (getattr(c, "body", "") or "").strip().lower()
    if body in ("[removed]", "[deleted]"):
        return True
    if getattr(c, "author", None) is None:
        return True
    if getattr(c, "banned_by", None):
        return True
    if getattr(c, "removal_reason", None):
        return True
    try:
        if hasattr(c, "mod") and getattr(c.mod, "removal_reason", None):
            return True
    except Exception:
        pass
    html = (getattr(c, "body_html", "") or "").lower()
    if html and "[removed]" in html:
        return True
    for attr in ("collapsed_reason", "collapse_reason", "collapsed_reason_code"):
        val = getattr(c, attr, None)
        if val and "removed" in str(val).lower():
            return True
    return False

# --- GŁÓWNA LOGIKA ---
def main():
    ap = argparse.ArgumentParser(description="Ustawianie ✅ Request Complete dla świeżych wątków (ostatnie N dni).")
    ap.add_argument("--days", type=int, default=3, help="ile dni wstecz przeglądać (domyślnie 3: dziś/wczoraj/przedwczoraj)")
    ap.add_argument("--limit", type=int, default=300, help="maks. ile najnowszych postów pobrać do przesiewu")
    ap.add_argument("--interactive", action="store_true", help="pytaj y/N przy każdym kandydacie")
    ap.add_argument("--confirm", action="store_true", help="ustawiaj flair bez pytania (ostrożnie)")
    ap.add_argument("--comment", type=str, default=None, help="opcjonalny komentarz po zmianie flaira")
    ap.add_argument("--debug", action="store_true", help="pokaż statystyki")
    args = ap.parse_args()

    reddit = get_reddit()
    me = str(reddit.user.me() or "")
    sub = reddit.subreddit(SUBREDDIT)
    require_flair_perms(sub, me)

    tid = get_template_id_for_text(sub, TARGET_FLAIR_TEXT)
    if not tid:
        raise SystemExit(f"❌ Nie znaleziono template_id dla '{TARGET_FLAIR_TEXT}'. Upewnij się, że taki flair istnieje.")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.days)

    scanned = 0
    candidates = []

    print(f"Zalogowano jako: u/{me}")
    print(f"Szukam postów z ostatnich {args.days} dni… (flair Request, link w komentarzu, aktywny, bez raw/no-subs/similar/different/trailer)")

    for s in sub.new(limit=args.limit):
        scanned += 1

        created = datetime.fromtimestamp(s.created_utc, tz=timezone.utc)
        if created < cutoff:
            break

        if not flair_is_request(s.link_flair_text):
            continue

        if (s.link_flair_text or "").strip() == TARGET_FLAIR_TEXT:
            continue

        try:
            s.comments.replace_more(limit=0)
        except Exception:
            continue

        best = None  # (url, author)
        for c in s.comments:
            if is_bot_comment(c):
                continue
            if is_removed_or_deleted_comment(c):
                continue

            body = c.body or ""

            # dyskwalifikacje: no-subs/raw/trailer/similar/different/alt version itp.
            if comment_disqualifies(body):
                continue

            # dozwolone: wiele linków w komentarzu; bierzemy pierwszy działający z whitelisty
            urls = extract_urls(body)
            for u in urls:
                d = domain_of(u)
                if d in DISALLOWED_DOMAINS:
                    continue
                if d not in ELIGIBLE_VIDEO_DOMAINS:
                    continue
                if is_active(u):
                    best = (u, c.author.name if c.author else "[deleted]")
                    break
            if best:
                break

        if best:
            candidates.append((s, best[0], best[1], created))

    if args.debug:
        print(f"[debug] Przeskanowano nowych postów: {scanned}")
        print(f"[debug] Kandydaci do ✅: {len(candidates)}")

    if not candidates:
        print("Brak kandydatów spełniających kryteria w zadanym oknie czasu.")
        return

    candidates.sort(key=lambda x: x[3], reverse=True)

    changed = 0
    for s, url, author, created in candidates:
        age_hours = (now - created).total_seconds() / 3600.0
        print(f"\n— {s.title[:90]} — https://redd.it/{s.id}")
        print(f"   flair teraz: {s.link_flair_text or '-'} | wiek: {age_hours:.1f}h")
        print(f"   ▶ link z komentarza u/{author}: {url}")

        do_it = False
        if args.confirm:
            do_it = True
        elif args.interactive:
            ans = input("   Ustawić flair na ✅ Request Complete? [y/N] ").strip().lower()
            do_it = (ans == "y")

        if do_it:
            try:
                s.flair.select(tid, text=TARGET_FLAIR_TEXT)
                if args.comment:
                    s.reply(args.comment)
                    time.sleep(0.4)
                changed += 1
                print("   ✅ Zmieniono flair na ✅ Request Complete.")
                time.sleep(0.3)
            except Exception as e:
                print(f"   ❌ Błąd zmiany flaira: {e}")
        else:
            print("   (pominięto)")

    print(f"\nGotowe. Zmieniono flair w {changed}/{len(candidates)} postach.")

if __name__ == "__main__":
    main()

