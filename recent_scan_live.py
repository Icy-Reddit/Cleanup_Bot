#!/usr/bin/env python3
# recent_scan_live.py
# Skaner /new + modqueue z walidacją tytułu, (opcjonalnie) matcherem i Decision Engine.
# Zawiera: --mark-checked on|off -> moderator report "Checked" dla modqueue gdy NO_ACTION.

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import yaml

try:
    import praw  # Reddit API
except Exception as e:
    print(f"[FATAL] PRAW import failed: {e}", file=sys.stderr)
    sys.exit(2)

# Moduły projektu (zakładamy, że są w PYTHONPATH lub w tym samym folderze)
try:
    import title_validator
except Exception as e:
    print(f"[FATAL] Cannot import title_validator: {e}", file=sys.stderr)
    sys.exit(2)

try:
    import title_matcher
except Exception as e:
    title_matcher = None  # pozwala działać walidatorowi nawet bez matchera
    print(f"[WARN] title_matcher not available: {e}", file=sys.stderr)

try:
    import decision_engine
except Exception as e:
    decision_engine = None
    print(f"[WARN] decision_engine not available: {e}", file=sys.stderr)


# ------------------------- Pomocnicze -------------------------

TARGET_FLAIRS = {"📌 Link Request", "🔍 Inquiry"}  # w pełni analizujemy tylko LR + częściowo Inquiry
SKIP_FULL_FLAIRS = {"🔗 Found & Shared", "✅ Request Complete"}  # pomijane jako nowe
POMIJAJ_INNE = True  # inne flairy całkowicie pomijane

def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def to_iso(ts: float) -> str:
    try:
        return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()
    except Exception:
        return "1970-01-01T00:00:00+00:00"

def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"[WARN] Cannot load config.yaml: {e}")
        return {}

def _cfg(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, None)
        if cur is None:
            return default
    return cur

def ensure_dir(p: str) -> None:
    if not p:
        return
    d = os.path.dirname(p)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

# ------------------------- Stan (TTL) -------------------------

def load_state(state_file: Optional[str]) -> Dict[str, Any]:
    if not state_file:
        return {"seen": {}}
    if not os.path.exists(state_file):
        return {"seen": {}}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen": {}}

def save_state(state_file: Optional[str], state: Dict[str, Any]) -> None:
    if not state_file:
        return
    ensure_dir(state_file)
    try:
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] Cannot save state: {e}")

def seen_recently(state: Dict[str, Any], post_id: str, ttl_min: int) -> bool:
    seen_map = state.setdefault("seen", {})
    ent = seen_map.get(post_id)
    if not ent:
        return False
    ts = ent.get("ts", 0)
    try:
        age_sec = time.time() - ts
        return age_sec < ttl_min * 60
    except Exception:
        return False

def mark_seen(state: Dict[str, Any], post_id: str) -> None:
    state.setdefault("seen", {})[post_id] = {"ts": time.time()}

# ------------------------- Reddit helpers -------------------------

def reddit_from_env_or_ini() -> praw.Reddit:
    """
    Preferuje wpis z praw.ini: [Cleanup_Bot], albo zmienne środowiskowe.
    """
    try:
        return praw.Reddit("Cleanup_Bot")
    except Exception:
        # Fallback na env (standardowe nazwy)
        return praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            password=os.getenv("REDDIT_PASSWORD"),
            username=os.getenv("REDDIT_USERNAME"),
            user_agent=os.getenv("REDDIT_USER_AGENT", "CleanupBot/Titlematch by u/yourbot"),
        )

def get_submission_url(submission) -> str:
    try:
        return f"https://www.reddit.com{submission.permalink}"
    except Exception:
        return "(url unavailable)"

def flair_text(submission) -> str:
    try:
        return (getattr(submission, "link_flair_text", None) or "").strip()
    except Exception:
        return ""

def author_name(submission) -> str:
    try:
        a = getattr(submission, "author", None)
        return (a.name if a else "[deleted]") or "[deleted]"
    except Exception:
        return "[deleted]"

# ------------------------- Pobieranie postów -------------------------

def iter_source(sub, source: str, limit: int):
    if source == "new":
        return sub.new(limit=limit)
    if source == "modqueue":
        return sub.mod.modqueue(limit=limit)
    raise ValueError("invalid source")

def collect_posts(
    reddit: praw.Reddit,
    subreddit_name: str,
    sources: List[str],
    window_min: int,
    limit_per_source: int,
) -> List[Tuple[str, Any]]:
    """
    Zwraca listę (source_name, submission), tylko z okna czasowego.
    """
    out: List[Tuple[str, Any]] = []
    sub = reddit.subreddit(subreddit_name)
    now = time.time()
    min_ts = now - window_min * 60
    for src in sources:
        try:
            for s in iter_source(sub, src, limit_per_source):
                try:
                    cu = float(getattr(s, "created_utc", 0.0))
                    if cu < min_ts:
                        continue
                    out.append((src, s))
                except Exception:
                    continue
        except Exception:
            continue
    return out

# ------------------------- Logi JSONL/CSV -------------------------

def append_jsonl(path: Optional[str], obj: Dict[str, Any]) -> None:
    if not path:
        return
    ensure_dir(path)
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[WARN] jsonl append failed: {e}")

def append_csv(path: Optional[str], row: List[str]) -> None:
    if not path:
        return
    ensure_dir(path)
    try:
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8", newline="") as f:
                f.write("when,action,category,post_id,author,flair,title,reason\n")
        with open(path, "a", encoding="utf-8", newline="") as f:
            f.write(",".join('"' + (c.replace('"', '""')) + '"' for c in row) + "\n")
    except Exception as e:
        print(f"[WARN] csv append failed: {e}")

# ------------------------- Główna pętla -------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Titlematch recent scanner (live)")
    ap.add_argument("--window", type=int, default=60, help="Time window in minutes to scan")
    ap.add_argument("--sources", choices=["new", "modqueue", "both"], default="both", help="Which sources to scan")
    ap.add_argument("--limit-per-source", type=int, default=200, help="Max posts to pull per source")
    ap.add_argument("--poster", choices=["never", "try", "always"], default="never", help="Poster matcher mode (compat)")
    ap.add_argument("--poster-pool", choices=["top", "wide"], default="top", help="Poster pool selection (compat)")
    ap.add_argument("--live", action="store_true", help="Execute actions for real (not dry-run)")
    ap.add_argument("--commit", action="store_true", help="Alias for --live (kept for compatibility)")
    ap.add_argument("--inquiry-generic-only", action="store_true", help="For Inquiry: only auto-remove generic titles")
    ap.add_argument("--state-file", default=None)
    ap.add_argument("--state-ttl-min", type=int, default=180)
    ap.add_argument("--log-jsonl", nargs="?", const="", default=None)
    ap.add_argument("--report-csv", nargs="?", const="", default=None)
    ap.add_argument("--subreddit", default="CShortDramas")
    ap.add_argument("--verbose", action="store_true")

    # NOWE: flaga znacznikowa "Checked" (modqueue, NO_ACTION)
    ap.add_argument(
        "--mark-checked",
        choices=["on", "off"],
        default="off",
        help="Add a moderator report 'Checked' for posts scanned from modqueue when decision is NO_ACTION",
    )

    args = ap.parse_args()
    # alias commit → live
    if args.commit:
        args.live = True

    mark_checked_on = (args.mark_checked == "on")

    # przygotuj ścieżki logów (puste "" → default)
    if args.log_jsonl == "":
        args.log_jsonl = os.path.join("logs", f"decisions_{utcnow().date().isoformat()}.jsonl")
    if args.report_csv == "":
        args.report_csv = os.path.join("logs", f"actions_{utcnow().date().isoformat()}.csv")

    cfg = load_config("config.yaml")
    time_window_days = _cfg(cfg, "decision", "time_window_days", default=14)

    reddit = reddit_from_env_or_ini()

    sources: List[str]
    if args.sources == "both":
        sources = ["new", "modqueue"]
    else:
        sources = [args.sources]

    state = load_state(args.state_file)

    posts = collect_posts(
        reddit=reddit,
        subreddit_name=args.subreddit,
        sources=sources,
        window_min=args.window,
        limit_per_source=args.limit_per_source,
    )

    decisions_count = {"AUTO_REMOVE": 0, "MOD_QUEUE": 0, "NO_ACTION": 0}
    skipped_due_to_state = 0
    processed = 0

    for source, s in posts:
        try:
            pid = getattr(s, "id", "")
            if not pid:
                continue
            if seen_recently(state, pid, args.state_ttl_min):
                skipped_due_to_state += 1
                continue

            a_name = author_name(s)
            flair = flair_text(s)
            created_iso = to_iso(getattr(s, "created_utc", 0.0))
            url = get_submission_url(s)
            title = (getattr(s, "title", None) or "").strip()

            # POST log
            print("=" * 70)
            print(f"[POST] u/{a_name} | flair={flair or ''} | id={pid} | at={created_iso}")
            if title:
                print("TITLE:", title)
            print(f"      {url}")

            # Filtr flaira (pełne pominięcia)
            if flair in SKIP_FULL_FLAIRS:
                print(f"[SKIP] flair={flair} | reason=non-target flair")
                mark_seen(state, pid)
                continue
            if POMIJAJ_INNE and flair not in TARGET_FLAIRS:
                print(f"[SKIP] flair={(flair or '(none)')} | reason=non-target flair")
                mark_seen(state, pid)
                continue

            # Walidator tytułu
            try:
                vres = title_validator.validate_title(title, flair, cfg)
                vstatus = vres.get("status", "OK")
                vreason = vres.get("reason", "")
                print(f"[VALID] status={vstatus} reason={vreason}")
            except Exception as e:
                vstatus, vreason = "OK", "validator_error"
                print(f"[WARN] validator error: {e}")

            decision: Dict[str, Any] = {}
            tm_info: Dict[str, Any] = {"best": {"score": 0, "certainty": "low", "relation": "unknown", "title": None, "flair": None, "link": None}}

            # 📌 Link Request — pełna ścieżka
            if flair == "📌 Link Request":
                if vstatus == "MISSING":
                    # Auto-remove przez walidator (brak tytułu/opisu)
                    decision = {
                        "action": "AUTO_REMOVE",
                        "category": "MISSING",
                        "reason": "Title missing per validator.",
                        "removal_reason": "Lack of Drama Name or Description in Title",
                        "removal_comment": _cfg(cfg, "comments", "missing_title_template", default=None),
                        "evidence": {},
                        "links": [],
                    }
                else:
                    # Matcher (jeśli dostępny)
                    if title_matcher is not None:
                        try:
                            m = title_matcher.find_best_match(
                                reddit=reddit,
                                subreddit_name=args.subreddit,
                                title=title,
                                author=a_name,
                                window_days=time_window_days,
                                limit_per_source=2500,
                                exclude_post_id=pid,
                                exclude_post_url=getattr(s, "permalink", None),
                            )
                            # oczekujemy dict-a z polami: score, certainty, relation, title, flair, link, type
                            best = m.get("best", m) if isinstance(m, dict) else {}
                            tm_info["best"] = {
                                "score": int(best.get("score", 0)),
                                "certainty": str(best.get("certainty", "low")),
                                "relation": str(best.get("relation", "unknown")),
                                "title": best.get("title"),
                                "flair": best.get("flair"),
                                "link": best.get("link"),
                                "type": best.get("type", "fuzzy"),
                            }
                            # Log TM
                            tscore = tm_info["best"]["score"]
                            tcert = tm_info["best"]["certainty"]
                            trel = tm_info["best"]["relation"]
                            print(f"[TM] best score={tscore} certainty={tcert} rel={trel}")
                            if tm_info["best"]["title"]:
                                print(f"     -> {tm_info['best']['title']} | {tm_info['best'].get('flair','')} | {tm_info['best'].get('link','')}")
                        except Exception as e:
                            print(f"[WARN] matcher error: {e}")
                    else:
                        print("[INFO] Matcher disabled/unavailable; skipping match step.")

                    # Decision Engine (jeśli dostępny)
                    if decision_engine is not None:
                        try:
                            decision = decision_engine.decide(
                                title=title,
                                flair=flair,
                                validator=vstatus,
                                match=tm_info["best"],
                                config=cfg,
                                poster_status="NO_REPORT",  # poster matcher wyłączony
                            )
                        except Exception as e:
                            print(f"[WARN] decision_engine error: {e}")
                            decision = {
                                "action": "MOD_QUEUE",
                                "category": "ENGINE_ERROR",
                                "reason": f"decision_engine_error: {e}",
                                "removal_reason": None,
                                "removal_comment": None,
                                "evidence": {},
                                "links": [],
                            }
                    else:
                        # fallback gdy brak DE: nic nie rób
                        decision = {
                            "action": "NO_ACTION",
                            "category": "NO_SIGNAL",
                            "reason": "DE unavailable",
                            "removal_reason": None,
                            "removal_comment": None,
                            "evidence": {},
                            "links": [],
                        }

            # 🔍 Inquiry — tylko walidacja (opcjonalnie generic-only)
            elif flair == "🔍 Inquiry":
                if args.inquiry_g
eneric_only:
                    if vstatus == "MISSING":
                        decision = {
                            "action": "AUTO_REMOVE",
                            "category": "MISSING",
                            "reason": "Generic inquiry title without concrete drama name/description",
                            "removal_reason": "Lack of Drama Name or Description in Title",
                            "removal_comment": _cfg(cfg, "comments", "missing_title_template", default=None),
                            "evidence": {},
                            "links": [],
                        }
                    else:
                        print("[INFO] Inquiry flair → matcher disabled; decision engine not run.")
                        decision = {"action": "NO_ACTION", "category": "NO_SIGNAL", "reason": "inquiry_generic_only", "removal_reason": None, "removal_comment": None, "evidence": {}, "links": []}
                else:
                    print("[INFO] Inquiry flair → matcher disabled; decision engine not run.")
                    decision = {"action": "NO_ACTION", "category": "NO_SIGNAL", "reason": "inquiry_skipped", "removal_reason": None, "removal_comment": None, "evidence": {}, "links": []}

            else:
                # inne flairy (teoretycznie już pominięte wyżej)
                print(f"[SKIP] flair={flair or '(none)'} | reason=non-target flair")
                mark_seen(state, pid)
                continue

            # ----------- Wykonanie decyzji -----------
            when = utcnow().isoformat()
            action = decision.get("action", "NO_ACTION")
            category = decision.get("category", "NO_SIGNAL")
            removal_reason = decision.get("removal_reason")
            removal_comment = decision.get("removal_comment")
            reason = decision.get("reason", "")

            # Wypisz blok decyzyjny (krótki)
            print("=" * 15, "DECISION ENGINE", "=" * 15)
            print(f"When: {when}")
            print(f"Action: {action} | Category: {category}")
            print(f"Reason: {reason}")
            print(f"Removal Reason: {removal_reason}")
            if tm_info["best"]["title"]:
                print("\n-- Title Match --")
                print(f"type={tm_info['best'].get('type','fuzzy')} | score={tm_info['best'].get('score')} | certainty={tm_info['best'].get('certainty')} | relation={tm_info['best'].get('relation')}")
                print(f"  title='{tm_info['best']['title']}' | flair={tm_info['best'].get('flair','')}")
                print(f"  link={tm_info['best'].get('link','')}")
            print("\n-- Poster Match --")
            print("status=NO_REPORT | distance=None | relation=unknown\n")

            # Akcje live
            if args.live:
                try:
                    if action == "AUTO_REMOVE":
                        s.mod.remove()
                        if removal_comment:
                            s.reply(removal_comment)
                        print(f"[ACTION] Removed with reason='{removal_reason or ''}' + public message")
                    elif action == "MOD_QUEUE":
                        # dodaj raport moderatorski, żeby było widoczne w żółtym boxie
                        s.mod.report(reason or "Ambiguous")
                        print(f"[ACTION] Reported to modqueue (from /{source})")
                    else:
                        # NO_ACTION → nic
                        pass
                except Exception as e:
                    print(f"[WARN] action failed: {e}")

            # Logi
            append_jsonl(
                args.log_jsonl,
                {
                    "when": when,
                    "post_id": pid,
                    "author": a_name,
                    "flair": flair,
                    "title": title,
                    "action": action,
                    "category": category,
                    "reason": reason,
                    "match": tm_info.get("best", {}),
                    "source": source,
                },
            )
            append_csv(
                args.report_csv,
                [when, action, category, pid, a_name, flair, title, reason],
            )

            # === NOWY BLOK: Mark as "Checked" na kafelku Mod Queue (tylko NO_ACTION) ===
            try:
                if mark_checked_on and args.live and source == "modqueue" and action == "NO_ACTION":
                    # unikamy duplikacji "Checked"
                    existing_reasons = []
                    try:
                        existing_reasons = [
                            ((r[1] if isinstance(r, (list, tuple)) and len(r) > 1 else str(r)) or "").strip().lower()
                            for r in getattr(s, "mod_reports", [])
                        ]
                    except Exception:
                        existing_reasons = []
                    if "checked" not in existing_reasons:
                        try:
                            s.mod.report("Checked")
                            print("[MARK] ModQueue: added moderator report 'Checked'")
                        except Exception as e:
                            print(f"[WARN] Could not add mod report 'Checked': {e}")
            except Exception as e:
                print(f"[WARN] Mark-checked block failed: {e}")

            mark_seen(state, pid)
            processed += 1
            decisions_count[action] = decisions_count.get(action, 0) + 1

        except Exception as e:
            print(f"[WARN] post loop error: {e}")
            continue

    save_state(args.state_file, state)

    total = len(posts)
    print(f"[SUMMARY] total={total} processed={processed} skipped_due_to_state={skipped_due_to_state} decisions={{AUTO_REMOVE:{decisions_count.get('AUTO_REMOVE',0)}, MOD_QUEUE:{decisions_count.get('MOD_QUEUE',0)}, NO_ACTION:{decisions_count.get('NO_ACTION',0)}}}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
