#!/usr/bin/env python3
# recent_scan_live.py — live scanner for r/CShortDramas
# - Scans last N minutes from /new + modqueue
# - Skips already processed posts via --state-file
# - Logs to JSONL/CSV (optional)
# - Poster matcher is disabled (reported as NO_REPORT)
# - Adapter-aware calls to title_validator / title_matcher / decision_engine

from __future__ import annotations
import argparse
import csv
import datetime as dt
import json
import os
import sys
import inspect
import warnings
warnings.filterwarnings("ignore", message="Version .* of praw is outdated")

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import asdict, is_dataclass

try:
    import yaml
    import praw
except Exception as e:
    print("[FATAL] Missing deps:", e, file=sys.stderr)
    sys.exit(1)

# Optional imports (modules from the repo)
try:
    import title_validator
except Exception:
    title_validator = None
try:
    import title_matcher
except Exception:
    title_matcher = None
try:
    import decision_engine
except Exception:
    decision_engine = None


# ------------------------ Utils ------------------------

def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def iso(ts: dt.datetime) -> str:
    return ts.astimezone(dt.timezone.utc).isoformat()

def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"config.yaml not found at: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def get_reddit():
    # Standardize on the Cleanup_Bot section (actions write praw.ini accordingly)
    try:
        return praw.Reddit("Cleanup_Bot")
    except Exception:
        # Fallback to DEFAULT if local dev uses that
        return praw.Reddit("DEFAULT")

def ensure_dir(p: str):
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)

def append_jsonl(path: str, obj: dict):
    ensure_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def append_csv(path: str, row: Dict[str, Any], header_order: Optional[List[str]] = None):
    ensure_dir(path)
    exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header_order or list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)

def load_state(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {"ids": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"ids": {}}

def save_state(path: Optional[str], state: Dict[str, Any]):
    if not path:
        return
    ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)

def gc_state(state: Dict[str, Any], ttl_min: int):
    if ttl_min <= 0:
        return
    cutoff = utcnow().timestamp() - ttl_min * 60
    ids = state.get("ids", {})
    for pid in [pid for pid, ts in ids.items() if ts < cutoff]:
        ids.pop(pid, None)
    state["ids"] = ids


# ------------------------ Fetching ------------------------

def fetch_candidates(
    r: praw.Reddit,
    sub_name: str,
    sources: str,
    limit_per_source: int,
    window_min: int
) -> List[Any]:
    sub = r.subreddit(sub_name)
    now = utcnow()
    min_ts = now - dt.timedelta(minutes=window_min)
    out: List[Tuple[str, Any]] = []

    def ok(t) -> bool:
        try:
            ts = dt.datetime.fromtimestamp(t, tz=dt.timezone.utc)
        except Exception:
            return False
        return ts >= min_ts

    if sources in ("new", "both"):
        try:
            for s in sub.new(limit=limit_per_source):
                if ok(getattr(s, "created_utc", 0.0)):
                    out.append(("new", s))
        except Exception as e:
            print(f"[WARN] Failed to fetch /new: {e}", file=sys.stderr)

    if sources in ("modqueue", "both"):
        try:
            for s in sub.mod.modqueue(limit=limit_per_source):
                if getattr(s, "created_utc", None) and ok(s.created_utc):
                    out.append(("modqueue", s))
        except Exception as e:
            print(f"[WARN] Failed to fetch modqueue: {e}", file=sys.stderr)

    out.sort(key=lambda it: getattr(it[1], "created_utc", 0.0))
    return out


# ------------------------ Module adapters ------------------------

def run_title_validator(title: str, flair: str, cfg: dict) -> Dict[str, Any]:
    if title_validator and hasattr(title_validator, "validate_title"):
        try:
            fn = title_validator.validate_title
            params = [p.lower() for p in inspect.signature(fn).parameters.keys()]
            if all(x in params for x in ("title", "flair", "config")):
                return fn(title, flair, cfg)
            else:
                return fn(title)
        except Exception as e:
            return {"status": "AMBIGUOUS", "reason": f"validator_error: {e}"}

    # Fallback heuristic (very permissive)
    if not title or len(title.split()) < 3:
        return {"status": "AMBIGUOUS", "reason": "short_or_missing"}
    return {"status": "OK", "reason": "title_candidate"}

def run_title_matcher(post: Any, cfg: dict) -> Dict[str, Any]:
    if not title_matcher:
        return {"best": None, "pool_ids": [], "top": []}

    order = []
    fn = getattr(title_matcher, "match_title_for_post", None)
    if callable(fn):
        order.append(("match_title_for_post", fn))
    fn2 = getattr(title_matcher, "match_title", None)
    if callable(fn2):
        order.append(("match_title", fn2))

    author_obj = getattr(post, "author", None)
    author_name = getattr(author_obj, "name", None)
    flair_in = getattr(post, "link_flair_text", None) or ""
    permalink = getattr(post, "permalink", None)
    pid = getattr(post, "id", None)
    title_raw = getattr(post, "title", None)
    subreddit = getattr(getattr(post, "subreddit", None), "display_name", None)
    created_utc = getattr(post, "created_utc", None)
    reddit_obj = getattr(post, "_reddit", None)

    for name, fn in order:
        try:
            code = getattr(fn, "__code__", None)
            pos = code.co_argcount if code else 0
            kwonly = code.co_kwonlyargcount if code else 0
            params = set(code.co_varnames[:pos + kwonly]) if code else set()

            kw = {}
            # common
            if "post" in params:
                kw["post"] = post
            if "config" in params:
                kw["config"] = cfg
            if "exclude_post_id" in params:
                kw["exclude_post_id"] = pid
            if "exclude_post_url" in params:
                kw["exclude_post_url"] = permalink
            # keyword-only / optional extras
            if "title_raw" in params and title_raw is not None:
                kw["title_raw"] = title_raw
            if "author_name" in params and author_name is not None:
                kw["author_name"] = author_name
            if "flair_in" in params:
                kw["flair_in"] = flair_in
            if "subreddit" in params and subreddit is not None:
                kw["subreddit"] = subreddit
            if "reddit" in params and reddit_obj is not None:
                kw["reddit"] = reddit_obj
            if "post_created_utc" in params and created_utc is not None:
                kw["post_created_utc"] = created_utc

            rep = fn(**kw)
            return rep or {"best": None, "pool_ids": [], "top": []}
        except TypeError as e:
            print(f"[WARN] title_matcher.{name} signature mismatch: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"[WARN] title_matcher.{name} failed: {e}", file=sys.stderr)
            continue

    return {"best": None, "pool_ids": [], "top": []}

def run_decision_engine(context, validator, title_report, poster_report, cfg):
    # Pass through to decision_engine.decide if present; never raise.
    if decision_engine and hasattr(decision_engine, "decide"):
        try:
            rep = decision_engine.decide(
                context=context,
                validator=validator,
                title_report=title_report,
                poster_report=poster_report,
                config=cfg,
            )
            if is_dataclass(rep):
                return asdict(rep)
            return rep
        except Exception as e:
            return {
                "action": "MOD_QUEUE",
                "category": "ENGINE_ERROR",
                "reason": f"decision_engine_error: {e}",
                "removal_reason": None,
                "removal_comment": None,
                "evidence": {},
                "links": [],
            }

    # Fallback minimalist logic (should rarely be used)
    status = validator.get("status", "OK")
    if status == "MISSING":
        return {
            "action": "AUTO_REMOVE",
            "category": "MISSING",
            "reason": "Title missing",
            "removal_reason": "Lack of Drama Name or Description in Title",
            "removal_comment": None,
            "evidence": {},
            "links": [],
        }
    best = (title_report or {}).get("best") or {}
    score = int(best.get("score") or 0)
    relation = best.get("relation") or "unknown"
    auto = int((cfg.get("decision", {}) or {}).get("title_threshold_auto", 93))
    if score >= auto and relation == "different_author":
        return {
            "action": "AUTO_REMOVE",
            "category": "REPEATED",
            "reason": f"Title fuzzy match >= {auto} with different author",
            "removal_reason": "Repeated Request",
            "removal_comment": None,
            "evidence": {"title_match": {"score": score, "relation": relation}},
            "links": [best.get("candidate", {}).get("permalink")] if best.get("candidate") else [],
        }
    return {
        "action": "NO_ACTION",
        "category": "NO_SIGNAL",
        "reason": "No strong signals",
        "removal_reason": None,
        "removal_comment": None,
        "evidence": {},
        "links": [],
    }


# ------------------------ Pretty printing ------------------------

def print_human_post(source: str, post: Any, body_preview: Optional[str] = None) -> None:
    created = dt.datetime.fromtimestamp(getattr(post, "created_utc", 0.0), tz=dt.timezone.utc).isoformat()
    author = f"u/{getattr(getattr(post, 'author', None), 'name', 'unknown')}"
    flair = getattr(post, "link_flair_text", None) or ""
    pid = getattr(post, "id", "")
    print("\n======================================================================")
    print(f"[POST] {author} | flair={flair} | id={pid} | at={created}")
    title_line = getattr(post, "title", "") or ""
    if title_line:
        print(f"TITLE: {title_line}")
    if body_preview:
        print(f"      {body_preview}")
    permalink = getattr(post, "permalink", None)
    if permalink:
        base = "https://www.reddit.com"
        print(f"      {permalink if permalink.startswith('http') else base + permalink}")

def print_validator(rep: Dict[str, Any]) -> None:
    print(f"[VALID] status={rep.get('status')} reason={rep.get('reason')}")


def flair_from_rep(rep: Dict[str, Any]) -> str:
    best = (rep or {}).get("best") or {}
    cand = best.get("candidate") or {}
    return cand.get("flair") or ""


def summarize_title_matcher(rep: Dict[str, Any]) -> Tuple[str, int, str, str, Optional[str]]:
    best = (rep or {}).get("best") or {}
    score = int(best.get("score") or 0)
    certainty = best.get("certainty") or "low"
    relation = best.get("relation") or "unknown"
    cand = best.get("candidate") or {}
    title = cand.get("title") or "(unknown)"
    link = cand.get("permalink") or None
    return title, score, certainty, relation, link


def print_decision(dec: Dict[str, Any], title_rep: Dict[str, Any], poster_rep: Optional[Dict[str, Any]]) -> None:
    print("=============== DECISION ENGINE ===============")
    print(f"When: {iso(utcnow())}")
    print(f"Action: {dec.get('action')} | Category: {dec.get('category')}")
    print(f"Reason: {dec.get('reason')}")
    rr = dec.get("removal_reason")
    print(f"Removal Reason: {rr if rr else 'None'}\n")

    t_title, t_score, t_cert, t_rel, t_link = summarize_title_matcher(title_rep)
    print("-- Title Match --")
    print(f"type=fuzzy | score={t_score} | certainty={t_cert} | relation={t_rel}")
    print(f"  title='{t_title}' | flair={flair_from_rep(title_rep)}")
    if t_link:
        print(f"  link={t_link}\n")
    else:
        print()

    p = poster_rep or {"status": "NO_REPORT", "distance": None, "relation": "unknown"}
    print("-- Poster Match --")
    print(f"status={p.get('status')} | distance={p.get('distance')} | relation={p.get('relation')}\n")

    links = dec.get("links") or []
    if links:
        print("Links:")
        for L in links:
            print(f"- {L}")
    print("===============================================")


# ------------------------ Main ------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--window", type=int, default=60)
    ap.add_argument("--sources", choices=("new", "modqueue", "both"), default="both")
    ap.add_argument("--limit-per-source", type=int, default=200)
    ap.add_argument("--poster", choices=("auto", "never", "always"), default="never")
    ap.add_argument("--poster-pool", choices=("top", "full"), default="top")
    ap.add_argument("--fetch-per-flair", type=int, default=None)
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--log-jsonl", nargs="?", const="", default=None)
    ap.add_argument("--report-csv", nargs="?", const="", default=None)
    ap.add_argument("--state-file", default=None)
    ap.add_argument("--state-ttl-min", type=int, default=180)
    ap.add_argument("--subreddit", default="CShortDramas")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"[FATAL] Cannot load config: {e}", file=sys.stderr)
        return 2

    r = get_reddit()
    state = load_state(args.state_file)
    gc_state(state, args.state_ttl_min)

    posts = fetch_candidates(r, args.subreddit, args.sources, args.limit_per_source, args.window)
    if args.verbose:
        print(f"[INFO] fetched={len(posts)} from sources={args.sources} window={args.window}min")

    processed = skipped = 0
    decisions_count = {"AUTO_REMOVE": 0, "MOD_QUEUE": 0, "NO_ACTION": 0, "OTHER": 0}

    jsonl_path = None
    if args.log_jsonl is not None:
        jsonl_path = os.path.join("logs", f"decisions_{utcnow().date().isoformat()}.jsonl") if args.log_jsonl == "" else args.log_jsonl
        ensure_dir(jsonl_path)

    csv_path = None
    if args.report_csv is not None:
        csv_path = args.report_csv or os.path.join("logs", f"decisions_{utcnow().date().isoformat()}.csv")
        ensure_dir(csv_path)

    for source, post in posts:
        pid = getattr(post, "id", None)
        if not pid:
            continue

        if args.state_file:
            seen = state.setdefault("ids", {})
            if pid in seen:
                skipped += 1
                if args.verbose:
                    print(f"[SKIP] already processed {pid}")
                continue
            seen[pid] = utcnow().timestamp()

        title = getattr(post, "title", "") or ""
        selftext = getattr(post, "selftext", "") or ""
        preview = (selftext or "")[:160].replace("\n", " ").strip()
        flair = getattr(post, "link_flair_text", None) or ""

        if args.live:
            print_human_post(source, post, body_preview=preview or None)

        validator = run_title_validator(title, flair, cfg)
        if args.live:
            print_validator(validator)

        # --- polityka per flair ---
        import re  # lokalny import; bez zmian w nagłówku pliku
        flair_norm = (flair or "").strip().lower()
        # Usuń emoji/symbole, zostaw litery/cyfry/spacje i ampersand (ważny dla "found & shared")
        flair_norm = re.sub(r"[^a-z0-9&\s]+", "", flair_norm)

        ALLOWED_FOR_MATCH = {"link request"}  # tylko Link Request jest analizowany
        if flair_norm not in ALLOWED_FOR_MATCH:
            if flair_norm in {"inquiry", "actor inquiry"}:
                if validator.get("status") == "MISSING":
                    print("[POLICY] Inquiry/Actor Inquiry: hard MISSING → rekomendacja remove (RR: Lack of Drama Name...)")
                else:
                    print("[POLICY] Inquiry/Actor Inquiry: tylko walidacja; pomijam title/poster matcher.")
            elif flair_norm in {"found & shared", "request complete"}:
                print(f"[POLICY] Found/Complete: wynikowy post, pomijam analizę.")
            else:
                print(f"[POLICY] Flair '{flair}' (norm='{flair_norm}') nieobsługiwany w matcherach — pomijam.")
            continue

        tmatch = run_title_matcher(post, cfg)
        if args.live:
            t_title, score, cert, rel, link = summarize_title_matcher(tmatch)
            print(f"[TM] best score={score} certainty={cert} rel={rel}")
            if t_title != "(unknown)":
                print(f"     -> {t_title} | {flair_from_rep(tmatch)} | {link or '(no link)'}")

        # Poster is disabled — keep a neutral stub that DE accepts
        poster_rep = {"status": "NO_REPORT", "distance": None, "relation": "unknown"}

        context = {
            "author": getattr(getattr(post, "author", None), "name", None),
            "flair_in": flair,
            "post_id": pid,
            "url": getattr(post, "permalink", None),
            "source": source,
        }

        decision = run_decision_engine(context, validator, tmatch, poster_rep, cfg)

        decisions_count[decision.get("action", "OTHER")] = decisions_count.get(decision.get("action", "OTHER"), 0) + 1
        if args.live:
            print_decision(decision, tmatch, poster_rep)

        if jsonl_path:
            payload = {
                "ts": iso(utcnow()),
                "source": source,
                "post_id": pid,
                "context": {"author": context["author"], "flair": flair, "title": title},
                "decision": decision,
            }
            try:
                append_jsonl(jsonl_path, payload)
            except Exception as e:
                print(f"[LOG][WARN] JSONL append failed: {e}", file=sys.stderr)

        if csv_path:
            row = {
                "ts": iso(utcnow()),
                "source": source,
                "post_id": pid,
                "author": context["author"],
                "flair": flair,
                "title": title,
                "action": decision.get("action"),
                "category": decision.get("category"),
                "reason": decision.get("reason"),
            }
            try:
                append_csv(csv_path, row, header_order=list(row.keys()))
            except Exception as e:
                print(f"[LOG][WARN] CSV append failed: {e}", file=sys.stderr)

        processed += 1

    if args.state_file:
        try:
            save_state(args.state_file, state)
        except Exception as e:
            print(f"[WARN] failed to save state: {e}", file=sys.stderr)

    if args.live or args.verbose:
        total = len(posts)
        print(f"[SUMMARY] total={total} processed={processed} skipped_due_to_state={skipped} decisions={{AUTO_REMOVE:{decisions_count.get('AUTO_REMOVE',0)}, MOD_QUEUE:{decisions_count.get('MOD_QUEUE',0)}, NO_ACTION:{decisions_count.get('NO_ACTION',0)}}}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Ctrl+C", file=sys.stderr)
        raise
