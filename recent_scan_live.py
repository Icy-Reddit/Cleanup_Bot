#!/usr/bin/env python3
"""
recent_scan_live.py — Titlematch live scanner (24/7-friendly)

- Scans /new and/or ModQueue for r/CShortDramas (default), window-based.
- Poster matcher is OFF by default; accepted flags exist for compatibility.
- Integrates Title Validator -> Title Matcher -> Decision Engine.
- Supports JSONL logging and simple state to avoid reprocessing the same posts between runs.

CLI:
  --config PATH                YAML config (default: ./config.yaml)
  --window MIN                 Lookback window in minutes (default: 60)
  --sources {new,modqueue,both}
  --limit-per-source N         Max items fetched from each source (default: 200)
  --poster {auto,never,always} Poster matcher mode (default: never; no-op here)
  --poster-pool {top,full}     Candidate pool source (default: top; advisory to title matcher)
  --fetch-per-flair N          Optional cap per flair when building candidate pools
  --live                       Print human-readable console output
  --log-jsonl [PATH]           Append decision JSON to JSONL (default: logs/decisions_YYYY-MM-DD.jsonl)
  --report-csv [PATH]          Append a flat CSV summary (optional)
  --state-file PATH            JSON state to deduplicate processed post IDs across runs
  --state-ttl-min MIN          TTL minutes for cached IDs (default: 180)
  --subreddit NAME             Subreddit to scan (default: CShortDramas)
  --verbose                    More detailed prints

Notes:
- This file tries to be resilient to small API differences in local modules by using adapters and introspection.
- PRAW site section expected: [Cleanup_Bot]; fallback to [DEFAULT].
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import inspect
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Tuple

# --- Third-party ---
try:
    import yaml
except Exception:
    print("[FATAL] PyYAML is required. pip install pyyaml", file=sys.stderr)
    sys.exit(1)

try:
    import praw
except Exception:
    print("[FATAL] PRAW is required. pip install praw", file=sys.stderr)
    sys.exit(1)

# --- Local modules ---
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


# ------------------------------- Utils ---------------------------------

def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def iso(ts: dt.datetime) -> str:
    return ts.astimezone(dt.timezone.utc).isoformat()

def load_config(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"config.yaml not found at: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return (yaml.safe_load(f) or {})

def get_reddit():
    """Create Reddit client using praw.ini [Cleanup_Bot], fallback to [DEFAULT]."""
    try:
        return praw.Reddit("Cleanup_Bot")
    except Exception:
        return praw.Reddit("DEFAULT")

def ensure_dir(p: str) -> None:
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)

def append_jsonl(path: str, obj: dict) -> None:
    ensure_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def append_csv(path: str, row: Dict[str, Any], header_order: Optional[List[str]] = None) -> None:
    ensure_dir(path)
    exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header_order or list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)

def load_state(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {"ids": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"ids": {}}

def save_state(path: Optional[str], state: Dict[str, Any]) -> None:
    if not path:
        return
    ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)

def gc_state(state: Dict[str, Any], ttl_min: int) -> None:
    """Garbage collect IDs older than ttl_min."""
    if ttl_min <= 0:
        return
    cutoff = utcnow().timestamp() - ttl_min * 60
    ids = state.get("ids", {})
    to_del = [pid for pid, ts in ids.items() if ts < cutoff]
    for pid in to_del:
        ids.pop(pid, None)
    state["ids"] = ids


# ------------------------------ Fetch ----------------------------------

def fetch_candidates(r: praw.Reddit, sub_name: str, sources: str, limit_per_source: int, window_min: int) -> List[Any]:
    """Fetch posts from /new and/or modqueue within window_min minutes."""
    sub = r.subreddit(sub_name)
    now = utcnow()
    min_ts = now - dt.timedelta(minutes=window_min)

    out = []

    def ok(created_utc: float) -> bool:
        return dt.datetime.fromtimestamp(created_utc, tz=dt.timezone.utc) >= min_ts

    if sources in ("new", "both"):
        try:
            for s in sub.new(limit=limit_per_source):
                if ok(s.created_utc):
                    out.append(("new", s))
        except Exception as e:
            print(f"[WARN] Failed to fetch /new: {e}", file=sys.stderr)

    if sources in ("modqueue", "both"):
        try:
            for s in sub.mod.modqueue(limit=limit_per_source):
                # Modqueue can include comments; filter to submissions
                if getattr(s, "created_utc", None) and ok(s.created_utc):
                    out.append(("modqueue", s))
        except Exception as e:
            print(f"[WARN] Failed to fetch modqueue: {e}", file=sys.stderr)

    # Sort ascending by creation time
    out.sort(key=lambda it: getattr(it[1], "created_utc", 0.0))
    return out


# ---------------------------- Adapters ---------------------------------

def run_title_validator(title: str, flair: str, cfg: dict) -> Dict[str, Any]:
    """Call title_validator.validate_title with compatible signature."""
    if title_validator and hasattr(title_validator, "validate_title"):
        try:
            fn = title_validator.validate_title
            params = [p.lower() for p in inspect.signature(fn).parameters.keys()]
            if all(x in params for x in ("title", "flair", "config")):
                return fn(title, flair, cfg)   # Your variant
            else:
                return fn(title)               # Fallback variant
        except Exception as e:
            return {"status": "AMBIGUOUS", "reason": f"validator_error: {e}"}
    # Fallback heuristic if module missing:
    title_clean = (title or "").strip()
    if not title_clean or len(title_clean.split()) < 3:
        return {"status": "AMBIGUOUS", "reason": "short_or_missing"}
    return {"status": "OK", "reason": "title_candidate"}

def run_title_matcher(post: Any, cfg: dict) -> Dict[str, Any]:
    """Call into title_matcher using only safe kwargs that match its signature.
    Now also supports matchers that require keyword-only args like `title_raw` and `author_name`.
    """
    if not title_matcher:
        return {"best": None, "pool_ids": [], "top": []}

    try_order = []
    fn = getattr(title_matcher, "match_title_for_post", None)
    if callable(fn):
        try_order.append(("match_title_for_post", fn))
    fn2 = getattr(title_matcher, "match_title", None)
    if callable(fn2):
        try_order.append(("match_title", fn2))

    # Gather common context once
    author_obj = getattr(post, "author", None)
    author_name = getattr(author_obj, "name", None)
    flair_in = getattr(post, "link_flair_text", None) or ""
    permalink = getattr(post, "permalink", None)
    pid = getattr(post, "id", None)
    title_raw = getattr(post, "title", None)
    subreddit = getattr(getattr(post, "subreddit", None), "display_name", None)
    created_utc = getattr(post, "created_utc", None)
    reddit_obj = getattr(post, "_reddit", None)

    for name, fn in try_order:
        try:
            code = getattr(fn, "__code__", None)
            params = set(code.co_varnames[:code.co_argcount]) if code else set()
            kw = {}
            # Core expected params
            if "post" in params:
                kw["post"] = post
            if "config" in params:
                kw["config"] = cfg
            if "exclude_post_id" in params:
                kw["exclude_post_id"] = pid
            if "exclude_post_url" in params:
                kw["exclude_post_url"] = permalink
            # Extra keyword-only style params (some implementations require these)
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

def run_decision_engine(context: Dict[str, Any], validator: Dict[str, Any], title_report: Dict[str, Any], poster_report: Optional[Dict[str, Any]], cfg: dict) -> Dict[str, Any]:
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
                return asdict(rep)  # type: ignore
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
    # Fallback minimal rule (should rarely trigger):
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


# ------------------------------ Rendering -------------------------------

def print_human_post(source: str, post: Any, body_preview: Optional[str] = None) -> None:
    created = dt.datetime.fromtimestamp(getattr(post, "created_utc", 0.0), tz=dt.timezone.utc).isoformat()
    author = f"u/{getattr(getattr(post, 'author', None), 'name', 'unknown')}"
    flair = getattr(post, "link_flair_text", None) or ""
    pid = getattr(post, "id", "")
    print("\n======================================================================")
    print(f"[POST] {author} | flair={flair} | id={pid} | at={created}")
    if body_preview:
        print(f"      {body_preview}")
    permalink = getattr(post, 'permalink', None)
    if permalink:
        base = "https://www.reddit.com"
        if permalink.startswith("http"):
            print(f"      {permalink}")
        else:
            print(f"      {base}{permalink}")

def print_validator(rep: Dict[str, Any]) -> None:
    print(f"[VALID] status={rep.get('status')} reason={rep.get('reason')}")

def flair_from_rep(rep: Dict[str, Any]) -> str:
    best = (rep or {}).get("best") or {}
    cand = best.get("candidate") or {}
    return cand.get("flair") or cand.get("link_flair_text") or ""

def summarize_title_matcher(rep: Dict[str, Any]) -> Tuple[str, int, str, str, Optional[str]]:
    best = (rep or {}).get("best") or {}
    score = int(best.get("score") or 0)
    certainty = best.get("certainty") or "low"
    relation = best.get("relation") or "unknown"
    cand = best.get("candidate") or {}
    title = cand.get("title") or cand.get("post_title") or "(unknown)"
    link = cand.get("permalink") or cand.get("url") or None
    return title, score, certainty, relation, link

def print_decision(dec: Dict[str, Any], title_rep: Dict[str, Any], poster_rep: Optional[Dict[str, Any]]) -> None:
    print("=============== DECISION ENGINE ===============")
    print(f"When: {iso(utcnow())}")
    print(f"Action: {dec.get('action')} | Category: {dec.get('category')}")
    rr = dec.get("removal_reason")
    print(f"Removal Reason: {rr if rr else 'None'}\n")
    # Title evidence
    t_title, t_score, t_cert, t_rel, t_link = summarize_title_matcher(title_rep)
    print("-- Title Match --")
    print(f"type=fuzzy | score={t_score} | certainty={t_cert} | relation={t_rel}")
    author = (title_rep.get("best", {}) or {}).get("candidate", {}) or {}
    print(f"  title='{t_title}' | flair={flair_from_rep(title_rep)} | author={author.get('author','')}")
    if t_link:
        print(f"  link={t_link}\n")
    else:
        print()
    # Poster (always NO_REPORT here)
    p = poster_rep or {"status": "NO_REPORT", "distance": None, "relation": "unknown"}
    print("-- Poster Match --")
    print(f"status={p.get('status')} | distance={p.get('distance')} | relation={p.get('relation')}\n")
    # Links
    links = dec.get("links") or []
    if links:
        print("Links:")
        for L in links:
            print(f"- {L}")
    print("===============================================")

# ------------------------------ Main ------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--window", type=int, default=60, help="Lookback window minutes")
    ap.add_argument("--sources", choices=("new","modqueue","both"), default="both")
    ap.add_argument("--limit-per-source", type=int, default=200)
    ap.add_argument("--poster", choices=("auto","never","always"), default="never")
    ap.add_argument("--poster-pool", choices=("top","full"), default="top")
    ap.add_argument("--fetch-per-flair", type=int, default=None)
    ap.add_argument("--live", action="store_true", help="Human-readable console output")
    ap.add_argument("--log-jsonl", nargs="?", const="", default=None, help="Append decisions to JSONL (optional path)")
    ap.add_argument("--report-csv", nargs="?", const="", default=None, help="Append flat CSV summary (optional path)")
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

    # State
    state = load_state(args.state_file)
    gc_state(state, args.state_ttl_min)

    posts = fetch_candidates(
        r=r,
        sub_name=args.subreddit,
        sources=args.sources,
        limit_per_source=args.limit_per_source,
        window_min=args.window,
    )

    if args.verbose:
        print(f"[INFO] fetched={len(posts)} from sources={args.sources} window={args.window}min")

    processed = 0
    skipped = 0
    decisions_count = {"AUTO_REMOVE":0, "MOD_QUEUE":0, "NO_ACTION":0, "OTHER":0}

    # Default JSONL path
    jsonl_path = None
    if args.log_jsonl is not None:
        if args.log_jsonl == "":
            jsonl_path = os.path.join("logs", f"decisions_{utcnow().date().isoformat()}.jsonl")
        else:
            jsonl_path = args.log_jsonl
        ensure_dir(jsonl_path)

    csv_path = None
    if args.report_csv is not None:
        csv_path = args.report_csv or os.path.join("logs", f"decisions_{utcnow().date().isoformat()}.csv")
        ensure_dir(csv_path)

    for source, post in posts:
        pid = getattr(post, "id", None)
        if not pid:
            continue
        # dedup by state
        if args.state_file:
            seen = state.setdefault("ids", {})
            if pid in seen:
                skipped += 1
                if args.verbose:
                    print(f"[SKIP] already processed {pid}")
                continue
            # mark as seen (timestamp)
            seen[pid] = utcnow().timestamp()

        title = getattr(post, "title", "") or ""
        selftext = getattr(post, "selftext", "") or ""
        preview = (selftext or "")[:160].replace("\n", " ").strip()
        flair = getattr(post, "link_flair_text", None) or ""

        if args.live:
            print_human_post(source, post, body_preview=preview if preview else None)

        validator = run_title_validator(title, flair, cfg)
        if args.live:
            print_validator(validator)

        # Title matcher (safe-call without unsupported kwargs)
        tmatch = run_title_matcher(post, cfg)
        if args.live:
            t_title, score, cert, rel, link = summarize_title_matcher(tmatch)
            print(f"[TM] best score={score} certainty={cert} rel={rel}")
            if t_title != "(unknown)":
                print(f"     -> {t_title} | {flair_from_rep(tmatch)} | {link or '(no link)'}")

        poster_rep = {"status":"NO_REPORT","distance":None,"relation":"unknown"}

        context = {
            "author": getattr(getattr(post, "author", None), "name", None),
            "flair_in": flair,
            "post_id": pid,
            "url": getattr(post, "permalink", None),
            "source": source,
        }

        decision = run_decision_engine(context, validator, tmatch, poster_rep, cfg)

        # Counters
        decisions_count[decision.get("action","OTHER")] = decisions_count.get(decision.get("action","OTHER"),0) + 1

        if args.live:
            print_decision(decision, tmatch, poster_rep)

        # JSONL
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

        # CSV summary
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

    # Save state
    if args.state_file:
        try:
            save_state(args.state_file, state)
        except Exception as e:
            print(f"[WARN] failed to save state: {e}", file=sys.stderr)

    # Summary
    if args.live or args.verbose:
        total = len(posts)
        print(f"[SUMMARY] total={total} processed={processed} skipped_due_to_state={skipped} "
              f"decisions={{AUTO_REMOVE:{decisions_count.get('AUTO_REMOVE',0)}, "
              f"MOD_QUEUE:{decisions_count.get('MOD_QUEUE',0)}, "
              f"NO_ACTION:{decisions_count.get('NO_ACTION',0)}}}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Ctrl+C", file=sys.stderr)
        raise
