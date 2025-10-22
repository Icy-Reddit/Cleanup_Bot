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
from typing import Any, Dict, List, Optional, Tuple

# Silence noisy warnings on GitHub runners (kept defensive even with pinned deps)
warnings.filterwarnings("ignore", message="Version .* of praw is outdated")

# --- deps (importy normalne; moduły są w repo) ---
import praw
import yaml
import title_validator
import title_matcher
import decision_engine


# ------------------------ Utils ------------------------
def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def load_yaml(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_state(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {"ids": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {"ids": {}}
            data.setdefault("ids", {})
            return data
    except Exception:
        return {"ids": {}}


def save_state(path: Optional[str], state: Dict[str, Any]) -> None:
    if not path:
        return
    ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ------------------------ Reddit I/O ------------------------
def make_reddit(cfg: Dict[str, Any]):
    section = (cfg.get("praw_section") or "Cleanup_Bot").strip()
    return praw.Reddit(site_name=section)


def fetch_candidates(
    reddit_obj,
    window_min: int,
    sources: str = "both",
    limit_per_source: int = 200,
) -> List[Tuple[str, Any]]:
    out: List[Tuple[str, Any]] = []
    sub = reddit_obj.subreddit("CShortDramas")
    min_ts = utcnow() - dt.timedelta(minutes=window_min)

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
                if ok(getattr(s, "created_utc", 0.0)):
                    out.append(("modqueue", s))
        except Exception as e:
            print(f"[WARN] Failed to fetch modqueue: {e}", file=sys.stderr)

    return out


# ------------------------ Adapters ------------------------
def run_title_validator(title: str, flair_in: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    # Try (title, flair, config) first; then (title, flair); then (title)
    for name in ("validate_title", "validate"):
        if not hasattr(title_validator, name):
            continue
        fn = getattr(title_validator, name)
        try:
            params = inspect.signature(fn).parameters
            if "config" in params:
                return fn(title, flair_in, cfg)
            elif len(params) >= 2:
                return fn(title, flair_in)
            else:
                return fn(title)
        except TypeError as e:
            print(f"[WARN] title_validator.{name} signature mismatch: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"[WARN] title_validator.{name} failed: {e}", file=sys.stderr)
            return {"status": "OK", "reason": "validator_error"}
    return {"status": "OK", "reason": "no_validator_fn"}


def run_title_matcher(post: Any, cfg: Dict[str, Any]) -> Dict[str, Any]:
    for name in ("match_title", "match"):
        if not hasattr(title_matcher, name):
            continue
        fn = getattr(title_matcher, name)
        try:
            params = inspect.signature(fn).parameters
            kw: Dict[str, Any] = {}
            title = getattr(post, "title", None)
            author_obj = getattr(post, "author", None)
            author_name = getattr(author_obj, "name", None) if author_obj else None
            flair_in = getattr(post, "link_flair_text", None) or ""
            permalink = getattr(post, "permalink", None)
            pid = getattr(post, "id", None)
            subreddit = getattr(getattr(post, "subreddit", None), "display_name", None)
            created_utc = getattr(post, "created_utc", None)

            if "title_raw" in params and title is not None:
                kw["title_raw"] = title
            if "post_id" in params and pid is not None:
                kw["post_id"] = pid
            if "permalink" in params and permalink is not None:
                kw["permalink"] = permalink
            if "flair_in" in params:
                kw["flair_in"] = flair_in
            if "author_name" in params and author_name is not None:
                kw["author_name"] = author_name
            if "subreddit" in params and subreddit is not None:
                kw["subreddit"] = subreddit
            if "reddit" in params:
                kw["reddit"] = reddit_obj
            if "post_created_utc" in params and created_utc is not None:
                kw["post_created_utc"] = created_utc
            if "config" in params:
                kw["config"] = cfg

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
    for name in ("decide", "decide_action"):
        if not hasattr(decision_engine, name):
            continue
        fn = getattr(decision_engine, name)
        try:
            rep = fn(
                context=context,
                validator=validator,
                title_match=title_report,
                poster_match=poster_report,
                config=cfg,
            )
            if not rep:
                raise ValueError("decision engine returned empty")
            return rep
        except TypeError as e:
            print(f"[WARN] decision_engine.{name} signature mismatch: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"[WARN] decision_engine.{name} failed: {e}", file=sys.stderr)
            break
    return {
        "action": "NO_ACTION",
        "category": "NO_SIGNAL",
        "reason": "decision_engine_error",
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
    t_title = cand.get("title") or "(unknown)"
    t_link = cand.get("permalink") or cand.get("url")
    return t_title, score, certainty, relation, t_link


def print_decision(dec: Dict[str, Any], title_rep: Dict[str, Any], poster_rep: Dict[str, Any]) -> None:
    print("=============== DECISION ENGINE ===============")
    print(f"When: {utcnow().isoformat()}")
    print(f"Action: {dec.get('action')} | Category: {dec.get('category')}")
    print(f"Reason: {dec.get('reason')}")
    print(f"Removal Reason: {dec.get('removal_reason') or 'None'}\n")

    print("-- Title Match --")
    t_title, score, certainty, relation, t_link = summarize_title_matcher(title_rep)
    print(f"type={'fuzzy' if score and score < 100 else 'exact'} | score={score} | certainty={certainty} | relation={relation}")
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
    ap.add_argument("--report-jsonl", default=None)
    ap.add_argument("--report-csv", default=None)
    # Backward-compat flags:
    ap.add_argument("--log-jsonl", dest="report_jsonl")
    ap.add_argument("--log-csv", dest="report_csv")
    ap.add_argument("--state-file", default=os.path.join("cache", "state.json"))
    ap.add_argument("--state-ttl-min", type=int, default=180)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    reddit_obj = make_reddit(cfg)

    posts = fetch_candidates(
        reddit_obj, args.window, sources=args.sources, limit_per_source=args.limit_per_source
    )
    posts.sort(key=lambda t: getattr(t[1], "created_utc", 0.0))

    # State handling
    state = load_state(args.state_file)
    ttl = dt.timedelta(minutes=max(1, args.state_ttl_min))

    # GC old entries
    seen = state.setdefault("ids", {})
    to_del: List[str] = []
    for pid, ts in list(seen.items()):
        try:
            when = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        except Exception:
            to_del.append(pid)
            continue
        if utcnow() - when > ttl:
            to_del.append(pid)
    for pid in to_del:
        seen.pop(pid, None)

    processed = 0
    skipped = 0
    decisions_count: Dict[str, int] = {}

    # Reporting setup
    jsonl_path = args.report_jsonl or os.path.join("logs", f"actions_{utcnow().date().isoformat()}.jsonl")
    ensure_dir(jsonl_path)
    csv_path = None
    if args.live:
        csv_path = args.report_csv or os.path.join("logs", f"decisions_{utcnow().date().isoformat()}.csv")
        ensure_dir(csv_path)

    for source, post in posts:
        pid = getattr(post, "id", None)
        if not pid:
            continue

        # Skip by state
        if args.state_file:
            seen = state.setdefault("ids", {})
            if pid in seen:
                skipped += 1
                if args.verbose:
                    print(f"[SKIP] already processed {pid}")
                continue
            seen[pid] = utcnow().timestamp()

        # Basic fields
        flair = getattr(post, "link_flair_text", None) or ""
        title = getattr(post, "title", "") or ""
        selftext = getattr(post, "selftext", "") or ""
        preview = (selftext or "")[:160].replace("\n", " ").strip()

        # --- HARD POLICY GATE: Only analyze Link Request (from /new and modqueue) ---
        flair_norm = (flair or "").strip().lower()
        if flair_norm != "link request":
            if args.verbose:
                if flair_norm in {"found & shared", "request complete"}:
                    print(f"[POLICY] Skip result post: {flair}")
                elif flair_norm in {"inquiry", "actor inquiry"}:
                    print(f"[POLICY] Skip inquiry: {flair}")
                else:
                    print(f"[POLICY] Skip unsupported flair: {flair} (norm='{flair_norm}')")
            continue

        # Human print for Link Request only
        if args.live:
            print_human_post(source, post, body_preview=preview or None)

        # Validate title
        validator = run_title_validator(title, flair, cfg)
        if args.live:
            print_validator(validator)

        # Title matcher
        tmatch = run_title_matcher(post, cfg)
        if args.live:
            t_title, score, cert, rel, link = summarize_title_matcher(tmatch)
            print(f"[TM] best score={score} certainty={cert} rel={rel}")
            if t_title != "(unknown)":
                print(f"     -> {t_title} | {flair_from_rep(tmatch)} | {link or '(no link)'}")

        # Poster is disabled — keep a neutral stub
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

        # Log JSONL
        try:
            with open(jsonl_path, "a", encoding="utf-8") as jf:
                rec = {
                    "when": utcnow().isoformat(),
                    "post_id": pid,
                    "source": source,
                    "flair": flair,
                    "validator": validator,
                    "title_match": tmatch,
                    "poster_match": poster_rep,
                    "decision": decision,
                }
                jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"[WARN] failed to write JSONL: {e}", file=sys.stderr)

        # Log CSV (only when live)
        if csv_path:
            try:
                fresh = not os.path.exists(csv_path)
                with open(csv_path, "a", newline="", encoding="utf-8") as cf:
                    wr = csv.writer(cf)
                    if fresh:
                        wr.writerow(["when", "post_id", "source", "flair", "action", "category", "reason", "score", "certainty", "relation"])
                    _, s, c, r, _ = summarize_title_matcher(tmatch)
                    wr.writerow([utcnow().isoformat(), pid, source, flair, decision.get("action"), decision.get("category"), decision.get("reason"), s, c, r])
            except Exception as e:
                print(f"[WARN] failed to write CSV: {e}", file=sys.stderr)

        processed += 1

    save_state(args.state_file, state)
    print("\n[SUMMARY] total={} processed={} skipped_due_to_state={} decisions={}".format(
        len(posts), processed, skipped, decisions_count
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
