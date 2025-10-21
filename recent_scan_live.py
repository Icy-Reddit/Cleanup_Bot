
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
recent_scan_live.py — LIVE scan and act with intra-batch dedup + idempotency ledger
"""
from __future__ import annotations
import argparse, os, re, json, csv
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import yaml, praw

from title_validator import validate_title
from title_matcher import match_title, normalize_title
from decision_engine import decide, pretty_print_decision

# --- helpers ---
_EMOJI_OR_SYMBOLS_RE = re.compile(r"[\u2600-\u27BF\U0001F300-\U0001FAFF]")
_MULTISPACE_RE = re.compile(r"\s+")

def normalize_flair(text: str | None) -> str:
    if not text:
        return ""
    t = _EMOJI_OR_SYMBOLS_RE.sub(" ", text)
    t = re.sub(r"[^\w\s&]+", " ", t).strip().lower()
    return _MULTISPACE_RE.sub(" ", t)

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def within_window(ts_utc: float, window_min: int) -> bool:
    dt = datetime.fromtimestamp(ts_utc, tz=timezone.utc)
    return (datetime.now(timezone.utc) - dt) <= timedelta(minutes=window_min)

def fetch_candidates(r: praw.Reddit, subreddit: str, window_min: int, sources: str, limit_per_source: int):
    s = r.subreddit(subreddit)
    out, seen = [], set()

    def add_if_ok(p):
        if getattr(p, "id", None) in seen:
            return
        if within_window(p.created_utc, window_min):
            out.append(p); seen.add(p.id)

    if sources in ("new", "both"):
        for p in s.new(limit=limit_per_source):
            add_if_ok(p)

    if sources in ("modqueue", "both"):
        for itm in s.mod.modqueue(limit=limit_per_source):
            try:
                if getattr(itm, "created_utc", None) is None: 
                    continue
                if getattr(itm, "title", None): 
                    add_if_ok(itm)
            except Exception:
                continue

    out.sort(key=lambda p: p.created_utc)
    return out

def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))

def _ensure_dir(p: str):
    Path(p).parent.mkdir(parents=True, exist_ok=True)

def _jsonl_path(base: str | None) -> str:
    if base:
        _ensure_dir(base); 
        return base
    d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(_script_dir(), "logs", f"decisions_{d}.jsonl")
    _ensure_dir(path)
    return path

def _append_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _csv_path(base: str | None) -> str:
    if base:
        _ensure_dir(base); 
        return base
    d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(_script_dir(), "logs", f"actions_{d}.csv")
    _ensure_dir(path)
    return path

def _append_csv(path: str, rows: List[Dict[str, Any]]):
    if not rows: 
        return
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp","action","category","flair","title","author","link","post_id","reason"
        ])
        if not exists: 
            w.writeheader()
        for r in rows: 
            w.writerow(r)

def _load_state(path: str, ttl_minutes: int) -> dict:
    state = {}
    p = Path(path)
    if p.exists():
        try:
            state = json.load(p.open("r", encoding="utf-8")) or {}
        except Exception:
            state = {}
    now = datetime.now(timezone.utc).timestamp()
    ttl = ttl_minutes * 60
    pruned = {pid: ts for pid, ts in state.items() if isinstance(ts, (int, float)) and (now - ts) < ttl}
    if pruned != state:
        try:
            _ensure_dir(path)
            json.dump(pruned, p.open("w", encoding="utf-8"), ensure_ascii=False)
        except Exception:
            pass
    return pruned

def _save_state(path: str, state: dict):
    _ensure_dir(path)
    try:
        json.dump(state, open(path, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception:
        pass

def _do_remove_with_comment(submission, *, comment_text: Optional[str]) -> None:
    already_removed = False
    try:
        submission.refresh()
        already_removed = bool(getattr(submission, "removed_by_category", None))
    except Exception:
        already_removed = False

    submission.mod.remove()
    if comment_text and not already_removed:
        try:
            submission.reply(comment_text)
        except Exception:
            pass

def _do_report(submission, *, reason: str) -> None:
    try: 
        submission.report(reason)
    except Exception: 
        pass

def main():
    ap = argparse.ArgumentParser(description="LIVE: scan N minutes and act per Decision Engine, with intra-batch dedup + idempotency ledger.")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--window", type=int, default=35)
    ap.add_argument("--sources", choices=["new", "modqueue", "both"], default="both")
    ap.add_argument("--limit-per-source", type=int, default=400)
    ap.add_argument("--poster", choices=["auto", "never", "always"], default="never")
    ap.add_argument("--poster-pool", choices=["top", "full"], default="top")
    ap.add_argument("--fetch-per-flair", type=int, default=200)

    ap.add_argument("--live", action="store_true")
    ap.add_argument("--log-jsonl", nargs="?", const="")
    ap.add_argument("--report-csv", nargs="?", const="")
    ap.add_argument("--state-file", default=os.path.join(_script_dir(), "logs", "state_actions.json"))
    ap.add_argument("--state-ttl-min", type=int, default=360)
    ap.add_argument("--verbose", action="store_true")

    args = ap.parse_args()
    cfg = load_config(args.config)

    reddit_site = (cfg.get("reddit", {}) or {}).get("praw_site") or "Cleanup_Bot"
    subreddit_name = (cfg.get("reddit", {}) or {}).get("subreddit") or "CShortDramas"

    repeated_tmpl = (cfg.get("comments", {}) or {}).get("repeated_request_template")
    missing_tmpl  = (cfg.get("comments", {}) or {}).get("missing_title_template")

    if args.verbose:
        print(f"[DEBUG] {iso_now()} — LIVE={'YES' if args.live else 'NO'}; window={args.window}min; sources={args.sources}")
        try:
            r = praw.Reddit(reddit_site); print(f"[DEBUG] Auth as: {r.user.me()}")
        except Exception as e:
            print(f"[WARN] Cannot verify user.me(): {e}"); r = praw.Reddit(reddit_site)
    else:
        r = praw.Reddit(reddit_site)

    state = _load_state(args.state_file, args.state_ttl_min)

    subs = fetch_candidates(r, subreddit_name, args.window, args.sources, args.limit_per_source)
    if args.verbose: 
        print(f"[DEBUG] fetched submissions: {len(subs)}")

    actions_csv_rows: List[Dict[str, Any]] = []

    # ---------------- Intra-batch dedup for Link Request ----------------
    lr_posts = [p for p in subs if normalize_flair(getattr(p, "link_flair_text", None) or "") == "link request"]
    groups: Dict[str, List[Any]] = {}
    for p in lr_posts:
        t = getattr(p, "title", "") or ""
        key = normalize_title(t)
        if not key:
            continue
        groups.setdefault(key, []).append(p)

    handled_ids = set()
    for key, items in groups.items():
        if len(items) < 2:
            continue
        items.sort(key=lambda x: x.created_utc)
        keeper = items[0]
        keeper_author = (getattr(keeper.author, "name", "[deleted]") or "").lower()

        for dup in items[1:]:
            if dup.id in handled_ids:
                continue
            if dup.id in state:
                if args.verbose:
                    print(f"[SKIP] already acted recently on {dup.id} (ledger).")
                handled_ids.add(dup.id)
                continue

            dup_author = (getattr(dup.author, "name", "[deleted]") or "").lower()
            same_author = (dup_author == keeper_author)

            decision = {
                "when": datetime.now(timezone.utc).isoformat(),
                "action": "AUTO_REMOVE",
                "category": "DUPLICATE" if same_author else "REPEATED",
                "removal_reason": "Duplicate Post" if same_author else "Repeated Request",
                "removal_comment": None,
                "evidence": {"intrabatch_key": key, "keeper_id": keeper.id, "dup_id": dup.id},
                "links": [f"https://www.reddit.com{keeper.permalink}", f"https://www.reddit.com{dup.permalink}"],
            }

            if args.live:
                try:
                    if same_author:
                        _do_remove_with_comment(dup, comment_text=None)
                    else:
                        _do_remove_with_comment(dup, comment_text=repeated_tmpl)
                except Exception as e:
                    print(f"[WARN] Reddit action failed for {dup.id}: {e}")

            state[dup.id] = datetime.now(timezone.utc).timestamp()

            if args.log_jsonl is not None:
                payload = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "source": ("modqueue" if args.sources == "modqueue" else ("both" if args.sources=="both" else "new")),
                    "post_id": dup.id,
                    "context": {"author": getattr(dup.author, "name", "[deleted]") if dup.author else "[deleted]",
                                "flair": getattr(dup, "link_flair_text", None) or "",
                                "title": getattr(dup, "title", "") or ""},
                    "decision": decision,
                }
                _append_jsonl(_jsonl_path(args.log_jsonl if args.log_jsonl != "" else None), payload)

            if args.report_csv is not None:
                actions_csv_rows.append({
                    "timestamp": decision["when"],
                    "action": decision["action"],
                    "category": decision["category"],
                    "flair": getattr(dup, "link_flair_text", None) or "",
                    "title": getattr(dup, "title", "") or "",
                    "author": f"u/{getattr(dup.author, 'name', '[deleted]') if dup.author else '[deleted]'}",
                    "link": f"https://www.reddit.com{dup.permalink}",
                    "post_id": dup.id,
                    "reason": decision["removal_reason"],
                })

            handled_ids.add(dup.id)

    # ---------------- Main loop: process remaining posts ----------------
    for p in subs:
        if p.id in handled_ids:
            continue
        if p.id in state:
            if args.verbose:
                print(f"[SKIP] already acted recently on {p.id} (ledger).")
            continue

        author = getattr(p.author, "name", "[deleted]") if p.author else "[deleted]"
        flair = getattr(p, "link_flair_text", None) or ""
        flair_norm = normalize_flair(flair)
        title = getattr(p, "title", "") or ""
        selftext = getattr(p, "selftext", "") or ""

        if flair_norm != "link request":
            if args.verbose:
                if flair_norm in {"found & shared", "request complete"}:
                    print(f"[POLICY] Skip result post: {flair}")
                elif flair_norm in {"inquiry", "actor inquiry"}:
                    print(f"[POLICY] Skip inquiry: {flair}")
                else:
                    print(f"[POLICY] Skip unsupported flair: {flair} (norm='{flair_norm}')")
            continue

        print("\n" + "=" * 70)
        print(f"[POST] u/{author} | flair={flair} | id={p.id} | at={datetime.fromtimestamp(p.created_utc, tz=timezone.utc).isoformat()}")
        print(f"      {title}")
        print(f"      https://www.reddit.com{p.permalink}")

        vres = validate_title(title, flair, cfg)
        print(f"[VALID] status={vres.get('status')} reason={vres.get('reason')}")

        trep = None
        if vres.get("status") in ("OK", "AMBIGUOUS"):
            trep = match_title(
                title_raw=title,
                author_name=author,
                config=cfg,
                max_candidates=10,
                fetch_limit_per_flair=args.fetch_per_flair,
                exclude_post_ids=[p.id],
            )
            best = trep.get("best") if trep else None
            if best:
                print(f"[TM] best score={best.get('score')} certainty={best.get('certainty')} rel={best.get('relation')}")
                print(f"     -> {best.get('title_raw')} | {best.get('flair')} | {best.get('permalink')}")
            else:
                print("[TM] no candidates")

        prep = None

        context = {"author": author, "flair_in": flair, "title": title, "selftext": selftext or ""}
        d = decide(title_validation=vres, title_report=trep, poster_report=prep, context=context, config=cfg)
        pretty_print_decision(d)

        action_taken = None
        reason_text = d.get("removal_reason") or ""
        if args.live:
            try:
                if d.get("action") == "AUTO_REMOVE":
                    cat = d.get("category")
                    if cat == "MISSING":
                        _do_remove_with_comment(p, comment_text=missing_tmpl); action_taken = ("AUTO_REMOVE", cat)
                    elif cat == "DUPLICATE":
                        _do_remove_with_comment(p, comment_text=None); action_taken = ("AUTO_REMOVE", cat)
                    elif cat == "REPEATED":
                        _do_remove_with_comment(p, comment_text=repeated_tmpl); action_taken = ("AUTO_REMOVE", cat)
                    else:
                        _do_remove_with_comment(p, comment_text=None); action_taken = ("AUTO_REMOVE", cat or "UNKNOWN")
                elif d.get("action") == "MOD_QUEUE":
                    _do_report(p, reason=(reason_text or "Titlematch: needs moderator review"))
                    action_taken = ("REPORT", d.get("category") or "MOD_QUEUE")
            except Exception as e:
                print(f"[WARN] Reddit action failed for {p.id}: {e}")

        if args.log_jsonl is not None:
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "source": ("modqueue" if args.sources == "modqueue" else ("both" if args.sources=="both" else "new")),
                "post_id": p.id,
                "context": {"author": author, "flair": flair, "title": title},
                "decision": d,
            }
            _append_jsonl(_jsonl_path(args.log_jsonl if args.log_jsonl != "" else None), payload)

        if args.report_csv is not None and action_taken:
            actions_csv_rows.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": action_taken[0],
                "category": action_taken[1],
                "flair": flair,
                "title": title,
                "author": f"u/{author}",
                "link": f"https://www.reddit.com{p.permalink}",
                "post_id": p.id,
                "reason": reason_text,
            })

        if action_taken:
            state[p.id] = datetime.now(timezone.utc).timestamp()

    if args.report_csv is not None:
        _append_csv(_csv_path(args.report_csv if args.report_csv != "" else None), actions_csv_rows)

    _save_state(args.state_file, state)

    if args.verbose:
        print(f"[DEBUG] {iso_now()} — live run done.")

if __name__ == "__main__":
    raise SystemExit(main())
