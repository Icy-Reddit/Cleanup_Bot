#!/usr/bin/env python3
"""
de_smoketest.py — quick I/O smoketests for decision_engine.decide()

Scenarios covered:
  1) MISSING          -> AUTO_REMOVE | MISSING
  2) REPEATED         -> AUTO_REMOVE | REPEATED
  3) DUPLICATE        -> AUTO_REMOVE | DUPLICATE
  4) BORDERLINE       -> MOD_QUEUE   | AMBIGUOUS
  5) NO_SIGNAL        -> NO_ACTION   | NO_SIGNAL

Usage:
  python de_smoketest.py --case all
  python de_smoketest.py --case repeated --print-json
  python de_smoketest.py --config config.yaml --case nosignal

Exit codes:
  0 = all selected cases passed
  1 = any failure (mismatch or exception)
"""

from __future__ import annotations
import argparse
import json
import os
import sys
from typing import Any, Dict, List, Tuple

# Local imports
try:
    import yaml
except Exception:
    yaml = None

try:
    import decision_engine
except Exception as e:
    print(f"[FATAL] Cannot import decision_engine: {e}", file=sys.stderr)
    sys.exit(1)


# ---------- Helpers ----------

DEFAULTS = {
    "decision": {
        "title_threshold_auto": 93,
        "title_threshold_border": 85,
        "text_short_title_min_tokens": 3,
        "time_window_days": 14,
    },
    "comments": {
        "repeated_request_template":
            "This link was already requested. Please use the search bar.",
        "missing_title_template":
            "Your post has been removed because it doesn't include the drama name...",
    },
}

def load_config(path: str | None) -> Dict[str, Any]:
    if not path:
        return DEFAULTS
    if not os.path.exists(path):
        print(f"[WARN] config.yaml not found at {path}, using defaults.", file=sys.stderr)
        return DEFAULTS
    if yaml is None:
        print("[WARN] pyyaml not available, using defaults.", file=sys.stderr)
        return DEFAULTS
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    # merge defaults (shallow)
    cfg = DEFAULTS.copy()
    for k, v in (data or {}).items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            merged = cfg[k].copy()
            merged.update(v)
            cfg[k] = merged
        else:
            cfg[k] = v
    return cfg

def ev_title(score: int, relation: str, typ: str = "fuzzy", title: str = "dummy", permalink: str = "/r/CShortDramas/comments/xyz/dummy/") -> dict:
    return {
        "best": {
            "score": score,
            "certainty": "certain" if score >= 93 else ("borderline" if score >= 85 else "low"),
            "relation": relation,
            "type": typ,
            "candidate": {
                "title": title,
                "permalink": permalink,
                "flair": "📌 Link Request",
                "author": "u/tester",
            },
        },
        "top": [],
        "pool_ids": [],
    }

def ev_poster(status: str = "NONE", distance: int | None = None) -> dict:
    # Accepts also NO_REPORT to mimic scanner
    return {"status": status, "distance": distance, "candidate": None}

def ctx() -> dict:
    return {"author": "tester", "flair_in": "📌 Link Request", "post_id": "abc123", "url": "/r/CShortDramas/comments/abc123/dummy/", "source": "manual_test"}


# ---------- Scenarios ----------

def mk_case_missing(cfg: dict) -> Tuple[str, dict, dict, dict, dict, Tuple[str, str]]:
    # Validator says title missing; title/poster evidence should be ignored for decision.
    return (
        "MISSING",
        ctx(),
        {"status": "MISSING", "reason": "only_empty_phrases"},
        ev_title(score=88, relation="different_author"),  # should be ignored
        ev_poster("NO_REPORT"),
        ("AUTO_REMOVE", "MISSING"),
    )

def mk_case_repeated(cfg: dict) -> Tuple[str, dict, dict, dict, dict, Tuple[str, str]]:
    # Different author + title >= auto_t (default 93) => REPEATED
    return (
        "REPEATED",
        ctx(),
        {"status": "OK", "reason": "title_candidate"},
        ev_title(score=95, relation="different_author"),
        ev_poster("NONE"),
        ("AUTO_REMOVE", "REPEATED"),
    )

def mk_case_duplicate(cfg: dict) -> Tuple[str, dict, dict, dict, dict, Tuple[str, str]]:
    # Same author + title >= auto_t => DUPLICATE
    return (
        "DUPLICATE",
        ctx(),
        {"status": "OK", "reason": "title_candidate"},
        ev_title(score=100, relation="same_author", typ="normalized_exact"),
        ev_poster("NONE"),
        ("AUTO_REMOVE", "DUPLICATE"),
    )

def mk_case_borderline(cfg: dict) -> Tuple[str, dict, dict, dict, dict, Tuple[str, str]]:
    # Borderline (85–92) + no poster => MOD_QUEUE / AMBIGUOUS
    return (
        "BORDERLINE",
        ctx(),
        {"status": "OK", "reason": "title_candidate"},
        ev_title(score=88, relation="unknown"),
        ev_poster("NO_REPORT"),
        ("MOD_QUEUE", "AMBIGUOUS"),
    )

def mk_case_nosignal(cfg: dict) -> Tuple[str, dict, dict, dict, dict, Tuple[str, str]]:
    # Low score + no poster => NO_ACTION / NO_SIGNAL
    return (
        "NO_SIGNAL",
        ctx(),
        {"status": "OK", "reason": "title_candidate"},
        ev_title(score=57, relation="different_author"),
        ev_poster("NONE"),
        ("NO_ACTION", "NO_SIGNAL"),
    )


CASES = {
    "missing": mk_case_missing,
    "repeated": mk_case_repeated,
    "duplicate": mk_case_duplicate,
    "borderline": mk_case_borderline,
    "nosignal": mk_case_nosignal,
}


# ---------- Runner ----------

def run_case(name: str, cfg: dict, print_json: bool) -> bool:
    mk = CASES[name]
    label, context, validator, title_report, poster_report, expected = mk(cfg)
    try:
        rep = decision_engine.decide(
            context=context,
            validator=validator,
            title_report=title_report,
            poster_report=poster_report,
            config=cfg,
        )
    except Exception as e:
        print(f"[{label}] EXCEPTION: {e}", file=sys.stderr)
        return False

    act = (rep or {}).get("action")
    cat = (rep or {}).get("category")
    ok = (act, cat) == expected

    status = "PASS" if ok else "FAIL"
    print(f"[{label}] {status} -> got action={act} category={cat} | expected={expected}")
    if print_json:
        print(json.dumps(rep, ensure_ascii=False, indent=2))

    if not ok:
        # quick diff hints
        print(f"[{label}] Reason: {(rep or {}).get('reason')}", file=sys.stderr)
    return ok

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml", help="Path to config.yaml (optional).")
    ap.add_argument("--case", choices=["all"] + list(CASES.keys()), default="all")
    ap.add_argument("--print-json", action="store_true", help="Print full DecisionReport JSON for each case.")
    args = ap.parse_args()

    cfg = load_config(args.config)

    names: List[str] = list(CASES.keys()) if args.case == "all" else [args.case]
    any_fail = False
    for n in names:
        ok = run_case(n, cfg, args.print_json)
        if not ok:
            any_fail = True

    if any_fail:
        print("[RESULT] FAIL", file=sys.stderr)
        return 1

    print("[RESULT] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
