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
from typing import Any, Dict, Iterable, List, Optional, Tuple


# Silence noisy warnings on GitHub runners (kept defensive even with pinned deps)
warnings.filterwarnings("ignore", message="Version .* of praw is outdated")


try:
import praw
except Exception:
praw = None


try:
import yaml
except Exception:
yaml = None


# Optional local modules (we'll guard calls)
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




def ensure_dir(path: str) -> None:
d = os.path.dirname(path)
if d and not os.path.exists(d):
os.makedirs(d, exist_ok=True)




def load_yaml(path: str) -> Dict[str, Any]:
if not path or not os.path.exists(path):
return {}
if yaml is None:
return {}
with open(path, "r", encoding="utf-8") as f:
return yaml.safe_load(f) or {}




def load_state(path: Optional[str]) -> Dict[str, Any]:
if not path:
return {}
if not os.path.exists(path):
return {}
try:
with open(path, "r", encoding="utf-8") as f:
return json.load(f)
except Exception:
return {}




def save_state(path: Optional[str], state: Dict[str, Any]) -> None:
if not path:
return
ensure_dir(path)
with open(path, "w", encoding="utf-8") as f:
raise SystemExit(main())
