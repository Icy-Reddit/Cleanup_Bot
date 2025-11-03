import argparse
import io
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import PIL.Image as Image
import requests
import yaml
import praw
import imagehash


# ---------- Utils ----------

def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dirs(cfg):
    os.makedirs(os.path.dirname(cfg["paths"]["index_db"]), exist_ok=True)
    os.makedirs(os.path.dirname(cfg["paths"]["state_file"]), exist_ok=True)
    os.makedirs(cfg["paths"]["reports_dir"], exist_ok=True)

def utc_ts(dt_str: str):
    # "YYYY-MM-DD" or ISO
    return int(datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc).timestamp())

def now_utc():
    return int(datetime.now(timezone.utc).timestamp())

def load_state(state_path):
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_indexed_utc": 0}

def save_state(state_path, state):
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def get_praw(cfg):
    return praw.Reddit(site_name=cfg["reddit"]["praw_site"])

def best_image_url(subm):
    # 1) gallery
    if getattr(subm, "is_gallery", False) and getattr(subm, "media_metadata", None):
        try:
            first = next(iter(subm.media_metadata.values()))
            if "s" in first and "u" in first["s"]:
                return first["s"]["u"]
        except Exception:
            pass

    # 2) preview highest quality
    try:
        if subm.preview and subm.preview.get("images"):
            src = subm.preview["images"][0]["source"]["url"]
            return src
    except Exception:
        pass

    # 3) direct override
    if getattr(subm, "url_overridden_by_dest", None):
        return subm.url_overridden_by_dest

    # 4) video thumb (v.redd.it)
    try:
        if subm.media and "reddit_video" in subm.media:
            if subm.preview and subm.preview.get("images"):
                return subm.preview["images"][0]["source"]["url"]
    except Exception:
        pass

    # 5) thumbnail fallback
    if getattr(subm, "thumbnail", None) and subm.thumbnail.startswith("http"):
        return subm.thumbnail

    return None

def fetch_image_bytes(url, timeout, max_bytes):
    headers = {"User-Agent": "PosterIndexer/1.0"}
    r = requests.get(url, timeout=timeout, stream=True, headers=headers)
    r.raise_for_status()
    data = io.BytesIO()
    size = 0
    for chunk in r.iter_content(8192):
        if not chunk:
            break
        size += len(chunk)
        if size > max_bytes:
            raise ValueError("image too large")
        data.write(chunk)
    return data.getvalue()

def open_image_safely(raw_bytes):
    img = Image.open(io.BytesIO(raw_bytes))
    img = img.convert("RGB")
    return img

def compute_hsv_hist(img: Image.Image):
    # bins: H=16, S=4, V=4 => 256 dims
    hsv = img.convert("HSV")
    arr = np.array(hsv, dtype=np.uint8)
    h, s, v = arr[..., 0], arr[..., 1], arr[..., 2]
    hist, _ = np.histogramdd(
        (h.flatten(), s.flatten(), v.flatten()),
        bins=(16, 4, 4),
        range=((0, 256), (0, 256), (0, 256))
    )
    hist = hist.astype(np.float32)
    hist /= (hist.sum() + 1e-9)
    # L2 norm (for późniejszej korelacji)
    norm = np.linalg.norm(hist)
    if norm > 0:
        hist = hist / norm
    return hist

def compute_hashes(img: Image.Image):
    # opcjonalne skalowanie do rozsądnej szerokości (przyspieszenie)
    max_side = 1024
    if max(img.size) > max_side:
        img = img.copy()
        img.thumbnail((max_side, max_side), Image.LANCZOS)

    ph16 = imagehash.phash(img, hash_size=16)
    ph8  = imagehash.phash(img, hash_size=8)
    dh16 = imagehash.dhash(img, hash_size=16)
    wh   = imagehash.whash(img, hash_size=16, image_scale=None, mode='haar')
    # center crop 80%
    w, h = img.size
    cw, ch = int(w*0.8), int(h*0.8)
    cx, cy = (w - cw)//2, (h - ch)//2
    ctr = img.crop((cx, cy, cx+cw, cy+ch))
    ctr_ph16 = imagehash.phash(ctr, hash_size=16)
    hsv_hist = compute_hsv_hist(img)

    return {
        "phash16": ph16.__str__(),
        "phash8":  ph8.__str__(),
        "dhash16": dh16.__str__(),
        "whash":   wh.__str__(),
        "center_phash16": ctr_ph16.__str__(),
        "hsv_hist": hsv_hist.tobytes()
    }

# ---------- DB ----------

DDL = """
CREATE TABLE IF NOT EXISTS posters (
  post_id TEXT PRIMARY KEY,
  created_utc INTEGER,
  author TEXT,
  flair TEXT,
  permalink TEXT,
  image_url TEXT,
  width INTEGER,
  height INTEGER,
  phash16 TEXT,
  phash8 TEXT,
  dhash16 TEXT,
  whash_haar TEXT,
  center_phash16 TEXT,
  hsv_hist BLOB,
  meta_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_created ON posters(created_utc);
CREATE INDEX IF NOT EXISTS idx_flair ON posters(flair);
"""

def db_open(path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    for stmt in DDL.strip().split(";\n"):
        if stmt.strip():
            conn.execute(stmt)
    return conn

def db_upsert(conn, rec):
    conn.execute("""
        INSERT INTO posters (post_id, created_utc, author, flair, permalink, image_url,
                             width, height, phash16, phash8, dhash16, whash_haar, center_phash16, hsv_hist, meta_json)
        VALUES (:post_id, :created_utc, :author, :flair, :permalink, :image_url,
                :width, :height, :phash16, :phash8, :dhash16, :whash_haar, :center_phash16, :hsv_hist, :meta_json)
        ON CONFLICT(post_id) DO UPDATE SET
          image_url=excluded.image_url,
          phash16=excluded.phash16,
          phash8=excluded.phash8,
          dhash16=excluded.dhash16,
          whash_haar=excluded.whash_haar,
          center_phash16=excluded.center_phash16,
          hsv_hist=excluded.hsv_hist,
          meta_json=excluded.meta_json
    """, rec)

# ---------- Reddit scan ----------

def iter_new_until_window(subreddit, since_ts, until_ts=None):
    # PRAW nie ma time-range, więc iterujemy .new() i break po dacie
    for subm in subreddit.new(limit=None):
        if until_ts and subm.created_utc > until_ts:
            # nowe, jeszcze nowsze — jedziemy dalej
            pass
        if since_ts and subm.created_utc < since_ts:
            break
        yield subm

def should_keep_image(img: Image.Image, cfg):
    w, h = img.size
    return (w >= cfg["indexing"]["min_width"]) and (h >= cfg["indexing"]["min_height"])

def run_index(cfg, since_ts, until_ts):
    ensure_dirs(cfg)
    conn = db_open(cfg["paths"]["index_db"])
    state = load_state(cfg["paths"]["state_file"])
    reddit = get_praw(cfg)
    sub = reddit.subreddit(cfg["reddit"]["subreddit"])

    last_indexed = state.get("last_indexed_utc", 0)
    if since_ts is None:
        since_ts = last_indexed

    count, new_last = 0, last_indexed

    for s in iter_new_until_window(sub, since_ts, until_ts):
        url = best_image_url(s)
        if not url:
            continue

        try:
            raw = fetch_image_bytes(url, cfg["indexing"]["timeout_sec"], cfg["indexing"]["max_image_bytes"])
            img = open_image_safely(raw)
            if not should_keep_image(img, cfg):
                continue
            feats = compute_hashes(img)

            rec = {
                "post_id": s.id,
                "created_utc": int(s.created_utc),
                "author": f"u/{getattr(s, 'author', None) or 'unknown'}",
                "flair": getattr(s, "link_flair_text", "") or "",
                "permalink": f"https://www.reddit.com{s.permalink}",
                "image_url": url,
                "width": img.size[0],
                "height": img.size[1],
                "phash16": feats["phash16"],
                "phash8": feats["phash8"],
                "dhash16": feats["dhash16"],
                "whash_haar": feats["whash"],
                "center_phash16": feats["center_phash16"],
                "hsv_hist": feats["hsv_hist"],
                "meta_json": json.dumps({"title": s.title}, ensure_ascii=False),
            }
            db_upsert(conn, rec)
            count += 1
            new_last = max(new_last, rec["created_utc"])

        except Exception as e:
            # Pomijamy błędne przypadki, ale nie przerywamy całego batcha
            print(f"[WARN] {s.id} {e}")

    conn.commit()
    conn.close()

    if new_last > last_indexed:
        state["last_indexed_utc"] = new_last
        save_state(cfg["paths"]["state_file"], state)

    print(f"[INFO] Indexed records: {count}; last_indexed_utc={state['last_indexed_utc']}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="poster_matcher/config.yaml")
    ap.add_argument("--since", help="YYYY-MM-DD lub ISO")
    ap.add_argument("--until", help="YYYY-MM-DD lub ISO")
    ap.add_argument("--delta", help="np. 48h")
    args = ap.parse_args()

    cfg = load_config(args.config)

    since_ts = None
    until_ts = None
    if args.since:
        since_ts = utc_ts(args.since)
    if args.until:
        until_ts = utc_ts(args.until)
    if args.delta:
        hours = int(args.delta.lower().replace("h", ""))
        since_ts = now_utc() - hours * 3600

    run_index(cfg, since_ts, until_ts)

if __name__ == "__main__":
    main()
