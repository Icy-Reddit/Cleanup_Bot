import argparse
import csv
import io
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

import numpy as np
import PIL.Image as Image
import requests
import yaml
import praw
import imagehash


# ---------- Config & helpers ----------

def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dirs(cfg):
    os.makedirs(cfg["paths"]["reports_dir"], exist_ok=True)

def now_utc():
    return int(datetime.now(timezone.utc).timestamp())

def get_praw(cfg):
    return praw.Reddit(site_name=cfg["reddit"]["praw_site"])

def best_image_url(subm):
    if getattr(subm, "is_gallery", False) and getattr(subm, "media_metadata", None):
        try:
            first = next(iter(subm.media_metadata.values()))
            if "s" in first and "u" in first["s"]:
                return first["s"]["u"]
        except Exception:
            pass

    try:
        if subm.preview and subm.preview.get("images"):
            src = subm.preview["images"][0]["source"]["url"]
            return src
    except Exception:
        pass

    if getattr(subm, "url_overridden_by_dest", None):
        return subm.url_overridden_by_dest

    try:
        if subm.media and "reddit_video" in subm.media:
            if subm.preview and subm.preview.get("images"):
                return subm.preview["images"][0]["source"]["url"]
    except Exception:
        pass

    if getattr(subm, "thumbnail", None) and subm.thumbnail.startswith("http"):
        return subm.thumbnail

    return None

def fetch_image_bytes(url, timeout, max_bytes):
    headers = {"User-Agent": "PosterMatcher/1.0"}
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
    # L2 norm
    norm = np.linalg.norm(hist)
    if norm > 0:
        hist = hist / norm
    return hist

def compute_hashes(img: Image.Image):
    max_side = 1024
    if max(img.size) > max_side:
        img = img.copy()
        img.thumbnail((max_side, max_side), Image.LANCZOS)

    ph16 = imagehash.phash(img, hash_size=16)
    ph8  = imagehash.phash(img, hash_size=8)
    dh16 = imagehash.dhash(img, hash_size=16)
    wh   = imagehash.whash(img, hash_size=16, image_scale=None, mode='haar')

    w, h = img.size
    cw, ch = int(w*0.8), int(h*0.8)
    cx, cy = (w - cw)//2, (h - ch)//2
    ctr = img.crop((cx, cy, cx+cw, cy+ch))
    ctr_ph16 = imagehash.phash(ctr, hash_size=16)

    return {
        "phash16": ph16,
        "phash8":  ph8,
        "dhash16": dh16,
        "whash":   wh,
        "center_phash16": ctr_ph16,
        "hsv_hist": compute_hsv_hist(img)
    }

def hamming(a, b, bits):
    # imagehash objects: distance == Hamming
    return (a - b)

def hsv_corr(a, b):
    # kosinusowa podobieństwo (po L2-normie = dot)
    return float(np.dot(a.flatten(), b.flatten()))

def normalize_score(dist, max_d):
    return max(0.0, 1.0 - (dist / float(max_d)))

# ---------- DB ----------

def db_open(path):
    return sqlite3.connect(path)

def load_candidates(conn, since_ts):
    # ostatnie lookback_days (dla całego indeksu)
    cur = conn.execute(
        "SELECT post_id, created_utc, flair, permalink, phash16, phash8, dhash16, whash_haar, center_phash16, hsv_hist "
        "FROM posters WHERE created_utc >= ?",
        (since_ts,)
    )
    rows = cur.fetchall()
    cands = []
    for r in rows:
        cands.append({
            "post_id": r[0],
            "created_utc": r[1],
            "flair": r[2],
            "permalink": r[3],
            "phash16": imagehash.hex_to_hash(r[4]),
            "phash8":  imagehash.hex_to_hash(r[5]),
            "dhash16": imagehash.hex_to_hash(r[6]),
            "whash":   imagehash.hex_to_hash(r[7]),
            "center_phash16": imagehash.hex_to_hash(r[8]),
            "hsv_hist": np.frombuffer(r[9], dtype=np.float32).reshape(16,4,4)
        })
    return cands

# ---------- Matching ----------

def shortlist(cands, q_feats, cfg):
    res = []
    for c in cands:
        d_ph16 = hamming(q_feats["phash16"], c["phash16"], 64)
        corr   = hsv_corr(q_feats["hsv_hist"], c["hsv_hist"])
        if (d_ph16 <= cfg["matching"]["shortlist"]["phash16_max_dist"]) or (corr >= cfg["matching"]["shortlist"]["hsv_min_corr"]):
            res.append((c, d_ph16, corr))
    # sortuj po d_ph16 asc, corr desc
    res.sort(key=lambda t: (t[1], -t[2]))
    return [c for (c, _, _) in res[: cfg["matching"]["shortlist"]["max_candidates"]]]

def ensemble_score(q, c, weights):
    d_ph16 = hamming(q["phash16"], c["phash16"], 64)
    d_ph8  = hamming(q["phash8"],  c["phash8"],  32)
    d_dh16 = hamming(q["dhash16"], c["dhash16"], 64)
    d_wh   = hamming(q["whash"],   c["whash"],   64)
    d_ctr  = hamming(q["center_phash16"], c["center_phash16"], 64)
    corr   = hsv_corr(q["hsv_hist"], c["hsv_hist"])

    s = (
        weights["phash16"] * normalize_score(d_ph16, 64) +
        weights["phash8"]  * normalize_score(d_ph8,  32) +
        weights["dhash16"] * normalize_score(d_dh16, 64) +
        weights["whash"]   * normalize_score(d_wh,   64) +
        weights["center"]  * normalize_score(d_ctr,  64) +
        weights["hsv"]     * corr
    )
    return float(s), {
        "dist_ph16": int(d_ph16),
        "dist_ph8": int(d_ph8),
        "dist_dh16": int(d_dh16),
        "dist_wh": int(d_wh),
        "dist_ctr": int(d_ctr),
        "hsv_corr": float(corr)
    }

def decide(score, parts, thresholds):
    if (score >= thresholds["certain"]) and (
        parts["dist_ph16"] <= 6 and parts["hsv_corr"] >= 0.92
        or (parts["dist_ph16"] <= 8 and parts["dist_dh16"] <= 10 and parts["dist_wh"] <= 12)
    ):
        return "CERTAIN"
    if score >= thresholds["unsure"]:
        return "UNSURE"
    return "NONE"

# ---------- LR scan & run ----------

def iter_lr_window(reddit, subreddit_name, hours, flair_allow):
    sub = reddit.subreddit(subreddit_name)
    cutoff = now_utc() - hours * 3600
    for s in sub.new(limit=None):
        if s.created_utc < cutoff:
            break
        flair = getattr(s, "link_flair_text", "") or ""
        if flair not in flair_allow:
            continue
        yield s

def run_match(cfg, window_hours, report_csv, report_jsonl):
    ensure_dirs(cfg)
    reddit = get_praw(cfg)

    # Index window
    lookback_days = cfg["indexing"]["lookback_days"]
    since_ts = now_utc() - lookback_days * 86400

    # Load candidates
    conn = db_open(cfg["paths"]["index_db"])
    candidates = load_candidates(conn, since_ts)
    conn.close()

    print(f"[INFO] Loaded candidates: {len(candidates)} (last {lookback_days} days)")

    weights = cfg["matching"]["weights"]
    thresholds = cfg["matching"]["thresholds"]

    rows_csv = []
    rows_jsonl = []

    for s in iter_lr_window(reddit, cfg["reddit"]["subreddit"], window_hours, cfg["matching"]["flair_allow"]):
        url = best_image_url(s)
        if not url:
            continue
        try:
            raw = fetch_image_bytes(url, cfg["indexing"]["timeout_sec"], cfg["indexing"]["max_image_bytes"])
            img = open_image_safely(raw)
        except Exception as e:
            print(f"[WARN] {s.id} download/open: {e}")
            continue

        q_feats = compute_hashes(img)
        cand_sl = shortlist(candidates, q_feats, cfg)

        scored = []
        for c in cand_sl:
            sc, parts = ensemble_score(q_feats, c, weights)
            scored.append( (sc, parts, c) )

        scored.sort(key=lambda t: t[0], reverse=True)

        status = "NONE"
        best = None
        if scored:
            sc, parts, c = scored[0]
            status = decide(sc, parts, thresholds)
            best = (sc, parts, c)

        # CSV row (one-line summary)
        best_post_id = best[2]["post_id"] if best else ""
        best_score = round(best[0], 4) if best else 0.0
        parts = best[1] if best else {"dist_ph16": "", "dist_dh16": "", "dist_wh": "", "dist_ctr": "", "hsv_corr": ""}
        rows_csv.append({
            "post_id": s.id,
            "created_utc": int(s.created_utc),
            "permalink": f"https://www.reddit.com{s.permalink}",
            "title": s.title,
            "status": status,
            "score": best_score,
            "best_post_id": best_post_id,
            "best_permalink": (best and best[2]["permalink"]) or "",
            "dist_ph16": parts["dist_ph16"],
            "dist_dh16": parts.get("dist_dh16", ""),
            "dist_wh": parts.get("dist_wh", ""),
            "dist_ctr": parts.get("dist_ctr", ""),
            "hsv_corr": parts.get("hsv_corr", "")
        })

        # JSONL row (topK with components)
        topk = []
        for sc, pt, c in scored[: cfg["matching"]["topk"]]:
            topk.append({
                "post_id": c["post_id"],
                "permalink": c["permalink"],
                "score": round(sc, 4),
                **pt
            })
        rows_jsonl.append({
            "post_id": s.id,
            "created_utc": int(s.created_utc),
            "permalink": f"https://www.reddit.com{s.permalink}",
            "title": s.title,
            "status": status,
            "top": topk
        })

    # write reports
    if report_csv:
        with open(report_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows_csv[0].keys()) if rows_csv else
                               ["post_id","created_utc","permalink","title","status","score","best_post_id","best_permalink","dist_ph16","dist_dh16","dist_wh","dist_ctr","hsv_corr"])
            w.writeheader()
            for r in rows_csv:
                w.writerow(r)
        print(f"[INFO] CSV report: {report_csv}")

    if report_jsonl:
        with open(report_jsonl, "w", encoding="utf-8") as f:
            for r in rows_jsonl:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"[INFO] JSONL report: {report_jsonl}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="poster_matcher/config.yaml")
    ap.add_argument("--window", default=None, help="np. 24h (domyślnie z config)")
    ap.add_argument("--report-csv", default=None)
    ap.add_argument("--report-jsonl", default=None)
    args = ap.parse_args()

    cfg = load_config(args.config)
    window_hours = int(args.window[:-1]) if args.window else cfg["matching"]["lr_window_hours"]

    # domyślne ścieżki raportów
    date_str = datetime.now().strftime("%Y-%m-%d")
    report_csv = args["report_csv"] if isinstance(args, dict) and "report_csv" in args else args.report_csv
    report_jsonl = args["report_jsonl"] if isinstance(args, dict) and "report_jsonl" in args else args.report_jsonl
    if not report_csv:
        report_csv = os.path.join(cfg["paths"]["reports_dir"], f"poster_matches_{date_str}.csv")
    if not report_jsonl:
        report_jsonl = os.path.join(cfg["paths"]["reports_dir"], f"poster_matches_{date_str}.jsonl")

    run_match(cfg, window_hours, report_csv, report_jsonl)

if __name__ == "__main__":
    main()
