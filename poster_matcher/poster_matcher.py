import argparse
import csv
import io
import json
import os
import sqlite3
from datetime import datetime, timezone

import numpy as np
import PIL.Image as Image
from PIL import ImageFile
import requests
import yaml
import praw
import imagehash
from html import unescape

ImageFile.LOAD_TRUNCATED_IMAGES = True


# ---------- Config & FS ----------

def load_config(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dirs(cfg):
    os.makedirs(cfg["paths"]["reports_dir"], exist_ok=True)

def now_utc_ts():
    return int(datetime.now(timezone.utc).timestamp())

def get_praw(cfg):
    return praw.Reddit(site_name=cfg["reddit"]["praw_site"])


# ---------- URL selection (jak w indexerze) ----------

def _pick_res_preview(preview_dict, wanted_width):
    try:
        imgs = preview_dict.get("images")
        if not imgs:
            return None
        p0 = imgs[0]
        res_list = p0.get("resolutions", [])
        chosen = None
        for r in res_list:
            if r.get("width", 0) <= wanted_width:
                if (chosen is None) or (r["width"] > chosen["width"]):
                    chosen = r
        if chosen is None:
            chosen = p0.get("source")
        if chosen and "url" in chosen:
            return chosen["url"]
    except Exception:
        return None
    return None

def best_image_url(subm, max_width=None):
    url = None

    if getattr(subm, "is_gallery", False) and getattr(subm, "media_metadata", None):
        try:
            first = next(iter(subm.media_metadata.values()))
            if "s" in first and "u" in first["s"]:
                url = first["s"]["u"]
        except Exception:
            pass

    if url is None:
        try:
            if subm.preview:
                if max_width:
                    cand = _pick_res_preview(subm.preview, wanted_width=max_width)
                    if cand:
                        url = cand
                if url is None:
                    imgs = subm.preview.get("images")
                    if imgs:
                        url = imgs[0]["source"]["url"]
        except Exception:
            pass

    if url is None and getattr(subm, "url_overridden_by_dest", None):
        url = subm.url_overridden_by_dest

    if url is None:
        try:
            if subm.media and "reddit_video" in subm.media:
                if subm.preview and subm.preview.get("images"):
                    url = subm.preview["images"][0]["source"]["url"]
        except Exception:
            pass

    if url is None and getattr(subm, "thumbnail", None) and str(subm.thumbnail).startswith("http"):
        url = subm.thumbnail

    if url:
        url = unescape(url).replace("&amp;", "&")
    return url


# ---------- Network & image ----------

def fetch_image_bytes(url, timeout, max_bytes):
    headers = {"User-Agent": "PosterMatcher/1.0"}
    with requests.get(url, timeout=timeout, stream=True, headers=headers, allow_redirects=True) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").lower()
        if not ctype.startswith("image/"):
            raise ValueError(f"not_an_image content-type={ctype}")
        if any(fmt in ctype for fmt in ("image/avif", "image/svg", "image/svg+xml")):
            raise ValueError(f"unsupported_image_format content-type={ctype}")
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
    try:
        img = Image.open(io.BytesIO(raw_bytes))
        img = img.convert("RGB")
        return img
    except Exception as e:
        raise ValueError(f"pillow_open_failed: {e}")


# ---------- Features ----------

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

def hamming(a, b):
    return (a - b)  # imagehash implements distance

def hsv_corr(a, b):
    return float(np.dot(a.flatten(), b.flatten()))

def normalize_score(dist, max_d):
    return max(0.0, 1.0 - (dist / float(max_d)))


# ---------- DB & candidates ----------

def db_open(path):
    return sqlite3.connect(path)

def load_candidates(conn, since_ts):
    cur = conn.execute(
        "SELECT post_id, created_utc, flair, permalink, phash16, phash8, dhash16, whash_haar, center_phash16, hsv_hist "
        "FROM posters WHERE created_utc >= ?",
        (since_ts,)
    )
    rows = cur.fetchall()
    cands = []
    for r in rows:
        try:
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
        except Exception:
            # pomiń pojedynczy uszkodzony rekord
            continue
    return cands


# ---------- Shortlist & ensemble ----------

def shortlist(cands, q_feats, cfg):
    ph_max = cfg["matching"]["shortlist"]["phash16_max_dist"]
    hsv_min = cfg["matching"]["shortlist"]["hsv_min_corr"]
    res = []
    for c in cands:
        try:
            d_ph16 = hamming(q_feats["phash16"], c["phash16"])
            corr   = hsv_corr(q_feats["hsv_hist"], c["hsv_hist"])
            if (d_ph16 <= ph_max) or (corr >= hsv_min):
                res.append((c, d_ph16, corr))
        except Exception:
            continue
    res.sort(key=lambda t: (t[1], -t[2]))
    return [c for (c, _, _) in res[: cfg["matching"]["shortlist"]["max_candidates"]]]

def ensemble_score(q, c, weights):
    try:
        d_ph16 = hamming(q["phash16"], c["phash16"])
        d_ph8  = hamming(q["phash8"],  c["phash8"])
        d_dh16 = hamming(q["dhash16"], c["dhash16"])
        d_wh   = hamming(q["whash"],   c["whash"])
        d_ctr  = hamming(q["center_phash16"], c["center_phash16"])
        corr   = hsv_corr(q["hsv_hist"], c["hsv_hist"])
    except Exception as e:
        raise ValueError(f"ensemble_parts_failed: {e}")

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
        (parts["dist_ph16"] <= 6 and parts["hsv_corr"] >= 0.92) or
        (parts["dist_ph16"] <= 8 and parts["dist_dh16"] <= 10 and parts["dist_wh"] <= 12)
    ):
        return "CERTAIN"
    if score >= thresholds["unsure"]:
        return "UNSURE"
    return "NONE"


# ---------- LR scan ----------

def iter_lr_window(reddit, subreddit_name, hours, flair_allow):
    sub = reddit.subreddit(subreddit_name)
    cutoff = now_utc_ts() - hours * 3600
    for s in sub.new(limit=None):
        if s.created_utc < cutoff:
            break
        flair = getattr(s, "link_flair_text", "") or ""
        if flair not in flair_allow:
            continue
        yield s


# ---------- Run ----------

def run_match(cfg, window_hours, report_csv, report_jsonl):
    ensure_dirs(cfg)
    reddit = get_praw(cfg)

    lookback_days = cfg["indexing"]["lookback_days"]
    since_ts = now_utc_ts() - lookback_days * 86400

    # Load candidates
    conn = db_open(cfg["paths"]["index_db"])
    candidates = load_candidates(conn, since_ts)
    conn.close()
    print(f"[INFO] Loaded candidates: {len(candidates)} (last {lookback_days} days)")

    weights = cfg["matching"]["weights"]
    thresholds = cfg["matching"]["thresholds"]
    max_w = cfg.get("download", {}).get("max_width", 1280)
    fb_w  = cfg.get("download", {}).get("fallback_width", 720)

    rows_csv = []
    rows_jsonl = []

    for s in iter_lr_window(reddit, cfg["reddit"]["subreddit"], window_hours, cfg["matching"]["flair_allow"]):
        url = best_image_url(s, max_width=max_w)
        if not url:
            continue

        # download z fallbackiem na mniejszą rozdzielczość
        try:
            raw = fetch_image_bytes(url, cfg["indexing"]["timeout_sec"], cfg["indexing"]["max_image_bytes"])
        except ValueError as e:
            msg = str(e)
            if "image too large" in msg:
                fb_url = best_image_url(s, max_width=fb_w)
                if fb_url and fb_url != url:
                    try:
                        raw = fetch_image_bytes(fb_url, cfg["indexing"]["timeout_sec"], cfg["indexing"]["max_image_bytes"])
                        url = fb_url
                    except Exception as e2:
                        print(f"[WARN] {s.id} {type(e2).__name__}: {e2} url={fb_url}")
                        continue
                else:
                    print(f"[WARN] {s.id} {type(e).__name__}: {e} url={url}")
                    continue
            else:
                print(f"[WARN] {s.id} {type(e).__name__}: {e} url={url}")
                continue
        except Exception as e:
            print(f"[WARN] {s.id} {type(e).__name__}: {e} url={url}")
            continue

        try:
            img = open_image_safely(raw)
        except Exception as e:
            print(f"[WARN] {s.id} {type(e).__name__}: {e} url={url}")
            continue

        # cechy i shortlist
        q_feats = compute_hashes(img)
        cand_sl = shortlist(candidates, q_feats, cfg)

        scored = []
        for c in cand_sl:
            try:
                sc, parts = ensemble_score(q_feats, c, weights)
                scored.append((sc, parts, c))
            except Exception as e:
                # pomiń pojedynczego kandydata
                continue

        scored.sort(key=lambda t: t[0], reverse=True)

        status = "NONE"
        best = None
        if scored:
            sc, parts, c = scored[0]
            status = decide(sc, parts, thresholds)
            best = (sc, parts, c)

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
        fieldnames = [
            "post_id","created_utc","permalink","title","status","score",
            "best_post_id","best_permalink","dist_ph16","dist_dh16","dist_wh","dist_ctr","hsv_corr"
        ]
        with open(report_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
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

    date_str = datetime.now().strftime("%Y-%m-%d")
    report_csv = args.report_csv or os.path.join(cfg["paths"]["reports_dir"], f"poster_matches_{date_str}.csv")
    report_jsonl = args.report_jsonl or os.path.join(cfg["paths"]["reports_dir"], f"poster_matches_{date_str}.jsonl")

    run_match(cfg, window_hours, report_csv, report_jsonl)


if __name__ == "__main__":
    main()
