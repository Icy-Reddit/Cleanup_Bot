import argparse, os, sys, json, time
from datetime import datetime, timedelta, timezone
import praw, yaml
from poster_shared import best_image_url, fetch_image_bytes, open_image_rgb, compute_features

def load_config(path):
    with open(path,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)

def target_day_utc(now=None):
    now = now or datetime.now(timezone.utc)
    return (now - timedelta(days=1)).date().strftime("%Y-%m-%d")

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def run_once(cfg, day_str, delta_hours=36, limit=1000):
    reddit = praw.Reddit(site_name=cfg["reddit"]["praw_site"])
    sub = reddit.subreddit(cfg["reddit"]["subreddit"])
    out_dir = os.path.join("data","shards"); ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"{day_str}.jsonl")

    # okno dnia UTC (z buforem wstecz)
    d0 = datetime.fromisoformat(day_str).replace(tzinfo=timezone.utc)
    start = d0
    end   = d0 + timedelta(days=1) - timedelta(seconds=1)
    back  = datetime.now(timezone.utc) - timedelta(hours=delta_hours)
    start_ts = int(max(start, back).timestamp())
    end_ts   = int(end.timestamp())

    max_w  = cfg.get("download",{}).get("max_width", 1280)
    fb_w   = cfg.get("download",{}).get("fallback_width", 720)
    tmo    = cfg.get("indexing",{}).get("timeout_sec", 10)
    max_mb = cfg.get("indexing",{}).get("max_image_bytes", 3_000_000)
    block  = (cfg.get("download",{}) or {}).get("block_hosts",[])

    seen = set()
    if os.path.exists(out_path):
        with open(out_path,"r",encoding="utf-8") as f:
            for line in f:
                try: seen.add(json.loads(line)["post_id"])
                except Exception: pass

    added=0
    with open(out_path,"a",encoding="utf-8") as out:
        for s in sub.new(limit=limit):
            cu = int(getattr(s,"created_utc",0))
            if cu<start_ts or cu>end_ts or s.id in seen: continue

            url = best_image_url(s, max_w, block_hosts=block)
            if not url: continue
            try:
                try:
                    raw = fetch_image_bytes(url, timeout=tmo, max_bytes=max_mb)
                except ValueError as e:
                    if "image too large" in str(e):
                        url2 = best_image_url(s, fb_w, block_hosts=block) or url
                        raw = fetch_image_bytes(url2, timeout=tmo, max_bytes=max_mb); url = url2
                    else:
                        continue
                img = open_image_rgb(raw)
                feats = compute_features(img)
                if feats["width"] < cfg["indexing"]["min_width"] or feats["height"] < cfg["indexing"]["min_height"]:
                    continue
            except Exception:
                continue

            rec = {
                "post_id": s.id,
                "created_utc": cu,
                "author": f"u/{getattr(s,'author',None) or 'unknown'}",
                "flair": getattr(s,"link_flair_text","") or "",
                "permalink": f"https://www.reddit.com{s.permalink}",
                "image_url": url,
                **feats,
                "meta": {"title": s.title}
            }
            out.write(json.dumps(rec, ensure_ascii=False)+"\n")
            added+=1

    print(f"[INFO] day={day_str} added={added} out={out_path}")
    return added

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--config", default="poster_matcher/config.yaml")
    ap.add_argument("--day", help="YYYY-MM-DD (UTC). Default = yesterday")
    ap.add_argument("--delta-hours", type=int, default=36)
    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--retries", type=int, default=12)   # ~2h odporności (12×10 min)
    ap.add_argument("--sleep-sec", type=int, default=600)
    args=ap.parse_args()

    cfg=load_config(args.config)
    day=args.day or target_day_utc()

    for i in range(1, args.retries+1):
        try:
            run_once(cfg, day, delta_hours=args.delta_hours, limit=args.limit)
            sys.exit(0)
        except Exception as e:
            print(f"[WARN] attempt {i}/{args.retries} failed: {type(e).__name__}: {e}")
            if i==args.retries: sys.exit(1)
            time.sleep(args.sleep_sec)

if __name__=="__main__":
    main()
