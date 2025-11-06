# gh_build_sqlite.py
import os, json, glob, sqlite3

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
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    for stmt in DDL.strip().split(";\n"):
        if stmt.strip():
            conn.execute(stmt)
    return conn

def main():
    shards = sorted(glob.glob(os.path.join("data","shards","*.jsonl")))
    dbp = os.path.join("data","poster_index.sqlite")
    conn = db_open(dbp)
    cur = conn.cursor()
    total=0
    for shp in shards:
        with open(shp,"r",encoding="utf-8") as f:
            for line in f:
                try:
                    o=json.loads(line)
                except Exception:
                    continue
                cur.execute("""
                INSERT INTO posters (post_id, created_utc, author, flair, permalink, image_url,
                  width, height, phash16, phash8, dhash16, whash_haar, center_phash16, hsv_hist, meta_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id) DO UPDATE SET
                  image_url=excluded.image_url,
                  phash16=excluded.phash16,
                  phash8=excluded.phash8,
                  dhash16=excluded.dhash16,
                  whash_haar=excluded.whash_haar,
                  center_phash16=excluded.center_phash16,
                  hsv_hist=excluded.hsv_hist,
                  meta_json=excluded.meta_json
                """, (
                    o["post_id"], o["created_utc"], o["author"], o.get("flair",""),
                    o["permalink"], o["image_url"],
                    int(o.get("width") or 0), int(o.get("height") or 0),
                    o["phash16"], o["phash8"], o["dhash16"], o["whash"],
                    o["center_phash16"],
                    json.dumps(o["hsv_hist"]).encode("utf-8"),
                    json.dumps(o.get("meta",{}), ensure_ascii=False)
                ))
                total+=1
    conn.commit()
    conn.close()
    print(f"[INFO] built sqlite from shards: rows upserted ~{total}; db={dbp}")

if __name__=="__main__":
    main()
