import io, json, os
from html import unescape
from urllib.parse import urlparse
from datetime import datetime, timezone
import numpy as np
import requests
import PIL.Image as Image
from PIL import ImageFile
import imagehash

ImageFile.LOAD_TRUNCATED_IMAGES = True

DEFAULT_BLOCK = {
    "facebook.com","www.facebook.com","m.facebook.com","l.facebook.com","web.facebook.com",
    "youtube.com","youtu.be",
    "tiktok.com","www.tiktok.com",
    "instagram.com","www.instagram.com",
    "x.com","twitter.com","mobile.twitter.com",
}

def utc_date(ts:int)->str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")

def _is_blocked_host(url:str, extra_block=None)->bool:
    host = urlparse(url).netloc.lower()
    blocked = set(DEFAULT_BLOCK)
    if extra_block:
        blocked |= set(extra_block)
    return any(host==b or host.endswith("."+b) for b in blocked)

def _pick_res_preview(preview, wanted_width:int):
    try:
        imgs = preview.get("images")
        if not imgs:
            return None
        p0 = imgs[0]
        res = p0.get("resolutions", [])
        chosen = None
        for r in res:
            if r.get("width",0) <= wanted_width:
                if chosen is None or r["width"]>chosen["width"]:
                    chosen = r
        if chosen is None:
            chosen = p0.get("source")
        if chosen and "url" in chosen:
            return chosen["url"]
    except Exception:
        pass
    return None

def best_image_url(subm, max_width:int, block_hosts=None):
    url = None
    if getattr(subm,"is_gallery",False) and getattr(subm,"media_metadata",None):
        try:
            first = next(iter(subm.media_metadata.values()))
            if "s" in first and "u" in first["s"]:
                url = first["s"]["u"]
        except Exception:
            pass
    if url is None:
        try:
            if subm.preview:
                cand = _pick_res_preview(subm.preview, max_width)
                if cand:
                    url = cand
                if url is None:
                    imgs=subm.preview.get("images")
                    if imgs:
                        url = imgs[0]["source"]["url"]
        except Exception:
            pass
    if url is None and getattr(subm,"url_overridden_by_dest",None):
        cand = subm.url_overridden_by_dest
        if not _is_blocked_host(cand, block_hosts):
            url = cand
    if url is None:
        try:
            if subm.media and "reddit_video" in subm.media:
                if subm.preview and subm.preview.get("images"):
                    url = subm.preview["images"][0]["source"]["url"]
        except Exception:
            pass
    if url is None and getattr(subm,"thumbnail",None) and str(subm.thumbnail).startswith("http"):
        if not _is_blocked_host(subm.thumbnail, block_hosts):
            url = subm.thumbnail
    if url:
        url = unescape(url).replace("&amp;","&")
    if url and _is_blocked_host(url, block_hosts):
        return None
    return url

def fetch_image_bytes(url, timeout, max_bytes, ua="PosterIndexer/1.0"):
    with requests.get(url, timeout=timeout, stream=True, headers={"User-Agent": ua}, allow_redirects=True) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type","").lower()
        if not ctype.startswith("image/"):
            raise ValueError(f"not_an_image content-type={ctype}")
        if any(fmt in ctype for fmt in ("image/avif","image/svg","image/svg+xml")):
            raise ValueError(f"unsupported_image_format content-type={ctype}")
        buf = io.BytesIO()
        size=0
        for chunk in r.iter_content(8192):
            if not chunk: break
            size += len(chunk)
            if size > max_bytes:
                raise ValueError("image too large")
            buf.write(chunk)
        return buf.getvalue()

def open_image_rgb(raw:bytes):
    if not raw: raise ValueError("empty_bytes")
    img = Image.open(io.BytesIO(raw))
    if img.mode=="P":
        img = img.convert("RGBA" if "transparency" in img.info else "RGB")
    elif img.mode in ("LA","RGBA"):
        img = img.convert("RGBA")
    else:
        img = img.convert("RGB")
    if img.mode=="RGBA":
        bg = Image.new("RGB", img.size, (255,255,255))
        bg.paste(img, mask=img.split()[-1])
        img = bg
    return img

def compute_features(img):
    if max(img.size)>1024:
        img = img.copy(); img.thumbnail((1024,1024), Image.LANCZOS)
    ph16 = imagehash.phash(img, hash_size=16)
    ph8  = imagehash.phash(img, hash_size=8)
    dh16 = imagehash.dhash(img, hash_size=16)
    wh   = imagehash.whash(img, hash_size=16, image_scale=None, mode='haar')
    w,h = img.size
    cw,ch = int(w*0.8), int(h*0.8)
    cx,cy = (w-cw)//2,(h-ch)//2
    ctr = img.crop((cx,cy,cx+cw,cy+ch))
    ctr_ph16 = imagehash.phash(ctr, hash_size=16)
    arr = np.array(img.convert("HSV"), dtype=np.uint8)
    H,S,V = arr[...,0],arr[...,1],arr[...,2]
    hist,_ = np.histogramdd((H.ravel(),S.ravel(),V.ravel()),
                             bins=(16,4,4), range=((0,256),(0,256),(0,256)))
    hist = hist.astype(np.float32); hist /= (hist.sum()+1e-9)
    norm = np.linalg.norm(hist);  hist = hist/norm if norm>0 else hist
    return {
        "phash16": str(ph16),
        "phash8":  str(ph8),
        "dhash16": str(dh16),
        "whash":   str(wh),
        "center_phash16": str(ctr_ph16),
        "hsv_hist": hist.flatten().tolist(),
        "width": w, "height": h,
    }
