# decision_engine.py
# r/CShortDramas — Decision Engine (robust; poster optional/disabled)
#
# Spec (skrót):
# Input:
#   context:  {"author","flair_in","post_id","url","source",...}
#   validator: {"status":"OK|AMBIGUOUS|MISSING", "reason": "..."}
#   title_report: {"best": {...} or None, "top": [...], "pool_ids": [...]}
#       best := {
#         "score": int,
#         "certainty": "certain|borderline|low",
#         "relation": "same_author|different_author|unknown",
#         "type": "normalized_exact|fuzzy|exact|none",
#         "candidate": {"title","permalink","flair","author"}
#       }
#   poster_report: {"status":"CERTAIN|UNSURE|NONE|NO_IMAGE|NO_REPORT", "distance": int|None, ...} or None
#   config: dict (thresholds, phrases, comments)
#
# Output (DecisionReport):
# {
#   "action": "AUTO_REMOVE|MOD_QUEUE|NO_ACTION",
#   "category": "MISSING|DUPLICATE|REPEATED|AMBIGUOUS|CONFLICT|NO_SIGNAL|AGE_WINDOW|ENGINE_ERROR",
#   "reason": "human-readable summary",
#   "removal_reason": "Duplicate Post|Repeated Request|Lack of Drama Name or Description in Title|None",
#   "removal_comment": str|None,
#   "evidence": {
#       "title_match": { "type":str, "score":int, "relation":str, "candidate": {...} },
#       "poster_match": { "status":str, "distance":int|None, "candidate": {...} }
#   },
#   "links": ["https://..."]
# }

from __future__ import annotations

# Minimal, defensive imports (no typing usage in runtime)
# — plik ma działać nawet w niestandardowych środowiskach runnera.

# ------------------------------- Defaults & helpers -------------------------------

_DEFAULTS = {
    "decision": {
        "title_threshold_auto": 93,
        "title_threshold_border": 85,
        "text_short_title_min_tokens": 3,
        "time_window_days": 14,
    },
    "phrases": {
        "allow_repost_hints": [
            "dead link", "links expired", "repost", "again", "previous request"
        ]
    },
    "comments": {
        "repeated_request_template": (
            "This link was already requested, you may have overlooked it. Please scroll in link requests or try keywords or title in the search bar. You may get lucky and it’s already found — or you click follow when you find the earlier post and we’re all fine.\n"
            "How to Use the Search Bar in Our Subreddit:\n"
            "https://www.reddit.com/r/CShortDramas/comments/1ig6a8j/how_to_use_the_search_bar_in_our_subreddit/"
        ),
        "missing_title_template": (
            "Your post has been removed because it doesn't include the drama name or a short description in the header. This is required to keep the subreddit organized and help others find and fulfill similar requests.\n"
            "If the name is visible on the poster, just add it to the header.\n"
            "If there’s no name, please add a short description instead. Think of what caught your attention, add the genre, storyline, actor’s name, or a brief summary of what you saw in the ad.\n"
            "Why do we ask this? Because search terms like “link” or “Do you know the title?” aren’t helpful for others looking for the same drama."
        ),
    },
}

def _cfg_get(cfg, path, default):
    cur = cfg or {}
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur

def _thresholds(cfg):
    auto_t = int(_cfg_get(cfg, "decision.title_threshold_auto", _DEFAULTS["decision"]["title_threshold_auto"]))
    border_t = int(_cfg_get(cfg, "decision.title_threshold_border", _DEFAULTS["decision"]["title_threshold_border"]))
    return auto_t, border_t

def _poster_status(poster_report):
    """
    Normalize poster status; treat NO_REPORT as NONE (poster disabled).
    """
    st = ((poster_report or {}).get("status") or "NONE").upper()
    if st == "NO_REPORT":
        return "NONE"
    return st

def _full_url(link):
    if not link:
        return None
    s = str(link)
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("/r/") or s.startswith("r/"):
        return "https://www.reddit.com" + (s if s.startswith("/") else "/" + s)
    return s

def _evidence_title(title_report):
    best = (title_report or {}).get("best") or {}
    cand = best.get("candidate") or {}
    return {
        "type": best.get("type") or ("fuzzy" if best else "none"),
        "score": int(best.get("score") or 0),
        "relation": best.get("relation") or "unknown",
        "candidate": {
            "title": cand.get("title"),
            "permalink": _full_url(cand.get("permalink")),
            "flair": cand.get("flair"),
            "author": cand.get("author"),
        }
    }

def _evidence_poster(poster_report):
    return {
        "status": _poster_status(poster_report),
        "distance": (poster_report or {}).get("distance"),
        "candidate": (poster_report or {}).get("candidate"),
    }

def _links_from_title(title_report):
    best = (title_report or {}).get("best") or {}
    cand = best.get("candidate") or {}
    link = _full_url(cand.get("permalink"))
    return [link] if link else []

def _comment_from_config(cfg, key, fallback):
    return _cfg_get(cfg, "comments." + key, fallback)

def _is_title_border(score, auto_t, border_t):
    return border_t <= int(score or 0) < auto_t

def _is_title_certain(score, auto_t):
    return int(score or 0) >= auto_t


# ---------------------------------- Core ----------------------------------

def decide(*, context, validator, title_report, poster_report, config=None):
    """
    Robust Decision Engine — never raises, always returns a valid DecisionReport.
    """
    try:
        auto_t, border_t = _thresholds(config)

        # 0) ALWAYS handle MISSING first — no other logic may block this.
        v_status = (validator or {}).get("status") or "OK"
        if v_status == "MISSING":
            rr = "Lack of Drama Name or Description in Title"
            comment = _comment_from_config(config, "missing_title_template", _DEFAULTS["comments"]["missing_title_template"])
            return {
                "action": "AUTO_REMOVE",
                "category": "MISSING",
                "reason": "Title missing per validator.",
                "removal_reason": rr,
                "removal_comment": comment,
                "evidence": {
                    "title_match": _evidence_title(title_report),
                    "poster_match": _evidence_poster(poster_report),
                },
                "links": _links_from_title(title_report),
            }

        # 1) Collect evidence (safe)
        t_evd = _evidence_title(title_report)
        t_score = int(t_evd.get("score") or 0)
        t_type  = t_evd.get("type") or "none"
        t_rel   = t_evd.get("relation") or "unknown"

        p_evd = _evidence_poster(poster_report)
        p_status = p_evd.get("status") or "NONE"  # CERTAIN|UNSURE|NONE|NO_IMAGE
        
         # --- Allowlist autora dopasowania tytułu (ignorujemy dopasowania od wskazanych autorów)
        try:
            cfg_allow = ((config or {}).get("matcher", {}) or {}).get("allow_authors", []) or []
            allow_authors = {str(a).strip().lower() for a in cfg_allow}
        except Exception:
            allow_authors = set()

        cand_author = (t_evd.get("candidate", {}) or {}).get("author") or ""
        # cand_author ma format "u/Name" -> zdejmij prefiks, znormalizuj:
        cand_author_norm = cand_author.lstrip("u/").strip().lower()

        if cand_author_norm and cand_author_norm in allow_authors:
            # Zneutralizuj sygnał tytułu: traktuj jak brak silnego dopasowania
            t_score = 0
            t_type = "none"
            t_rel = "unknown"
            # i zaktualizuj t_evd, żeby ładnie pokazało się w logach
            t_evd = {
                **t_evd,
                "score": 0,
                "type": "none",
                "relation": "unknown",
            }
        
        # 2) Duplicate / Repeated (title certain or poster CERTAIN)
        # same_author / different_author / unknown
        if t_rel == "same_author" and (_is_title_certain(t_score, auto_t) or p_status == "CERTAIN"):
            return {
                "action": "AUTO_REMOVE",
                "category": "DUPLICATE",
                "reason": "Duplicate: same author and either title match is certain or poster is CERTAIN.",
                "removal_reason": "Duplicate Post",
                "removal_comment": None,
                "evidence": {
                    "title_match": t_evd,
                    "poster_match": p_evd,
                },
                "links": _links_from_title(title_report),
            }

        if t_rel == "different_author" and (t_type in ("exact", "normalized_exact") or p_status == "CERTAIN"):
            comment = _comment_from_config(config, "repeated_request_template", _DEFAULTS["comments"]["repeated_request_template"])
            return {
                "action": "AUTO_REMOVE",
                "category": "REPEATED",
                "reason": "Repeated request: different author and either title match is certain or poster is CERTAIN.",
                "removal_reason": "Repeated Request",
                "removal_comment": comment,
                "evidence": {
                    "title_match": t_evd,
                    "poster_match": p_evd,
                },
                "links": _links_from_title(title_report),
            }

        # 3) Borderline title without poster confirmation → Mod Queue
        if _is_title_border(t_score, auto_t, border_t) and p_status in ("NONE", "NO_IMAGE", "UNSURE", "NO_REPORT"):
            return {
                "action": "MOD_QUEUE",
                "category": "AMBIGUOUS",
                "reason": f"Borderline title score ({t_score}) without poster confirmation.",
                "removal_reason": None,
                "removal_comment": None,
                "evidence": {
                    "title_match": t_evd,
                    "poster_match": p_evd,
                },
                "links": _links_from_title(title_report),
            }

        # 4) Poster uncertain alone (if poster ever re-enabled)
        if p_status == "UNSURE":
            return {
                "action": "MOD_QUEUE",
                "category": "AMBIGUOUS",
                "reason": "Poster uncertain.",
                "removal_reason": None,
                "removal_comment": None,
                "evidence": {
                    "title_match": t_evd,
                    "poster_match": p_evd,
                },
                "links": _links_from_title(title_report),
            }

        # 5) Conflict (exact/normalized_exact title but poster not confirming) — only if poster active
        if t_type in ("exact", "normalized_exact") and p_status not in ("NONE", "NO_IMAGE"):
            if p_status != "CERTAIN":
                return {
                    "action": "MOD_QUEUE",
                    "category": "CONFLICT",
                    "reason": "Exact/normalized-exact title, but poster evidence conflicts or is inconclusive.",
                    "removal_reason": None,
                    "removal_comment": None,
                    "evidence": {
                        "title_match": t_evd,
                        "poster_match": p_evd,
                    },
                    "links": _links_from_title(title_report),
                }

        # 6) No strong signals
        return {
            "action": "NO_ACTION",
            "category": "NO_SIGNAL",
            "reason": "No strong signals from title and poster.",
            "removal_reason": None,
            "removal_comment": None,
            "evidence": {
                "title_match": t_evd,
                "poster_match": p_evd,
            },
            "links": _links_from_title(title_report),
        }

    except Exception as e:
        # Defensive catch-all for any unexpected issue
        return {
            "action": "MOD_QUEUE",
            "category": "ENGINE_ERROR",
            "reason": f"decision_engine_exception: {e}",
            "removal_reason": None,
            "removal_comment": None,
            "evidence": {
                "title_match": _evidence_title(title_report),
                "poster_match": _evidence_poster(poster_report),
            },
            "links": _links_from_title(title_report),
        }
