# action_executor.py
# Executes moderation actions on Reddit based on DecisionReport.

from __future__ import annotations
import sys
from typing import Any, Dict, Optional

REMOVAL_REASON_TITLES = {
    "MISSING": "Lack of Drama Name or Description in Title",
    "REPEATED": "Repeated Request",
    "DUPLICATE": "Duplicate Post",
}

def _full_url(permalink: Optional[str]) -> Optional[str]:
    if not permalink:
        return None
    s = str(permalink)
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("/r/") or s.startswith("r/"):
        return "https://www.reddit.com" + (s if s.startswith("/") else "/" + s)
    return s

def _get_submission(reddit, post_id: str):
    # PRAW accepts base36 id without t3_
    return reddit.submission(id=post_id)

def _fetch_removal_reasons(subreddit):
    try:
        # returns list of RemovalReason objects
        return list(subreddit.mod.removal_reasons)  # PRAW 7.8+
    except Exception:
        return []

def _find_reason_id(subreddit, wanted_title: str) -> Optional[str]:
    """
    Try to match removal reason by its 'title' (what mods see in the preset list).
    Return None if not found; in that case we'll still remove and send manual message.
    """
    reasons = _fetch_removal_reasons(subreddit)
    wanted = (wanted_title or "").strip().casefold()
    for rr in reasons:
        try:
            title = (getattr(rr, "title", None) or "").strip().casefold()
            if title == wanted:
                return getattr(rr, "id", None)
        except Exception:
            continue
    return None

def _send_removal_message(submission, body: str):
    """
    Send a mod removal message to OP (visible in their inbox). Fallback to comment if needed.
    """
    if not body:
        return
    try:
        submission.mod.send_removal_message(message=body, title="Your post was removed", type="public")
        return
    except Exception as e:
        # Some subs do not allow public removal messages; fallback to a mod-distinguished comment
        try:
            comm = submission.reply(body)
            comm.mod.distinguish(sticky=False)
        except Exception as e2:
            print(f"[EXEC][WARN] removal message failed: {e} | comment fallback failed: {e2}", file=sys.stderr)

def _apply_removal_reason(submission, reason_title: str, message_body: Optional[str]):
    """
    Try to set the official removal reason (if presets exist). Regardless, ensure the post is removed.
    """
    try:
        submission.mod.remove(spam=False)
    except Exception as e:
        print(f"[EXEC][WARN] remove() failed: {e}", file=sys.stderr)

    if not reason_title:
        # still try to notify OP if message provided
        if message_body:
            _send_removal_message(submission, message_body)
        return

    try:
        reason_id = _find_reason_id(submission.subreddit, reason_title)
        if reason_id:
            # Apply the preset reason + message (if provided)
            try:
                # PRAW RemovalReasons API:
                # subreddit.mod.removal_reasons.mod_remove(submission, reason_id, mod_note=None, spam=False)
                submission.subreddit.mod.removal_reasons.mod_remove(
                    submission, reason_id=reason_id, mod_note=None, spam=False
                )
            except Exception as e:
                # Some endpoints vary; keep at least the basic remove
                print(f"[EXEC][WARN] mod_remove with reason_id failed: {e}", file=sys.stderr)
        # Always send a human-readable message (template) to OP if provided
        if message_body:
            _send_removal_message(submission, message_body)
    except Exception as e:
        print(f"[EXEC][WARN] applying removal reason failed: {e}", file=sys.stderr)
        if message_body:
            _send_removal_message(submission, message_body)

def execute_decision(
    *,
    reddit,
    decision: Dict[str, Any],
    context: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Execute actions implied by DecisionReport.
    Returns an execution report dict.
    """
    action = (decision or {}).get("action")
    category = (decision or {}).get("category")
    removal_reason = (decision or {}).get("removal_reason")
    removal_comment = (decision or {}).get("removal_comment")
    post_id = (context or {}).get("post_id")
    permalink = _full_url((context or {}).get("url"))

    report = {
        "post_id": post_id,
        "permalink": permalink,
        "action": action,
        "category": category,
        "performed": False,
        "details": "",
    }

    if not post_id:
        report["details"] = "missing post_id"
        return report

    if action not in ("AUTO_REMOVE", "MOD_QUEUE", "NO_ACTION"):
        report["details"] = f"unsupported action: {action}"
        return report

    if dry_run or action != "AUTO_REMOVE":
        report["details"] = "dry-run or non-destructive action"
        return report

    # map category to reason title if not provided
    reason_title = removal_reason or REMOVAL_REASON_TITLES.get(category or "", "")
    message_body = removal_comment or None

    try:
        submission = _get_submission(reddit, post_id)
    except Exception as e:
        report["details"] = f"cannot fetch submission: {e}"
        return report

    try:
        _apply_removal_reason(submission, reason_title, message_body)
        report["performed"] = True
        report["details"] = f"removed with reason '{reason_title}'"
    except Exception as e:
        report["details"] = f"execution failed: {e}"

    return report
