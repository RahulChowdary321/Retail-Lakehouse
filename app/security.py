"""Request signature verification for Slack and Microsoft Teams."""
import base64
import hashlib
import hmac
import os
import time


def verify_slack(body: bytes, timestamp: str, signature: str, secret: str | None = None) -> bool:
    secret = secret or os.getenv("SLACK_SIGNING_SECRET", "")
    if not (secret and timestamp and signature):
        return False
    try:
        if abs(time.time() - int(timestamp)) > 300:  # replay protection
            return False
    except ValueError:
        return False
    base = b"v0:" + timestamp.encode() + b":" + body
    expected = "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def verify_teams(body: bytes, auth_header: str, secret_b64: str | None = None) -> bool:
    """Teams outgoing webhook: `Authorization: HMAC <base64(hmac_sha256(base64decode(secret), body))>`."""
    secret_b64 = secret_b64 or os.getenv("TEAMS_SECURITY_TOKEN", "")
    if not (secret_b64 and auth_header.startswith("HMAC ")):
        return False
    digest = hmac.new(base64.b64decode(secret_b64), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), auth_header[5:].strip())
