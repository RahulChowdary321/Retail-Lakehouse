import base64
import hashlib
import hmac
import time

from security import verify_slack, verify_teams


def test_slack_signature():
    body, ts, secret = b"text=hi", str(int(time.time())), "s3cret"
    sig = "v0=" + hmac.new(secret.encode(), b"v0:" + ts.encode() + b":" + body, hashlib.sha256).hexdigest()
    assert verify_slack(body, ts, sig, secret)
    assert not verify_slack(body, ts, "v0=bad", secret)
    assert not verify_slack(body, str(int(time.time()) - 1000), sig, secret)


def test_teams_hmac():
    secret = base64.b64encode(b"teams-secret").decode()
    body = b'{"text":"hi"}'
    good = "HMAC " + base64.b64encode(hmac.new(b"teams-secret", body, hashlib.sha256).digest()).decode()
    assert verify_teams(body, good, secret)
    assert not verify_teams(body, "HMAC nope", secret)
