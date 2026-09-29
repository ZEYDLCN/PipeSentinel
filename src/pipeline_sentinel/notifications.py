"""Explicitly enabled HTTPS webhook delivery from the durable outbox."""
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from sqlalchemy import select, update

from . import reliability_store as store


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def deliver_one(engine):
    url = os.environ.get("SENTINEL_WEBHOOK_URL", "")
    secret = os.environ.get("SENTINEL_WEBHOOK_SECRET", "")
    if not url:
        return None
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or len(secret) < 24:
        raise ValueError("Webhook HTTPS adresi ve en az 24 karakter imza sırrı gerekli")
    with engine.begin() as conn:
        conn.execute(update(store.notifications).where(store.notifications.c.status == "sending",
                     store.notifications.c.lease_until < store.now()).values(status="failed", error="Teslim işleyicisi kesildi"))
        query = select(store.notifications).where(store.notifications.c.status.in_(["pending", "failed"]),
                                                  store.notifications.c.attempts < 3).order_by(store.notifications.c.created_at).limit(1)
        if engine.dialect.name == "postgresql":
            query = query.with_for_update(skip_locked=True)
        row = conn.execute(query).mappings().first()
        if not row:
            return None
        row = dict(row)
        updated = conn.execute(update(store.notifications).where(store.notifications.c.id == row["id"],
                               store.notifications.c.status == row["status"]).values(status="sending", attempts=row["attempts"] + 1,
                               lease_until=(datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()))
        if updated.rowcount != 1:
            return None
    body = json.dumps(row["body"], sort_keys=True).encode()
    request = Request(url, data=body, headers={"Content-Type": "application/json",
                      "X-Sentinel-Event": row["id"], "X-Sentinel-Signature": hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()})
    status, error = "sent", None
    try:
        with build_opener(NoRedirect).open(request, timeout=10) as response:
            if not 200 <= response.status < 300:
                raise ValueError()
    except Exception:
        status, error = "failed", "Webhook teslim edilemedi"
    with engine.begin() as conn:
        conn.execute(update(store.notifications).where(store.notifications.c.id == row["id"]).values(status=status, error=error, lease_until=None))
    return {"id": row["id"], "status": status}
