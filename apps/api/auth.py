"""Shared API access policy. Tokens live in server environment, never in metadata."""
import hmac
import json
import os

from fastapi import HTTPException, Request


def identity(request: Request):
    return getattr(request.state, "identity", {"actor": "local-dev", "role": "operator"})


async def access_policy(request: Request, call_next):
    from starlette.responses import JSONResponse
    if not request.url.path.startswith("/api/") or request.url.path == "/api/v1/health":
        return await call_next(request)
    mode = os.environ.get("SENTINEL_MODE", "development")
    configured = os.environ.get("SENTINEL_API_TOKENS", "")
    if mode not in {"development", "pilot"}:
        return JSONResponse({"detail": "Geçersiz SENTINEL_MODE"}, status_code=503)
    user = None
    if configured:
        try:
            tokens = json.loads(configured)
            supplied = request.headers.get("authorization", "").removeprefix("Bearer ")
            for token, candidate in tokens.items():
                if len(token) < 24 or candidate.get("role") not in {"reader", "operator"} or not candidate.get("actor"):
                    raise ValueError()
                if hmac.compare_digest(token, supplied):
                    user = candidate
        except (ValueError, AttributeError, TypeError):
            return JSONResponse({"detail": "API kimlik yapılandırması geçersiz"}, status_code=503)
        if user is None:
            return JSONResponse({"detail": "Geçerli Bearer token gerekli"}, status_code=401)
    elif mode == "pilot":
        return JSONResponse({"detail": "Pilot modunda SENTINEL_API_TOKENS zorunlu"}, status_code=503)
    else:
        user = {"actor": "local-dev", "role": "operator"}
    request.state.identity = user
    if request.method not in {"GET", "HEAD", "OPTIONS"} and user["role"] != "operator":
        return JSONResponse({"detail": "Operatör yetkisi gerekli"}, status_code=403)
    if mode == "pilot" and request.method == "POST" and request.url.path in {"/api/v1/pipeline-runs", "/api/v1/bootstrap"}:
        return JSONResponse({"detail": "Pilot modunda sentetik veri/hata enjeksiyonu kapalı"}, status_code=403)
    return await call_next(request)
