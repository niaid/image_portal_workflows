# Ref: https://gist.github.com/AutumnSun1996/830c5944b8d83b324b34342d39ee008c
import base64
import os
import json

from fastapi import Response

from starlette.requests import HTTPConnection
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.responses import JSONResponse
from starlette.authentication import (
    AuthCredentials,
    AuthenticationBackend,
    SimpleUser,
    AuthenticationError,
)

from prefect.server.api.server import create_app

server_api_keys = json.loads(
    os.environ.get(
        'SERVER_KEYS',
        '{"PREFECT_API_KEY": "prefect-api-key", "BASIC_AUTHS": "admin:password"}'
    )
)
apikey = 'Bearer ' + server_api_keys['PREFECT_API_KEY']
# base64 encoded user:password key value pair
basic_auths = [
    'Basic ' + base64.b64encode(basic_auth.encode()).decode()
    for basic_auth in server_api_keys["BASIC_AUTHS"].split(",")
]


class CustomAuth(AuthenticationBackend):
    async def authenticate(self, conn: HTTPConnection):
        if conn.scope.get("type") == "websocket":
            return None
        if conn.url.path == '/api/health':
            return None
        if "Authorization" not in conn.headers:
            raise AuthenticationError('no token')
        auth = conn.headers["Authorization"]
        if auth == apikey:
            return AuthCredentials(["auth"]), SimpleUser('api')
        if auth in basic_auths:
            return AuthCredentials(["auth"]), SimpleUser('user')
        raise AuthenticationError('invalid token')


def handler_error(conn: HTTPConnection, exc: Exception) -> Response:
    return JSONResponse(
        {"detail": "Login required", "message": str(exc)},
        status_code=401,
        headers={'WWW-Authenticate': f'Basic realm="Unauthorized: {exc}"'},
    )


def create_auth_app():
    app = create_app()
    app.add_middleware(
        AuthenticationMiddleware,
        backend=CustomAuth(),
        on_error=handler_error,
    )
    return app
