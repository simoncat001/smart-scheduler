from __future__ import annotations

import os

import uvicorn

from app.main import create_app


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


app = create_app()


def main() -> None:
    host = os.getenv("SMART_CHEDULER_HOST", "127.0.0.1")
    port = int(os.getenv("SMART_CHEDULER_PORT", "8001"))
    reload_enabled = _as_bool(os.getenv("SMART_CHEDULER_RELOAD"), False)

    if reload_enabled:
        uvicorn.run("main:app", host=host, port=port, reload=True)
        return

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
