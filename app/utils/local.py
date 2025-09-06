from fastapi import Request


def is_local_only(request: Request) -> bool:
    """Return True if the client requested local-only processing.

    Checks both the ``x-local-only`` header and ``localOnly`` query parameter
    for a truthy value such as ``"true"``.
    """
    val = (request.headers.get("x-local-only") or request.query_params.get("localOnly") or "").lower()
    return val in ("1", "true", "yes")
