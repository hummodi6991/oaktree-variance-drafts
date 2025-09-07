from fastapi import Request


def is_local_only(request: Request) -> bool:
    """Return True if the client requested local-only processing.

    Accepts both snake_case and camelCase variations in headers or query
    parameters (``local_only`` / ``localOnly``).  A truthy value such as
    ``"true"`` or ``"1"`` triggers local-only mode.
    """
    val = (
        request.headers.get("x-local-only")
        or request.headers.get("local-only")
        or request.query_params.get("localOnly")
        or request.query_params.get("local_only")
        or ""
    ).lower()
    return val in ("1", "true", "yes")
