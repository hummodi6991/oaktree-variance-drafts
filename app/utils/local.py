from fastapi import Request


def is_local_only(request: Request) -> bool:
    """Always return ``False`` as local-only mode is disabled.

    The application previously supported bypassing the LLM and generating
    drafts locally.  This behaviour has been removed so that all outputs are
    AI-assisted.  The helper now ignores any request flags and simply returns
    ``False``.
    """
    return False
