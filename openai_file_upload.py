from openai import OpenAI
import io
from pathlib import Path
from typing import Union


def _get_client() -> OpenAI:
    return OpenAI()


def upload_bytes_as_file(data: bytes, filename: str, purpose: str = "assistants") -> str:
    """Upload in-memory bytes to OpenAI Files (SDK v1.x). Returns file_id."""
    bio = io.BytesIO(data)
    bio.name = filename  # optional friendly name
    client = _get_client()
    return client.files.create(file=bio, purpose=purpose).id


def upload_path_as_file(path: Union[str, Path], purpose: str = "assistants") -> str:
    """Upload a local path to OpenAI Files (SDK v1.x). Returns file_id."""
    p = Path(path)
    client = _get_client()
    return client.files.create(file=p, purpose=purpose).id
