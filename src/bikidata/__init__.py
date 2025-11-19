from .main import (
    build,
    build_ftss,
    build_from_iterator,
    decode_unicode_escapes,
    literal_to_parts,
    log,
)

try:
    from .semantic import build_semantic
except:
    log.warning(
        "Failed to import build_semantic. "
        "Make sure you have the required dependencies installed and COHERE_API_KEY env var defined."
    )

from .query import (
    spo,
    sp,
    query,
    raw,
    total,
    count_by_property,
    properties,
)

from .workers import query_async, insert_async, delete_async, TimeoutError
