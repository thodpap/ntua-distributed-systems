import hashlib

M = 8
BUFF_SIZE = 1024

def chord_hash(key: str) -> int:
    """
    Returns an integer in [0, 2^M).
    Use SHA-1 or something similar for real usage.
    """
    # For demonstration, we'll just use SHA-1 and take the last M bits
    # but you could do a simpler approach for demonstration.
    h = hashlib.sha1(key.encode()).hexdigest()
    # Convert hex -> int, then mod 2^M
    return int(h, 16) % (2**M)

def in_interval(key_id: int, start_id: int, end_id: int, inclusive=False):
    """
    Check if key_id is in interval (start_id, end_id) on a circular ring.
    If inclusive=True, end boundary is included.
    """
    assert isinstance(key_id, int), f"key_id must be an int, got {type(key_id)}"
    assert isinstance(start_id, int), f"start_id must be an int, got {type(start_id)}"
    assert isinstance(end_id, int), f"end_id must be an int, got {type(end_id)}"
    
    start_id %= (2**M)
    end_id %= (2**M)
    key_id %= (2**M)

    if start_id < end_id:
        # Normal interval
        if inclusive:
            return start_id < key_id <= end_id
        else:
            return start_id < key_id < end_id
    else:
        # Interval wraps around the ring
        if inclusive:
            return not (end_id < key_id <= start_id)
        else:
            return not (end_id < key_id < start_id)

def _serialize_for_json(obj):
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: _serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_for_json(item) for item in obj]
    try:
        return str(obj)  # Fallback to string conversion for non-serializable types.
    except Exception:
        print("ERROR STRING",obj, type(obj))
        return None  # Handle cases where str(obj) fails.

def _deserialize_from_json(obj):
    if isinstance(obj, dict):
        return {k: _deserialize_from_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return set([_deserialize_from_json(item) for item in obj])
    return obj
