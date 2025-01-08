import hashlib

M = 8

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
