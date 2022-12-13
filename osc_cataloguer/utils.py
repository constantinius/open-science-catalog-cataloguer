import posixpath


def get_relative_url(url, other):
    """
    Return given url relative to other.
    """
    if other != '.':
        # Remove filename from other url if it has one.
        parts = posixpath.split(other)
        other = parts[0] if '.' in parts[1] else other
    relurl = posixpath.relpath(url, other)
    return relurl + '/' if url.endswith('/') else relurl
