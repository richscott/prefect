"""
Loading logo assets that an integration ships inside its own Python package.

A worker normally points `_logo_url` at an externally hosted image. An integration
that is installed alongside the server but absent from the published collections
registry has nowhere to host one, so it may instead carry the image in its own
package and name it with `_logo_resource`. `BaseWorker.get_logo_url` then returns a
data URL built from the packaged bytes, which travels through the existing
`logo_url` metadata field without any new endpoint or API shape.

This module deliberately imports only from the standard library so that it stays
usable from `prefect-client` and cannot introduce an import cycle with the worker
base class that calls it.
"""

from __future__ import annotations

import base64
import logging
from functools import lru_cache
from importlib.resources import files

logger: logging.Logger = logging.getLogger(__name__)

# Logos are embedded in every `aggregate-worker-metadata` response and in the
# server's cached copy of it, so the cap keeps a package from bloating a response
# that is shared by every work pool page.
MAX_LOGO_BYTES = 65_536

# Read one byte past the cap so an oversized resource is detected without ever
# holding an unbounded amount of a file in memory.
_READ_LIMIT = MAX_LOGO_BYTES + 1

# Only formats an `<img>` element renders without further processing. SVG loaded
# through `<img>` disables scripting and external references, so no rewriting is done
# here; the bytes are served exactly as the package shipped them.
_MEDIA_TYPES = {
    ".svg": "image/svg+xml",
    ".png": "image/png",
}

# Errors raised when a package is missing, a resource does not exist, or a
# resource cannot be read. Broader exceptions are left to propagate so that a
# genuine defect in worker registration is not silently turned into a missing logo.
_RESOURCE_ERRORS = (ImportError, OSError, TypeError, ValueError)


def _split_resource(resource: str) -> tuple[str, ...] | None:
    """
    Splits a package-relative resource string into path components.

    Returns `None` when `resource` is anything other than a relative POSIX path to
    a supported image file. Components are validated exactly as written, before any
    normalization, so a traversal cannot be hidden behind a path that collapses to
    something harmless.
    """
    if not resource or "\\" in resource or "\x00" in resource:
        return None

    if resource.startswith("/"):
        return None

    parts = tuple(resource.split("/"))
    # Colons can introduce a Windows drive even after a relative prefix.
    if any(part in ("", ".", "..") or ":" in part for part in parts):
        return None

    name = parts[-1]
    if not any(
        name.endswith(suffix) and len(name) > len(suffix) for suffix in _MEDIA_TYPES
    ):
        return None

    return parts


@lru_cache(maxsize=128)
def load_packaged_logo(package: str, resource: str) -> str | None:
    """
    Reads an image from an installed package and returns it as a data URL.

    Args:
        package: The name of the installed Python package holding the image.
        resource: A relative POSIX path to the image within that package, such as
            `frontend/armada.svg`.

    Returns:
        A `data:` URL carrying the image's exact bytes, or `None` if the resource
        is not a readable, supported, appropriately sized image.

    Results are cached, including failures, because worker metadata is rebuilt on
    every collections request. Installing or upgrading a package requires a server
    restart to be picked up, so the cache never needs invalidating at runtime; tests
    may call `load_packaged_logo.cache_clear()`.
    """
    parts = _split_resource(resource)
    if parts is None:
        logger.debug(
            "Ignoring logo resource %r for package %r: not a relative path to a"
            " supported image (%s)",
            resource,
            package,
            ", ".join(sorted(_MEDIA_TYPES)),
        )
        return None

    media_type = _MEDIA_TYPES[f".{parts[-1].rsplit('.', 1)[-1]}"]

    try:
        traversable = files(package).joinpath(*parts)
        with traversable.open("rb") as handle:
            data = handle.read(_READ_LIMIT)
    except _RESOURCE_ERRORS as exc:
        logger.debug(
            "Unable to read logo resource %r from package %r: %s",
            resource,
            package,
            exc,
        )
        return None

    if not data:
        logger.debug("Ignoring empty logo resource %r in package %r", resource, package)
        return None

    if len(data) > MAX_LOGO_BYTES:
        logger.debug(
            "Ignoring logo resource %r in package %r: larger than %s bytes",
            resource,
            package,
            MAX_LOGO_BYTES,
        )
        return None

    return f"data:{media_type};base64,{base64.b64encode(data).decode('ascii')}"
