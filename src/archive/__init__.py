from .local import LocalArchiveStorage
from .storage import ArchiveStorage

try:
    from .s3_compat import S3ArchiveStorage
except ImportError:
    S3ArchiveStorage = None  # type: ignore[misc, assignment]

__all__ = ["ArchiveStorage", "LocalArchiveStorage", "S3ArchiveStorage"]
