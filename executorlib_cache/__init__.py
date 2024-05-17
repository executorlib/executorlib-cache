from ._version import get_versions
from executorlib_cache.filecache import FileExecutor


__version__ = get_versions()["version"]
__all__ = [FileExecutor]
