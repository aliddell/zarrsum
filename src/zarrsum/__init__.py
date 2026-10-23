"""zarrsum - a Zarr-aware content id for versioning Zarr datasets.

See ``data-versioning/design/zarr-content-hash.md`` for the design and the decisions.
"""
from .core import hash_dataset, DatasetChangedError, DatasetBusyError

__version__ = "0.1.0"
__all__ = ["hash_dataset", "DatasetChangedError", "DatasetBusyError", "__version__"]
