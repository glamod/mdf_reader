# Following to access the subpackages main modules (or/and functions) directly wihout loops through the full subpackage path
from .data_models import code_tables  # noqa
from .data_models import schemas  # noqa
from mdf_reader.read import read  # noqa


def _get_version():
    __version__ = "unknown"
    try:
        from ._version import __version__
    except ImportError:
        pass
    return __version__


__version__ = _get_version()
