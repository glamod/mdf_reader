from .common.plots import plot_model_validation  # noqa
from .data import test_data  # noqa
from .data_models import code_tables  # noqa
from .data_models import schemas  # noqa
from .read import read  # noqa
from .reader.get_sections import get_sections  # noqa
from .reader.import_data import import_data  # noqa
from .reader.read_sections import read_sections  # noqa


def _get_version():
    __version__ = "unknown"
    try:
        from ._version import __version__
    except ImportError:
        pass
    return __version__


__version__ = _get_version()
