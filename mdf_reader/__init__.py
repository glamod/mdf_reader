# Following to access the subpackages main modules (or/and functions) directly wihout loops through the full subpackage path
from .data_models import code_tables
from .data_models import schemas
from mdf_reader.read import main as read
__version__ = '1.1'
