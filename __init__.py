 # Following to access the subpackages main modules (or/and functions) directly wihout loops through the full subpackage path
from .schemas import schemas as schemas
from .schemas import code_tables as code_tables
from .tests import tests as tests
from .read import read as read
__version__ = '1.1'
