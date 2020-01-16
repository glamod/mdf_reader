#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


"""
import glob
import os
import io
import pandas as pd


# Supported formats, sources and internal data models -------------------------
schema_path = os.path.join(os.path.dirname(__file__),'schemas','lib')
supported_file_formats = [ os.path.basename(x).split(".")[0] for x in glob.glob(schema_path + '/*/*.json') if os.path.basename(x).split(".")[0] == os.path.dirname(x).split("/")[-1]]
supported_sources = [pd.DataFrame, pd.io.parsers.TextFileReader, io.StringIO]

# Data types ------------------------------------------------------------------
numpy_integers = ['int8','int16','int32','int64','uint8','uint16','uint32','uint64']
numpy_floats = ['float16','float32','float64']
numeric_types = numpy_integers.copy()
numeric_types.extend(numpy_floats)

object_types = ['str','object','key','datetime']

data_types = object_types.copy()
data_types.extend(numpy_integers)
data_types.extend(numpy_floats)

pandas_dtypes = {}
for dtype in object_types:
    pandas_dtypes[dtype] = 'object'
pandas_dtypes.update({ x:x for x in numeric_types })

# ....and how they are managed
data_type_conversion_args = {}
for dtype in numeric_types:
    data_type_conversion_args[dtype] = ['scale','offset']
data_type_conversion_args['str'] = ['disable_white_strip']
data_type_conversion_args['object'] = ['disable_white_strip'] 
data_type_conversion_args['key'] = ['disable_white_strip']
data_type_conversion_args['datetime'] = ['datetime_format'] 

# Misc ------------------------------------------------------------------------
tol = 1E-10
dummy_level = 'SECTION__'
# Length of reports in initial read
MAX_FULL_REPORT_WIDTH = 100000