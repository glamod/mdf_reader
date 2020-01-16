#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 09:38:17 2019

Reads source data from a data model to a pandas DataFrame.

Optionally, it reads supplemental data from the same source (from a different
 data model) and pastes that to the output DataFrame

Uses the meta_formats generic submodules ('delimited' and 'fixed_width') to
pre-format data source and read either generic type of data model.

@author: iregon
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# CAREFULL HERE:
# Note that in Python 3, the io.open function is an alias for the built-in open function.
# The built-in open function only supports the encoding argument in Python 3, not Python 2.
# https://docs.python.org/3.4/library/io.html?highlight=io
from io import StringIO as StringIO
import sys
import pandas as pd
import numpy as np
import logging
from . import meta_formats

from .. import properties
from . import import_data
from . import get_sections
from . import read_sections

import copy

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False
    from io import BytesIO as BytesIO

# Get pandas dtype for time_stamps
pandas_timestamp_dtype = pd.to_datetime(pd.DataFrame(['20000101'])[0],format='%Y%m%d').dtypes

def read_model(source,schema, sections = None, chunksize = None, skiprows = None):

    # 0. GET META FORMAT SUBCLASS ---------------------------------------------
    # For future use: some work already done in schema reading
    if schema['header'].get('multiple_reports_per_line'):
        logging.error('File format not yet supported')
        sys.exit(1)

    # 1. PARSE SCHEMA ---------------------------------------------------------

    parsing_order = schema['header'].get('parsing_order')

    # 2. DEFINE OUTPUT --------------------------------------------------------
    # 2.1 Sections to read
    if not sections:
        sections = [ x.get(y) for x in parsing_order for y in x ]
        read_sections_list = [y for x in sections for y in x]
    else:
        read_sections_list = sections
        

    # 3. HOMOGENEIZE INPUT DATA (FILE OR TEXTREADER) TO AN ITERABLE TEXTREADER
    logging.info("Getting data string from source...")
    TextParser = import_data.import_data(source, chunksize = chunksize, skiprows = skiprows)
    
    # 4. EXTRACT SECTIONS IN A PARSER; EXTRACT SECTIONS HERE AND READ DATA IN 
    # SAME LOOP? SHOULD DO....
    logging.info("Extracting sections...")
    data_buffer = StringIO()
    
    
#    valid_buffer = ...
    for i,string_df in enumerate(TextParser):
        # Get sections separated in a dataframe: one per column, only requested
        # sections, ignore rest.
        sections_df = get_sections.get_sections(string_df, schema, read_sections_list)
        # Read elements from sections: along data chunks, resulting data types
        # may vary if gaps
        [data_df,out_dtypesi ] = read_sections.read_sections(sections_df, schema)
        if i == 0:
            out_dtypes = copy.deepcopy(out_dtypesi)
            
        for k in out_dtypesi: 
            if out_dtypesi in properties.numpy_floats:
                out_dtypes.update({ k:out_dtypesi.get(k) })

        data_df.to_csv(data_buffer,header = False, mode = 'a', encoding = 'utf-8',index = False)
#        [output_buffer,valid_buffer,dtypes] = reader_function(TextParser, schema, read_sections = read_sections, idx_offset = idx_offset )
#        
#    # 5. OUTPUT DATA:----------------------------------------------------------
#    # WE'LL NEED TO POSPROCESS THIS WHEN READING MULTIPLE REPORTS PER LINE
    data_buffer.seek(0)
#    valid_buffer.seek(0)
#    logging.info("Wrapping output....")
#    chunksize = TextParser.orig_options['chunksize'] if isinstance(TextParser,pd.io.parsers.TextFileReader) else None
#    logging.info('Data')
#    # 'datetime' is not a valid pandas dtype: Only on output (on reading) will be then converted (via parse_dates) to datetime64[ns] type, cannot specify 'datetime' (of any kind) here: will fail
#    date_columns = [] # Needs to be the numeric index of the column, as seems not to be able to work with tupples....
#    for i,element in enumerate(list(dtypes)):
#        if dtypes.get(element) == 'datetime':
#            date_columns.append(i)
    data_reader = pd.read_csv(data_buffer,names = data_df.columns, chunksize = chunksize, dtype = out_dtypes)#, parse_dates = date_columns)
#    logging.info('Mask')
#    valid_reader = pd.read_csv(valid_buffer,names = out_names, chunksize = chunksize)

#    return data_reader, valid_reader
    return data_reader
