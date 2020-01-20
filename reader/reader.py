#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 09:38:17 2019

Reads source data from a data model to a pandas DataFrame.

@author: iregon
"""

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


# Get pandas dtype for time_stamps
pandas_timestamp_dtype = pd.to_datetime(pd.DataFrame(['20000101'])[0],format='%Y%m%d').dtypes

def read_model(source,schema, sections = None, chunksize = None, skiprows = None):

    # 0. GET META FORMAT SUBCLASS ---------------------------------------------
    # For future use: some work already done in schema reading
    if schema['header'].get('multiple_reports_per_line'):
        logging.error('File format not yet supported')
        sys.exit(1)

    # 1. DEFINE OUTPUT --------------------------------------------------------
    # Subset data model sections to requested sections
    parsing_order = schema['header'].get('parsing_order')
    # 1.1 Sections to read
    if not sections:
        sections = [ x.get(y) for x in parsing_order for y in x ]
        read_sections_list = [y for x in sections for y in x]
    else:
        read_sections_list = sections 

    # 2. HOMOGENEIZE INPUT DATA TO AN ITERABLE WITH DATAFRAMES:
    # a list with a single dataframe or a pd.io.parsers.TextFileReader
    logging.info("Getting data string from source...")
    TextParser = import_data.import_data(source, chunksize = chunksize, skiprows = skiprows)
    
    # 3. EXTRACT AND READ DATA IN SAME LOOP -----------------------------------
    logging.info("Extracting sections...")
    data_buffer = StringIO()
    valid_buffer = StringIO()
    
    for i,string_df in enumerate(TextParser):
        # a. Get sections separated in a dataframe columns:
        # one per column, only requested sections, ignore rest.
        sections_df = get_sections.get_sections(string_df, schema, read_sections_list)
        # b. Read elements from sections: along data chunks, resulting data types
        # may vary if gaps, keep track of data types!
        [data_df, valid_df, out_dtypesi ] = read_sections.read_sections(sections_df, schema)
        if i == 0:
            out_dtypes = copy.deepcopy(out_dtypesi)
            
        for k in out_dtypesi: 
            if out_dtypesi in properties.numpy_floats:
                out_dtypes.update({ k:out_dtypesi.get(k) })
        # Save to buffer
        data_df.to_csv(data_buffer,header = False, mode = 'a', encoding = 'utf-8',index = False)
        valid_df.to_csv(data_buffer,header = False, mode = 'a', encoding = 'utf-8',index = False)
       
    # 4. OUTPUT DATA ----------------------------------------------------------
    # WE'LL NEED TO POSPROCESS THIS WHEN READING MULTIPLE REPORTS PER LINE
    data_buffer.seek(0)
    valid_buffer.seek(0)
    logging.info("Wrapping output....")
#   Chunksize from the imported TextParser if it is a pd.io.parsers.TextFileReader
#   (source is either pd.io.parsers.TextFileReader or a file with chunksize specified on input):
#   This way it supports direct chunksize property inheritance if the input source was a pd.io.parsers.TextFileReader
    chunksize = TextParser.orig_options['chunksize'] if isinstance(TextParser,pd.io.parsers.TextFileReader) else None
    # 'datetime' is not a valid pandas dtype: Only on output (on reading) will be then converted (via parse_dates) to datetime64[ns] type, cannot specify 'datetime' (of any kind) here: will fail
    date_columns = [] # Needs to be the numeric index of the column, as seems not to be able to work with tupples....
    for i,element in enumerate(list(out_dtypes)):
        if out_dtypes.get(element) == 'datetime':
            date_columns.append(i)
            
    data_reader = pd.read_csv(data_buffer,names = data_df.columns, chunksize = chunksize, dtype = out_dtypes, parse_dates = date_columns)
    valid_reader = pd.read_csv(valid_buffer,names = data_df.columns, chunksize = chunksize)

    return data_reader, valid_reader
