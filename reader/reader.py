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

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False
    from io import BytesIO as BytesIO

# Get pandas dtype for time_stamps
pandas_timestamp_dtype = pd.to_datetime(pd.DataFrame(['20000101'])[0],format='%Y%m%d').dtypes

def add_supplemental(data, supp_section, supp_schema, valid):
    # Supplemental data needs to have no sectioning: cannot merge dfs with different level depths in the columns...
    try:

        supp_format = supp_schema['header'].get('format')

        if supp_format in properties.supported_meta_file_formats:
            TextParser = data if isinstance(data, pd.io.parsers.TextFileReader) else [data]
            TextParser_valid = valid if isinstance(valid, pd.io.parsers.TextFileReader) else [valid]
            chunksize = data.orig_options['chunksize'] if isinstance(TextParser,pd.io.parsers.TextFileReader) else None
            iidx_offset = chunksize if chunksize else 0
            output_buffer = StringIO() if py3 else BytesIO()
            output_buffer_valid = StringIO() if py3 else BytesIO()
            I_CHUNK = 0
            for idata,ivalid in zip(TextParser,TextParser_valid):
                date_columns = list(np.where(idata.dtypes == pandas_timestamp_dtype)[0])
                dtypes = idata.dtypes.to_dict()
                supp, supp_valid = read_model(idata[supp_section],supp_schema, idx_offset = I_CHUNK*iidx_offset )
                supp_date_columns = list(np.where(supp.dtypes == pandas_timestamp_dtype)[0] + len(idata.columns) - 1 )
                date_columns.extend(supp_date_columns)
                date_columns = [ int(x) for x in date_columns ] # reader date parser won't take numpy.int64 from np.where as col index
                if I_CHUNK == 0:
                    o_supp_dtypes = supp.dtypes.to_dict()
                else:
                    o_supp_dtypes.update({ i:supp[i].dtype for i in supp if supp[i].dtype in properties.numpy_floats})
                supp_elements = supp.columns.to_list()
                supp_dtypes = {}
                for element in supp_elements:
                    supp_dtypes[(supp_section,element)] =  o_supp_dtypes.get(element)
                dtypes.pop((supp_section,idata[supp_section].columns.to_list()[0]), None)
                idata.drop(supp_section, axis = 1, inplace = True, level = 0)# OMG: apparently, with multiindex, this does not drop the columns from idata.columns
                ivalid.drop(supp_section, axis = 1, inplace = True, level = 0)
                supp.columns = [ (supp_section,x) for x in supp.columns ]
                supp_valid.columns = [ (supp_section,x) for x in supp_valid.columns ]
                dtypes.update(supp_dtypes)
                supp.index = idata.index
                supp_valid.index = ivalid.index
                column_names = [ x for x in idata if x[0] != supp_section ]
                column_names.extend([ x for x in supp ])
                new_dtypes = { x:dtypes.get(x) for x in column_names }
                idata = pd.concat([idata,supp],sort = False,axis=1)
                ivalid = pd.concat([ivalid,supp_valid],sort = False,axis=1)
                idata.to_csv(output_buffer,header=False, mode = 'a', encoding = 'utf-8',index = False)
                ivalid.to_csv(output_buffer_valid,header=False, mode = 'a', encoding = 'utf-8',index = False)
                I_CHUNK += 1
            
            output_buffer.seek(0)
            output_buffer_valid.seek(0)
            for element in list(dtypes):
                if new_dtypes.get(element) == pandas_timestamp_dtype:
                    new_dtypes[element] = 'object' # Only on output (on reading) will be then converted to datetime64[ns] type, cannot specify 'datetime' here: have to go through parser
            data = pd.read_csv(output_buffer,names = idata.columns, dtype = new_dtypes, chunksize = chunksize, parse_dates = date_columns )
            valid = pd.read_csv(output_buffer_valid,names = ivalid.columns, chunksize = chunksize)
            return data, valid
        else:
            logging.error('Supplemental file format not supported: {}'.format(supp_format))
            logging.warning('Supplemental data not extracted from supplemental section')
            return data, valid
    except Exception as e:
        logging.warning('Supplemental data not extracted from supplemental section', exc_info=True)
        return data, valid


def read_model(source,schema, sections = None, chunksize = None, skiprows = None, idx_offset = 0):

    meta_format = schema['header'].get('format')
    if meta_format not in properties.supported_meta_file_formats:
        logging.error('File format read from input schema not supported: {}'.format(meta_format))
        return
    meta_reader = ".".join(['meta_formats',meta_format])

    # 0. GET META FORMAT SUBCLASS ---------------------------------------------
    if schema['header'].get('multiple_reports_per_line'): # needs to eval to True if set and True and to false if not set or false, without breaking
        format_subclass = '1x'
    else:
        format_subclass = '11'

    # 1. PARSE SCHEMA ---------------------------------------------------------

    delimiter = schema['header'].get('delimiter')
    parsing_order = schema['header'].get('parsing_order')

    # 2. DEFINE OUTPUT --------------------------------------------------------
    # 2.1 Sections to read
    if not sections:
        sections = [ x.get(y) for x in parsing_order for y in x ]
        read_sections = [y for x in sections for y in x]
    else:
        read_sections = sections
    multiindex = True if len(read_sections) > 1 or read_sections[0] != properties.dummy_level else False
    
    if format_subclass == '1x':
        return schema
    # 2.1 Elements names: same order as declared in schema, which is the order in which the readers read them...
    names = []
    if schema['header'].get('date_parser'):
        if multiindex:
            names.extend([('_datetime','_datetime')])
        else:
            names.extend(['_datetime'])

    for section in read_sections:
        if multiindex:
            names.extend([ (section,x) for x in schema['sections'][section]['elements'].keys() if not schema['sections'][section]['elements'][x].get('ignore') ])
        else:
            names.extend([ x for x in schema['sections'][section]['elements'].keys() if not schema['sections'][section]['elements'][x].get('ignore') ])

    # 3. GET DATA FROM SOURCE (DF, FILE OR TEXTREADER):------------------------
    #    SIMPLE STRING PER REPORT/LINE
    logging.info("Getting input data from source...")
    source_function = eval(meta_reader + "." + "_".join(['source',format_subclass]))
    TextParser = source_function(source,schema, chunksize = chunksize, skiprows = skiprows, delimiter = delimiter)
    # 4. DO THE ACTUAL READING
    reader_function = eval(meta_reader + "." + 'source_to_df')
    logging.info("Reading data...")
    [output_buffer,valid_buffer,dtypes] = reader_function(TextParser, schema, read_sections = read_sections, idx_offset = idx_offset )
    # 5. OUTPUT DATA:----------------------------------------------------------
    # WE'LL NEED TO POSPROCESS THIS WHEN READING MULTIPLE REPORTS PER LINE
    output_buffer.seek(0)
    valid_buffer.seek(0)
    logging.info("Wrapping output....")
    chunksize = TextParser.orig_options['chunksize'] if isinstance(TextParser,pd.io.parsers.TextFileReader) else None
    logging.info('Data')
    # 'datetime' is not a valid pandas dtype: Only on output (on reading) will be then converted (via parse_dates) to datetime64[ns] type, cannot specify 'datetime' (of any kind) here: will fail
    date_columns = [] # Needs to be the numeric index of the column, as seems not to be able to work with tupples....
    for i,element in enumerate(list(dtypes)):
        if dtypes.get(element) == 'datetime':
            date_columns.append(i)
    df_reader = pd.read_csv(output_buffer,names = names, chunksize = chunksize, dtype = dtypes, parse_dates = date_columns)
    logging.info('Mask')
    valid_reader = pd.read_csv(valid_buffer,names = names, chunksize = chunksize)

    return df_reader, valid_reader
