#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 09:38:17 2019

Reads source data from a data model with delimited fields to a pandas
DataFrame.

Assumes source data as data model layout and all sections and elements in data.
Reads in full data content, then decodes and converts the elements.

Internally works assuming highest complexity in the input data model:
multiple sequential sections

@author: iregon
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# CAREFULL HERE:
# Note that in Python 3, the io.open function is an alias for the built-in open function.
# The built-in open function only supports the encoding argument in Python 3, not Python 2.
# https://docs.python.org/3.4/library/io.html?highlight=io
import io
from io import open # To allow for encoding definition
from io import StringIO as StringIO

import sys
import pandas as pd
import numpy as np
from copy import deepcopy
import logging
import csv # To disable quoting
import mdf_reader.common.functions as functions
from mdf_reader.common.converters import converters
from mdf_reader.common.decoders import decoders
import mdf_reader.properties as properties

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False
    from io import BytesIO as BytesIO

#   ---------------------------------------------------------------------------
#   MAIN FUNCTIONS
#   ---------------------------------------------------------------------------
def source_to_df(source, schema, read_sections, idx_offset = 0):

    column_names = []
    for section in schema['sections']:
        column_names.extend([ (section,i) for i in schema['sections'][section]['elements'] ])
    multiindex = True if len(read_sections) > 1 or read_sections[0] != properties.dummy_level else False
    out_dtypes = dict()
    out_dtypes.update({ (section,i):properties.pandas_dtypes.get(schema['sections'][section]['elements'][i].get('column_type')) for i in schema['sections'][section]['elements'].keys() } )

    I_CHUNK = 0
    output_buffer = StringIO() if py3 else BytesIO()
    valid_buffer = StringIO() if py3 else BytesIO()
    for df in source: # Indices here are kept are those according to the full record
        logging.info('Processing chunk {}'.format(I_CHUNK))
        # 0. Name columns
        df.columns = pd.MultiIndex.from_tuples(column_names)
        # 1. Remove sections not requested
        drop_sections = [ x for x in schema['sections'] if x not in read_sections ]
        df.drop(drop_sections, axis = 1, level = 0, inplace = True)
        # 2. Decode, scale|offset and convert to dtype (ints will be floats if NaNs)
        dtypes = dict()
        encodings = dict()
        valid = pd.DataFrame(index = df.index, columns = df.columns)
        for section in read_sections:
            dtypes.update({ (section,i):schema['sections'][section]['elements'][i]['column_type'] for i in schema['sections'][section]['elements'] })
            encoded = [ (x) for x in schema['sections'][section]['elements'] if 'encoding' in schema['sections'][section]['elements'][x]]
            encodings.update({ (section,i):schema['sections'][section]['elements'][i]['encoding'] for i in encoded })
        for element in dtypes.keys():
                missing = df[element].isna()
                if element in encoded:
                    df[element] = decoders.get(encodings.get(element)).get(dtypes.get(element))(df[element])

                kwargs = { converter_arg:schema['sections'][element[0]]['elements'][element[1]].get(converter_arg) for converter_arg in properties.data_type_conversion_args.get(dtypes.get(element))  }
                df[element] = converters.get(dtypes.get(element))(df[element], **kwargs)

                valid[element] = missing | df[element].notna()
        # Add _datetime section: watch this if we mean to do multiple reports in record!!!
        # for this to be valid, would have to assume that same day reports and that date in common report section....
        # If datetime is derived from within the actual report, then we would have add _datetime after expansion on dataframe posprocessing
        if schema['header'].get('date_parser'):
            date_parser = schema['header'].get('date_parser')
            date_name = ('_datetime','_datetime') if multiindex else '_datetime'
            date_elements = [(date_parser['section'],x) for x in date_parser['elements'] ] if date_parser.get('section') else date_parser.get('elements')
            out_dtypes.update({ date_name: 'object' })
            df = functions.df_prepend_datetime(df, date_elements, date_parser['format'], date_name = date_name )
            valid = pd.concat([pd.DataFrame(index = valid.index, data = True,columns = [date_name]),valid],sort = False,axis=1)

        out_dtypes.update({ i:df[i].dtype.name for i in df if df[i].dtype.name in properties.numpy_floats})
        if idx_offset > 0:
            df.index = df.index + idx_offset
        # If I get into the section: is it really only removing that named element from that section???? have to check
        #element[section].drop([element],axis=1,level=1,inplace = True)
        # Drop level 0 in multilevel if len(read_sections)==1 or section is dummy
        # 3. Add chunk data to output
        header = False if I_CHUNK == 0 else False
        df.to_csv(output_buffer,header = header, mode = 'a', encoding = 'utf-8',index = False)
        valid.to_csv(valid_buffer,header=header, mode = 'a', encoding = 'utf-8',index = False)
        I_CHUNK += 1

    return output_buffer, valid_buffer, out_dtypes
