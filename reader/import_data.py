#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 13:17:43 2020

FUNCTION TO PREPARE SOURCE DATA TO WHAT GET_SECTIONS() EXPECTS:
    AN ITERABLE WITH DATAFRMAES

INPUT IS EITHER:
    - pd.io.parsers.textfilereader
    - io.StringIO
    - file path

OUTPUT IS AN ITERABLE, DEPENDING ON SOURCE TYPE AND CHUNKSIZE BEING SET:
    - a single dataframe in a list
    - a pd.io.parsers.textfilereader


WITH BASICALLY 1 RECORD (ONE OR MULTIPLE REPORTS) IN ONE LINE

delimiter="\t" option in pandas.read_fwf avoids white spaces at tails
to be stripped

@author: iregon

DEV NOTES:
1) What this module is able to ingest needs to align with properties.supported_sources
2) Check io.StringIO input: why there, does it actually work as it is?
3) Check pd.io.parsers.textfilereader input: why there, does it actually work as it is?


OPTIONS IN OLD DEVELOPMENT:
    1. DLMT: delimiter = ',' default
        names = [ (x,y) for x in schema['sections'].keys() for y in schema['sections'][x]['elements'].keys()]
        missing = { x:schema['sections'][x[0]]['elements'][x[1]].get('missing_value') for x in names }
        TextParser = pd.read_csv(source,header = None, delimiter = delimiter, encoding = 'utf-8',
                                 dtype = 'object', skip_blank_lines = False, chunksize = chunksize,
                                 skiprows = skiprows, names = names, na_values = missing)

    2. FWF:# delimiter = '\t' so that it reads blanks as blanks, otherwise reads as empty: NaN
    this applies mainly when reading elements from sections, but we leave it also here
    TextParser = pd.read_fwf(source,widths=[FULL_WIDTH],header = None, skiprows = skiprows, delimiter="\t", chunksize = chunksize)

"""

import pandas as pd
import os
import io

from .. import properties

def to_iterable_df(source,skiprows = None, chunksize = None):
    TextParser = pd.read_fwf(source,widths=[properties.MAX_FULL_REPORT_WIDTH],header = None, delimiter="\t", skiprows = skiprows, chunksize = chunksize)
    if not chunksize:
        TextParser = [TextParser]
    return TextParser


def import_data(source,chunksize = None, skiprows = None):

    if isinstance(source,pd.io.parsers.TextFileReader):
        return source
    elif isinstance(source, io.StringIO):
        TextParser = to_iterable_df(source,skiprows = skiprows, chunksize = chunksize)
        return TextParser
    elif os.path.isfile(source):
        TextParser = to_iterable_df(source,skiprows = skiprows, chunksize = chunksize)
        return TextParser
    else:
        print('Error')
        return
