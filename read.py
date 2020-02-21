#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 09:38:17 2019

Reads a data file to a pandas DataFrame using a pre-defined data model.

The data model needs to be input to the module as a named model (included in the module) or as the path to a valid data model.

Data elements are validated against its data model after reading, producing a boolean mask.

Uses submodules:
- schemas
- reader
- valiate

@author: iregon
"""
import os
import sys
import pandas as pd
import logging
import json
import copy
from io import StringIO as StringIO

from . import schemas
from . import properties
from .common import pandas_TextParser_hdlr
from .reader import import_data
from .reader import get_sections
from .reader import read_sections
from .validate import validate

toolPath = os.path.dirname(os.path.abspath(__file__))
schema_lib = os.path.join(toolPath,'schemas','lib')

# AUX FUNCTIONS ---------------------------------------------------------------
def ERV(TextParser,read_sections_list, schema, code_tables_path):

    data_buffer = StringIO()
    valid_buffer = StringIO()

    for i_chunk, string_df in enumerate(TextParser):
        
        # 1. Get a DF with 1 column per sections:
        # - only sections requested, ignore rest
        # - requested NA sections as NaN columns
        # - columns(sections) order as in read_sections_list
        
        sections_df = get_sections.get_sections(string_df, schema, read_sections_list)

        # 2. Read elements from sections: along data chunks, resulting data types
        # may vary if gaps, keep track of data types: add Intxx pandas classes rather than intxx to avoid this!
        # Sections are parsed in the same order as sections_df.columns
        
        [data_df, valid_df, out_dtypesi ] = read_sections.read_sections(sections_df, schema)
        if i_chunk == 0:
            out_dtypes = copy.deepcopy(out_dtypesi)

        for k in out_dtypesi:
            if out_dtypesi in properties.numpy_floats:
                out_dtypes.update({ k:out_dtypesi.get(k) })
        
        # 3. Validate data elements
        
        valid_df = validate.validate(data_df, valid_df, schema, code_tables_path)
        
        # 4. Save to buffer
        data_df.to_csv(data_buffer,header = False, mode = 'a', encoding = 'utf-8',index = False)
        valid_df.to_csv(valid_buffer,header = False, mode = 'a', encoding = 'utf-8',index = False)
        
    # Create the output
    # WE'LL NEED TO POSPROCESS THIS WHEN READING MULTIPLE REPORTS PER LINE, IF EVER...
    data_buffer.seek(0)
    valid_buffer.seek(0)
    logging.info("Wrapping output....")
    # Chunksize from the imported TextParser if it is a pd.io.parsers.TextFileReader
    # (source is either pd.io.parsers.TextFileReader or a file with chunksize specified on input):
    # This way it supports direct chunksize property inheritance if the input source was a pd.io.parsers.TextFileReader
    chunksize = TextParser.orig_options['chunksize'] if isinstance(TextParser,pd.io.parsers.TextFileReader) else None
    
    # 'datetime' is not a valid pandas dtype: Only on output (on reading) will be then converted (via parse_dates) to datetime64[ns] type,
    # cannot specify 'datetime' (of any kind) here: would fail, need to change to 'object' and tell the date parser where it is
    date_columns = [] # Needs to be the numeric index of the column, as seems not to be able to work with tupples....
    for i,element in enumerate(list(out_dtypes)):
        if out_dtypes.get(element) == 'datetime':
            date_columns.append(i)
            out_dtypes.update({element:'object'})

    data = pd.read_csv(data_buffer,names = data_df.columns, chunksize = chunksize, dtype = out_dtypes, parse_dates = date_columns)
    valid = pd.read_csv(valid_buffer,names = data_df.columns, chunksize = chunksize)

    return data, valid

def validate_arg(arg_name,arg_value,arg_type):
    if arg_value and not isinstance(arg_value,arg_type):
        logging.error('Argument {0} must be {1}, input type is {2}'.format(arg_name,arg_type,type(arg_value)))
        return False
    else:
        return True

def validate_path(arg_name,arg_value):
    if arg_value and not os.path.isdir(arg_value):
        logging.error('{0} could not find path {1}'.format(arg_name,arg_value))
        return False
    else:
        return True

# END AUX FUNCTIONS -----------------------------------------------------------
        

def read(source, data_model = None, data_model_path = None, sections = None,chunksize = None,
         skiprows = None, out_path = None ):

    logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                    level=logging.INFO,datefmt='%Y%m%d %H:%M:%S',filename=None)

    # 0. VALIDATE INPUT
    if not data_model and not data_model_path:
        logging.error('A valid data model name or path to data model must be provided')
        return
    if not os.path.isfile(source):
        logging.error('Can\'t find input data file {}'.format(source))
        return
    if not validate_arg('sections',sections,list):
        return
    if not validate_arg('chunksize',chunksize,int):
        return
    if not validate_arg('skiprows',skiprows,int):
        return
    if not validate_path('data_model_path',data_model_path):
        return
    if not validate_path('out_path',out_path):
        return

    # 1. GET DATA MODEL
    # Schema reader will return empty if cannot read schema or is not valid
    # and will log the corresponding error
    # multiple_reports_per_line error also while reading schema
    logging.info("READING DATA MODEL SCHEMA FILE...")
    schema = schemas.read_schema( schema_name = data_model, ext_schema_path = data_model_path)
    if not schema:
        return
    if data_model:
        model_path = os.path.join(schema_lib,data_model)
    else:
        model_path = data_model_path
    code_tables_path = os.path.join(model_path,'code_tables')


    # 2. READ AND VALIDATE DATA
    imodel = data_model if data_model else data_model_path
    logging.info("EXTRACTING DATA FROM MODEL: {}".format(imodel))

    # 2.1. Subset data model sections to requested sections
    parsing_order = schema['header'].get('parsing_order')
    if not sections:
        sections = [ x.get(y) for x in parsing_order for y in x ]
        read_sections_list = [y for x in sections for y in x]
    else:
        read_sections_list = sections

    # 2.2 Homogeneize input data to an iterable with dataframes:
    # a list with a single dataframe or a pd.io.parsers.TextFileReader
    logging.info("Getting data string from source...")
    TextParser = import_data.import_data(source, chunksize = chunksize, skiprows = skiprows)

    # 2.3. Extract, read and validate data in same loop
    logging.info("Extracting and reading sections")
    data,valid = ERV(TextParser,read_sections_list, schema, code_tables_path)

    # 3. CREATE OUTPUT DATA ATTRIBUTES 
    logging.info("CREATING OUTPUT DATA ATTRIBUTES FROM DATA MODEL")
    data_columns = [ x for x in data ] if isinstance(data,pd.DataFrame) else data.orig_options['names']
    out_atts = schemas.df_schema(data_columns, schema)

    # 4. OUTPUT TO FILES IF REQUESTED
    if out_path:
        enlisted = False
        if not isinstance(data,pd.io.parsers.TextFileReader):
            data = [data]
            valid = [valid]
            enlisted = True
        logging.info('WRITING DATA TO FILES IN: {}'.format(out_path))

        for i, (data_df,valid_df) in enumerate(zip(data,valid)):
            header = False
            mode = 'a'
            if i == 0:
                mode = 'w'
                cols = [ x for x in data_df ]
                if isinstance(cols[0],tuple):
                    header = [":".join(x) for x in cols]
                    out_atts_json = { ":".join(x):out_atts.get(x) for x in out_atts.keys() }
                else:
                    header = cols
                    out_atts_json = out_atts
            data_df.to_csv(os.path.join(out_path,'data.csv'), header = header, mode = mode, encoding = 'utf-8',index = True, index_label='index')
            valid_df.to_csv(os.path.join(out_path,'mask.csv'), header = header, mode = mode, encoding = 'utf-8',index = True, index_label='index')
        if enlisted:
            data = data[0]
            valid = valid[0]
        else:
            data = pandas_TextParser_hdlr.restore(data.f,data.orig_options)
            valid = pandas_TextParser_hdlr.restore(valid.f,valid.orig_options)
        with open(os.path.join(out_path,'atts.json'),'w') as fileObj:
            json.dump(out_atts_json,fileObj,indent=4)

    # 5. RETURN DATA
    class output():
        def __init__(self):
            self.data = data
            self.atts = out_atts
            self.mask = valid
        
    
    return output()

if __name__=='__main__':
    kwargs = dict(arg.split('=') for arg in sys.argv[2:])
    if 'sections' in kwargs.keys():
        kwargs.update({ 'sections': [ x.strip() for x in kwargs.get('sections').split(",")] })
    read(sys.argv[1], **kwargs) # kwargs
