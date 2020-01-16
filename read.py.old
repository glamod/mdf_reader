#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 09:38:17 2019

Reads source data (file, pandas DataFrame or pd.io.parsers.TextFileReader) to 
a pandas DataFrame. The source data model needs to be input to the module as 
a named model (included in the module) or as the path to a data model.

Data is validated against its data model after reading, producing a boolean mask.

Calls the schemas, reader and valiate modules in the tool to access the data models,
read the data and validate it.

@author: iregon
"""
import os
import sys
import pandas as pd
from mdf_reader.reader import reader as reader
from mdf_reader.validate import validate as validate
import mdf_reader.schemas as schemas
import mdf_reader.properties as properties
import mdf_reader.common.pandas_TextParser_hdlr as pandas_TextParser_hdlr
import logging
import json

def read(source, data_model = None, data_model_path = None, sections = None,chunksize = None,
         supp_section = None, supp_model = None, supp_model_path = None,
         skiprows = None, out_path = None ):

    logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                    level=logging.INFO,datefmt='%Y%m%d %H:%M:%S',filename=None)

    # 0. Make sure min info is available
    if not data_model and not data_model_path:
        logging.error('A valid data model name or path to data model must be provided')
        return
    if not isinstance(source,tuple(properties.supported_sources)):
        if not source:
            logging.error('Data source is empty (first argument to read()) ')
            return
        elif not os.path.isfile(source):
            logging.error('Can\'t reach data source {} as a file'.format(source))
            logging.info('Supported in-memory data sources are {}'.format(",".join(properties.supported_sources)))
            return

    # 1. Read schema(s) and get file format
    logging.info("READING DATA MODEL SCHEMA FILE...")
    schema = schemas.read_schema( schema_name = data_model, ext_schema_path = data_model_path)
    if not schema:
        return
    if supp_section:
        logging.info("READING SUPPLEMENTAL DATA MODEL SCHEMA FILE...")
        supp_schema = schemas.read_schema( schema_name = supp_model, ext_schema_path = supp_model_path)
        if not supp_schema:
            return
    else:
        supp_schema = None
    # 2. Read data
    imodel = data_model if data_model else data_model_path
    logging.info("EXTRACTING DATA FROM MODEL: {}".format(imodel))
    data, valid = reader.read_model(source,schema, sections = sections, chunksize = chunksize, skiprows = skiprows)
    # 3. Read additional format: on error, return what's been read so far...
    # Mmmmm, make sure we can mix meta_file_formats: eg. core('FIXED_WIDTH')-supp("DELIMITED")           
    if supp_section:
        i_suppmodel = supp_model if supp_model else supp_model_path
        logging.info("EXTRACTING SUPPLEMENTAL DATA FROM MODEL: {}".format(i_suppmodel))
        data, valid = reader.add_supplemental(data, supp_section, supp_schema, valid)
        if isinstance(data,pd.io.parsers.TextFileReader):
            logging.info('...RESTORING DATA PARSER')
            data = pandas_TextParser_hdlr.restore(data.f,data.orig_options)

    # 4. Create out data attributes
    logging.info("CREATING OUTPUT DATA ATTRIBUTES FROM DATA MODEL(S)")
    data_columns = [ x for x in data ] if isinstance(data,pd.DataFrame) else data.orig_options['names']
    out_atts = schemas.df_schema(data_columns, schema, data_model, supp_section = supp_section, supp_schema = supp_schema, supp_model = supp_model )

    # 5. Complete data validation
    logging.info("VALIDATING DATA")
    valid = validate.validate(data, out_atts, valid, data_model = data_model, data_model_path = data_model_path, supp_section = supp_section, supp_model = supp_model, supp_model_path = supp_model_path) 
    if isinstance(data,pd.io.parsers.TextFileReader):
            logging.info('...RESTORING DATA PARSER')
            data = pandas_TextParser_hdlr.restore(data.f,data.orig_options)
            
    if out_path:
        logging.info('WRITING DATA TO FILES IN: {}'.format(out_path))
        cols = [ x for x in data ]
        if isinstance(cols[0],tuple):
            header = [":".join(x) for x in cols]
            out_atts_json = { ":".join(x):out_atts.get(x) for x in out_atts.keys() }
        else:
            header = cols
            out_atts_json = out_atts
        data.to_csv(os.path.join(out_path,'data.csv'), header = header, encoding = 'utf-8',index = True, index_label='index')
        valid.to_csv(os.path.join(out_path,'valid_mask.csv'), header = header, encoding = 'utf-8',index = True, index_label='index')
        with open(os.path.join(out_path,'atts.json'),'w') as fileObj:
            json.dump(out_atts_json,fileObj,indent=4)
            
    return {'data':data,'atts':out_atts,'valid_mask':valid}

if __name__=='__main__':
    kwargs = dict(arg.split('=') for arg in sys.argv[2:])
    if 'sections' in kwargs.keys():
        kwargs.update({ 'sections': [ x.strip() for x in kwargs.get('sections').split(",")] })
    read(sys.argv[1], 
         **kwargs) # kwargs