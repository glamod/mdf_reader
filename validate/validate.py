#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 09:38:17 2019

Validates elements in a pandas DataFrame against its input data model. Output
is a boolean DataFrame

Validated elements are those with the following column_types:
    - any in properties.numeric_types: range validation
    - 'key': code table validation
    - 'datetime': because of the way they are converted, read into datetime,
    they should already be NaT if they not validate as a valid datetime. The
    correspoding mask is just created for them

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
import os
import pandas as pd
import numpy as np
import logging
from .. import properties
from ..schemas import code_tables

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False
    from io import BytesIO as BytesIO

# Get pandas dtype for time_stamps
toolPath = os.path.dirname(os.path.abspath(__file__))
dirname=os.path.dirname
schema_lib = os.path.join(dirname(toolPath),'schemas','lib')

def validate_numeric(elements,df,schema): 
    # Find thresholds in schema. Flag if not available -> warn 
    mask = pd.DataFrame(index = df.index, data = False, columns = elements)
    lower = { x:schema.get(x).get('valid_min', -np.inf) for x in elements }
    upper = { x:schema.get(x).get('valid_max', np.inf) for x in elements }
    set_elements = [ x for x in lower.keys() if lower.get(x) != -np.inf and upper.get(x) != np.inf ]
    if len([ x for x in elements if x not in set_elements ]) > 0:
        logging.warning('Data numeric elements with missing upper or lower threshold: {}'.format(",".join([ str(x) for x in elements if x not in set_elements ])))
        logging.warning('Corresponding upper and/or lower bounds set to +/-inf for validation')
    #mask[set_elements] = ((df[set_elements] >= [ lower.get(x) for x in set_elements ] ) & (df[set_elements] <= [ upper.get(x) for x in set_elements ])) | df[set_elements].isna()
    mask[elements] = ((df[elements] >= [ lower.get(x) for x in elements ] ) & (df[elements] <= [ upper.get(x) for x in elements ])) | df[elements].isna()
    return mask

def validate_codes(elements, df, code_tables_path, schema, supp = False):
    
    mask = pd.DataFrame(index = df.index, data = False, columns = elements)
    
    if os.path.isdir(code_tables_path):
        for element in elements:
            code_table = schema.get(element).get('codetable')
            if not code_table:
                logging.error('Code table not defined for element {}'.format(element))
                logging.warning('Element mask set to False')
            else:
                code_table_path = os.path.join(code_tables_path, code_table + '.json')
                # Eval elements: if ._yyyy, ._xxx in name: pd.DateTimeIndex().xxxx is the element to pass
                # Additionally, on doing this, should make sure that element is a datetime type:
                if os.path.isfile(code_table_path):
                    try:
                        table = code_tables.read_table(code_table_path)
                        if supp:
                            key_elements = [ element[1] ] if not table.get('_keys') else list(table['_keys'].get(element[1]))
                        else:
                            key_elements = [ element ] if not table.get('_keys') else list(table['_keys'].get(element))
                        if supp:
                            key_elements = [ (element[0],x) for x in key_elements ]
                        else:
                            key_elements = [ (properties.dummy_level,x) if not isinstance(x,tuple) else x for x in key_elements ]
                        dtypes =  { x:properties.pandas_dtypes.get(schema.get(x).get('column_type')) for x in key_elements } 
                        table_keys = code_tables.table_keys(table)
                        table_keys_str = [ "∿".join(x) if isinstance(x,list) else x for x in table_keys ]
                        validation_df = df[key_elements]                          
                        imask = pd.Series(index = df.index, data =True)
                        imask.iloc[np.where(validation_df.notna().all(axis = 1))[0]] = validation_df.iloc[np.where(validation_df.notna().all(axis = 1))[0],:].astype(dtypes).astype('str').apply("∿".join, axis=1).isin(table_keys_str)
                        mask[element] = imask
                    except Exception as e:
                        logging.error('Error validating coded element {}:'.format(element))
                        logging.error('Error is {}:'.format(e))
                        logging.warning('Element mask set to False')
                else:
                    logging.error('Error validating coded element {}:'.format(element))
                    logging.error('Code table file {} not found'.format(code_table_path))
                    logging.warning('Element mask set to False')
                    continue
    else:
        logging.error('Code tables path {} not found'.format(code_tables_path)) 
        logging.warning('All coded elements set to False')

    return mask

def validate(data, schema, mask0, data_model = None, data_model_path = None, supp_section = None, supp_model = None, supp_model_path = None ):  
    # schema is the input data schema: collection of attributes for DF elements, not the data model schema
    # data model schema info is nevertheless needed to access code tables
    logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                    level=logging.INFO,datefmt='%Y%m%d %H:%M:%S',filename=None)
    
    # 0. Check arguments are valid---------------------------------------------
    if not data_model and not data_model_path: 
        logging.error('A valid data model or data model path must be provided')
        return
    if supp_section:
        if not supp_model and not supp_model_path: 
            logging.error('A valid data model or data model path must be provided for supplemental data')
            return    
    if not isinstance(data,pd.DataFrame) and not isinstance(data,pd.io.parsers.TextFileReader):
        logging.error('Input data must be a data frame or a TextFileReader object')
        return
    # 1. Get data models' path------------------------------------------------- 
    if data_model:
        model_path = os.path.join(schema_lib,data_model)
    else:
        model_path = data_model_path 
    code_tables_path = os.path.join(model_path,'code_tables')
    
    if supp_section:
        if supp_model:
            supp_path = os.path.join(schema_lib,supp_model)
        else:
            supp_path = supp_model_path  
        supp_code_tables_path = os.path.join(supp_path,'code_tables')
        
    # 2. Go--------------------------------------------------------------------
    TextParserData = [data.copy()] if isinstance(data,pd.DataFrame) else data
    TextParserMask = [mask0.copy()] if isinstance(mask0,pd.DataFrame) else mask0
    output_buffer = StringIO() if py3 else BytesIO()
    for df, mk in zip(TextParserData, TextParserMask):
        elements = [ x for x in df if x in schema ]
        # See what elements we need to validate: coded go to different code table paths if supplemental
        numeric_elements =  [ x for x in elements if schema.get(x).get('column_type') in properties.numeric_types ]
        datetime_elements = [ x for x in elements if schema.get(x).get('column_type') == 'datetime' ]   
        coded_elements =    [ x for x in elements if schema.get(x).get('column_type') == 'key' ]
        if supp_section:
            supp_coded_elements = [ x for x in coded_elements if x[0] == supp_section ]
            for x in supp_coded_elements:
                coded_elements.remove(x)
        
        if any([isinstance(x,tuple) for x in numeric_elements + datetime_elements + coded_elements ]):
            validated_columns = pd.MultiIndex.from_tuples(list(set(numeric_elements + coded_elements + datetime_elements)))
        else:
            validated_columns = list(set(numeric_elements + coded_elements + datetime_elements))
        imask = pd.DataFrame(index = df.index, columns = df.columns)

        # Validate elements by dtype
        # Table coded elements can be as well numeric -> initially should not have its bounds defined in schema, but:
        # Numeric validation will be overriden by code table validation!!!     
        # 1. NUMERIC ELEMENTS
        imask[numeric_elements] = validate_numeric(numeric_elements, df, schema)
        
        # 2. TABLE CODED ELEMENTS
        # See following: in multiple keys code tables, the non parameter element, won't have a code_table attribute in the schema:
        # So we need to check the code_table.keys files in addition to the schema
        # Additionally, a YEAR key can fail in one table, but be compliant with anbother, then, how would we mask this?
        #               also, a YEAR defined as an integer, will undergo its own check.....
        # So I think we need to check nested keys as a whole, and mask only the actual parameterized element:
        # Get the full list of keys combinations (tuples, triplets...) and check the column combination against that: if it fails, mark the element!
        # Need to see how to grab the YEAR part of a datetime when YEAR comes from a datetime element
        # pd.DatetimeIndex(df['_datetime']).year
        if len(coded_elements)> 0:
            imask[coded_elements] = validate_codes(coded_elements, df, code_tables_path, schema)
        try:
            if len(supp_coded_elements)>0:
                imask[supp_coded_elements] = validate_codes(supp_coded_elements, df, supp_code_tables_path, schema, supp = True)
        except:
            pass         
        # 3. DATETIME ELEMENTS
        # only those declared as such in schema, not _datetime
        # Because of the way they are converted, read into datetime, they should already be NaT if they not validate as a valid datetime;
        # let's check: hurray! they are!
        imask[datetime_elements] = df[datetime_elements].notna()
        imask[validated_columns] = imask[validated_columns].mask(mk[validated_columns] == False, False)

        imask.to_csv(output_buffer,header = False, mode = 'a', encoding = 'utf-8',index = False)
        
    output_buffer.seek(0)   
    chunksize = None if isinstance(data,pd.DataFrame) else data.orig_options['chunksize']
    mask = pd.read_csv(output_buffer,names = [ x for x in imask ], chunksize = chunksize)
    
    return mask
    