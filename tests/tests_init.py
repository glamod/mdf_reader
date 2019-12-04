#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 20 08:05:50 2019

@author: iregon
"""

import os
import mdf_reader
import pandas as pd
import numpy as np
from io import StringIO
import mdf_reader.common.pandas_TextParser_hdlr as pandas_TextParser_hdlr

funPath = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(funPath,'data')
schema_lib = os.path.join(os.path.dirname(funPath),'schemas','lib')

# A. TESTS TO READ FROM DATA FROM DIFFERENT INPUTS
# -----------------------------------------------------------------------------
#   FROM FILE: WITH AND WIHTOUT SUPPLEMENTAL
def imma1_buoys_nosupp():
    schema = 'imma1'
    data_file_path = os.path.join(data_path,'meds_2010-07_subset.imma')
    return mdf_reader.read(data_file_path, data_model = schema)

def imma1_buoys_supp():
    schema = 'imma1'
    schema_supp = 'cisdm_dbo_imma1'
    data_file_path = os.path.join(data_path,'meds_2010-07_subset.imma')
    supp_section = 'c99'
    supp_model = schema_supp
    return mdf_reader.read(data_file_path, data_model = schema, supp_section = supp_section, supp_model = supp_model )

#   FROM DATA FRAME: WITH AND WIHTOUT SUPPLEMENTAL
def td11_deck187_nosupp():
    schema = 'td11'
    deck = '187'
    data_file_path = os.path.join(data_path,'AZH1.ascii')
    TextParser = pd.read_fwf(data_file_path,widths=[100000],header=None,delimiter="\t")
    deck_data = TextParser.loc[TextParser[0].str[0:3] == deck]
    deck_data.index = range(0,len(deck_data))
    return mdf_reader.read(deck_data,data_model = schema)

def td11_deck187_supp():
    schema = 'td11'
    schema_supp = 'deck187_td11'
    deck = '187'
    data_file_path = os.path.join(data_path,'AZH1.ascii')
    TextParser = pd.read_fwf(data_file_path,widths=[100000],header=None,delimiter="\t")
    deck_data = TextParser.loc[TextParser[0].str[0:3] == deck]
    deck_data.index = range(0,len(deck_data))
    supp_section = 'supplemental'
    supp_model = schema_supp
    return mdf_reader.read(deck_data,data_model = schema,supp_section = supp_section, supp_model = supp_model )

# B. TESTS TO ASSESS CHUNKING
# -----------------------------------------------------------------------------
# FROM FILE: WITH AND WITHOUT SUPPLEMENTAL
def read_imma1_buoys_nosupp_chunks():
    data_model = 'imma1'
    chunksize = 10000
    data_file_path = os.path.join(data_path,'meds_2010-07_subset.imma')
    return mdf_reader.read(data_file_path, data_model = data_model, chunksize = chunksize)

def read_imma1_buoys_supp_chunks():
    data_file_path = os.path.join(data_path,'meds_2010-07_subset.imma')
    chunksize = 10000
    data_model = 'imma1'
    supp_section = 'c99'
    supp_model = 'cisdm_dbo_imma1'
    return mdf_reader.read(data_file_path, data_model = data_model,supp_section = supp_section, supp_model = supp_model, chunksize = chunksize)

def assess_read_from_file_supp_chunk_options():
    nosupp_nochunk = read_imma1_buoys_nosupp()
    supp_nochunk = read_imma1_buoys_supp()
    io_nosupp_chunk = read_imma1_buoys_nosupp_chunks()
    nosupp_chunk = pd.DataFrame()
    for df in io_nosupp_chunk:
        nosupp_chunk = pd.concat([nosupp_chunk,df])
    io_supp_chunk = read_imma1_buoys_supp_chunks()
    supp_chunk = pd.DataFrame()
    for df in io_supp_chunk:
        supp_chunk = pd.concat([supp_chunk,df])
    
    print('Checking differences in core data when adding supplemental data with no chunking')
    if not nosupp_nochunk.drop('c99',axis = 1,level=0).equals(supp_nochunk.drop('c99',axis = 1,level=0)):
        print('...ERROR: differences found')
    else:
        print('...OK')
     
    print('\nChecking differences in core data when adding supplemental data with chunking')
    if not nosupp_chunk.drop('c99',axis = 1,level=0).equals(supp_chunk.drop('c99',axis = 1,level=0)):
        print('...ERROR: differences found')
    else:
        print('...OK') 
    
    print('\nChecking differences in data when chunking with no supplemental')
    if not nosupp_nochunk.equals(nosupp_chunk):
        print('...ERROR: differences found')
    else:
        print('...OK')
    
    print('\nChecking differences in full data when chunking with supplemental')
    if not supp_nochunk.equals(supp_chunk):
        print('...ERROR: differences found')
    else:
        print('...OK') 
    return 

# FROM PD.IO.PARSER.TEXTREADER: WITH AND WITHOUT SUPPLEMENTAL
def read_td11_deck187_nosupp_chunks():
    data_model =  'td11'
    deck = '187'
    data_file_path = os.path.join(data_path,'AZH1.ascii')
    TextParser = pd.read_fwf(data_file_path,widths=[100000],header=None,delimiter="\t")
    deck_data = TextParser.loc[TextParser[0].str[0:3] == deck]
    deck_data.index = range(0,len(deck_data))
    output_buffer = StringIO()
    deck_data.to_csv(output_buffer,header = False, index = False)
    chunksize = 10000
    output_buffer.seek(0)
    TextParser = pd.read_fwf(output_buffer,widths=[100000],chunksize = chunksize, header = None)
    return mdf_reader.read(TextParser,data_model = data_model)

def read_td11_deck187_supp_chunks():
    data_model = 'td11'
    supp_model = 'deck187_td11'
    supp_section = 'supplemental'
    deck = '187'
    data_file_path = os.path.join(data_path,'AZH1.ascii')
    TextParser = pd.read_fwf(data_file_path,widths=[100000],header=None,delimiter="\t")
    deck_data = TextParser.loc[TextParser[0].str[0:3] == deck]
    deck_data.index = range(0,len(deck_data))
    output_buffer = StringIO()
    deck_data.to_csv(output_buffer,header = False, index = False)
    chunksize = 10000
    output_buffer.seek(0)
    TextParser = pd.read_fwf(output_buffer,widths=[100000],chunksize = chunksize, header = None)
    return mdf_reader.read(TextParser,data_model = data_model ,supp_section = supp_section, supp_model = supp_model)

# C. TESTS TO READ DATA MODEL SCHEMA FROM EXTERNAL SOURCE
# -----------------------------------------------------------------------------
def read_imma1_buoys_supp_external_models():
    data_file_path = os.path.join(data_path,'meds_2010-07_subset.imma')
    schema = 'imma1'
    schema_supp = 'cisdm_dbo_imma1'
    data_model_path = os.path.join(schema_lib,schema)
    supp_section = 'c99'
    supp_model_path = os.path.join(schema_lib,schema_supp)
    return mdf_reader.read(data_file_path, data_model_path = data_model_path,supp_section = supp_section, supp_model_path = supp_model_path)


# D. CHECK DATA SOURCES -------------------------------------------------------
def check_data_sources():
    data_file_path = os.path.join(data_path,'meds_2010-07_subset.imma')
    data_ioStringIO = StringIO()
    data_model = 'imma1'
    with open(data_file_path,'r') as fileO:
        data_ioStringIO.writelines(fileO.readlines())
    data_ioStringIO.seek(0)
    data_pandas_df = pd.read_fwf(data_file_path,widths=[100000],header=None,delimiter="\t")
    data_pandas_tfr = pd.read_fwf(data_file_path,widths=[100000],header=None,delimiter="\t", chunksize = 1000)
    
    sources = {'data_file_path': data_file_path, 'data_ioStringIO': data_ioStringIO,
               'data_pandas_df': data_pandas_df, 'data_pandas_tfr': data_pandas_tfr}
    
    for source in sources.keys():
        print('Reading from source {} ....'.format(source))
        try:
            data = mdf_reader.read(sources.get(source), data_model = data_model, sections = ['core'])
            if source == 'data_pandas_tfr':
                data_c = data.get_chunk()
                print(data_c['core']['SST'][0])
            else:
                print(data['core']['SST'][0]) 
            print('.....OK')
        except Exception as e:
            print('ERROR: {}'.format(e))
    
        
        
  
