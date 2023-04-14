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
import mdf_reader.common.plots as plots

funPath = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(funPath,'data')
schema_lib = os.path.join(os.path.dirname(funPath),'data_models','lib')

# A. TESTS TO READ FROM DATA FROM DIFFERENT DATA MODELS WITH AND WITHOUT SUPP
# -----------------------------------------------------------------------------
def read_imma1_buoys_nosupp(plot_validation=False):
    schema = 'imma1'
    data_file_path = os.path.join(data_path,'063-714_2010-07_subset.imma')
    data = mdf_reader.read(data_file_path, data_model = schema)
    if plot_validation:
        plots.plot_model_validation(data)
    return data

def read_imma1_buoys_supp(plot_validation=False):
    schema = 'imma1'
    schema_supp = 'cisdm_dbo_imma1'
    data_file_path = os.path.join(data_path,'063-714_2010-07_subset.imma')
    supp_section = 'c99'
    supp_model = schema_supp
    data = mdf_reader.read(data_file_path, data_model = schema, supp_section = supp_section, supp_model = supp_model )
    if plot_validation:
        plots.plot_model_validation(data) 
    return data

# B. TESTS TO TEST CHUNKING
# -----------------------------------------------------------------------------
# FROM FILE: WITH AND WITHOUT SUPPLEMENTAL
def read_imma1_buoys_nosupp_chunks():
    data_model = 'imma1'
    chunksize = 10000
    data_file_path = os.path.join(data_path,'063-714_2010-07_subset.imma')
    return mdf_reader.read(data_file_path, data_model = data_model, chunksize = chunksize)

def read_imma1_buoys_supp_chunks():
    data_file_path = os.path.join(data_path,'063-714_2010-07_subset.imma')
    chunksize = 10000
    data_model = 'imma1'
    supp_section = 'c99'
    supp_model = 'cisdm_dbo_imma1'
    return mdf_reader.read(data_file_path, data_model = data_model,supp_section = supp_section, supp_model = supp_model, chunksize = chunksize)



        
        
  
