#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 13:17:43 2020

@author: iregon
"""

import pandas as pd
from io import StringIO as StringIO
import mdf_reader.properties as properties
import csv # To disable quoting
from mdf_reader.common.converters import converters
from mdf_reader.common.decoders import decoders

def extract_fixed_width(section_serie_bf,section_schema):
    # Read section elements descriptors
    section_names = section_schema['elements'].keys()
    section_widths = list(map(lambda x: x if x else properties.MAX_FULL_REPORT_WIDTH, [ section_schema['elements'][i].get('field_length') for i in section_names ]))
    section_missing = { i:section_schema['elements'][i].get('missing_value') if section_schema['elements'][i].get('disable_white_strip') == True
                               else [section_schema['elements'][i].get('missing_value')," "*section_schema['elements'][i].get('field_length', properties.MAX_FULL_REPORT_WIDTH)]
                               for i in section_names }
    section_elements = pd.read_fwf(section_serie_bf, widths = section_widths, header = None, names = section_names , na_values = section_missing, delimiter="\t", encoding = 'utf-8', dtype = 'object', skip_blank_lines = False )
    return section_elements

def extract_delimited(section_serie_bf,section_schema): 
    delimiter = section_schema['header'].get('delimiter')
    section_names = section_schema['elements'].keys()
    section_missing = { x:section_schema['elements'][x].get('missing_value') for x in section_names }
    section_elements = pd.read_csv(section_serie_bf,header = None, delimiter = delimiter, encoding = 'utf-8',
                                 dtype = 'object', skip_blank_lines = False,
                                 names = section_names, na_values = section_missing)
    
    return section_elements

def read_data(section_df,section_schema): 
    section_names = section_df.columns
    section_dtypes = { i:section_schema['elements'][i]['column_type'] for i in section_names }
    encoded = [ (x) for x in section_names if 'encoding' in section_schema['elements'][x]]
    section_encoding = { i:section_schema['elements'][i]['encoding'] for i in encoded }
    
    for element in section_dtypes.keys():
        #missing = section_elements[element].isna()
        if element in encoded:
            section_df[element] = decoders.get(section_encoding.get(element)).get(section_dtypes.get(element))(section_df[element])

        kwargs = { converter_arg:section_schema['elements'][element].get(converter_arg) for converter_arg in properties.data_type_conversion_args.get(section_dtypes.get(element))  }
        section_df[element] = converters.get(section_dtypes.get(element))(section_df[element], **kwargs)

#        section_valid[element] = missing | section_elements[element].notna()
                
    return section_df

def read_sections(sections_df, schema):
    
    multiindex = True if len(sections_df.columns) > 1 or sections_df.columns[0] != properties.dummy_level else False
    data_df = pd.DataFrame()
    
    out_dtypes = dict()
    
    for section in sections_df.columns: 
        print('Reading section {}'.format(section))
        section_schema = schema['sections'].get(section)
        disable_read = section_schema.get('header').get('disable_read')
        
        if not disable_read:     
            field_layout = section_schema.get('header').get('field_layout')
            ignore = [ i for i in section_schema['elements'].keys() if section_schema['elements'][i].get('ignore') ] # evals to True if set and true, evals to False if not set or set and false
             # Get rid of false delimiters in fixed_width
            delimiter = section_schema['header'].get('delimiter')
            if delimiter and field_layout == 'fixed_width':
                sections_df[section] = sections_df[section].str.replace(delimiter,'')
        
            section_buffer = StringIO()
            # Writing options from quoting on to prevent supp buoy data to be quoted:
            # maybe this happenned because buoy data has commas, and pandas makes its own decission about
            # how to write that.....
            #https://stackoverflow.com/questions/21147058/pandas-to-csv-output-quoting-issue
            # quoting=csv.QUOTE_NONE was failing when a section is empty (or just one record in a section,...)
            # Here indices are lost, have to give the real ones, those in section_strings:
            # we'll see if we do that in the caller module or here....
            sections_df[section].to_csv(section_buffer,header=False, encoding = 'utf-8',index = False)#,quoting=csv.QUOTE_NONE,escapechar="\\",sep="\t") 
            ssshh = section_buffer.seek(0)
        # Get the individual elements as objects
            if field_layout == 'fixed_width':
                section_elements_obj = extract_fixed_width(section_buffer,section_schema)
            elif field_layout == 'delimited':
                section_elements_obj = extract_delimited(section_buffer,section_schema)
                
            section_elements_obj.drop(ignore, axis = 1, inplace = True)
            # Read the objects to their data types and apply decoding, scaling and so on...
            section_elements = read_data(section_elements_obj,section_schema)
            section_elements.index = sections_df[section].index
        else:
            section_elements = pd.DataFrame(sections_df[section],columns = [section])
               
        if not disable_read:
            if multiindex:
                out_dtypes.update({ (section,i):properties.pandas_dtypes.get(section_schema['elements'][i].get('column_type')) for i in section_elements.columns } )
                out_dtypes.update({ (section,i):section_elements[i].dtype.name for i in section_elements if section_elements[i].dtype.name in properties.numpy_floats})
            else:
                out_dtypes.update({ i:properties.pandas_dtypes.get(section_schema['elements'][i].get('column_type')) for i in section_elements.columns } ) 
                out_dtypes.update({ i:section_elements[i].dtype.name for i in section_elements if section_elements[i].dtype.name in properties.numpy_floats})
        else:
            if multiindex:
                    out_dtypes.update({ (section,section):'object' } )
            else:
                out_dtypes.update({ section:'object' } )        
        
        section_elements.columns = [ (section, x) for x in section_elements.columns] if multiindex else section_elements.columns
        data_df = pd.concat([data_df,section_elements],sort = False,axis=1)
           
    return data_df,out_dtypes
