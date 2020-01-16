#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 09:38:17 2019

Reads source data from a data model with fixed width fields to a pandas
DataFrame.

Uses the data model layout to first find sections in the data and internally
store the data in sections, then reads in, decodes and converts the elements on
a section by section basis and finally merges that together in the output
dataframe.

Internally works assuming highest complexity in the input data model:
multiple non sequential sections

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
from copy import deepcopy
import logging
import csv # To disable quoting
import mdf_reader.properties as properties
import mdf_reader.common.functions as functions
from mdf_reader.common.converters import converters
from mdf_reader.common.decoders import decoders

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False
    from io import BytesIO as BytesIO


FULL_WIDTH = 100000

#   ---------------------------------------------------------------------------
#   FUNCTIONS TO PERFORM INITIAL SEPARATION OF SECTIONS: MAIN IS GET_SECTIONS()
#   ---------------------------------------------------------------------------
def extract_data():
    section_len = section_lens.get(threads[thread_id]['section'])
    if section_len:
        threads[thread_id]['data'] = pd.DataFrame(threads[thread_id]['parent_data'][0].str[0:section_len]) # object consistency needed here
        threads[thread_id]['modulo'] = pd.DataFrame(threads[thread_id]['parent_data'][0].str[section_len:]) # object consistency needed here
    else:
        threads[thread_id]['data'] = pd.DataFrame(threads[thread_id]['parent_data'][0].str[0:]) #threads[thread_id]['parent_data'].copy()
        # Could even be like with section_len (None in section_len will read to the end)
        threads[thread_id]['modulo'] = pd.DataFrame(columns = [0]) # Just for consistency
    del threads[thread_id]['parent_data']

def add_next_children():
    global children_parsing_order, branch_parsing_order, children_group_type, children_group_number
    children_parsing_order = deepcopy(threads[thread_id]['parsing_order'])
    branch_parsing_order = deepcopy(threads[thread_id]['parsing_order'])
    children_group_type = list(children_parsing_order[0])[0]
    children_group_number = threads[thread_id]['children_group_number']
    threads[thread_id]['children_no'] = 0
    threads[thread_id]['children'] = []
    add_children()

def add_higher_group_children():
    global children_parsing_order, branch_parsing_order, children_group_type, children_group_number
    children_parsing_order = deepcopy(threads[thread_id]['parsing_order'])
    children_parsing_order.pop(0) # Move to next group of sections
    if len(children_parsing_order) > 0:
        branch_parsing_order = deepcopy(threads[thread_id]['parsing_order'])
        branch_parsing_order.pop(0)
        children_group_type = list(children_parsing_order[0])[0]
        children_group_number = threads[thread_id]['children_group_number'] + 1
        add_children()

def add_children():
    if children_group_type == 's':
        add_static_children()
    else:
        add_dynamic_children()

def add_static_children():
    threads[thread_id]['children_no'] += 1
    children_thread_id = str(children_group_number) + str(0) + thread_id
    threads[thread_id]['children'].append(children_thread_id)
    # Now build children's thread
    children_section = children_parsing_order[0][children_group_type].pop(0)
    grandchildren_group_number = children_group_number
    if len(children_parsing_order[0][children_group_type]) == 0:
        children_parsing_order.pop(0)
        if len(children_parsing_order) > 0:
            grandchildren_group_number += 1
        else:
            grandchildren_group_number = None
    threads[children_thread_id] = {'parsing_order':children_parsing_order}
    threads[children_thread_id]['group_number'] = children_group_number
    threads[children_thread_id]['group_type'] = children_group_type
    threads[children_thread_id]['section'] = children_section
    threads[children_thread_id]['parent_data'] = threads[thread_id]['modulo']
    threads[thread_id]['modulo'].iloc[0:0] # Remove reports from modulo
    threads[children_thread_id]['children_group_number'] = grandchildren_group_number

def add_dynamic_children():
    for i in range(0,len(children_parsing_order[0][children_group_type])):
        branch_i_parsing_order = deepcopy(branch_parsing_order)
        children_thread_id = str(children_group_number) + str(i+1) + thread_id
        # Now build children's thread
        children_section = children_parsing_order[0][children_group_type].pop(0)
        children_idx = threads[thread_id]['modulo'].loc[threads[thread_id]['modulo'][0].str[0:sentinals_lens.get(children_section)] == sentinals.get(children_section)].index
        if len(children_idx) == 0:
            continue
        threads[thread_id]['children'].append(children_thread_id)
        threads[thread_id]['children_no'] += 1
        branch_i_parsing_order[0][children_group_type].remove(children_section)
        grandchildren_group_number = children_group_number
        if len(branch_i_parsing_order[0][children_group_type]) == 0 or children_group_type == 'e':
            branch_i_parsing_order.pop(0)
            if len(children_parsing_order) > 0:
                grandchildren_group_number += 1
            else:
                grandchildren_group_number = None
        threads[children_thread_id] = {'parsing_order':branch_i_parsing_order}
        threads[children_thread_id]['group_number'] = children_group_number
        threads[children_thread_id]['group_type'] = children_group_type
        threads[children_thread_id]['section'] = children_section
        threads[children_thread_id]['parent_data'] = threads[thread_id]['modulo'].loc[children_idx]
        threads[thread_id]['modulo'].drop(children_idx,inplace = True)
        threads[children_thread_id]['children_group_number'] = grandchildren_group_number
    if (len(threads[thread_id]['modulo'])) > 0:
        add_higher_group_children()

def get_sections(df_in):
    # threads elements:
    #    'parsing_order'            What needs to be applied to current parent data
    #    'group_number'             Order in the global parsing order
    #    'group_type'               Is it sequential, exclusive or optional
    #    'section'                  Section name to be extracted from parent_data to data
    #    'parent_data'              Inital data from which section must be extracted
    #    'data'                     Section data extracted from parent_data
    #    'modulo'                   Reminder of parent_data after extracting section (data)
    #    'children_no'              Number of children threads to build, based on next parsing order list element. Resets to number of active children
    #    'children'                 Thread id for every child
    #    'children_group_number'    Group number (in the global parsing order, of the children)
    global sentinals, section_lens, sentinal_lens, parsing_order
    global children_group_type
    global threads
    global thread_id
    global group_type

    # Initial "node': input data
    threads = dict()
    thread_id = '00'
    threads_queue = [thread_id]
    threads[thread_id] = {'parsing_order':parsing_order}
    threads[thread_id]['group_number'] = 0
    threads[thread_id]['group_type'] = None
    threads[thread_id]['section'] = None
    threads[thread_id]['parent_data'] = df_in
    threads[thread_id]['data'] = None
    threads[thread_id]['modulo'] = threads[thread_id]['parent_data']
    del threads[thread_id]['parent_data']
    threads[thread_id]['children_group_number'] = 1
    add_next_children()
    threads_queue.extend(threads[thread_id]['children'])
    threads_queue.remove(thread_id)
    # And now, once initialized, let it grow:
    logging.info('Processing section partitioning threads')
    while threads_queue:
        thread_id = threads_queue[0]
        logging.info('{} ...'.format(thread_id))
        group_type = threads[thread_id]['group_type']
        # get section data
        extract_data()
        # kill thread if nothing there
        if len(threads[thread_id]['data']) == 0:
            del threads[thread_id]
            logging.info('{} deleted: no data'.format(thread_id))
            threads_queue.pop(0)
            continue
        # build children threads
        if len(threads[thread_id]['parsing_order']) > 0  and len(threads[thread_id]['modulo']) > 0:
            add_next_children()
            threads_queue.extend(threads[thread_id]['children'])
            #del threads[thread_id]['modulo'] # not until we control what to do whit leftovers....
        threads_queue.pop(0)
        logging.info('done')
    section_dict = dict()
    section_groups = [ d[x] for d in parsing_order for x in d.keys() ]
    sections = [item for sublist in section_groups for item in sublist]
    for section in sections:
        section_dict[section] = pd.DataFrame() # Index as initial size to help final merging
        thread_ids = [ x for x in threads.keys() if threads[x]['section'] == section ]
        for thread_id in thread_ids:
            section_dict[section] = section_dict[section].append(threads[thread_id]['data'],ignore_index=False,sort=True)
        section_dict[section].sort_index(inplace=True)
    return section_dict

#   ---------------------------------------------------------------------------
#   MAIN FUNCTIONS
#   ---------------------------------------------------------------------------
def source_to_df(TextParser, schema, read_sections, idx_offset = 0):
    global sentinals, section_lens, sentinals_lens
    global parsing_order

    section_lens = { section: schema['sections'][section]['header'].get('length') for section in schema['sections'].keys()}
    sentinals = { section: schema['sections'][section]['header'].get('sentinal') for section in schema['sections'].keys()}
    sentinals_lens = { section: schema['sections'][section]['header'].get('sentinal_length') for section in schema['sections'].keys()}
    parsing_order = schema['header']['parsing_order']
    chunk_len = TextParser.orig_options['chunksize'] if isinstance(TextParser,pd.io.parsers.TextFileReader) else 0
    multiindex = True if len(read_sections) > 1 or read_sections[0] != properties.dummy_level else False
    out_dtypes = dict()
    if multiindex:
        for section in read_sections:
            out_dtypes.update({ (section,i):properties.pandas_dtypes.get(schema['sections'][section]['elements'][i].get('column_type')) for i in schema['sections'][section]['elements'].keys() } )
    else:
        for section in read_sections:
            out_dtypes.update({ i:properties.pandas_dtypes.get(schema['sections'][section]['elements'][i].get('column_type')) for i in schema['sections'][section]['elements'].keys() } )
    I_CHUNK = 0
    output_buffer = StringIO() if py3 else BytesIO()
    valid_buffer = StringIO() if py3 else BytesIO()
    for df in TextParser: # Indices here are kept are those according to the full record
        logging.info('Processing chunk {}'.format(I_CHUNK))
        # 1. Remove delimiter from mixed type (like meds-buoys) if exists
        if schema['header'].get('delimiter'):
            df.loc[:,0] = df[0].str.replace(',','')
        # 2. Separate sections
        logging.info('Accessing sections'.format(I_CHUNK))
        section_strings = get_sections(df)
        # 3. Read section elements
        # Look below, if names are passed to read_csv as tuples, it creates automatically a multiindex. Initially I did not want this,  wanted tuples
        # but they do not seem to be recommended:https://github.com/pandas-dev/pandas/issues/11799
        # The mapping (like in CDM or others), would be a bit harder if using multiindex...or will get the tuples equally fine
        # 3.0. Prepare df to paste all the sections of current chunk and get initially defined dtypes
        df_out = pd.DataFrame()
        valid_out = pd.DataFrame()
        # We have to cat the data, and also the names and properties to write this to the buffer
        logging.info('Reading and coverting section elements')
        # 3.1. Loop through sections
        for section in read_sections:
            logging.info('{} ...'.format(section))
            section_buffer = StringIO() if py3 else BytesIO()
            # Writing options from quoting on to prevent supp buoy data to be quoted:
            # maybe this happenned because buoy data has commas, and pandas makes its own decission about
            # how to write that.....
            #https://stackoverflow.com/questions/21147058/pandas-to-csv-output-quoting-issue
            section_strings[section].to_csv(section_buffer,header=False, encoding = 'utf-8',index = False,quoting=csv.QUOTE_NONE,escapechar="\\",sep="\t") # Here indices are lost, have to give the real ones, those in section_strings
            shut_up = section_buffer.seek(0)
            # 3.1.1. Read section elements from schema and read from buffer with pandas as objects
            section_names = schema['sections'][section]['elements'].keys()
            ignore = [ i for i in section_names if schema['sections'][section]['elements'][i].get('ignore') ] # evals to True if set and true, evals to False if not set or set and false
            section_widths = list(map(lambda x: x if x else FULL_WIDTH, [ schema['sections'][section]['elements'][i].get('field_length') for i in section_names ]))
            section_dtypes = { i:'object' for i in section_names }
            section_missing = { i:schema['sections'][section]['elements'][i].get('missing_value') if schema['sections'][section]['elements'][i].get('disable_white_strip') == True
                               else [schema['sections'][section]['elements'][i].get('missing_value')," "*schema['sections'][section]['elements'][i].get('field_length', FULL_WIDTH)]
                               for i in section_names }
            section_elements = pd.read_fwf(section_buffer, widths = section_widths, header = None, names = section_names , na_values = section_missing, delimiter="\t", encoding = 'utf-8', dtype = section_dtypes, skip_blank_lines = False )
            section_valid = pd.DataFrame(index = section_elements.index, columns = section_elements.columns)
            # 3.1.2. Decode, scale|offset and convert to dtype (ints will be floats if NaNs)
            section_dtypes = { i:schema['sections'][section]['elements'][i]['column_type'] for i in section_names }
            encoded = [ (x) for x in section_names if 'encoding' in schema['sections'][section]['elements'][x]]
            section_encoding = { i:schema['sections'][section]['elements'][i]['encoding'] for i in encoded }

            for element in section_dtypes.keys():
                missing = section_elements[element].isna()
                if element in encoded:
                    section_elements[element] = decoders.get(section_encoding.get(element)).get(section_dtypes.get(element))(section_elements[element])

                kwargs = { converter_arg:schema['sections'][section]['elements'][element].get(converter_arg) for converter_arg in properties.data_type_conversion_args.get(section_dtypes.get(element))  }
                section_elements[element] = converters.get(section_dtypes.get(element))(section_elements[element], **kwargs)

                section_valid[element] = missing | section_elements[element].notna()

            # 3.1.3. Format section:
            #   - Put data in its rightfull place of the original data (indexing!) and remove section elements not desired
            #   - Name columns: tuples (section, element_name) for multisection, element_name if one section
            #   tuples will turn into a  multiindex in the df_reader below
            section_elements.index = section_strings[section].index
            section_valid.index = section_strings[section].index
            section_frame = pd.DataFrame(data = { x:pd.Series(index=range(idx_offset + I_CHUNK*chunk_len, idx_offset + len(df) + I_CHUNK*chunk_len )) for x in section_names})
            valid_frame = section_frame.copy()
            if len(section_elements) > 0:
                section_frame.loc[section_elements.index] = section_elements
                valid_frame.loc[section_elements.index] = section_valid
            section_frame.drop(ignore, axis = 1, inplace = True)
            valid_frame.drop(ignore, axis = 1, inplace = True)
            section_frame.columns = [ (section, x) for x in section_frame.columns] if multiindex else section_frame.columns
            valid_frame.columns = section_frame.columns
            out_dtypes.update({ i:section_frame[i].dtype.name for i in section_frame if section_frame[i].dtype.name in properties.numpy_floats})
            # 3.1.4. Paste section to rest of chunk
            df_out = pd.concat([df_out,section_frame],sort = False,axis=1)
            valid_out = pd.concat([valid_out,valid_frame],sort = False,axis=1)
        # 4.3. Add _datetime section: watch this if we mean to do multiple reports in record!!!
        # for this to be valid, would have to assume that same day reports and that date in common report section....
        # If datetime is derived from within the actual report, then we would have add _datetime after expansion on dataframe posprocessing
        if schema['header'].get('date_parser'):
            date_parser = schema['header'].get('date_parser')
            date_name = ('_datetime','_datetime') if multiindex else '_datetime'
            date_elements = [(date_parser['section'],x) for x in date_parser['elements'] ] if date_parser.get('section') else date_parser.get('elements')
            out_dtypes.update({ date_name: 'object' })
            df_out = functions.df_prepend_datetime(df_out, date_elements, date_parser['format'], date_name = date_name )
            valid_out = pd.concat([pd.DataFrame(index = valid_out.index, data = True,columns = [date_name]),valid_out],sort = False,axis=1)
        # 4.4. Add chunk data to output
        header = False if I_CHUNK == 0 else False
        df_out.to_csv(output_buffer,header=header, mode = 'a', encoding = 'utf-8',index = False)
        valid_out.to_csv(valid_buffer,header=header, mode = 'a', encoding = 'utf-8',index = False)
        I_CHUNK += 1

    return output_buffer,valid_buffer,out_dtypes
