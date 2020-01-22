#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 13 15:14:51 2018


Read data file format json schema to dictionary

Add schema validation:
    - check mandatory are not null
    - check fixed options
..and return None if it does not validate

"""


import os
import sys
import json
import logging
import shutil
from copy import deepcopy
import glob

from .. import properties

if sys.version_info[0] >= 3:
    py3 = True
else:
    py3 = False


toolPath = os.path.dirname(os.path.abspath(__file__))
schema_lib = os.path.join(toolPath,'lib')
templates_path = os.path.join(schema_lib,'templates','schemas')

def templates():
    schemas = glob.glob(os.path.join(templates_path,'*.json'))
    return [ os.path.basename(x).split(".")[0] for x in schemas ]

def copy_template(schema, out_dir = None,out_path = None):
    schemas = templates()
    if schema in schemas:
        schema_path = os.path.join(templates_path,schema + '.json')
        schema_out = out_path if out_path else os.path.join(out_dir,schema + '.json')
        shutil.copyfile(schema_path,  schema_out)
        if os.path.isfile( schema_out):
            print('Schema template {0} copied to {1}'.format(schema, schema_out))
            return
        else:
            print('copy_template ERROR:')
            print('\tError copying schema template {0} copied to {1}'.format(schema, schema_out))
            return
    else:
        print('copy_template ERROR:')
        print('\tRequested template {} must be a valid name.'.format(schema))
        print('\tValid names are: {}'.format(", ".join(schemas)))
        return

def read_schema(schema_name = None, ext_schema_path = None):

    if schema_name:
        if schema_name not in properties.supported_file_formats:
            print('ERROR: \n\tInput schema "{}" not supported. See mdf_reader.properties.supported_file_formats for supported file format schemas'.format(schema_name))
            return
        else:
            schema_path = os.path.join(schema_lib,schema_name)
    else:
        schema_path = os.path.abspath(ext_schema_path)
        schema_name = os.path.basename(schema_path)
        
    schema_file = os.path.join(schema_path, schema_name + '.json')
    if not os.path.isfile(schema_file):
        logging.error('Can\'t find input schema file {}'.format(schema_file))
        return
    with open(schema_file) as fileObj:
        schema = json.load(fileObj)
    #   ---------------------------------------------------------------------------
    #   FILL IN THE INITIAL SCHEMA TO "FULL COMPLEXITY"
    #   EXPLICITY ADD INFO THAT IS IMPLICIT TO GIVEN SITUATIONS/SUBFORMATS
    #   ---------------------------------------------------------------------------
    # One report per record: make sure later changes are reflected in MULTIPLE
    # REPORTS PER RECORD case below if we ever use it!
    if not schema['header'].get('multiple_reports_per_line'):
        # Make no section formats be 1 section format
        if not schema.get('sections'):
            schema['sections'] = {properties.dummy_level:{'header':{},'elements':schema.get('elements')}}
            schema['header']['parsing_order'] = [{'s':[properties.dummy_level]}]
            schema.pop('elements',None)
            schema['sections'][properties.dummy_level]['header']['delimiter'] = schema['header'].get('delimiter')
            schema['header'].pop('delimiter',None)
            schema['sections'][properties.dummy_level]['header']['field_layout'] = schema['header'].get('field_layout')
            schema['header'].pop('field_layout',None)
        # Make parsing order explicit
        if not schema['header'].get('parsing_order'):# assume sequential
            schema['header']['parsing_order'] = [{'s':list(schema['sections'].keys())}]
        # Make disable_read and field_layout explicit: this is ruled by delimiter or length being set,
        # unless explicitly set
        for section in schema['sections'].keys():
            if schema['sections'][section]['header'].get('disable_read'):
                continue
            else:
                schema['sections'][section]['header']['disable_read'] = False
            if not schema['sections'][section]['header'].get('field_layout'):
                delimiter = schema['sections'][section]['header'].get('delimiter')
                schema['sections'][section]['header']['field_layout'] = 'delimited' if delimiter else 'fixed_width'   
        return schema
    else:
        # 1X: MULTIPLE REPORTS PER RECORD
        # !!!! NEED TO ADD SECTION LENS TO THE REPORT'S SECTION'S HEADER!!!
        # CAN INFER FROM ELEMENTS LENGHT AND ADD, OR MAKE AS REQUIREMENT TO BE GIVEN
        # global name_report_section
        # Have to assess how the section splitting works when x sequential
        # sections are declared, and only x-y are met.
        if not schema['header'].get('reports_per_line'):
            schema['header']['reports_per_line'] = 24
        if not schema.get('sections'):
            schema['sections'] = dict()
            schema['header']['parsing_order'] = [{'s':[]}]
            for i in range(1,schema['header']['reports_per_line'] + 1):
                schema['sections'].update({str(i):{'header':{},'elements':deepcopy(schema.get('elements'))}})
        else:
            name_report_section = list(schema['sections'].keys())[-1]
            schema['header']['name_report_section'] == name_report_section
            schema['header']['parsing_order'] = [{'s':list(schema['sections'].keys())[:-1]}]
            for i in range(1,schema['header']['reports_per_line'] + 1):
                schema['sections'].update({str(i):schema['sections'].get(name_report_section)})
            schema['sections'].pop(name_report_section,None)
        for i in range(1,schema['header']['reports_per_line'] + 1):
            schema['header']['parsing_order'][0]['s'].append(str(i))
        return schema

def df_schema(df_columns, schema):
    def clean_schema(columns,schema):
        # Could optionally add cleaning of element descriptors that only apply
        # to the initial reading of the data model: field_length, etc....
        for element in list(schema):
            if element not in columns:
                schema.pop(element)
        return

    flat_schema = dict()
    # Flatten main model schema
    for section in schema.get('sections'):
        if section == properties.dummy_level:
            flat_schema.update(schema['sections'].get(section).get('elements'))
        elif schema['sections'].get(section).get('header').get('disable_read'):
            flat_schema.update( { (section, section): {'column_type':'object'} })    
        else:
            flat_schema.update( { (section, x): schema['sections'].get(section).get('elements').get(x) for x in schema['sections'].get(section).get('elements') })

    clean_schema(df_columns, flat_schema)


    return flat_schema
