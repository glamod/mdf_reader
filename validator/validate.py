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

DEV notes:
need to add tolerance to the numeric range validation

@author: iregon
"""

import os
import pandas as pd
import numpy as np
import logging
from .. import properties
from ..data_models import code_tables
from ..data_models import schemas

def validate_numeric(elements,data,schema):
    # Find thresholds in schema. Flag if not available -> warn
    mask = pd.DataFrame(index = data.index, data = False, columns = elements)
    lower = { x:schema.get(x).get('valid_min', -np.inf) for x in elements }
    upper = { x:schema.get(x).get('valid_max', np.inf) for x in elements }

    set_elements = [ x for x in lower.keys() if lower.get(x) != -np.inf and upper.get(x) != np.inf ]
    if len([ x for x in elements if x not in set_elements ]) > 0:
        logging.warning('Data numeric elements with missing upper or lower threshold: {}'.format(",".join([ str(x) for x in elements if x not in set_elements ])))
        logging.warning('Corresponding upper and/or lower bounds set to +/-inf for validation')

    mask[elements] = ((data[elements] >= [ lower.get(x) for x in elements ] ) & (data[elements] <= [ upper.get(x) for x in elements ])) | data[elements].isna()
    return mask

def validate_codes(elements, data, code_tables_path, schema, supp = False):

    mask = pd.DataFrame(index = data.index, data = False, columns = elements)
    
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
                        validation_df = data[key_elements]
                        imask = pd.Series(index = data.index, data =True)
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


def validate(data, mask0, schema, code_tables_path):
    logging.basicConfig(format='%(levelname)s\t[%(asctime)s](%(filename)s)\t%(message)s',
                    level=logging.INFO,datefmt='%Y%m%d %H:%M:%S',filename=None)

    # Check input
    if not isinstance(data,pd.DataFrame) or not isinstance(mask0,pd.DataFrame):
        logging.error('Input data and mask must be a pandas data frame object')
        return

    # Get the data elements from the input data: might be just a subset of
    # data model and flatten the schema to get a simple and sequential list
    # of elements included in the input data
    elements = [ x for x in data ]
    element_atts = schemas.df_schema(elements, schema)
    # See what elements we need to validate
    numeric_elements =  [ x for x in elements if element_atts.get(x).get('column_type') in properties.numeric_types ]
    datetime_elements = [ x for x in elements if element_atts.get(x).get('column_type') == 'datetime' ]
    coded_elements =    [ x for x in elements if element_atts.get(x).get('column_type') == 'key' ]

    if any([isinstance(x,tuple) for x in numeric_elements + datetime_elements + coded_elements ]):
        validated_columns = pd.MultiIndex.from_tuples(list(set(numeric_elements + coded_elements + datetime_elements)))
    else:
        validated_columns = list(set(numeric_elements + coded_elements + datetime_elements))

    mask = pd.DataFrame(index = data.index, columns = data.columns)

    # Validate elements by dtype:
    # 1. Numeric elements
    mask[numeric_elements] = validate_numeric(numeric_elements, data, element_atts)

    # 2. Table coded elements
    # See following: in multiple keys code tables, the non parameter element,
    # won't have a code_table attribute in the element_atts:
    # So we need to check the code_table.keys files in addition to the element_atts
    # Additionally, a YEAR key can fail in one table, but be compliant with anbother, then, how would we mask this?
    #               also, a YEAR defined as an integer, will undergo its own check.....
    # So I think we need to check nested keys as a whole, and mask only the actual parameterized element:
    # Get the full list of keys combinations (tuples, triplets...) and check the column combination against that: if it fails, mark the element!
    # Need to see how to grab the YEAR part of a datetime when YEAR comes from a datetime element
    # pd.DatetimeIndex(df['_datetime']).year
    if len(coded_elements)> 0:
        mask[coded_elements] = validate_codes(coded_elements, data, code_tables_path, element_atts)

    # 3. Datetime elements
    # Those declared as such in element_atts
    # Because of the way they are converted, read into datetime,
    # they should already be NaT if they not validate as a valid datetime;
    # let's check: hurray! they are!
    mask[datetime_elements] = data[datetime_elements].notna()

    mask[validated_columns] = mask[validated_columns].mask(mask0[validated_columns] == False, False)

    return mask
