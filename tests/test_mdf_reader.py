#!/usr/bin/env python3
"""
Created on Wed Feb 20 08:05:50 2019

@author: iregon
"""

import os

import mdf_reader
import mdf_reader.common.plots as plots

cwd = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(cwd, "data")


# A. TESTS TO READ FROM DATA FROM DIFFERENT DATA MODELS WITH AND WITHOUT SUPP
# -----------------------------------------------------------------------------
def test_read_imma1_buoys_nosupp(plot_validation=False):
    schema = "imma1"
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    output = mdf_reader.read(data_file_path, data_model=schema, out_path=".")
    if plot_validation:
        plots.plot_model_validation(output)
    assert output


def test_read_imma1_buoys_supp(plot_validation=False):
    schema = "imma1"
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    supp_section = "c99"
    # supp_model = "cisdm_dbo_imma1"
    output = mdf_reader.read(
        data_file_path,
        data_model=schema,
        sections=[
            supp_section,
        ],
        out_path=".",
    )
    if plot_validation:
        plots.plot_model_validation(output)
    assert output


# B. TESTS TO TEST CHUNKING
# -----------------------------------------------------------------------------
# FROM FILE: WITH AND WITHOUT SUPPLEMENTAL
def test_read_imma1_buoys_nosupp_chunks():
    data_model = "imma1"
    chunksize = 10000
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    assert mdf_reader.read(
        data_file_path, data_model=data_model, chunksize=chunksize, out_path="."
    )


def test_read_imma1_buoys_supp_chunks():
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    chunksize = 10000
    data_model = "imma1"
    supp_section = "c99"
    # supp_model = "cisdm_dbo_imma1"
    assert mdf_reader.read(
        data_file_path,
        data_model=data_model,
        sections=[supp_section],
        chunksize=chunksize,
        out_path=".",
    )
