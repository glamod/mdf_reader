import os

import pytest  # noqa

import mdf_reader

from .data import test_data

# A. TESTS TO READ FROM DATA FROM DIFFERENT DATA MODELS WITH AND WITHOUT SUPP
# -----------------------------------------------------------------------------

def test_read_imma1_buoys_nosupp(plot_validation=False):
    output = mdf_reader.read(**test_data.test_063_714)
    if plot_validation:
        mdf_reader.plot_model_validation(output)
    assert output


def test_read_imma1_buoys_supp(plot_validation=False):
    supp_section = "c99"
    # supp_model = "cisdm_dbo_imma1"
    output = mdf_reader.read(
        **test_data.test_063_714,
        sections=[
            supp_section,
        ],
    )
    if plot_validation:
        mdf_reader.plot_model_validation(output)
    assert output


# B. TESTS TO TEST CHUNKING
# -----------------------------------------------------------------------------
# FROM FILE: WITH AND WITHOUT SUPPLEMENTAL
def test_read_imma1_buoys_nosupp_chunks():
    chunksize = 10000
    assert mdf_reader.read(
        **test_data.test_063_714,
        chunksize=chunksize,
    )


def test_read_imma1_buoys_supp_chunks():
    chunksize = 10000
    supp_section = "c99"
    # supp_model = "cisdm_dbo_imma1"
    assert mdf_reader.read(
        **test_data.test_063_714,
        sections=[supp_section],
        chunksize=chunksize,
    )

test_read_imma1_buoys_nosupp_()