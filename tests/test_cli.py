import os

cwd = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(cwd, "data")


# A. TESTS TO READ FROM DATA FROM DIFFERENT DATA MODELS WITH AND WITHOUT SUPP
# -----------------------------------------------------------------------------
def test_cli_imma1_buoys_nosupp():
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    schema = "imma1"
    s = "mdf_reader " f"{data_file_path} " f"--data_model {schema} " "--out_path ."
    os.system(s)


def test_cli_imma1_buoys_supp():
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    schema = "imma1"
    supp_section = "c99"
    s = (
        "mdf_reader "
        f"{data_file_path} "
        f"--data_model {schema} "
        f"--sections {supp_section} "
        "--out_path ."
    )
    os.system(s)


# B. TESTS TO TEST CHUNKING
# -----------------------------------------------------------------------------
# FROM FILE: WITH AND WITHOUT SUPPLEMENTAL


def test_cli_imma1_buoys_nosupp_chunks():
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    schema = "imma1"
    chunksize = 10000
    s = (
        "mdf_reader "
        f"{data_file_path} "
        f"--data_model {schema} "
        f"--chunksize {chunksize} "
        "--out_path ."
    )
    os.system(s)


def test_cli_imma1_buoys_supp_chunks():
    data_file_path = os.path.join(data_path, "063-714_2010-07_subset.imma")
    schema = "imma1"
    supp_section = "c99"
    chunksize = 10000
    s = (
        "mdf_reader "
        f"{data_file_path} "
        f"--data_model {schema} "
        f"--sections {supp_section} "
        f"--chunksize {chunksize} "
        "--out_path ."
    )
    os.system(s)
