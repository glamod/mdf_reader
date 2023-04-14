.. mdf_reader documentation master file, created by
   sphinx-quickstart on Fri Apr 16 14:18:24 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _getting-started:

Getting started
===============

1. Test the tool

You can test the tool very easy by using a sample data set that comes with the repository. For this you need to run the following code::

   import sys
   sys.path.append('/path_to_folder_directory_containing_the_mdf_reader_folder/')

   import mdf_reader
   import matplotlib.pyplot as plt

   data = mdf_reader.tests.read_imma1_buoys_nosupp()

2. Read an IMMA file

Read a sample ``.imma`` file from the folder ``~/mdf_reader/test/data/`` via the following code::

   filepath = '~/mdf_reader/test/data/069-701_1845-04_subset.imma'
   imma_data = mdf_reader.read(filepath, data_model = 'imma1',sections = ['core','c1','c98'])


For more details on how to run this in your python session see :py:func:`mdf_reader.read.main()`

3. To call the function from a terminal type::

   $ python mdf_reader_dir/read.py source data_model data_model_path sections chunksize skiprows out_path

For more details and an overview of the tool check out the following python notebook:

- `Test and overview of the mdf_reader tool <https://git.noc.ac.uk/brecinosrivas/mdf_reader/-/blob/master/docs/notebooks/mdf_reader_test_overview.ipynb>`_
