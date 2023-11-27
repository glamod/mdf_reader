.. mdf_reader documentation master file, created by
   sphinx-quickstart on Fri Apr 16 14:18:24 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root ``toctree`` directive.

.. _getting-started:

Getting started
===============

1. Read an IMMA file
~~~~~~~~~~~~~~~~~~~~

You can test the tool very easy by using a sample data set that comes with the repository. For this you need to run the following code:

.. code-block:: console

   from mdf_reader.test_data import test_069_701 as test_data

   filepath = test_data.source
   data_model = test_data.data_model

   data = mdf_reader.read(filepath, data_model=data_model)

or simplify the command by passing `test_data`:

.. code-block:: console

  data = mdf_reader.read(**test_data)

2. Read  subsection of an IMMA file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also read subsections from the IMMA test file:

.. code-block:: console

   imma_data = mdf_reader.read(filepath, data_model=data_model, sections = ["core", "c1", "c98"])


3. Run **mdf_reader** as an command-line interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also run **mdf_reader** as a command-line interface. To call the function from a terminal type:

   mdf_reader_dir <your-file-path> --data_model imma1 --out_path <yout-output-path>

For more details how to run the command-line interface please call the heklper function:

.. code-block:: console

   mdf_reader -h
