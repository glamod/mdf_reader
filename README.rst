================================================
Model Data Format reader: ``mdf_reader`` toolbox
================================================

+----------------------------+-----------------------------------------------------+
| Versions                   | |pypi| |versions|                                   |
+                            +                                                     +
+                            +                                                     +
|                            | |tag| |release|                                     |
+----------------------------+-----------------------------------------------------+
| Documentation and Support  | |docs|                                              |
+----------------------------+-----------------------------------------------------+
| Open Source                | |license| |zenodo|                                  |
+----------------------------+-----------------------------------------------------+
| Coding Standards           | |black| |ruff| |pre-commit| |fossa|                 |
+----------------------------+-----------------------------------------------------+
| Development Status         | |status| |build| |coveralls|                        |
+----------------------------+-----------------------------------------------------+
| Funding                    | |funding|                                           |
+----------------------------+-----------------------------------------------------+

The ``mdf_reader`` is a python_ tool designed to read data files compliant with a user specified `data model`_.
It was developed to read the IMMA_ (International Maritime Meteorological Archive) data format, but it has been enhanced to account for meteorological data formats in the case of:

* Data that is stored in a human-readable manner: “ASCII” format.
* Data that is organized in single line reports
* Reports that have a coherent internal structure and can be modelised.
* Reports that have a fixed width or field delimited types
* Reports that can be organized in sections, in which case each section can be of different types (fixed width of delimited)

Installation
------------

You can install the package directly with pip:

.. code-block:: console

     pip install  mdf_reader

If you want to contribute, I recommend cloning the repository and installing the package in development mode, e.g.

.. code-block:: console

    git clone https://github.com/glamod/mdf_reader
    cd mdf_reader
    pip install -e .

This will install the package but you can still edit it and you don't need the package in your :code:`PYTHONPATH`


Run a test
----------

.. code-block:: console

		import mdf_reader
		import matplotlib.pyplot as plt

		data = mdf_reader.tests.read_imma1_buoys_nosupp()

		imma_data = mdf_reader.read(filepath, data_model = 'imma1',sections = ['core','c1','c98'])


For more details on how to use the ``mdf_reader`` tool see the following `jupyter notebooks`_.
