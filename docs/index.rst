.. mdf_reader documentation master file, created by
   sphinx-quickstart on Fri Apr 16 14:18:24 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Data reader toolbox documentation
---------------------------------

The **mdf_reader** is a `python3 <https://www.python.org/download/releases/3.0/>`_ tool designed to read data files compliant with a user specified data model.

It was developed with the initial idea of reading data from the `International Comprehensive Ocean-Atmosphere Data Set (ICOADS) <https://icoads.noaa.gov/>`_ stored in the `International Maritime Meteorological Archive (IMMA) data format <https://icoads.noaa.gov/e-doc/imma/R3.0-imma1.pdf>`_.

The tool has been further enhanced to account for any marine meteorological data format, provided that this data meets the following specifications:

-	Data is stored in a human-readable manner: `ASCII <https://en.wikipedia.org/wiki/ASCII>`_.
-	Data is organized in single line reports (e.g. rows of observations separated by a delimiter like .csv).
-	Reports have a coherent internal structure that can be modelized.
-	Reports are fixed width or field delimited types.
-	Reports can be organized in sections, in which case each section can be of different types (fixed width of delimited).

The mdf_reader uses the information provided in a `data model <https://en.wikipedia.org/wiki/Data>`_ to read meteorological data into a python `pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_, with the column names and data types set according to each data element’s description specified in the data model or **schema**. In addition to reading, the mdf_reader validates data elements against the **schema** provided.

This tool outputs a python object with the following attributes:

1.	A `pandas.DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_ (DF) with the data values.
2.	A `boolean pandas <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.bool.html>`_ DF with the data validation mask.
3.	A `dictionary <https://realpython.com/python-dicts/>`_ with a simplified version of the input data model.

The reader allows for basic transformations of the data. This feature includes `basic numeric data decoding <https://realpython.com/python-encodings-guide/#enter-unicode>`_ (base36, signed_overpunch) and numeric data conversion (scale and offset).

Several data models have been added to the tool including the IMMA schema: ``~/mdf_reader/data_models/lib/imma1``.

.. note:: **Data from other data models than those already available can be read, providing that this data meets the basic specifications listed above. A data model can be built externally and fed into the tool.**

.. toctree::
   :maxdepth: 2
   :glob:
   :hidden:
   :caption: Guide

   tool-set-up.rst
   tool-overview.rst
   getting-started.rst
   data-models.rst
   how-to-build-a-data-model.rst



About
-----

:Version:

:Citation:

:License:

:Authors:
   David Berry, Irene Perez Gonzalez and Beatriz Recinos


.. image:: _static/images/logos_c3s/logo_c3s-392x154.png
    :width: 25%
    :target: https://climate.copernicus.eu/
.. image:: _static/images/logos_c3s/LOGO_2020_-_NOC_1_COLOUR.png
    :width: 25%
    :target: https://noc.ac.uk/
.. image:: _static/images/logos_c3s/icoadsLogo.png
    :width: 20%
    :target: https://icoads.noaa.gov/