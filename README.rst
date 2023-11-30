================================================
Model Data Format reader: ``mdf_reader`` toolbox
================================================

+----------------------------+-----------------------------------------------------+
| Versions                   | |pypi| |versions|                                   |
+----------------------------+-----------------------------------------------------+
| Documentation and Support  | |docs|                                              |
+----------------------------+-----------------------------------------------------+
| Open Source                | |license| |fair| |zenodo|                           |
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


.. _python: https://www.python.org
.. _data model: https://cds.climate.copernicus.eu/toolbox/doc/how-to/15_how_to_understand_the_common_data_model/15_how_to_understand_the_common_data_model.html
.. _IMMA: https://icoads.noaa.gov/e-doc/imma/R3.0-imma1.pdf
.. _jupyter notebooks: https://git.noc.ac.uk/brecinosrivas/mdf_reader/-/tree/master/docs/notebooks

.. |pypi| image:: https://img.shields.io/pypi/v/mdf_reader.svg
        :target: https://pypi.python.org/pypi/mdf_reader
        :alt: Python Package Index Build

.. |versions| image:: https://img.shields.io/pypi/pyversions/mdf_reader.svg
        :target: https://pypi.python.org/pypi/mdf_reader
        :alt: Supported Python Versions

.. |docs| image:: https://readthedocs.org/projects/mdf_reader/badge/?version=latest
        :target: https://mdf-reader.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status

.. |license| image:: https://img.shields.io/github/license/glamod/mdf_reader.svg
        :target: https://github.com/glamod/mdf_reader/blob/master/LICENSE
        :alt: License

.. |fair| image::
        :target:
        :alt: FAIR Software Compliance

.. |zenodo| image::
        :target:
 	      :alt: DOI

.. |black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
        :target: https://github.com/psf/black
        :alt: Python Black

.. |ruff| image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
        :target: https://github.com/astral-sh/ruff
        :alt: Ruff

.. |pre-commit| image:: https://results.pre-commit.ci/badge/github/glamod/mdf_reader/master.svg
        :target: https://results.pre-commit.ci/latest/github/glamod/mdf_reader/master
        :alt: pre-commit.ci status

.. |fossa| image:: https://app.fossa.com/api/projects/git%2Bgithub.com%2Fglamod%2Fmdf_reader.svg?type=shield
        :target: https://app.fossa.com/projects/git%2Bgithub.com%2Fglamod%2Fmdf_reader?ref=badge_shield
        :alt: FOSSA

.. |status| image:: https://www.repostatus.org/badges/latest/active.svg
        :target: https://www.repostatus.org/#active
        :alt: Project Status: Active – The project has reached a stable, usable state and is being actively developed.

.. |build| image:: https://github.com/glamod/mdf_reader/actions/workflows/ci.yml/badge.svg
        :target: https://github.com/glamod/mdf_reader/actions/workflows/ci.yml
        :alt: Build Status

.. |coveralls| image:: https://codecov.io/gh/glamod/mdf_reader/branch/master/graph/badge.svg
	      :target: https://codecov.io/gh/glamod/mdf_reader
	      :alt: Coveralls

.. |funding| image:: https://img.shields.io/badge/Powered%20by-Copernicus-blue.svg
        :target: https://climate.copernicus.eu/
        :alt: Funding
