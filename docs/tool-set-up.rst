.. mdf_reader documentation master file, created by
   sphinx-quickstart on Fri Apr 16 14:18:24 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Tool set up
===========

The mdf_reader is a pure Python package, but it has a few dependencies that rely in a specific python and module version. The tool has been tested with Python version 3.7 on Linux and Mac OS systems.

1. Clone the repository
~~~~~~~~~~~~~~~~~~~~~~~~

Clone the latest version via::

      $ git clone git@git.noc.ac.uk:brecinosrivas/mdf_reader.git

.. _git: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

2. Install a python environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For this you can use and install `pyenv <https://github.com/pyenv/pyenv>`_ and create a new virtual environment
with a the python version needed (**3.7.3**) using `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`_.

If you install pyenv and pyenv-virtualenv you can create an environment with a fix python version::

    $ pyenv install 3.7.3
    $ pyenv virtualenv 3.7.3 mdfreader_env
    $ pyenv activate mdfreader_env

As another option you can use conda. See the `conda docs <https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands>`_
for more information about how to create an environment from the command line.

Or you can do what I usually do (much faster), install `mamba <https://github.com/mamba-org/mamba>`_.

3. Install dependencies
~~~~~~~~~~~~~~~~~~~~~~~

If you used **pyenv** for your environment, once activated you can install the dependencies using `pip <https://pip.pypa.io/en/stable/>`_::

 $ pip install numpy==1.16.2 pandas==0.24.2 matplotlib==3.0.3

Check the conda or mamba documentation to install dependencies via those tools.

.. warning:: **The pandas version is particularly important since needs to be compatible with the way of importing the json module used in the code.**

4. Optional step: install jupyter notebook
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install `jupyter notebook <https://jupyter.org/install>`_ and `IPython <https://jupyter.readthedocs.io/en/latest/install.html>`_ for an easy overview of the tool and to make use of the tutorials under ``~/mdf_reader/docs/notebooks``::

    $ pip install notebook
    $ pip install ipykernel

Check the libraries documentation in the links above to install them via conda or mamba.

Add a new kernel to load your notebooks with the right environment (``mdfreader_env``) run::

    $ python -m ipykernel install --user --name=mdfreader_env
    $ jupyter notebook

When you open the notebook, make sure you select the kernel or environment with the name ``mdfreader_env``. You can also
test the notebook by adding and executing the following code in a jupyter-notebook cell::

    from platform import python_version
    import sys
    print(python_version())
    print(sys.executable)
    print(sys.version)
    print(sys.version_info)

And you should see the following information for your ``mdfreader_env``::

    /Users/username/.pyenv/versions/3.7.3/envs/mdfreader_env/bin/python
    3.7.3 (default, Feb  4 2021, 14:32:54)
    [Clang 12.0.0 (clang-1200.0.32.28)]
    sys.version_info(major=3, minor=7, micro=3, releaselevel='final', serial=0)
