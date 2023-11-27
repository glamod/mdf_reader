.. mdf_reader documentation master file, created by
   sphinx-quickstart on Fri Apr 16 14:18:24 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root ``toctree`` directive.

Tool set up
===========

The **mdf_reader** is a pure Python package, but it has a few dependencies that rely in a specific python and module version.

From source
~~~~~~~~~~~

The source for the **mdf_reader** can be downloaded from the `GitHub repository`_ via git_.

You can either clone the public repository:
 
.. code-block:: console

    git clone https://github.com/glamod/mdf_reader
    
or download th tarball_:

.. code-block:: console

   curl -OJL https://github.com/glamod/mdf_reader/tarball/master   
   
Once you have a copy of the source, you caninstall it with pip_:

.. code-block:: console

   pi install -e .
   
Stable release (not possible yet)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To install the **mdf_reader** in your current conda_ environment, run this command in your terminal:

.. code-block:: console

  pip install mdf_reader
  
In the future, this will be the preferred method to install **mdf_reader**, as it will always install the moste recent stable release.   

    
.. _git: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

.. _Github repository: https://github.com/glamod/mdf_reader

.. _tarball: https://github.com/glamod/mdf_reader/tarball/master

.. _pip: https://pypi.org/

.. _conda: https://docs.conda.io/en/latest/