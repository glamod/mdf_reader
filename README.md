A. Get code from GitHub

Download zip from https://github.com/perezgonzalez-irene/mdf_reader.git

Clone repo: git clone https://github.com/perezgonzalez-irene/mdf_reader.git

B. Create \____init____.py in code parent directory so python treats it as containing a package
  touch \____init____.py

C. Create python3 virtual environment in code directory

(1) cd mdf_reader

(2) Create environment

    local:
	     python3 -m virtualenv --system-site-packages myenv

    jasmin: (python3 still not default/operative, but following works)

      1. Set path and activate conda environment
      export PATH=/apps/contrib/jaspy/miniconda_envs/jaspy3.7/m3-4.5.11/bin:$PATH
      source activate jaspy3.7-m3-4.5.11-r20181219

      2. Create your own virtualenv - you only do this once!
      virtualenv --system-site-packages myenv

(3) Activate environment:

    source ./myenv/bin/activate

(4) Install specific package versions:

    pip install -r requirements.txt

To deactivate environment:

	deactivate


D. Add module parent directory to python path (PYTHONPATH env variable)
  - from terminal:
  export PYTHONPATH=$toolParentDirectory:${PYTHONPATH}
  - In python:
  import sys
  sys.path.append(toolParentDirectory)

E. Run a test:

import mdf_reader

import matplotlib.pyplot as plt

data = mdf_reader.tests.imma1_buoys_nosupp()

data = mdf_reader.tests.imma1_buoys_supp()

data = td11_deck187_nosupp()

ax = data[section_name][field_name].plot(label='x')

data[section_name][field_name].plot(ax = ax ,label='y')

....

plt.show()
