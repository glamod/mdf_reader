
1. Run a test:

import mdf_reader

import matplotlib.pyplot as plt

data = mdf_reader.tests.read_imma1_buoys_nosupp()

data = mdf_reader.tests.read_imma1_buoys_supp()

ax = data[section_name][field_name].plot(label='x')

data[section_name][field_name].plot(ax = ax ,label='y')

plt.show()

2. Read imma data

imma_data = mdf_reader.read(filepath, data_model = 'imma1',sections = ['core','c1','c98'])

