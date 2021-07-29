# Model Data Format reader: `mdf_reader` toolbox

The `mdf_reader` is a [python3](https://www.python.org/) tool designed to read data files compliant with a user specified [data
model](https://glamod.github.io/cdm-obs-documentation/#). It was developed to read the [IMMA](https://icoads.noaa.gov/e-doc/imma/R3.0-imma1.pdf) (International Maritime Meteorological Archive) data format, but it has been enhanced to account for meteorological data formats in the case of:

- Data that is stored in a human-readable manner: “ASCII” format.
- Data that is organized in single line reports
- Reports that have a coherent internal structure and can be modelised.
- Reports that have a fixed width or field delimited types
- Reports that can be organized in sections, in which case each section can be of different types (fixed width of delimited)

1. Clone the repository

```
git clone git@github.com:glamod/mdf_reader.git
```
2. Install the tool, more information in the [documentation website](https://glamod.github.io/mdf_reader_documentation/tool-set-up.html#)

3. Run a test:
```
import sys
sys.path.append('/path_to_folder_directory_containing_the_mdf_reader_folder/')
import mdf_reader
import matplotlib.pyplot as plt

data = mdf_reader.tests.read_imma1_buoys_nosupp()
```
4. Read imma data
```
imma_data = mdf_reader.read(filepath, data_model = 'imma1',sections = ['core','c1','c98'])
```

For more details on how to use the `mdf_reader` tool see the following [documenation website](https://glamod.github.io/mdf_reader_documentation).
