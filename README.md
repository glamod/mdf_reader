# Mdf_reader

The `mdf_reader` is a [python3](https://www.python.org/) tool designed to read data files compliant with a user specified [data
model](https://cds.climate.copernicus.eu/toolbox/doc/how-to/15_how_to_understand_the_common_data_model/15_how_to_understand_the_common_data_model.html). It was developed to read the [IMMA](https://icoads.noaa.gov/e-doc/imma/R3.0-imma1.pdf) (International Maritime Meteorological Archive) data format, but it has been enhanced to account for meteorological data formats in the case of:

- Data that is stored in a human-readable manner: “ASCII” format.
- Data that is organized in single line reports
- Reports that have a coherent internal structure and can be modelised.
- Reports that have a fixed width or field delimited types
- Reports that can be organized in sections, in which case each section can be of different types (fixed width of delimited)

1. Clone the repository

```
git clone git@git.noc.ac.uk:brecinosrivas/mdf_reader.git
```

2. Run a test:
```
import sys
sys.path.append('/path_to_folder_directory_containing_the_mdf_reader_folder/')
import mdf_reader
import matplotlib.pyplot as plt

data = mdf_reader.tests.read_imma1_buoys_nosupp()
```
3. Read imma data
```
imma_data = mdf_reader.read(filepath, data_model = 'imma1',sections = ['core','c1','c98'])
```

For more details on how to use the `mdf_reader` tool see the following [jupyter notebooks](https://git.noc.ac.uk/brecinosrivas/mdf_reader/-/tree/master/docs/notebooks).
