# Downloading the USSC Case-Level Sentence Data

Get CSV versions of the USSC data instead of the fixed-width SAS/SPSS versions published by [USSC](https://www.ussc.gov/research/datafiles/commission-datafiles#individual).

1. Download the zipped CSVs for the relevant years from github:
https://github.com/khwilson/SentencingCommissionDatasets/tree/main/data
2. Install `xz` in order to unzip the compressed CSVs

```$ brew install xz```
3. Unzip each file

```$ unxz [FILENAME].xz```

4. Move the files to this sentencing directory

```$ cp [FILENAME].csv ~/Recidiviz/recidiviz/calculator/modeling/population_projection/state/FED/sentencing_data/```
