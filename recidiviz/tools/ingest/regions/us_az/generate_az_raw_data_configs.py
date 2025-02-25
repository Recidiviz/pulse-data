#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
# AZ shared a codebook with the names, fields, field types, and field descriptions
# for each table in their ACIS system. The file is saved in the Recidiviz Google Drive's
# AZ folder as a Google Sheet. Once that Google Sheet is downloaded, update the
# codebook_file_path to point to it, and list the tables you're interested in the
# configs_to_generate variable. Then you can run this script in your terminal using this
# syntax:
#
# python -m recidiviz.tools.ingest.regions.us_az.generate_az_raw_data_configs
#
# Relatively crudely-hydrated raw data configs will be stored in the folder you
# specify in the output_directory_path variable. The default is the us_az/raw_data folder.
#
# The YAMLS generated will not have field_type flags or is_pii flags; those will need
# to be added manually. The primary key listing feature here is also currently broken,
# so the primary keys will need to be specified manually for the time being. They are
# typically identified in their field description, so they are not hard to spot.
""" A script to generate raw data config YAMLs from the AZ codebook, located in Google
Drive at go/arizona. Each file tag you want to generate a YAML for should be added to the 
configs_to_generate list, and the codebook_file_path should be pointed at your local
copy of the AZ codebook. 
"""
import datetime

import pandas as pd

# Change
codebook_file_path = "../../Downloads/ACIS_DATA_DICTIONARY.csv"
# Output file path assumes root directory is pulse-data. Adjust as needed!
output_directory_path = "recidiviz/ingest/direct/regions/us_az/raw_data/us_az_"

# List of tables you want to generate configuration files for. These tables names
# must match exactly to those listed in the data dictionary.
configs_to_generate = ["OMS_CHRONO_NOTES"]

with open(codebook_file_path, encoding="utf-8") as file:
    df = pd.read_csv(file)

_HEADER = "# yaml-language-server: $schema=./../../../raw_data/yaml_schema/schema.json"
_FILE_TAG = "file_tag: "
_FILE_DESCRIPTION = "file_description: |- "
_DATA_CLASSIFICATION = "data_classification: source"
_PRIMARY_KEY_COLS = "primary_key_cols:"
_COLUMNS = "columns:"
_FIELD_NAME = "  - name: "
_FIELD_DESCRIPTION = "    description: |- "
_NEWLINE = "\n"


for table in configs_to_generate:
    # find primary keys of each table by searching for columns with "primary key"
    # or "PK" in the "comments" field that contains a field description
    primary_keys = df.loc[
        (df["Table Name"] == table)
        & (
            (" PK " in df["Comments"])
            | (" pk " in df["Comments"])
            | ("PRIMARY KEY" in df["Comments"])
            | ("primary key" in df["Comments"])
            | ("Primary Key" in df["Comments"])
            | ("Primary key" in df["Comments"])
        ),
        "Column Name",
    ]
    fields = {}
    # find all column names in each table,
    column_names = df.loc[(df["Table Name"] == table), "Column Name"]

    # find description of each field
    # save to a dict to unpack more easily later on
    for column in column_names:
        fields[column] = list(
            df.loc[
                (df["Column Name"] == column) & (df["Table Name"] == table),
                "Comments",
            ]
        )[0]

    template = (
        _HEADER
        + _NEWLINE
        + _FILE_TAG
        + table  # FILE TAG
        + _NEWLINE
        + _FILE_DESCRIPTION
        + _NEWLINE
        + "  TODO(#000): FILL IN"  # FILE DESCRIPTION (blank for now, not in codebook)
        + _NEWLINE
        + _DATA_CLASSIFICATION  # STATIC
        + _NEWLINE
        + _PRIMARY_KEY_COLS
    )

    # List PKs if there are any, otherwise put "[]"
    # Note: this doesn't work right now for some reason
    if len(primary_keys) != 0:
        for primary_key in primary_keys:
            template += _NEWLINE + "  - " + primary_key
    else:
        template += " []"

    # adds header for columns section
    template += _NEWLINE + _COLUMNS + _NEWLINE

    # for every field in a table, add the following structure to the YAML
    # - name: <FIELD NAME>
    #   description: |-
    #       <FIELD DESCRIPTION>
    for field in fields.items():
        template += (
            _FIELD_NAME + str(field.index) + _NEWLINE + _FIELD_DESCRIPTION + _NEWLINE
        )
        if isinstance(fields[field], str):
            template += "      " + fields[field] + _NEWLINE
        else:
            template += (
                "      No description available in AZ data dictionary as of "
                + datetime.datetime.today().strftime("%Y-%m-%d")
                + _NEWLINE
            )
    file_name = output_directory_path + table + ".yaml"
    # output = open(file_name, "x")
    # output.write(template)
    with open(file_name, "w+", encoding="utf-8") as output:
        output.write(template)
