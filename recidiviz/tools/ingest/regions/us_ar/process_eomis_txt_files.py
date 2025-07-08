#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
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
"""Tool to process raw .txt files pulled from eOMIS in Arkansas. Includes the following steps:
1: Converts the .txt files to CSVs and stores them in a temporary folder, which can optionally be deleted during cleanup.
2: Creates config skeletons for the converted CSVs.
3: Updates config skeletons, along with any existing configs for the provided files, using the eOMIS reference tables.
This includes placeholder column descriptions, along with auto-populated:
    - table descriptions
    - primary keys
    - known values
    - datetime and PII flags (the latter may be incomplete, and should be reviewed manually in case there are undetected PII columns)
4 (Optional): Pushes the CSVs to a scratch bucket.

Note: the eOMIS reference tables are pulled from live google sheets to support updates to the metadata, which occur occasionally.
These sheets have been compiled into a single "Script-friendly metadata" workbook, which can be found in the the "eOMIS reference"
subfolder within the AR drive folder (https://drive.google.com/drive/folders/1FSCyRS-sGvY7kCkVkjUdcOg3n21VG9P4?usp=drive_link).
To run this script, you need access to the sheet, along with credentials.json downloaded from
https://console.developers.google.com/apis/credentials. Pass the path to the directory containing the credentials to the
`--credentials-directory` argument when running the script.

Usage:
python -m recidiviz.tools.ingest.regions.us_ar.process_eomis_txt_files \
    --credentials-directory /path/to/username/credentials \
    --source_folder /path/to/input/folder --csv_storage_folder /path/to/csv/storage/folder \
    --update_configs True --delimiter ‡ --encoding cp1252 --project_id recidiviz-staging --files_update_date 2023-11-06 \
    [--destination_bucket recidiviz-staging-us-ar-scratch-2]
"""

import argparse
import logging
import os
import sys
from shutil import rmtree
from typing import List, Optional

import pandas as pd

import recidiviz.tools.justice_counts.google_drive as gd
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawTableColumnFieldType,
    RawTableColumnInfo,
    get_region_raw_file_config,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.ingest.development.create_ingest_config_skeleton import (
    create_ingest_config_skeleton,
)
from recidiviz.tools.ingest.development.raw_data_config_writer import (
    RawDataConfigWriter,
)
from recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date import (
    upload_raw_state_files_to_ingest_bucket_with_date,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

US_AR_REFERENCE_SHEET_ID = "1W8NDQ0LtF4LArcsL6lCu3nxgxNKQLpCC9-niRsOXPGk"

# Columns with these strings in the name tend to be PII and should be flagged accordingly.
KNOWN_PII_COLS = [
    "OFFENDERID",
    "OFFICERID",
    "STAFFLASTUPDATE",
    "PARTYID",
    "LASTNAME",
    "FIRSTNAME",
    "MIDDLENAME",
    "SUFFIX",
    "DATEOFBIRTH",
    "BIRTHDATE",
    "DOB",
    "DOCKETNUMBER",
    "COMMENT",
    "CONTACTSTAFF",
    "EMAIL",
]

# These strings get interpreted as booleans in the YAML schema (case-insensitive). A
# column's known value names/descriptions get wrapped in quotes if they belong to this list,
# along with digit-only columns.
BOOL_LIKE_STRS = ["true", "false", "y", "n", "yes", "no"]


def _quote_special_vals(val: str) -> str:
    """Escape special (bool-like, numeric, or with special characters in certain positions)
    known values or known value descriptions with quotes."""
    if (
        val.lower() in BOOL_LIKE_STRS
        or val.replace(".", "").isdigit()
        or not val[0].isalnum()
        or val.endswith(":")
    ):
        return '"' + val + '"'
    return val


def _read_and_convert_txt_to_csv(
    filepaths: List[str],
    output_storage_folder: str,
    delimiter: str,
    encoding: str,
) -> None:
    """Converts txt files to CSV files, which are used by other functions in this script to
    generate configs and push to a scratch bucket."""

    os.makedirs(output_storage_folder, exist_ok=True)

    # Convert each txt file in the folder, skipping other files (hidden .DS_Store files can cause errors otherwise)
    for fp in [filepath for filepath in filepaths if ".txt" in filepath]:
        filename = fp.split("/")[-1].split(".")[0] + ".csv"

        if filename in os.listdir(output_storage_folder):
            logging.info(
                "Found %s in storage folder, proceeding to next file for CSV conversion",
                filename,
            )

        else:
            csv_path = os.path.join(
                output_storage_folder,
                filename,
            )

            # Read the .txt file and convert it to a CSV.
            logging.info("Reading in AR .txt file from path: [%s]", fp)
            try:
                ar_txt_csv = pd.read_csv(
                    fp,
                    delimiter=delimiter,
                    encoding=encoding,
                    engine="python",
                    dtype=str,
                )
            # Some tables have quotes in comment fields, resulting in a parser error on the
            # read_csv call. If a table has this issue, we try reading the csv with QUOTE_NONE.
            except pd.errors.ParserError:
                logging.info(
                    "ParserError raised by read_csv call: trying with QUOTE_NONE"
                )
                ar_txt_csv = pd.read_csv(
                    fp,
                    delimiter=delimiter,
                    encoding=encoding,
                    engine="python",
                    dtype=str,
                    quoting=3,
                )

            logging.info(
                "Converting txt file to CSV and storing in path: %s",
                csv_path,
            )

            ar_txt_csv.to_csv(
                csv_path, encoding=encoding, index=None, header=True, sep="‡"
            )

            # Allow user to view preview of table to make sure parsing looks generally right.
            logging.info("Table preview:\n %s", ar_txt_csv.iloc[:3])


def _get_col_details_from_ref_sheet(
    column: RawTableColumnInfo,
    file_tag: str,
    metadata_dfs: dict,
) -> RawTableColumnInfo:
    """Use the supplied eOMIS metadata tables to populate column details in the created config.
    The code table is used to fill in known values and descriptions, and the metadata table
    is used to set the field type for datetime columns. Also checks if the column name contains
    substrings that tend to indicate PII columns, setting is_pii = True accordingly.
    """

    # If a column has COLTYPE = DATE in the metadata reference table, set the field type accordingly.
    ft = column.field_type
    if (
        metadata_dfs["column_info"]
        .loc[
            (metadata_dfs["column_info"]["tabname"] == file_tag)
            & (metadata_dfs["column_info"]["colname"] == column.name),
            "coltype",
        ]
        .item()
        == "DATE"
    ):
        ft = RawTableColumnFieldType.DATETIME

    # If a column contains a substring flagged in KNOWN_PII_COLS, set is_pii to True.
    is_pii = column.is_pii
    if any(pii_colname_substr in column.name for pii_colname_substr in KNOWN_PII_COLS):
        is_pii = True

    code_mappings = metadata_dfs["code_values"].loc[
        (metadata_dfs["code_values"]["tabname"] == file_tag)
        & (metadata_dfs["code_values"]["colname"] == column.name)
    ]

    # Some code columns exist in multiple tables, and sometimes these columns are encoded
    # either without a table specification or with all the relevant tables in the tabname
    # column. Additionally, some code columns have the same value mappings, such as
    # SENTENCETYPE1 and SENTENCETYPE2, SENTENCETYPE3, etc.: these columns are often grouped
    # into one entry in the colname column. To catch these cases, we use pattern matching
    # to see if a column/table name is contained entirely within the relevant column, surrounded
    # only by non-word characters like commas, spaces, and newlines: if this fails, we do the
    # same but ignore the table column entirely.

    if code_mappings.empty:
        code_mappings = metadata_dfs["code_values"].loc[
            (
                metadata_dfs["code_values"]["tabname"].str.contains(
                    rf"(?:^|\W+){file_tag}(?:$|\W+)"
                )
            )
            & (
                metadata_dfs["code_values"]["colname"].str.contains(
                    rf"(?:^|\W+){column.name}(?:$|\W+)"
                )
            )
        ]

    if code_mappings.empty:
        code_mappings = metadata_dfs["code_values"].loc[
            metadata_dfs["code_values"]["colname"].str.contains(
                rf"(?:^|\W+){column.name}(?:$|\W+)"
            )
        ]

    # If a column doesn't have any mappings in the code table, it only receives updates
    # to field_type and is_pii.
    if code_mappings.empty:
        return RawTableColumnInfo(
            state_code=StateCode.US_AR,
            file_tag=column.file_tag,
            name=column.name,
            description=column.description,
            field_type=ft,
            is_pii=is_pii,
        )

    coldesc = code_mappings["coltitle"].iloc[0]
    if not coldesc:
        coldesc = column.description

    column.known_values = []
    mapped_codes = column.known_values_nonnull.copy()

    # Set known values based on the values and descriptions pulled from the code table.

    for _, mapping in code_mappings.iterrows():
        if mapping["codevalue"] and mapping["codevaluedesc"]:
            mapped_codes.append(
                ColumnEnumValueInfo(
                    value=_quote_special_vals(str(mapping["codevalue"])),
                    description=_quote_special_vals(str(mapping["codevaluedesc"])),
                )
            )
    return RawTableColumnInfo(
        state_code=StateCode.US_AR,
        file_tag=column.file_tag,
        name=column.name,
        description=coldesc,
        field_type=ft,
        is_pii=is_pii,
        known_values=mapped_codes,
    )


def _update_config_using_sheet(
    original_config: DirectIngestRawFileConfig,
    delimiter: str,
    encoding: str,
    metadata_dfs: dict,
) -> DirectIngestRawFileConfig:
    """Updates a config using the eOMIS reference sheets. Pulls in the table description
    and primary keys from the table names and metadata tables, respectively. Then, uses
    _get_col_details_from_ref_sheet to set column-level details."""

    desc = original_config.file_description

    description_from_sheet = (
        metadata_dfs["table_descriptions"]
        .loc[
            metadata_dfs["table_descriptions"]["tabname"] == original_config.file_tag,
            "tabdesc",
        ]
        .item()
    )

    # In the table names reference sheet, tables missing descriptions have a period for
    # their TABLE DESCRIPTION. In these cases, use the description from the original config.
    if description_from_sheet != ".":
        desc = description_from_sheet

    # Set primary keys using the metadata reference table. In this table, primary keys are
    # numbered, and columns not belonging to the primary key have a null KEYSEQ.
    pks = (
        metadata_dfs["column_info"]
        .loc[
            (metadata_dfs["column_info"]["tabname"] == original_config.file_tag)
            & (metadata_dfs["column_info"]["keyseq"] != "null"),
            "colname",
        ]
        .tolist()
    )

    bad_pks = [
        pk
        for pk in pks
        if pk not in [col.name for col in original_config.current_columns]
    ]

    if len(bad_pks) != 0:
        logging.warning(
            "Found primary key columns in metadata that don't exist in the table:\n%s\nFill in primary keys manually for this table.",
            bad_pks,
        )
        pks = []

    new_columns = [
        _get_col_details_from_ref_sheet(column, original_config.file_tag, metadata_dfs)
        for column in original_config.current_columns
    ]

    return DirectIngestRawFileConfig(
        state_code=original_config.state_code,
        file_tag=original_config.file_tag,
        file_path=original_config.file_path,
        file_description=desc,
        data_classification=original_config.data_classification,
        primary_key_cols=pks,
        columns=new_columns,
        encoding=encoding,
        separator=delimiter,
        custom_line_terminator=original_config.custom_line_terminator,
        ignore_quotes=original_config.ignore_quotes,
        supplemental_order_by_clause=original_config.supplemental_order_by_clause,
        export_lookback_window=original_config.export_lookback_window,
        no_valid_primary_keys=original_config.no_valid_primary_keys,
        infer_columns_from_config=original_config.infer_columns_from_config,
        table_relationships=original_config.table_relationships,
        update_cadence=original_config.update_cadence,
        is_code_file=original_config.is_code_file,
        is_chunked_file=original_config.is_chunked_file,
    )


def _push_converted_tables(
    csv_storage_folder: str,
    project_id: str,
    destination_bucket: Optional[str],
    files_update_date: str,
) -> None:
    # Trigger `upload_raw_state_files_to_ingest_bucket_with_date`, but confirm first that push to bucket should proceed.
    if project_id and files_update_date:
        if prompt_for_confirmation(
            input_text=f"{len(os.listdir(csv_storage_folder))} tables have been converted to CSVs, and the accompanying "
            "config skeletons have been generated and updated with `_update_config_using_sheet`. Would you like "
            f" to push the files to {project_id} to {destination_bucket if destination_bucket else 'the ingest bucket'}?",
            exit_on_cancel=False,
        ):
            logging.info(
                "Launching script `upload_raw_state_files_to_ingest_bucket_with_date` to upload tables in %s to bucket...",
                csv_storage_folder,
            )

            upload_raw_state_files_to_ingest_bucket_with_date(
                paths=[csv_storage_folder],
                date=files_update_date,
                project_id=project_id,
                region="us_ar",
                dry_run=False,
                destination_bucket=destination_bucket,
            )


def main(
    cred_directory: str,
    folder_path: str,
    csv_storage_folder: str,
    update_configs: bool,
    delimiter: str,
    encoding: str,
    project_id: Optional[str],
    destination_bucket: Optional[str],
    files_update_date: Optional[str],
) -> None:
    """Update columns in raw data configs with known values fetched from BigQuery."""

    # Supply paths to the cached_ arguments in order to use a locally saved CSV instead
    # of the respective reference sheet hosted in Google Sheets. Doing so will fail to
    # account for live updates, but runs much more quickly since the large Google Sheets
    # don't have to be converted to CSVs. Any tables lacking a provided path will default
    # to using the Google Sheet.

    drive = gd.Drive(cred_directory)
    metadata_dfs = dict.fromkeys(
        ["code_values", "column_info", "table_descriptions", "table_recency"]
    )
    fullsheet = drive.gc.open_by_key(US_AR_REFERENCE_SHEET_ID)
    for worksheet in fullsheet.worksheets():
        metadata_dfs[worksheet.title] = pd.DataFrame(worksheet.get_all_records())

    # Assumes that the supplied files are in txt format, and converts them to CSVs which
    # are stored in the folder with the path supplied in csv_storage_folder.
    txt_file_paths = [
        os.path.join(folder_path, file)
        for file in os.listdir(folder_path)
        if file.endswith(".txt")
    ]
    logging.info(
        "\n************\nConverting %s txt files to CSVs...",
        len(txt_file_paths),
    )
    _read_and_convert_txt_to_csv(
        filepaths=txt_file_paths,
        output_storage_folder=csv_storage_folder,
        delimiter=delimiter,
        encoding=encoding,
    )
    logging.info(
        "Conversion complete. Files are stored in %s\n************", csv_storage_folder
    )

    existing_configs = get_region_raw_file_config("US_AR")

    missing_configs = [
        filename
        for filename in os.listdir(csv_storage_folder)
        if filename.split(".")[0] not in existing_configs.raw_file_configs
    ]

    # Creates skeleton configs for the CSVs that lack configs, which will be updated
    # using `_update_config_using_sheet`.
    logging.info(
        "\n************\nMissing configs for %s, creating skeletons...",
        [filename.split(".")[0] for filename in missing_configs],
    )
    create_ingest_config_skeleton(
        raw_table_paths=[
            os.path.join(csv_storage_folder, filename) for filename in missing_configs
        ],
        state_code=StateCode.US_AR.value,
        delimiter=delimiter,
        encoding=encoding,
        data_classification=RawDataClassification.SOURCE,
        allow_overwrite=False,
        initialize_state=False,
        add_description_placeholders=True,
    )

    logging.info("Generated %s config skeletons.\n************", len(missing_configs))

    region_config = get_region_raw_file_config("US_AR")

    default_config = region_config.default_config()

    if update_configs:
        configs_to_update = [
            file_tag.split(".")[0] for file_tag in os.listdir(folder_path)
        ]
    else:
        configs_to_update = [mc.split(".")[0] for mc in missing_configs]

    raw_file_configs: List[DirectIngestRawFileConfig] = [
        rfc
        for rfc in region_config.raw_file_configs.values()
        if rfc.file_tag in configs_to_update
    ]

    # Updates configs, including the skeleton configs added in previous step.
    logging.info(
        "\n************\nUpdating configs for %s",
        [rfc.file_tag for rfc in raw_file_configs],
    )
    for original_raw_file_config in raw_file_configs:
        logging.info(
            "Using eOMIS reference tables to update %s",
            original_raw_file_config.file_tag,
        )
        updated_raw_file_config = _update_config_using_sheet(
            original_raw_file_config, delimiter, encoding, metadata_dfs
        )
        raw_data_config_writer = RawDataConfigWriter()
        raw_data_config_writer.output_to_file(
            raw_file_config=updated_raw_file_config,
            output_path=original_raw_file_config.file_path,
            default_encoding=default_config.default_encoding,
            default_separator=default_config.default_separator,
            default_ignore_quotes=default_config.default_ignore_quotes,
            default_export_lookback_window=default_config.default_export_lookback_window,
            default_no_valid_primary_keys=default_config.default_no_valid_primary_keys,
            default_custom_line_terminator=default_config.default_custom_line_terminator,
            default_update_cadence=default_config.default_update_cadence,
            default_infer_columns_from_config=default_config.default_infer_columns_from_config,
            default_import_blocking_validation_exemptions=default_config.default_import_blocking_validation_exemptions,
        )
    logging.info(
        "Configs have been updated using eOMIS reference tables.\n************"
    )

    if project_id and files_update_date:
        logging.info("\n************\n************\n************")
        _push_converted_tables(
            csv_storage_folder=csv_storage_folder,
            project_id=project_id,
            destination_bucket=destination_bucket,
            files_update_date=files_update_date,
        )
        logging.info("Run complete: files successfully uploaded to bucket.")

    elif project_id or files_update_date:
        logging.info(
            "Run complete. \nNote: found at least one of the [project_id, files_update_date] arguments, but not all. Both arguments are required to upload the files."
        )
    else:
        logging.info(
            "Run complete. \nNote: to have the option to push the converted files to a bucket, supply the [project_id, files_update_date] arguments."
        )

    prompt_for_confirmation(
        f"Would you like to delete the temporary directory containing {len(os.listdir(csv_storage_folder))} converted CSV files "
        f"({csv_storage_folder})?"
    )
    rmtree(csv_storage_folder)


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    """Parses the named arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--credentials-directory",
        dest="cred_directory",
        required=False,
        default=".",
        type=str,
        help="Directory where the 'credentials.json' live, as well as the cached token.",
    )

    parser.add_argument(
        "--source_folder",
        dest="folder_path",
        help="Folder containing new .txt files",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--csv_storage_folder",
        dest="csv_storage_folder",
        help="Path to the folder where the converted CSV files will be stored.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--update_configs",
        dest="update_configs",
        default=False,
        type=str_to_bool,
        help="Whether or not existing configs should be updated (defaults to False).",
    )

    parser.add_argument(
        "--delimiter",
        dest="delimiter",
        default="‡",
        help="Delimiter for the CSV files.",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--encoding",
        default="cp1252",
        dest="encoding",
        help="The encoding of the CSV files (e.g. cp1252, utf-8).",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--project_id",
        dest="project_id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="The environment would like the converted CSV to be uploaded to (recidiviz-staging or recidiviz-123)",
        type=str,
        required=False,
    )

    parser.add_argument(
        "--destination_bucket",
        dest="destination_bucket",
        help="The destination bucket would like the converted CSV to be uploaded to"
        " (ex: `recidiviz-staging-my-test-bucket`)",
        type=str,
        required=False,
    )

    parser.add_argument(
        "--files_update_date",
        dest="files_update_date",
        help="The date that the txt file was generated (format must be: YYYY-mm-dd, ex: 2022-02-22).",
        type=str,
        required=False,
    )

    known_args, _ = parser.parse_known_args(argv)

    return known_args


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_arguments(sys.argv)
    with local_project_id_override(args.project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            main(
                args.cred_directory,
                args.folder_path,
                args.csv_storage_folder,
                args.update_configs,
                args.delimiter,
                args.encoding,
                args.project_id,
                args.destination_bucket,
                args.files_update_date,
            )
