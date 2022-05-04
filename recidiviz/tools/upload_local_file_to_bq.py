# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
Script for uploading a local, delimited file into BigQuery.

When in dry-run mode (default), this will only log the output, rather than uploading it to BigQuery.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.upload_local_file_to_bq \
    --project-id recidiviz-staging \
    --destination-table test_dataset.table_name \
    --local-filepath ~/Downloads/test.csv \
    --separator , \
    --dry-run True
"""
import argparse
import csv
import logging

import pandas as pd

from recidiviz.utils.params import str_to_bool


def main(
    *,
    dry_run: bool,
    project_id: str,
    destination_table: str,
    local_filepath: str,
    separator: str,
    chunksize: int,
    ignore_quotes: bool,
    encoding: str,
    overwrite_if_exists: bool,
) -> None:
    """Reads and uploads data in the |local_filepath| to BigQuery based on the input args.."""
    # Set maximum display options
    pd.options.display.max_columns = 999
    pd.options.display.max_rows = 999

    quoting = csv.QUOTE_NONE if ignore_quotes else csv.QUOTE_MINIMAL

    dfcolumns = pd.read_csv(local_filepath, nrows=1, sep=separator)
    df = pd.read_csv(
        local_filepath,
        sep=separator,
        dtype=str,
        index_col=False,
        header=0,
        names=dfcolumns.columns,
        encoding=encoding,
        quoting=quoting,
        keep_default_na=False,
    )

    logging.info("\n\nLoaded dataframe has %d rows\n", df.shape[0])

    if dry_run:
        logging.info(
            "[DRY RUN] Would have uploaded dataframe to %s.%s: \n\n%s",
            project_id,
            destination_table,
            str(df.head()),
        )
        return

    i = input(
        f"\nWill upload dataframe to {project_id}.{destination_table}: \n\n{df.head()}\n\nContinue? [y/n]: "
    )

    if i.upper() != "Y":
        return

    df.to_gbq(
        destination_table=destination_table,
        project_id=project_id,
        progress_bar=True,
        if_exists=("replace" if overwrite_if_exists else "fail"),
        chunksize=chunksize,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs copy in dry-run mode, only prints the file copies it would do.",
    )

    parser.add_argument(
        "--destination-table",
        required=True,
        help="The '{dataset}.{table_name}' destination for the local csv.",
    )

    parser.add_argument(
        "--local-filepath",
        required=True,
        help="The local filepath for the local csv to upload.",
    )

    parser.add_argument(
        "--separator",
        required=False,
        default=",",
        help="Separator for the csv. Defaults to ','",
    )

    parser.add_argument(
        "--project-id", required=True, help="The project_id for the destination table"
    )

    parser.add_argument(
        "--chunksize",
        required=False,
        default=100000,
        help="Number of rows to be inserted into BQ at a time. Defaults to 100,000.",
    )

    parser.add_argument(
        "--encoding",
        required=False,
        default="UTF-8",
        help="Encoding for the file to be parsed. Defaults to UTF-8. If you are parsing files from "
        "US_ID, you might need ISO-8859-1.",
    )

    parser.add_argument(
        "--ignore-quotes",
        required=False,
        default=False,
        type=str_to_bool,
        help="If false, assumes text between quotes should be treated as a unified string (even if the "
        "text contains the specified separator). If True, does not treat quotes as a special "
        "character. Defaults to False.",
    )

    parser.add_argument(
        "--overwrite-if-exists",
        required=False,
        default=False,
        type=str_to_bool,
        help="If True, overwrites the existing table with the data from this upload.",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main(
        dry_run=args.dry_run,
        destination_table=args.destination_table,
        project_id=args.project_id,
        local_filepath=args.local_filepath,
        separator=args.separator,
        chunksize=args.chunksize,
        encoding=args.encoding,
        ignore_quotes=args.ignore_quotes,
        overwrite_if_exists=args.overwrite_if_exists,
    )
