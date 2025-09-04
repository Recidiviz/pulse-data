# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Script for identifying and purging bad bytes from text files.


For a local file --

python -m recidiviz.tools.ingest.one_offs.identify_and_purge_bad_bytes \
    --local-in-path '~/data/us_ne/bad bytes/unprocessed_2025-05-03T11_23_04_000000_raw_PIMSContactDashboard.csv' \
    --encoding windows-1252

    
For a file in a gcs bucket (note this can be slower than just downloading a file
locally and then running the command as it is downloading it in parts, but is helpful
if you're lazy like me) --

python -m recidiviz.tools.ingest.one_offs.identify_and_purge_bad_bytes \
    --local-gcs-path 'gs://recidiviz-staging-direct-ingest-state-storage/us_ne/deprecated/deprecated_on_2025-05-05/unprocessed_2025-05-03T11:23:04:000000_raw_PIMSContactDashboard.csv' \
    --encoding windows-1252

    
If you want to have remove the bad bytes in the file and re-upload it w/o download it, 
specify an output gcs path like (note this can be slow since uploading like this
can be slower than just uploading!) --

python -m recidiviz.tools.ingest.one_offs.identify_and_purge_bad_bytes \
    --gcs-in-path 'gs://recidiviz-staging-direct-ingest-state-storage/us_ne/deprecated/deprecated_on_2025-05-05/unprocessed_2025-05-03T11:23:04:000000_raw_PIMSContactDashboard.csv' \
    --gcs-out-path 'gs://recidiviz-staging-direct-ingest-state-storage/us_ne/deprecated/deprecated_on_2025-05-05/unprocessed_2025-05-03T11:23:04:000000_raw_PIMSContactDashboard_fixed.csv' \
    --encoding windows-1252
"""

import argparse
import logging
from contextlib import contextmanager, nullcontext
from io import BytesIO
from typing import ContextManager, Iterator

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


def remove_bad_bytes(
    *, input_io: BytesIO, output_io: BytesIO | None, encoding: str
) -> None:
    """Reads in the contents of |input_io| using the provided |encoding|, catching any
    bytes that cannot be decoded by |encoding|. If |output_io| is provided, it will write
    out the contents to that io.
    """
    logging.info("starting file read...")
    cursor = 0
    counter = 0
    while True:
        input_io.seek(cursor)
        next_chunk = input_io.read()
        try:
            next_chunk.decode(encoding)
            if output_io is not None:
                logging.info("\twriting bytes [%s] -> EOF to output file", cursor)
                output_io.write(next_chunk)
            break
        except UnicodeDecodeError as ude:
            counter += 1
            input_io.seek(cursor)
            to_append = input_io.read(ude.start)
            excluded = input_io.read(ude.end - ude.start)
            next_offset = ude.start + (ude.end - ude.start)
            logging.info(
                "\t byte offset [%s]: %s; stripping %s",
                cursor + ude.start,
                str(ude),
                excluded,
            )

            if output_io is not None:
                logging.info(
                    "\t\twriting bytes [%s] -> [%s] to output file",
                    cursor,
                    next_offset,
                )
                output_io.write(to_append)
            cursor += next_offset

    logging.info("number of bad bites is: %i", counter)


@contextmanager
def _get_io(
    *,
    local_path: str | None,
    gcs_path: str | None,
    write: bool,
    fs: GCSFileSystem,
) -> Iterator[BytesIO]:
    """Yields a binary io stream from either the local path or gcs path. An
    invariant of this func is that it assumes that both local_path and gcs_path will not
    be specified.
    """
    if local_path and gcs_path:
        raise ValueError("Cannot specify both a local and gcs path, just one!")

    mode = "wb" if write else "rb"
    if local_path is not None:
        with open(local_path, mode=mode) as f:
            yield f  # type: ignore
    elif gcs_path is not None:
        gcs_file_path = GcsfsFilePath.from_absolute_path(gcs_path)
        if write and not fs.exists(gcs_file_path):
            fs.upload_from_string(gcs_file_path, "", content_type="text/csv")
        with fs.open(gcs_file_path, mode=mode, verifiable=False) as f:
            yield f  # type: ignore
    else:
        raise ValueError("Must specify either local_path or gcs_path, but not neither")


def _get_opt_io(
    *,
    local_path: str | None,
    gcs_path: str | None,
    write: bool,
    fs: GCSFileSystem,
) -> ContextManager[BytesIO | None]:
    """If both local_path and gcs_path are not specified, it returns null context manager;
    otherwise, returns a binary io stream.
    """
    if not local_path and not gcs_path:
        return nullcontext()
    return _get_io(local_path=local_path, gcs_path=gcs_path, fs=fs, write=write)


def main(
    *,
    local_in_path: str | None,
    gcs_in_path: str | None,
    local_out_path: str | None,
    gcs_out_path: str | None,
    encoding: str,
) -> None:
    fs = GcsfsFactory.build()
    with _get_io(
        local_path=local_in_path, gcs_path=gcs_in_path, fs=fs, write=False
    ) as input_io, _get_opt_io(
        local_path=local_out_path, gcs_path=gcs_out_path, fs=fs, write=True
    ) as output_io:
        remove_bad_bytes(
            input_io=input_io,
            output_io=output_io,
            encoding=encoding,
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--local-in-path")
    input_group.add_argument("--gcs-in-path")

    output_group = parser.add_mutually_exclusive_group(required=False)
    output_group.add_argument("--local-out-path")
    output_group.add_argument("--gcs-out-path")

    parser.add_argument("--encoding", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    main(
        local_in_path=args.local_in_path,
        gcs_in_path=args.gcs_in_path,
        local_out_path=args.local_out_path,
        gcs_out_path=args.gcs_out_path,
        encoding=args.encoding,
    )
