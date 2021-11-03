# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
Script for uploading a file/files manually to a region's ingest bucket so that the paths will be normalized with the
date that should be associated with that file/files. Should be used for any new historical files or files we're asked to
upload manually due to an upload script failure.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date \
    ~/Downloads/MyHistoricalDump/ --date 2019-08-12 \
    --project-id recidiviz-staging --region us_nd --dry-run True \
    [--destination-bucket recidiviz-staging-my-test-bucket]
"""
import argparse
import datetime
import logging
import os
import threading
from typing import List, Optional, Tuple

from progress.bar import Bar

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.ingest.direct.controllers.base_upload_state_files_to_ingest_bucket_controller import (
    BaseUploadStateFilesToIngestBucketController,
    UploadStateFilesToIngestBucketDelegate,
)
from recidiviz.tools.gsutil_shell_helpers import gsutil_cp
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.params import str_to_bool


class ManualUploadStateFilesToIngestBucketDelegate(
    UploadStateFilesToIngestBucketDelegate
):
    def __init__(
        self,
        region_code: str,
        dry_run: bool,
        destination_bucket_override: Optional[GcsfsBucketPath],
    ) -> None:
        self.dry_run = dry_run
        self.region_code = region_code.upper()
        self.destination_bucket_override = destination_bucket_override

    def should_pause_processing(self) -> bool:
        return not self.dry_run and not self.destination_bucket_override

    def pause_processing(self) -> None:
        prompt_for_confirmation(
            f"Have you paused ingest queues for [{self.region_code}] via the admin "
            f"panel, if they are not paused already?",
            "Y",
        )

    def unpause_processing(self) -> None:
        prompt_for_confirmation(
            f"Have you unpaused ingest queues for [{self.region_code}] via the admin "
            f"panel?",
            "Y",
        )


class ManualUploadStateFilesToIngestBucketController(
    BaseUploadStateFilesToIngestBucketController
):
    """Class with functionality to upload a file or files from a local filesystem to a region's ingest bucket."""

    def __init__(
        self,
        paths: str,
        project_id: str,
        region: str,
        date: str,
        dry_run: bool,
        destination_bucket_override: Optional[GcsfsBucketPath],
    ):
        super().__init__(
            paths_with_timestamps=[
                (path, datetime.datetime.fromisoformat(date)) for path in paths
            ],
            project_id=project_id,
            region=region,
            delegate=ManualUploadStateFilesToIngestBucketDelegate(
                region_code=region,
                dry_run=dry_run,
                destination_bucket_override=destination_bucket_override,
            ),
            destination_bucket_override=destination_bucket_override,
        )

        self.dry_run = dry_run

        self.mutex = threading.Lock()
        self.move_progress: Optional[Bar] = None
        self.copies_list: List[Tuple[str, str]] = []
        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f"upload_to_ingest_result_{region}_{self.project_id}_date_{date}"
            f"_dry_run_{self.dry_run}_{datetime.datetime.now().isoformat()}.txt",
        )

    def _copy_to_ingest_bucket(
        self,
        path: str,
        full_file_upload_path: GcsfsFilePath,
    ) -> None:
        if not self.dry_run:
            try:
                gsutil_cp(path, full_file_upload_path.uri())
                self.uploaded_files.append(path)
                self.copies_list.append((path, full_file_upload_path.uri()))
            except ValueError:
                self.unable_to_upload_files.append(path)
        else:
            self.copies_list.append((path, full_file_upload_path.uri()))

        with self.mutex:
            if self.move_progress:
                # pylint: disable=not-callable
                self.move_progress.next()

    def get_paths_to_upload(self) -> List[Tuple[str, datetime.datetime]]:
        path_candidates = []
        for path, timestamp in self.paths_with_timestamps:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if self._is_supported_extension(os.path.join(path, filename)):
                        path_candidates.append(
                            (os.path.join(path, filename), timestamp)
                        )
                    else:
                        self.skipped_files.append(os.path.join(path, filename))
            elif os.path.isfile(path):
                if self._is_supported_extension(path):
                    path_candidates.append((path, timestamp))
                else:
                    self.skipped_files.append(path)

            else:
                self.unable_to_upload_files.append(path)
                raise ValueError(
                    f"Could not tell if path [{path}] is a file or directory."
                )

        return path_candidates

    def write_copies_to_log_file(self) -> None:
        self.copies_list.sort()
        with open(self.log_output_path, "w", encoding="utf-8") as f:
            if self.dry_run:
                prefix = "[DRY RUN] Would copy"
            else:
                prefix = "Copied"

            f.writelines(
                f"{prefix} {original_path} -> {new_path}\n"
                for original_path, new_path in self.copies_list
            )
            if self.unable_to_upload_files:
                f.writelines(
                    f"Failed to copy {path}" for path in self.unable_to_upload_files
                )


def main() -> None:
    """Executes the main flow of the script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "paths",
        metavar="PATH",
        nargs="+",
        help="Path to files to move, either single file path or directory path.",
    )

    parser.add_argument(
        "--project-id",
        required=True,
        help="Which project the file(s) should be uploaded to (e.g. recidiviz-123).",
    )

    parser.add_argument("--region", required=True, help="E.g. 'us_nd'")

    parser.add_argument(
        "--date", required=True, help="The date to be associated with this file."
    )

    parser.add_argument(
        "--dry-run",
        type=str_to_bool,
        default=True,
        help="Whether or not to run this script in dry run (log only) mode.",
    )
    parser.add_argument(
        "--destination-bucket",
        type=str,
        default=None,
        help="Override destination bucket for the upload. Can be used to upload files "
        "to an arbitrary testing bucket with normalized names.",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    override_bucket = (
        GcsfsBucketPath(args.destination_bucket) if args.destination_bucket else None
    )
    controller = ManualUploadStateFilesToIngestBucketController(
        paths=args.paths,
        project_id=args.project_id,
        region=args.region,
        date=args.date,
        dry_run=args.dry_run,
        destination_bucket_override=override_bucket,
    )

    if controller.dry_run:
        logging.info("Running in DRY RUN mode for region [%s]", controller.region)
    else:
        i = input(
            f"This will upload raw files to the [{controller.region}] ingest bucket "
            f"[{controller.destination_ingest_bucket.uri()}] with datetime "
            f"[{args.date}]. Type {controller.project_id} to continue: "
        )

        if i != controller.project_id:
            return

    if override_bucket:
        if not controller.dry_run:
            i = input(
                f"Are you sure you want to upload to non-standard bucket "
                f"[{controller.destination_ingest_bucket.uri()}]?. Type "
                f"{controller.destination_ingest_bucket.bucket_name} to continue: "
            )

            if i != controller.destination_ingest_bucket.bucket_name:
                return

    msg_prefix = "[DRY RUN] " if controller.dry_run else ""
    controller.move_progress = Bar(
        f"{msg_prefix}Uploading files...", max=len(controller.get_paths_to_upload())
    )

    controller.do_upload()

    if not controller.move_progress:
        raise ValueError("Progress bar should not be None")
    controller.move_progress.finish()

    controller.write_copies_to_log_file()

    if controller.dry_run:
        logging.info(
            "[DRY RUN] See results in [%s].\nRerun with [--dry-run False] to execute move.",
            controller.log_output_path,
        )
    else:
        logging.info(
            "Upload complete! See results in [%s].", controller.log_output_path
        )


if __name__ == "__main__":
    main()
