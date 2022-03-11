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
"""Defines types used for direct ingest."""
import abc
import datetime
from typing import Any, Dict, Optional, Type

import attr
import cattr

from recidiviz.cloud_functions.direct_ingest_bucket_name_utils import (
    get_region_code_from_direct_ingest_bucket,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common import serialization
from recidiviz.common.date import snake_case_datetime
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_instance_factory import (
    DirectIngestInstanceFactory,
)
from recidiviz.utils.types import ClsT


@attr.s(frozen=True)
class CloudTaskArgs:
    @abc.abstractmethod
    def task_id_tag(self) -> Optional[str]:
        """Tag to add to the name of an associated cloud task."""

    def to_serializable(self) -> Dict[str, Any]:
        converter = serialization.with_datetime_hooks(cattr.Converter())
        return converter.unstructure(self)

    @classmethod
    def from_serializable(cls: Type[ClsT], serializable: Dict[str, Any]) -> ClsT:
        converter = serialization.with_datetime_hooks(cattr.Converter())
        return converter.structure(serializable, cls)


@attr.s(frozen=True)
class ExtractAndMergeArgs(CloudTaskArgs):
    """Arguments for a task that extracts Python schema objects from the row-based
    results of an ingest view query, then merges those schema objects into our central
    data model.
    """

    # The time this extract and merge task was scheduled.
    ingest_time: datetime.datetime = attr.ib()

    @abc.abstractmethod
    def task_id_tag(self) -> str:
        pass

    @abc.abstractmethod
    def ingest_instance(self) -> DirectIngestInstance:
        pass

    @abc.abstractmethod
    def job_tag(self) -> str:
        """Returns a (short) string tag to identify an ingest run in logs."""

    @property
    @abc.abstractmethod
    def ingest_view_name(self) -> str:
        pass


# TODO(#11424): Eliminate all usages of this class in favor of a non-file-based
#  implementation of ExtractAndMergeArgs once BQ-based materialization has shipped
#  for all states.
@attr.s(frozen=True)
class LegacyExtractAndMergeArgs(ExtractAndMergeArgs):
    """The legacy argument type for the persist step of ingest."""

    file_path: GcsfsFilePath = attr.ib()

    def task_id_tag(self) -> str:
        parts = filename_parts_from_path(self.file_path)
        return f"ingest_job_{parts.stripped_file_name}_{parts.date_str}"

    def ingest_instance(self) -> DirectIngestInstance:
        return DirectIngestInstanceFactory.for_ingest_bucket(self.file_path.bucket_path)

    def job_tag(self) -> str:
        """Returns a (short) string tag to identify an ingest run in logs."""
        region_code = (
            get_region_code_from_direct_ingest_bucket(
                self.file_path.bucket_path.bucket_name
            )
            or "unknown_region"
        )
        return (
            f"{region_code.lower()}/{self.file_path.file_name}:" f"{self.ingest_time}"
        )

    @property
    def ingest_view_name(self) -> str:
        return filename_parts_from_path(self.file_path).file_tag


@attr.s(frozen=True)
class GcsfsRawDataBQImportArgs(CloudTaskArgs):
    raw_data_file_path: GcsfsFilePath = attr.ib()

    def task_id_tag(self) -> str:
        parts = filename_parts_from_path(self.raw_data_file_path)
        return f"raw_data_import_{parts.stripped_file_name}_{parts.date_str}"

    def ingest_instance(self) -> DirectIngestInstance:
        return DirectIngestInstanceFactory.for_ingest_bucket(
            self.raw_data_file_path.bucket_path
        )


@attr.s(frozen=True)
class IngestViewMaterializationArgs(CloudTaskArgs):
    # The file tag of the ingest view to export. Used to determine which query to run
    # to generate the exported file.
    ingest_view_name: str = attr.ib()

    # The lower bound date for updates this query should include. Any rows that have not
    # changed since this date will not be included.
    upper_bound_datetime_prev: Optional[datetime.datetime] = attr.ib()

    # The upper bound date for updates this query should include. Updates will only
    # reflect data received up until this date.
    upper_bound_datetime_to_export: datetime.datetime = attr.ib()

    @abc.abstractmethod
    def task_id_tag(self) -> str:
        pass

    @abc.abstractmethod
    def ingest_instance(self) -> DirectIngestInstance:
        pass


@attr.s(frozen=True)
class GcsfsIngestViewExportArgs(IngestViewMaterializationArgs):
    # The bucket to output the generated ingest view to.
    output_bucket_name: str = attr.ib()

    def task_id_tag(self) -> str:
        tag = f"ingest_view_export_{self.ingest_view_name}-{self.output_bucket_name}"
        if self.upper_bound_datetime_prev:
            tag += f"-{snake_case_datetime(self.upper_bound_datetime_prev)}"
        else:
            tag += "-None"
        tag += f"-{snake_case_datetime(self.upper_bound_datetime_to_export)}"
        return tag

    def ingest_instance(self) -> DirectIngestInstance:
        return DirectIngestInstanceFactory.for_ingest_bucket(
            GcsfsBucketPath(self.output_bucket_name)
        )
