# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Interface and standard implementation for a class that will take the contents of
an ingest view file and persist the contents appropriately to the Recidiviz schema
in Postgres.
"""

import abc
from typing import List, cast

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.io.contents_handle import ContentsHandle
from recidiviz.ingest.direct.ingest_mappings.ingest_view_file_parser import (
    IngestViewFileParser,
)
from recidiviz.ingest.direct.types.cloud_task_args import ExtractAndMergeArgs
from recidiviz.persistence import persistence
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.ingest_info_converter.base_converter import (
    EntityDeserializationResult,
)


class IngestViewProcessor:
    """Interface for a class that will take the contents of an ingest view file and
    persist the contents appropriately to the Recidiviz schema in Postgres.
    """

    @abc.abstractmethod
    def parse_and_persist_contents(
        self,
        args: ExtractAndMergeArgs,
        contents_handle: ContentsHandle,
        ingest_metadata: IngestMetadata,
    ) -> bool:
        pass


class IngestViewProcessorImpl(IngestViewProcessor):
    """Standard (new) implementation of the IngestViewProcessor, which takes the
    contents of an ingest view file and persists the contents appropriately to the
    Recidiviz schema in Postgres.
    """

    def __init__(self, ingest_view_file_parser: IngestViewFileParser):
        self.ingest_view_file_parser = ingest_view_file_parser

    def parse_and_persist_contents(
        self,
        args: ExtractAndMergeArgs,
        contents_handle: ContentsHandle,
        ingest_metadata: IngestMetadata,
    ) -> bool:
        parsed_entities = self.ingest_view_file_parser.parse(
            file_tag=args.file_tag,
            contents_handle=contents_handle,
        )

        if all(isinstance(e, state_entities.StatePerson) for e in parsed_entities):
            return persistence.write_entities(
                conversion_result=EntityDeserializationResult(
                    people=cast(List[state_entities.StatePerson], parsed_entities),
                    # We set these to zero because we now just crash if there are any
                    # parsing errors. If we get to this point, there were no errors.
                    enum_parsing_errors=0,
                    general_parsing_errors=0,
                    protected_class_errors=0,
                ),
                ingest_metadata=ingest_metadata,
                total_people=len(parsed_entities),
            )
        if all(isinstance(e, county_entities.Person) for e in parsed_entities):
            return persistence.write_entities(
                conversion_result=EntityDeserializationResult(
                    people=cast(List[county_entities.Person], parsed_entities),
                    # We set these to zero because we now just crash if there are any
                    # parsing errors. If we get to this point, there were no errors.
                    enum_parsing_errors=0,
                    general_parsing_errors=0,
                    protected_class_errors=0,
                ),
                ingest_metadata=ingest_metadata,
                total_people=len(parsed_entities),
            )
        raise ValueError("Found non-person top-level entities parsing file")
