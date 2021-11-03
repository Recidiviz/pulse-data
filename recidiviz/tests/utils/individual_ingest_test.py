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

"""Base class for tests that ingest individual people."""
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import serialization
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.ingest_info_diff import diff_ingest_infos
from recidiviz.persistence.entity_validator import entity_validator
from recidiviz.persistence.ingest_info_converter import ingest_info_converter
from recidiviz.persistence.ingest_info_validator.ingest_info_validator import validate


class IndividualIngestTest:
    """A base class for tests which ingest individuals."""

    def validate_ingest(
        self,
        ingest_info: IngestInfo,
        expected_ingest_info: IngestInfo,
        metadata: IngestMetadata,
    ) -> IngestInfo:
        """This function runs validation on a computed and expected ingest_info.

        Args:
            ingest_info: the computed ingest info object
            expected_ingest_info: the ingest info expected to be returned from
                `populate_data`. If `expected_ingest_info` is `None`, then
                expects the return value of `populate_data` to be `None`.
            metadata: an ingest info metadata struct to pass along to the proto
                converter.

        Returns:
            The result from populate_data in case the user needs to do any
            extra validations on the output.

        """

        if expected_ingest_info is None:
            assert ingest_info == expected_ingest_info
            return ingest_info

        # Attempt to convert the ingest_info to the ingest info proto,
        # validate the proto, and finally attempt to convert the proto into
        # our entitiy/ objects (which includes parsing strings into types)
        ingest_info_proto = serialization.convert_ingest_info_to_proto(ingest_info)
        validate(ingest_info_proto)
        res = ingest_info_converter.convert_to_persistence_entities(
            ingest_info_proto, metadata
        )

        assert res.enum_parsing_errors == 0
        assert res.general_parsing_errors == 0
        assert res.protected_class_errors == 0

        entity_validator.validate(res.people)

        differences = diff_ingest_infos(expected_ingest_info, ingest_info)

        if differences:
            differences_string = "\n".join(differences)
            self.fail(  # type: ignore[attr-defined]
                f"IngestInfo objects do not match.\n"
                f"Expected:\n{expected_ingest_info}\n"
                f"Actual:\n{ingest_info}\n"
                f"Differences:\n{differences_string}\n\n"
                f"(paste the following) scraped object:\n"
                f"{repr(ingest_info)}"
            )

        return ingest_info
