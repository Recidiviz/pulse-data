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

"""Direct ingest controller implementation for us_tx_brazos."""
from collections import defaultdict
import csv
import logging
import os
from typing import Any, Optional

from gcsfs import GCSFileSystem

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.charge import ChargeDegree
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller \
    import GcsfsDirectIngestController, GcsfsIngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.ingest.extractor.csv_data_extractor import CsvDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape import scraper_utils
from recidiviz.ingest.scrape.task_params import ScrapedData


class UsTxBrazosDataExtractor(CsvDataExtractor):
    """Data extractor for us_tx_brazos.

    This is a child of CsvDataExtractor only for
    _set_value_if_key_exists.

    TODO (#2104) add this functionality into CsvDataExtractor.
    """

    def __init__(self, key_mapping_file):
        super().__init__(key_mapping_file, should_cache=False)

    def extract_and_populate_data(
            self, content: str, ingest_info: IngestInfo = None) -> \
            Optional[ScrapedData]:
        if not isinstance(content, str):
            raise DirectIngestError(msg=f"{content} is not a string",
                                    error_type=DirectIngestErrorType.READ_ERROR)

        if not ingest_info:
            ingest_info = IngestInfo()

        rows = csv.DictReader(content.splitlines())
        for row_index, row in enumerate(rows):
            row_ii = IngestInfo()
            for k, v in row.items():
                if k not in self.all_keys:
                    raise ValueError("Unmapped key: [%s]" % k)

                if not v:
                    continue

                self._set_value_if_key_exists(
                    k, v, row_ii, defaultdict(set), {})

            try:
                self._merge_row_into_ingest_info(ingest_info, row_ii)
            except DirectIngestError as e:
                e.msg = f"While parsing CSV row {row_index + 1}: " + e.msg
                raise(e)

        return ScrapedData(ingest_info, persist=True)

    def _merge_row_into_ingest_info(self, ingest_info, row_ii):
        row_person = scraper_utils.one('person', row_ii)
        existing_person = ingest_info.get_person_by_id(row_person.person_id)
        if not existing_person:
            ingest_info.people.append(row_person)
            return

        if len(row_person.bookings) != 1:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PARSE_ERROR,
                msg="Exactly one booking must be on each row.")
        row_booking = row_person.bookings[0]

        existing_booking = existing_person.get_booking_by_id(
            row_booking.booking_id)
        if not existing_booking:
            existing_person.bookings.append(row_booking)
            return

        if len(row_booking.charges) != 1:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PARSE_ERROR,
                msg="Exactly one charge must be on each row.")
        row_charge = row_booking.charges[0]
        existing_booking.charges.append(row_charge)


class UsTxBrazosController(GcsfsDirectIngestController):
    """Direct ingest controller implementation for us_tx_brazos."""

    def __init__(self, fs: GCSFileSystem):
        super(UsTxBrazosController, self).__init__(
            'us_tx_brazos', SystemLevel.COUNTY, fs)

        self._mapping_filepath = os.path.join(
            os.path.dirname(__file__), 'us_tx_brazos.yaml')

    def _parse(self, args: GcsfsIngestArgs, contents: Any) -> IngestInfo:
        data_extractor = UsTxBrazosDataExtractor(self._mapping_filepath)
        data = data_extractor.extract_and_populate_data(contents)
        return data

    def get_enum_overrides(self):
        overrides_builder = super(UsTxBrazosController,
                                  self).get_enum_overrides().to_builder()

        overrides_builder.ignore(lambda x: True, CustodyStatus)
        overrides_builder.ignore('CLASS A MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('CLASS B MISDEMEANOR', ChargeDegree)
        overrides_builder.ignore('FELONY UNASSIGNED', ChargeDegree)
        overrides_builder.ignore('NOT APPLICABLE', ChargeDegree)
        overrides_builder.ignore('STATE JAIL FELONY', ChargeDegree)

        return overrides_builder.build()
