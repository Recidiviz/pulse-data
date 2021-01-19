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

"""Direct ingest parsing for us_ma_middlesex.
"""
import logging
import os
import re
from collections import defaultdict
from typing import Iterable, Dict, Optional, List

import more_itertools

from recidiviz import IngestInfo
from recidiviz.common.constants.bond import BondType
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import Person
from recidiviz.ingest.scrape import scraper_utils


class UsMaMiddlesexParser:
    """Parses exported data to IngestInfos after it has been read into
    dataframes."""

    def __init__(self) -> None:
        region_name = 'us_ma_middlesex'
        self.yaml_file = os.path.join(os.path.dirname(__file__),
                                      region_name + '.yaml')
        self.bond_yaml_file = self.yaml_file.replace('.yaml', '_bond.yaml')

    def parse(self, json_people: Iterable[Dict]) -> IngestInfo:
        """Uses the JsonDataExtractor to convert JSON data at the person level
        to IngestInfo objects."""
        extractor = JsonDataExtractor(self.yaml_file)
        bond_extractor = JsonDataExtractor(self.bond_yaml_file)

        # Group people by person id. Since we're iterating over bookings, not
        # people, we have to manually merge people's bookings.
        people: Dict[str, List[Person]] = defaultdict(list)

        for person_dict in json_people:
            ii = extractor.extract_and_populate_data(person_dict)
            person = scraper_utils.one('person', ii)
            person.place_of_residence = self.get_address(person_dict)
            # TODO(#1802): parse ethnicity in enum overrides
            if person.race == 'HISPANIC' or person.ethnicity == 'Y':
                person.race, person.ethnicity = None, 'HISPANIC'
            else:
                person.ethnicity = None

            booking = scraper_utils.one('booking', ii)
            booking.admission_reason = self.get_admission_reason(person_dict)

            for hold in person_dict['hold']:
                jurisdiction_name = hold['holding_for_agency']
                if jurisdiction_name == 'Request to Hold':
                    jurisdiction_name = hold['charges']
                booking.create_hold(hold_id=hold['pkey'],
                                    jurisdiction_name=jurisdiction_name)

            # Bonds are shared across all charges within a single case
            for bond_dict in person_dict['bond']:
                bond = scraper_utils.one(
                    'bond', bond_extractor.extract_and_populate_data(bond_dict))
                case_pk = bond_dict['case_pk']
                matching_charges = (c for c in ii.get_all_charges()
                                    if c.case_number == case_pk)
                if matching_charges:
                    for charge in matching_charges:
                        charge.bond = bond
                else:
                    # Some bonds have no charges associated with their case
                    booking.create_charge(bond=bond)

            court_type = person_dict['booking']['commiting_authority']
            for charge in ii.get_all_charges():
                charge.court_type = court_type
                charge.status = self.get_charge_status(person_dict,
                                                       charge.charge_id)

                if charge.degree:
                    logging.info("Charge degree found, but we don't expect it "
                                 "to be filled in: \n%s", ii)
                if charge.charge_class:
                    logging.info(
                        "Charge class found, but we don't expect it "
                        "to be filled in: \n%s", ii)
                if charge.number_of_counts:
                    match = re.search(r"([0-9]+) (?:other )?mitts",
                                      charge.number_of_counts,
                                      re.IGNORECASE)
                    charge.number_of_counts = match.group(1) if match else None

            for bond in ii.get_all_bonds(lambda b: b.bond_agent):
                # bond.speccond (stored temporarily in bond.bond_agent) might
                # have two cash values separated by a slash, indicating a
                # partial bond.
                if re.search(r'[0-9]+ */ *[0-9]+', bond.bond_agent):
                    bond.bond_type = BondType.PARTIAL_CASH.value
                bond.bond_agent = None

            people[person.person_id].append(person)

        def merge_bookings(dupes: List[Person]) -> Person:
            base = dupes.pop()
            for p in dupes:
                base.bookings.extend(p.bookings)
            return base

        merged_people = [merge_bookings(dupes) for dupes in people.values()]
        return IngestInfo(people=merged_people)

    def get_address(self, person_dict: Dict) -> Optional[str]:
        if len(person_dict['address']) > 1:
            logging.debug("Found multiple addresses for sysid [%s]",
                          person_dict['booking']['sysid'])
        if person_dict['address']:
            return ' '.join(person_dict['address'][0][field]
                            for field in ('addr1', 'city', 'state', 'zip'))
        return None

    def get_admission_reason(self, person_dict: Dict) -> Optional[str]:
        """Returns the appropriate admission reason from the booking admission
        document's types."""
        admission_reasons = {admission['booking_admission_doc_type']
                             for admission in person_dict['admission']
                             if admission['booking_admission_doc_type']}
        if len(admission_reasons) == 1:
            return admission_reasons.pop()

        admission_reason_hierarchy = [
            'SENTENCE MITTIMUS',
            '15-DAY PAROLE DETAINER',
            'PERMANENT PAROLE DETAINER',
            'BAIL MITTIMUS',
            'MITTIMUS FOR FINES',
            'CRIMINAL COMPLAINT',
            'CIVIL CAPIAS',
            'CONTEMPT OF COURT',
            'GOVERNORS WARRANT',
            'WARRANT MANAGEMENT SYSTEM',
            'FEDERAL DETAINER',
        ]

        for reason in admission_reasons:
            if reason not in admission_reason_hierarchy:
                raise DirectIngestError(
                    error_type=DirectIngestErrorType.PARSE_ERROR,
                    msg=
                    f"Unknown admission document type seen for person with "
                    f"sysid [{person_dict['booking']['sysid']}]: [{reason}]")


        for reason in admission_reason_hierarchy:
            if reason in admission_reasons:
                return reason

        return None

    def get_charge_status(
            self, person_dict: Dict, charge_id: str) -> Optional[str]:
        """
        discharge_type:
        - Other
        - Purged by Court
        - Nol Pros
        - Turned over to New Jurisdiction
        - GUILTY/FILED
        - Personal Recognizance
        - BAILED OUT
        - Released by Authority of Court
        - DISMISSED
        reason_for_discharge:
        - NP dropped
        - BA bail
        - PR personal recognizance
        - CT court
        - JT jail transfer
        disposition:
        - PT pretrial
        - VL parole violation
        - 2 sentenced
        """
        charge_dict = more_itertools.one(charge
                                         for charge in person_dict['charge']
                                         if charge['charge_pk'] == charge_id)

        def clean(s: Optional[str]) -> Optional[str]:
            if s and not s.isspace() and s not in {'OT', 'Other',
                                                   'GUILTY/FILED'}:
                return s
            return None

        discharge_type = clean(charge_dict['discharge_type'])
        reason_for_discharge = clean(charge_dict['reason_for_discharge'])
        disposition = clean(charge_dict['disposition'])

        return discharge_type or reason_for_discharge or disposition
