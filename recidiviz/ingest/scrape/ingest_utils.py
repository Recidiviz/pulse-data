# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Utils file for ingest module"""

import logging
from datetime import tzinfo
from typing import List, Optional

import pytz

from recidiviz.common import common_utils
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.utils import regions, environment


def lookup_timezone(timezone: Optional[str]) -> Optional[tzinfo]:
    return pytz.timezone(timezone) if timezone else None


def validate_regions(region_list, timezone: tzinfo = None):
    """Validates the region arguments.

    If any region in |region_list| is "all", then all supported regions will be
    returned.

    Args:
        region_list: List of regions from URL parameters
        timezone: If set, returns only regions in the matching timezone

    Returns:
        False if invalid regions
        List of regions to scrape if successful
    """
    regions_list_output = region_list

    supported_regions = regions.get_supported_region_codes(timezone=timezone)
    for region in region_list:
        if region == "all":
            # If we got all regions, we only want to start them in the right
            # environment.  We only do this if all is passed to still allow
            # people to manually run any scrapers they wish.
            supported_regions = set(
                filter(
                    lambda x: regions.get_region(x).environment ==
                    environment.get_gae_environment(),
                    supported_regions))
            regions_list_output = supported_regions
        elif region not in supported_regions:
            logging.error("Region '%s' not recognized.", region)
            return False

    return regions_list_output


def validate_scrape_types(
        scrape_type_list: List[str]) -> List[constants.ScrapeType]:
    """Validates the scrape type arguments.

    If any scrape type in |scrape_type_list| is "all", then all supported scrape
    types will be returned.

    Args:
        scrape_type_list: List of scrape types from URL parameters

    Returns:
        False if invalid scrape types
        List of scrape types if successful
    """
    if not scrape_type_list:
        return [constants.ScrapeType.BACKGROUND]

    scrape_types_output = []
    all_types = False
    for scrape_type in scrape_type_list:
        if scrape_type == 'all':
            all_types = True
        else:
            try:
                scrape_types_output.append(constants.ScrapeType(scrape_type))
            except ValueError:
                logging.error("Scrape type '%s' not recognized.", scrape_type)
                return []

    return list(constants.ScrapeType) if all_types else scrape_types_output


def convert_ingest_info_to_proto(ingest_info):
    """Converts an ingest_info python object to an ingest info proto.

    Args:
        ingest_info: An IngestInfo python object
    Returns:
        An IngestInfo proto.
    """
    proto = IngestInfo()

    person_map = {}
    booking_map = {}
    charge_map = {}
    hold_map = {}
    arrest_map = {}
    bond_map = {}
    sentence_map = {}

    def _populate_proto(proto_name, ingest_info_source, id_name, proto_map):
        """Populates all of the proto fields from an IngestInfo object.

        Args:
            proto_name: The name of the proto we are populating, like 'people',
                'arrests', etc...
            ingest_info_source: The source object we are copying from
            id_name: The name of the id, is 'person_id', 'arrest_id', etc...
                This is used to decide whether or not to generate a new proto,
                use the given id, or generate a new one
            proto_map: A map of protos we have already created to know whether
                we need to go ahead creating one or if we can just return
                the already created one.
        Returns:
            A populated proto
        """
        obj_id = getattr(ingest_info_source, id_name)
        if not obj_id:
            obj_id = common_utils.create_generated_id(ingest_info_source)
        # If we've already seen this id, we don't need to create it, we can
        # simply return it as is.  Not that we use this local in memory
        # proto map so as to avoid using the map provided in proto to make it
        # simpler for external people to read it.  If we decide that no one
        # but us will ever read the proto then we can remove this logic here
        # and use the built in proto map.
        if obj_id in proto_map:
            return proto_map[obj_id]
        proto_to_populate = getattr(proto, proto_name).add()
        setattr(proto_to_populate, id_name, obj_id)
        # Since we expect all of the fields to exist in both the destination
        # and the source, we can just run through all of the fields in the proto
        # and use python magic to retrieve the value from the source and set
        # them on the destination.
        field_names = [i.name for i in proto_to_populate.DESCRIPTOR.fields]
        for field in field_names:
            val = getattr(ingest_info_source, field, None)
            if val is not None:
                setattr(proto_to_populate, field, val)
        proto_map[obj_id] = proto_to_populate
        return proto_to_populate

    for person in ingest_info.people:
        proto_person = _populate_proto(
            'people', person, 'person_id', person_map)
        for booking in person.bookings:
            proto_booking = _populate_proto(
                'bookings', booking, 'booking_id', booking_map)
            # Can safely append the ids now since they should be unique.
            proto_person.booking_ids.append(proto_booking.booking_id)

            if booking.arrest:
                proto_arrest = _populate_proto(
                    'arrests', booking.arrest, 'arrest_id', arrest_map)
                proto_booking.arrest_id = proto_arrest.arrest_id

            for hold in booking.holds:
                proto_hold = _populate_proto('holds', hold, 'hold_id', hold_map)
                proto_booking.hold_ids.append(proto_hold.hold_id)

            for charge in booking.charges:
                proto_charge = _populate_proto(
                    'charges', charge, 'charge_id', charge_map)
                proto_booking.charge_ids.append(proto_charge.charge_id)

                if charge.bond:
                    proto_bond = _populate_proto(
                        'bonds', charge.bond, 'bond_id', bond_map)
                    proto_charge.bond_id = proto_bond.bond_id

                if charge.sentence:
                    proto_sentence = _populate_proto(
                        'sentences', charge.sentence,
                        'sentence_id', sentence_map)
                    proto_charge.sentence_id = proto_sentence.sentence_id

    return proto
