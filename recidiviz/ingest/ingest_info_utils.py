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

"""Convenience code for an ingest_info object."""

from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo


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
            obj_id = str(id(ingest_info_source)) + '_generate'
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
            if val:
                setattr(proto_to_populate, field, val)
        proto_map[obj_id] = proto_to_populate
        return proto_to_populate

    for person in ingest_info.person:
        proto_person = _populate_proto(
            'people', person, 'person_id', person_map)
        for booking in person.booking:
            proto_booking = _populate_proto(
                'bookings', booking, 'booking_id', booking_map)
            # Can safely append the ids now since they should be unique.
            proto_person.booking_ids.append(proto_booking.booking_id)

            if booking.arrest:
                proto_arrest = _populate_proto(
                    'arrests', booking.arrest, 'arrest_id', arrest_map)
                proto_booking.arrest_id = proto_arrest.arrest_id

            for charge in booking.charge:
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
