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
# ============================================================================
"""Converts an ingest_info proto Arrest to a persistence entity."""
from recidiviz.persistence import entities
from recidiviz.persistence.converter.converter_utils import normalize, \
    parse_date, fn, parse_external_id


def convert(proto) -> entities.Arrest:
    """Converts an ingest_info proto Arrest to a persistence entity."""
    new = entities.Arrest.builder()

    new.external_id = fn(parse_external_id, 'arrest_id', proto)
    new.date = fn(parse_date, 'date', proto)
    new.location = fn(normalize, 'location', proto)
    new.agency = fn(normalize, 'agency', proto)
    new.officer_name = fn(normalize, 'officer_name', proto)
    new.officer_id = fn(normalize, 'officer_id', proto)
    new.agency = fn(normalize, 'agency', proto)

    return new.build()
