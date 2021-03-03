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
# ============================================================================
"""Converts an ingest_info proto Arrest to a persistence entity."""
from recidiviz.common.str_field_utils import normalize, normalize_truncated, parse_date
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
)


def convert(proto) -> entities.Arrest:
    """Converts an ingest_info proto Arrest to a persistence entity."""
    new = entities.Arrest.builder()

    new.external_id = fn(parse_external_id, "arrest_id", proto)
    new.arrest_date = fn(parse_date, "arrest_date", proto)
    new.location = fn(normalize, "location", proto)
    new.agency = fn(normalize, "agency", proto)
    new.officer_name = fn(normalize, "officer_name", proto)
    new.officer_id = fn(normalize, "officer_id", proto)
    new.agency = fn(normalize_truncated, "agency", proto)

    return new.build()
