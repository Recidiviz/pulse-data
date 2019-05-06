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
"""Tests for schema_utils.py."""

from recidiviz.persistence.database.schema_utils import get_all_table_classes,\
    get_aggregate_table_classes


def test_get_all_table_classes():
    assert _classes_to_names(get_all_table_classes()) == \
           ['CaFacilityAggregate',
            'DcFacilityAggregate',
            'FlCountyAggregate',
            'FlFacilityAggregate',
            'GaCountyAggregate',
            'HiFacilityAggregate',
            'KyFacilityAggregate',
            'NyFacilityAggregate',
            'PaCountyPreSentencedAggregate',
            'PaFacilityPopAggregate',
            'TnFacilityAggregate',
            'TnFacilityFemaleAggregate',
            'TxCountyAggregate',
            'Arrest',
            'ArrestHistory',
            'Bond',
            'BondHistory',
            'Booking',
            'BookingHistory',
            'Charge',
            'ChargeHistory',
            'Hold',
            'HoldHistory',
            'Person',
            'PersonHistory',
            'Sentence',
            'SentenceHistory',
            'SentenceRelationship',
            'SentenceRelationshipHistory',
            'StatePerson',
            'StatePersonHistory']


def test_get_aggregate_table_classes():
    assert _classes_to_names(get_aggregate_table_classes()) == \
           ['CaFacilityAggregate',
            'DcFacilityAggregate',
            'FlCountyAggregate',
            'FlFacilityAggregate',
            'GaCountyAggregate',
            'HiFacilityAggregate',
            'KyFacilityAggregate',
            'NyFacilityAggregate',
            'PaCountyPreSentencedAggregate',
            'PaFacilityPopAggregate',
            'TnFacilityAggregate',
            'TnFacilityFemaleAggregate',
            'TxCountyAggregate']


def _classes_to_names(table_classes):
    return [cls.__name__ for cls in list(table_classes)]
