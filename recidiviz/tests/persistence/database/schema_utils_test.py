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
from typing import List

from recidiviz.persistence.database.schema_utils import get_all_table_classes,\
    get_aggregate_table_classes

from recidiviz.persistence.database.schema.aggregate import (
    schema as aggregate_schema
)
from recidiviz.persistence.database.schema.county import (
    schema as county_schema
)
from recidiviz.persistence.database.schema.state import schema as state_schema


def test_get_all_table_classes():
    aggregate_table_names = [
        'CaFacilityAggregate',
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
    ]
    county_table_names = [
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
    ]
    state_table_names = [
        'StateAssessment',
        'StateAssessmentHistory',
        'StatePerson',
        'StatePersonHistory',
        'StatePersonEthnicity',
        'StatePersonEthnicityHistory',
        'StatePersonExternalId',
        'StatePersonExternalIdHistory',
        'StatePersonRace',
        'StatePersonRaceHistory',
        'StateSentenceGroup',
        'StateSentenceGroupHistory',
    ]

    expected_qualified_names = \
        _prefix_module_name(aggregate_schema.__name__,
                            aggregate_table_names) + \
        _prefix_module_name(county_schema.__name__,
                            county_table_names) + \
        _prefix_module_name(state_schema.__name__,
                            state_table_names)

    assert sorted(_classes_to_qualified_names(
        get_all_table_classes())) == sorted(expected_qualified_names)


def test_get_aggregate_table_classes():
    aggregate_table_names = [
        'CaFacilityAggregate',
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
    ]

    assert _classes_to_qualified_names(get_aggregate_table_classes()) == \
        _prefix_module_name(aggregate_schema.__name__, aggregate_table_names)


def _prefix_module_name(module_name: str,
                        class_name_list: List[str]) -> List[str]:
    return [f'{module_name}.{class_name}' for class_name in class_name_list]


def _classes_to_qualified_names(table_classes):
    return [f'{cls.__module__}.{cls.__name__}' for cls in list(table_classes)]
