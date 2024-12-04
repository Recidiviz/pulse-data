# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines all Justice Counts metrics for superagencies."""

from recidiviz.justice_counts.dimensions.superagency import (
    ExpenseType,
    FundingType,
    StaffType,
)
from recidiviz.justice_counts.includes_excludes.common import (
    CountyOrMunicipalAppropriationIncludesExcludes,
    GrantsIncludesExcludes,
    StaffIncludesExcludes,
    StateAppropriationIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.superagency import (
    SuperagencyExpensesTimeframeIncludesExcludes,
    SuperagencyExpensesTypeIncludesExcludes,
    SuperagencyFacilitiesEquipmentExpensesIncludesExcludes,
    SuperagencyFilledVacantStaffTypeIncludesExcludes,
    SuperagencyFundingPurposeIncludesExcludes,
    SuperagencyFundingTimeframeIncludesExcludes,
    SuperagencyPersonnelExpensesIncludesExcludes,
    SuperagencyTrainingExpensesIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    IncludesExcludesSet,
    MetricCategory,
    MetricDefinition,
)
from recidiviz.justice_counts.utils.constants import MetricUnit
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)

funding = MetricDefinition(
    system=System.SUPERAGENCY,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of the agency.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.INSTANT,
    includes_excludes=[
        IncludesExcludesSet(
            members=SuperagencyFundingTimeframeIncludesExcludes,
            description="Funding timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=SuperagencyFundingPurposeIncludesExcludes,
            description="Funding purpose",
        ),
    ],
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_description={
                FundingType.STATE_APPROPRIATIONS: "The amount of funding appropriated by the state for the operation and maintenance of the agency.",
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATIONS: "The amount of funding appropriated by counties or municipalities for the operation and maintenance of the agency.",
                FundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for the operation and maintenance of the agency.",
                FundingType.OTHER: "The amount of funding to be used for the operation and maintenance of the agency that is not appropriations from the state, appropriations from counties or cities, or funding from grants.",
                FundingType.UNKNOWN: "The amount of funding for the operation and maintenance of the agency for which the source is not known.",
            },
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATIONS: [
                    IncludesExcludesSet(
                        members=StateAppropriationIncludesExcludes,
                        excluded_set={
                            StateAppropriationIncludesExcludes.PROPOSED,
                            StateAppropriationIncludesExcludes.PRELIMINARY,
                            StateAppropriationIncludesExcludes.GRANTS,
                        },
                    ),
                ],
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATIONS: [
                    IncludesExcludesSet(
                        members=CountyOrMunicipalAppropriationIncludesExcludes,
                        excluded_set={
                            CountyOrMunicipalAppropriationIncludesExcludes.PROPOSED,
                            CountyOrMunicipalAppropriationIncludesExcludes.PRELIMINARY,
                        },
                    ),
                ],
                FundingType.GRANTS: [
                    IncludesExcludesSet(
                        members=GrantsIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)


expenses = MetricDefinition(
    system=System.SUPERAGENCY,
    metric_type=MetricType.EXPENSES,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Expenses",
    description="The amount spent on the operation and maintenance of the agency.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.INSTANT,
    includes_excludes=[
        IncludesExcludesSet(
            members=SuperagencyExpensesTimeframeIncludesExcludes,
            description="Expenses timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=SuperagencyExpensesTypeIncludesExcludes,
            description="Expense purpose",
        ),
    ],
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=False,
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount spent on employing personnel involved in the operation and maintenance of the agency.",
                ExpenseType.TRAINING: "The amount spent on the training of personnel involved in the operation and maintenance of the agency.",
                ExpenseType.FACILITIES_AND_EQUIPMENT: "The amount spent on the purchase and use of the physical plant and property owned and operated by the agency.",
                ExpenseType.OTHER: "The amount spent on other costs relating to the operation and maintenance of the agency that are not personnel, training, or facilities and equipment expenses.",
                ExpenseType.UNKNOWN: "The amount spent on the operation and maintenance of the agency for a purpose that is not known.",
            },
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: [
                    IncludesExcludesSet(
                        members=SuperagencyPersonnelExpensesIncludesExcludes,
                        excluded_set={
                            SuperagencyPersonnelExpensesIncludesExcludes.COMPANIES_CONTRACTED
                        },
                    ),
                ],
                ExpenseType.TRAINING: [
                    IncludesExcludesSet(
                        members=SuperagencyTrainingExpensesIncludesExcludes,
                        excluded_set={
                            SuperagencyTrainingExpensesIncludesExcludes.FREE_COURSES
                        },
                    ),
                ],
                ExpenseType.FACILITIES_AND_EQUIPMENT: [
                    IncludesExcludesSet(
                        members=SuperagencyFacilitiesEquipmentExpensesIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)


staff = MetricDefinition(
    system=System.SUPERAGENCY,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the agency.",
    unit=MetricUnit.FULL_TIME,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=StaffIncludesExcludes,
            excluded_set={
                StaffIncludesExcludes.VOLUNTEER,
                StaffIncludesExcludes.INTERN,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_description={
                StaffType.FILLED: "The number of full-time equivalent positions that are currently filled.",
                StaffType.VACANT: "The number of full-time equivalent positions that are budgeted but not currently filled.",
            },
            dimension_to_includes_excludes={
                StaffType.FILLED: [
                    IncludesExcludesSet(
                        members=SuperagencyFilledVacantStaffTypeIncludesExcludes,
                        excluded_set={
                            SuperagencyFilledVacantStaffTypeIncludesExcludes.VACANT
                        },
                    ),
                ],
                StaffType.VACANT: [
                    IncludesExcludesSet(
                        members=SuperagencyFilledVacantStaffTypeIncludesExcludes,
                        excluded_set={
                            SuperagencyFilledVacantStaffTypeIncludesExcludes.FILLED
                        },
                    ),
                ],
            },
        )
    ],
)
