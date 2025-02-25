# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""This class implements tests for Justice Counts MetricDefinition class."""

from unittest import TestCase

from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.metric_definition import Context
from recidiviz.justice_counts.metrics.metric_registry import METRICS


class TestAggregatedDimension(TestCase):
    """Implements tests for the Justice Counts AggregatedDimension class."""

    def test_dimension_to_contexts(self) -> None:
        # Has an OTHER and UNKNOWN member
        if (
            law_enforcement.reported_crime.aggregated_dimensions is not None
        ):  # pylint: disable=unsubscriptable-object
            agg_dim = law_enforcement.reported_crime.aggregated_dimensions[0]
            self.assertEqual(
                agg_dim.dimension_to_contexts,
                {
                    agg_dim.dimension.PERSON: [  # type: ignore[attr-defined]
                        Context(
                            key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                            label="If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                        )
                    ],
                    agg_dim.dimension.PROPERTY: [  # type: ignore[attr-defined]
                        Context(
                            key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                            label="If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                        )
                    ],
                    agg_dim.dimension.DRUG: [  # type: ignore[attr-defined]
                        Context(
                            key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                            label="If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                        )
                    ],
                    agg_dim.dimension.PUBLIC_ORDER: [  # type: ignore[attr-defined]
                        Context(
                            key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                            label="If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                        )
                    ],
                    agg_dim.dimension.OTHER: [  # type: ignore[attr-defined]
                        Context(
                            key=ContextKey.ADDITIONAL_CONTEXT,
                            label="Please describe what data is being included in this breakdown.",
                        )
                    ],
                    agg_dim.dimension.UNKNOWN: [  # type: ignore[attr-defined]
                        Context(
                            key=ContextKey.ADDITIONAL_CONTEXT,
                            label="Please describe what data is being included in this breakdown.",
                        )
                    ],
                },
            )

        # Does not have an OTHER or UNKNOWN member
        if law_enforcement.staff.aggregated_dimensions is not None:
            # pylint: disable=unsubscriptable-object
            agg_dim = law_enforcement.staff.aggregated_dimensions[1]
            self.assertEqual(len(agg_dim.dimension_to_contexts), 24)  # type: ignore[arg-type]
            self.assertEqual(
                agg_dim.dimension_to_contexts[  # type: ignore[index]
                    agg_dim.dimension.HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE  # type: ignore[attr-defined]
                ],
                [
                    Context(
                        key=ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
                        label="If the listed categories do not adequately describe your breakdown, please describe additional data elements included in your agency’s definition.",
                    )
                ],
            )


class TestMetricDefinition(TestCase):
    """Implements tests for the Justice Counts MetricDefinition class."""

    def test_metric_keys_are_unique(self) -> None:
        metric_keys = [metric.key for metric in METRICS]
        self.assertEqual(len(metric_keys), len(set(metric_keys)))

    def test_law_enforcement_metrics(self) -> None:
        self.assertEqual(
            law_enforcement.funding.key,
            "LAW_ENFORCEMENT_FUNDING",
        )

    def test_additional_context(self) -> None:
        # Does not have metric level includes_excludes
        self.assertEqual(len(law_enforcement.reported_crime.contexts), 1)
        # Has metric level includes_excludes
        self.assertEqual(len(law_enforcement.funding.contexts), 1)
        self.assertEqual(
            law_enforcement.funding.contexts[0].key,
            ContextKey.INCLUDES_EXCLUDES_DESCRIPTION,
        )

    def test_unit_from_metric_type(self) -> None:
        self.assertEqual(law_enforcement.funding.metric_type.unit, "USD")
        self.assertEqual(law_enforcement.calls_for_service.metric_type.unit, "CALLS")
        self.assertEqual(law_enforcement.arrests.metric_type.unit, "ARRESTS")
        self.assertEqual(law_enforcement.staff.metric_type.unit, "PEOPLE")
        self.assertEqual(
            law_enforcement.civilian_complaints_sustained.metric_type.unit,
            "COMPLAINTS SUSTAINED",
        )
        self.assertEqual(
            law_enforcement.use_of_force_incidents.metric_type.unit,
            "USE OF FORCE INCIDENTS",
        )

    def test_display_name(self) -> None:
        self.assertEqual(
            GenderRestricted.display_name(),
            "Gender",
        )

        self.assertEqual(
            RaceAndEthnicity.display_name(),
            "Race / Ethnicities",
        )
