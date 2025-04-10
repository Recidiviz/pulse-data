# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Classes that define a collections of aggregated metrics that will be deployed
together to the same dataset.
"""

import attr

from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.aggregated_metric_query_utils import (
    AggregatedMetricClassType,
)


@attr.define
class AggregatedMetricsCollectionPopulationConfig:
    """Defines a collections of aggregated metrics for a single population that will be
    deployed together in a collection of views to the specified dataset.
    """

    # Which dataset to deploy views to
    output_dataset_id: str

    # Which population to calculate metrics for
    population_type: MetricPopulationType

    # Which unit of analysis to calculate metrics for
    units_of_analysis: set[MetricUnitOfAnalysisType]

    # The configurations for the metrics we want to generate
    metrics: list[AggregatedMetric]

    def metrics_of_class(
        self,
        metric_class: AggregatedMetricClassType,
    ) -> list[AggregatedMetric]:
        return [metric for metric in self.metrics if isinstance(metric, metric_class)]


@attr.define
class AggregatedMetricsCollection:
    """Defines a collections of aggregated metrics that will be deployed together in a
    collection of views to the specified dataset.
    """

    # Which dataset to deploy views to
    output_dataset_id: str

    # Which metrics to calculate for each population type
    population_configs: dict[
        MetricPopulationType, AggregatedMetricsCollectionPopulationConfig
    ]

    # Which time periods to calculate the metrics for
    time_periods: list[MetricTimePeriodConfig]

    # A list of observation attributes that will be used to disaggregate all
    # metrics in the collection. If disaggregate_by_observation_attributes are
    # included, the output schema will include a column for every attribute in the list.
    disaggregate_by_observation_attributes: list[str] | None = attr.ib()

    # A tag that will be prepended to all view names for this metrics collection.
    # This tag may be used when we want to store more than one collection in the same
    # dataset and those collections have overlapping populations.
    collection_tag: str | None = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        time_periods_by_name: dict[str, MetricTimePeriodConfig] = {}
        for period in self.time_periods:
            if not period.config_name:
                raise ValueError(
                    f"Found MetricTimePeriodConfig for collection with no config name: "
                    f"{period}. All time periods for deployed collections must have a "
                    f"config_name."
                )
            if period.config_name in time_periods_by_name:
                raise ValueError(
                    f"Found multiple time periods defined in this collection with the "
                    f"same name [{period.config_name}]: {period} and "
                    f"{time_periods_by_name[period.config_name]}."
                )
            time_periods_by_name[period.config_name] = period

        for population_config in self.population_configs.values():
            if population_config.output_dataset_id != self.output_dataset_id:
                raise ValueError(
                    f"Output dataset [{population_config.output_dataset_id}] for "
                    f"population [{population_config.population_type.value}] does not "
                    f"match expected output_dataset_id [{self.output_dataset_id}]."
                )

    @classmethod
    def build(
        cls,
        output_dataset_id: str,
        time_periods: list[MetricTimePeriodConfig],
        unit_of_analysis_types_by_population_type: dict[
            MetricPopulationType, list[MetricUnitOfAnalysisType]
        ],
        metrics_by_population_type: dict[MetricPopulationType, list[AggregatedMetric]],
        collection_tag: str | None = None,
        disaggregate_by_observation_attributes: list[str] | None = None,
    ) -> "AggregatedMetricsCollection":
        return AggregatedMetricsCollection(
            output_dataset_id=output_dataset_id,
            collection_tag=collection_tag,
            time_periods=time_periods,
            population_configs={
                population_type: AggregatedMetricsCollectionPopulationConfig(
                    output_dataset_id=output_dataset_id,
                    population_type=population_type,
                    units_of_analysis=set(
                        unit_of_analysis_types_by_population_type[population_type]
                    ),
                    metrics=metrics_by_population_type[population_type],
                )
                for population_type in MetricPopulationType
                if population_type in unit_of_analysis_types_by_population_type
                and population_type in metrics_by_population_type
            },
            disaggregate_by_observation_attributes=disaggregate_by_observation_attributes,
        )
