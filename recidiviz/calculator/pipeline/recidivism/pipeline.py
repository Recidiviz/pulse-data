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
"""The recidivism calculation pipeline. See recidiviz/tools/run_sandbox_calculation_pipeline.py
for details on how to launch a local run.
"""

from __future__ import absolute_import

from recidiviz.calculator.pipeline.base_pipeline import BasePipeline, PipelineConfig
from recidiviz.calculator.pipeline.pipeline_type import PipelineType
from recidiviz.calculator.pipeline.recidivism import identifier, metric_producer
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
)
from recidiviz.persistence.entity.state import entities


class RecidivismPipeline(BasePipeline):
    """Defines the recidivism calculation pipeline."""

    def __init__(self) -> None:
        self.pipeline_config = PipelineConfig(
            pipeline_type=PipelineType.RECIDIVISM,
            identifier=identifier.RecidivismIdentifier(),
            metric_producer=metric_producer.RecidivismMetricProducer(),
            required_entities=[
                entities.StatePerson,
                entities.StatePersonRace,
                entities.StatePersonEthnicity,
                entities.StateIncarcerationPeriod,
                entities.StateSupervisionPeriod,
                entities.StatePersonExternalId,
            ],
            required_reference_tables=[PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME],
            state_specific_required_reference_tables={},
        )
        self.include_calculation_limit_args = False
