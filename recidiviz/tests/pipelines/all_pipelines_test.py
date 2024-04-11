# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the pipeline names."""
import unittest
from typing import Set, Type

from recidiviz.calculator.query.state.views.reference.reference_views import (
    REFERENCE_VIEW_BUILDERS,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.ingest.state.pipeline import StateIngestPipeline
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.normalization.comprehensive.pipeline import (
    ComprehensiveNormalizationPipeline,
)
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.utils.pipeline_run_utils import (
    collect_all_pipeline_classes,
    collect_all_pipeline_names,
)


class TestPipelineNames(unittest.TestCase):
    """Tests the names of all pipelines that can be run."""

    def test_collect_all_pipeline_names(self) -> None:
        """Tests that each pipeline has a unique pipeline_name."""
        pipeline_names = collect_all_pipeline_names()

        self.assertCountEqual(set(pipeline_names), pipeline_names)


class TestReferenceViews(unittest.TestCase):
    """Tests reference views are appropriately referenced."""

    def test_all_reference_views_in_dataset(self) -> None:
        """Asserts that all the reference views required by the pipelines are in the
        reference_views dataset."""
        pipelines = collect_all_pipeline_classes()

        all_required_reference_tables: Set[str] = set()

        for pipeline in pipelines:
            if issubclass(pipeline, StateIngestPipeline):
                continue
            self.assertTrue(
                hasattr(pipeline, "required_reference_tables")
                or hasattr(pipeline, "state_specific_required_reference_tables")
            )

            # TODO(#21376) Properly refactor once strategy for separate normalization is defined.
            if hasattr(pipeline, "required_reference_tables"):
                all_required_reference_tables.update(
                    {
                        table
                        for _, reference_tables in pipeline.required_reference_tables().items()
                        for table in reference_tables
                    }
                    if issubclass(pipeline, ComprehensiveNormalizationPipeline)
                    else set(pipeline.required_reference_tables())
                )

            if hasattr(pipeline, "state_specific_required_reference_tables"):
                all_required_reference_tables.update(
                    {
                        view
                        for _, state_dict in pipeline.state_specific_required_reference_tables().items()
                        for _, state_views in state_dict.items()
                        for view in state_views
                    }
                    if issubclass(pipeline, ComprehensiveNormalizationPipeline)
                    else {
                        view
                        for state_views in pipeline.state_specific_required_reference_tables().values()
                        for view in state_views
                    }
                )

        deployed_reference_view_names: Set[str] = {
            view_builder.view_id for view_builder in REFERENCE_VIEW_BUILDERS
        }

        self.assertEqual(
            set(),
            all_required_reference_tables.difference(deployed_reference_view_names),
        )


class TestPipelineValidations(unittest.TestCase):
    """Tests that specific pipelines are set up correctly."""

    def test_all_pipelines_are_validated(self) -> None:
        pipeline_classes = collect_all_pipeline_classes()

        for pipeline_class in pipeline_classes:
            if issubclass(pipeline_class, SupplementalDatasetPipeline):
                self.assertTrue("SUPPLEMENTAL" in pipeline_class.pipeline_name())
            elif issubclass(pipeline_class, ComprehensiveNormalizationPipeline):
                self.assertTrue("NORMALIZATION" in pipeline_class.pipeline_name())
            elif issubclass(pipeline_class, MetricPipeline):
                default_entities: Set[Type[Entity]] = {
                    entities.StatePerson,
                    entities.StatePersonRace,
                    entities.StatePersonEthnicity,
                }
                self.assertFalse(len(pipeline_class.required_entities()) == 0)
                missing_default_entities = default_entities.difference(
                    set(pipeline_class.required_entities())
                )
                self.assertTrue(len(missing_default_entities) == 0)
