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
from unittest.mock import MagicMock, patch

from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    REFERENCE_VIEWS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
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
from recidiviz.tools.find_unused_bq_views import get_all_pipeline_input_views


class TestPipelineNames(unittest.TestCase):
    """Tests the names of all pipelines that can be run."""

    def test_collect_all_pipeline_names(self) -> None:
        """Tests that each pipeline has a unique pipeline_name."""
        pipeline_names = collect_all_pipeline_names()

        self.assertCountEqual(set(pipeline_names), pipeline_names)


class TestReferenceViews(unittest.TestCase):
    """Tests reference views are appropriately referenced."""

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-456")
    )
    def test_all_reference_views_in_dataset(self) -> None:
        """Asserts that all the reference views required by the pipelines are in the
        reference_views dataset."""
        for state_code in get_existing_direct_ingest_states():
            for view_address in get_all_pipeline_input_views(
                state_code, address_overrides=None
            ):
                self.assertEqual(
                    REFERENCE_VIEWS_DATASET,
                    view_address.dataset_id,
                    f"Found view [{view_address.to_str()}] that is referenced by "
                    f"pipelines but which does not live in the reference_views dataset.",
                )

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-456")
    )
    def test_input_reference_views_have_valid_parents(self) -> None:
        """Require that all view builder queries for view builders referenced by the
        our calc pipelines only query an allowed subset of source tables. We don't want
        our reference queries to be querying other views because those are not updated
        before pipelines run post-deploy.
        """
        all_pipelines_allowed_datasets = {
            *{
                raw_latest_views_dataset_for_region(
                    state_code=state_code, instance=DirectIngestInstance.PRIMARY
                )
                for state_code in get_existing_direct_ingest_states()
            },
            *{
                raw_tables_dataset_for_region(
                    state_code=state_code, instance=DirectIngestInstance.PRIMARY
                )
                for state_code in get_existing_direct_ingest_states()
            },
            EXTERNAL_REFERENCE_DATASET,
            STATIC_REFERENCE_TABLES_DATASET,
        }

        for pipeline in collect_all_pipeline_classes():
            for state_code in get_existing_direct_ingest_states():
                if issubclass(pipeline, ComprehensiveNormalizationPipeline):
                    allowed_parent_datasets = {
                        *all_pipelines_allowed_datasets,
                        STATE_BASE_DATASET,
                    }
                elif issubclass(
                    pipeline, (MetricPipeline, SupplementalDatasetPipeline)
                ):
                    allowed_parent_datasets = {
                        *all_pipelines_allowed_datasets,
                        NORMALIZED_STATE_DATASET,
                    }
                elif issubclass(pipeline, StateIngestPipeline):
                    allowed_parent_datasets = all_pipelines_allowed_datasets
                else:
                    raise ValueError(f"Unexpected pipeline type [{type(pipeline)}]")

                for (
                    provider_name,
                    provider,
                ) in pipeline.all_input_reference_query_providers(
                    state_code=state_code,
                    address_overrides=None,
                ).items():
                    parents = provider.parent_tables
                    for parent in parents:
                        if parent.dataset_id in allowed_parent_datasets:
                            continue
                        raise ValueError(
                            f"Found reference view builder [{provider_name}] for "
                            f"pipeline [{pipeline.__name__}] referencing a table in a "
                            f"dataset that is not allowed: {parent.to_str()}."
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
