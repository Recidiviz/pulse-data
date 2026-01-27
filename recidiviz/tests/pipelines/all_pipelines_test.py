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
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.terraform_managed.config import (
    GCS_BACKED_TABLES_DATASET,
)
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonRace,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.pipelines.ingest.state.pipeline import StateIngestPipeline
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.pipeline import (
    UsIxCaseNoteExtractedEntitiesPipeline,
)
from recidiviz.pipelines.supplemental.us_me_snoozed_opportunities.pipeline import (
    UsMeSnoozedOpportunitiesPipeline,
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

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-456")
    )
    def test_input_reference_queries_have_valid_parents(self) -> None:
        """Require that all reference queries used by the calc pipelines only query an
        allowed subset of source tables. We don't want our reference queries to be
        querying other views because those are not updated before pipelines run
        post-deploy.
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
            GCS_BACKED_TABLES_DATASET,
            STATIC_REFERENCE_TABLES_DATASET,
        }

        for pipeline in collect_all_pipeline_classes():
            for state_code in get_existing_direct_ingest_states():
                if (
                    issubclass(pipeline, UsIxCaseNoteExtractedEntitiesPipeline)
                    and state_code != StateCode.US_IX
                ):
                    continue
                if (
                    issubclass(pipeline, UsMeSnoozedOpportunitiesPipeline)
                    and state_code != StateCode.US_ME
                ):
                    continue

                allowed_parent_datasets = all_pipelines_allowed_datasets
                if issubclass(pipeline, (MetricPipeline, SupplementalDatasetPipeline)):
                    allowed_parent_datasets.add(
                        normalized_state_dataset_for_state_code(state_code)
                    )
                elif issubclass(pipeline, StateIngestPipeline):
                    # No extra allowed datasets
                    pass
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
                            f"dataset that is not allowed: {parent.to_str()}. Allowed "
                            f"parent datasets: {allowed_parent_datasets}."
                        )


class TestPipelineValidations(unittest.TestCase):
    """Tests that specific pipelines are set up correctly."""

    def test_all_pipelines_are_validated(self) -> None:
        pipeline_classes = collect_all_pipeline_classes()

        for pipeline_class in pipeline_classes:
            if issubclass(pipeline_class, SupplementalDatasetPipeline):
                self.assertTrue("SUPPLEMENTAL" in pipeline_class.pipeline_name())
            elif issubclass(pipeline_class, MetricPipeline):
                default_entities: Set[Type[Entity]] = {
                    NormalizedStatePerson,
                    NormalizedStatePersonRace,
                }
                self.assertFalse(len(pipeline_class.required_entities()) == 0)
                missing_default_entities = default_entities.difference(
                    set(pipeline_class.required_entities())
                )
                self.assertTrue(len(missing_default_entities) == 0)
