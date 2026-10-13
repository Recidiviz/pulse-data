# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Entrypoint that runs one synchronous LLM document-extraction job to completion"""

import argparse

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_resources.platform_resource_labels import StateCodeResourceLabel
from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.document_extraction_processor import (
    DocumentExtractionProcessor,
)
from recidiviz.documents.extraction.extraction_resource_labels import (
    ExtractionModelResourceLabel,
)
from recidiviz.documents.extraction.llm_client.vertex_ai_sync_llm_client import (
    VertexAISyncLLMClient,
)
from recidiviz.documents.extraction.llm_extraction_job_manager import (
    LLMExtractionJobManager,
)
from recidiviz.documents.extraction.llm_extraction_job_tasks import (
    run_sync_extraction_job,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_extractor_config,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface


class LLMDocumentExtractionSyncJobEntrypoint(EntrypointInterface):
    """Entrypoint that runs one synchronous LLM document-extraction job to
    completion."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--state_code",
            type=StateCode,
            required=True,
            help="The state whose extraction job to run.",
        )
        parser.add_argument(
            "--extractor_collection_name",
            required=True,
            help="The extractor collection whose job to run (e.g. "
            "CASE_NOTE_EMPLOYMENT_INFO).",
        )
        parser.add_argument(
            "--job_id",
            required=True,
            help="The id of the already-created extraction job to run to "
            "completion.",
        )
        return parser

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        config = get_extractor_config(
            state_code=args.state_code,
            collection_name=args.extractor_collection_name,
        )
        job_manager = LLMExtractionJobManager()
        summary = run_sync_extraction_job(
            config=config,
            job_id=args.job_id,
            job_manager=job_manager,
            processor=DocumentExtractionProcessor(
                config=config,
                results_sandbox_prefix=None,
                document_store_sandbox=None,
                labels=_build_billing_labels(config=config),
                bq_client=BigQueryClientImpl(),
                fs=GcsfsFactory.build(),
                sync_client=VertexAISyncLLMClient(model_config=config.model_config),
                job_manager=job_manager,
            ),
        )
        if summary is not None:
            summary.log()


def _build_billing_labels(*, config: LLMExtractorConfig) -> list[ResourceLabel]:
    """Returns the billing labels attached to every Vertex request this job makes,
    for cost attribution.

    TODO(OBT-46622): When the extraction DAG is written, attach the
    platform_orchestration_mechanism label for the DAG that schedules this job.

    TODO(OBT-42711) I'm again deferring the actual labels design to a later time,
    and just adding a placeholder here.
    """
    return [
        StateCodeResourceLabel(value=config.state_code.value.lower()),
        ExtractionModelResourceLabel.for_model_name(config.model_config.name),
    ]
