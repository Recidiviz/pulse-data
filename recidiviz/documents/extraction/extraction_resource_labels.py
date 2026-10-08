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
"""Resource labels attached to the Vertex AI requests an LLM document-extraction
run makes, for cost attribution in the GCP billing export."""

import attr

from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.common.google_cloud.utils import format_resource_label

LLM_MODEL: str = "llm_model"


@attr.define(kw_only=True)
class ExtractionModelResourceLabel(ResourceLabel):
    key: str = attr.ib(default=LLM_MODEL)

    @classmethod
    def for_model_name(cls, model_name: str) -> "ExtractionModelResourceLabel":
        return cls(value=format_resource_label(model_name))


# TODO(OBT-42711) i think we will actually want to create this label
# based on the dag run configuration we use to denote a golden eval run,
# for now just adding a placeholder label to match what was existing in
# the golden eval script.
@attr.define(kw_only=True)
class GoldenEvaluationResourceLabel(ResourceLabel):
    key: str = attr.ib(default="job_type")
    value: str = attr.ib(default="golden-eval")
