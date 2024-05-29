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
"""Classes for running all supplemental dataset calculation pipelines."""
import abc
from typing import Dict, List, Type

from google.cloud import bigquery

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import StateFilteredQueryProvider
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)


class SupplementalDatasetPipeline(BasePipeline[SupplementalPipelineParameters]):
    """Delegate for running a supplemental dataset pipeline."""

    @classmethod
    def parameters_type(cls) -> Type[SupplementalPipelineParameters]:
        return SupplementalPipelineParameters

    @classmethod
    def all_input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, StateFilteredQueryProvider]:
        view_builders = cls.input_reference_view_builders()
        return {
            vb.view_id: StateFilteredQueryProvider(
                original_query=vb.build(address_overrides=address_overrides),
                state_code_filter=state_code,
            )
            for vb in view_builders
        }

    # TODO(#29518): When we update this pipeline to start reading directly from
    #  us_xx_normalized_state, we will need to create reference queries from templates
    #  so we can dynamically hydrate the source tables datasets. When that happens we
    #  should no longer be pulling in BigQueryViewBuilder objects.
    @classmethod
    @abc.abstractmethod
    def input_reference_view_builders(cls) -> List[BigQueryViewBuilder]:
        """Returns a list of builders for views whose queries should be run to
        produce input data for the pipeline."""

    @classmethod
    @abc.abstractmethod
    def table_id(cls) -> str:
        """Table_id of the output table for the supplemental dataset. Must be overwritten
        by subclasses."""

    @classmethod
    @abc.abstractmethod
    def table_fields(cls) -> Dict[str, Type]:
        """Contains the field names and their corresponding field types for the
        supplemental dataset. Must be overwritten by subclasses."""

    @classmethod
    def bq_schema_for_table(cls) -> List[bigquery.SchemaField]:
        """Returns the necessary BigQuery schema for the table, which is a
        list of SchemaField objects containing the column name and value type for
        each field in |table_fields|."""
        return [
            schema_field_for_type(field_name=field_name, field_type=field_type)
            for field_name, field_type in cls.table_fields().items()
        ]
