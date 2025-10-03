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
"""
Contains a helper function to load root entities into the BigQueryEmulator
using (essentially) the same code in our ingest pipelines.
"""
from unittest.mock import patch

import apache_beam as beam
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity_class,
)
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.persistence_utils import NormalizedRootEntityT, RootEntityT
from recidiviz.pipelines.ingest.state import write_root_entities_to_bq
from recidiviz.pipelines.ingest.state.write_root_entities_to_bq import (
    WriteRootEntitiesToBQ,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.fake_bigquery import FakeWriteToBigQueryEmulator
from recidiviz.utils.types import assert_type


def write_root_entities_to_emulator(
    emulator_tc: BigQueryEmulatorTestCase,
    schema_mapping: dict[BigQueryAddress, list[bigquery.SchemaField]],
    people: list[RootEntityT] | list[NormalizedRootEntityT],
) -> None:
    """
    Writes the given root entities into the BigQueryEmulator
    using (essentially) the same code in our ingest pipelines.
    Requires a BigQueryEmulatorTestCase and a dictionary mapping
    the address of tables you would like loaded to the schemas of
    those tables.
    """
    if not any(people):
        raise ValueError(
            "Cannot pass empty list to 'people' argument of write_root_entities_to_emulator"
        )
    state_code = StateCode(people[0].state_code)
    root_entity_type = type(people[0])
    dataset = (
        STATE_BASE_DATASET
        if "normalized" not in root_entity_type.get_entity_name().lower()
        else NORMALIZED_STATE_DATASET
    )
    for address, schema in schema_mapping.items():
        emulator_tc.create_mock_table(address, schema)
    with patch(
        f"{write_root_entities_to_bq.__name__}.WriteToBigQuery",
        FakeWriteToBigQueryEmulator.get_mock_write_to_big_query_constructor(
            emulator_tc
        ),
    ):
        entities_module_context = entities_module_context_for_entity_class(
            root_entity_type
        )
        pipeline = create_test_pipeline()
        _ = (
            pipeline
            | beam.Create(
                [
                    assert_type(
                        set_backedges(person, entities_module_context), root_entity_type
                    )
                    for person in people
                ]
            )
            | WriteRootEntitiesToBQ(
                state_code=state_code,
                output_dataset=dataset,
                output_table_ids=[addr.table_id for addr in schema_mapping],
                entities_module=entities_module_context.entities_module(),
            )
        )
        pipeline.run()
