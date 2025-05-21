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
"""Controller for deleting rows associated with a specific file_id from big query raw tables."""
import logging
from typing import Dict, List

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation


@attr.define
class DeleteBQRawTableRowsController:
    """Controller for deleting rows for a state_code/ingest_instance from big query raw tables."""

    bq_client: BigQueryClientImpl = attr.ib(
        validator=attr.validators.instance_of(BigQueryClientImpl)
    )
    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    ingest_instance: DirectIngestInstance = attr.ib(
        validator=attr.validators.instance_of(DirectIngestInstance)
    )
    dry_run: bool = attr.ib(validator=attr_validators.is_bool)
    skip_prompts: bool = attr.ib(validator=attr_validators.is_bool)

    @property
    def dataset_id(self) -> str:
        return raw_tables_dataset_for_region(self.state_code, self.ingest_instance)

    def run(self, file_tag_to_file_ids_to_delete: Dict[str, List[int]]) -> None:
        """For the class's state_code/ingest_instance, this method will delete the rows from a file
        tag's raw table where the file_id is in the list of file_ids to delete, using the file_tag_to_file_ids_to_delete
        mapping to determine which rows to delete."""
        if self.dry_run:
            logging.info(
                "[DRY RUN] Would have deleted rows from dataset [%s] for files: %s",
                self.dataset_id,
                file_tag_to_file_ids_to_delete,
            )
            return

        if not self.skip_prompts:
            prompt_for_confirmation(
                f"This operation will delete rows from dataset [{self.dataset_id}] for files: {file_tag_to_file_ids_to_delete}. ",
                dry_run=self.dry_run,
            )

        for file_tag, file_ids in file_tag_to_file_ids_to_delete.items():
            self.bq_client.delete_from_table_async(
                address=BigQueryAddress(dataset_id=self.dataset_id, table_id=file_tag),
                filter_clause=f"WHERE file_id IN ({', '.join(map(str, file_ids))})",
            )
        logging.info(
            "Deleted rows from dataset [%s] for files: %s",
            self.dataset_id,
            file_tag_to_file_ids_to_delete,
        )
