# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helper functions for triggering DAGs."""
import json
import logging
from typing import Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import pubsub_helper


def trigger_calculation_dag_pubsub(
    ingest_instance: DirectIngestInstance,
    state_code_filter: Optional[StateCode],
    sandbox_prefix: Optional[str] = None,
) -> None:
    """Sends a message to the PubSub topic to trigger the post-deploy CloudSQL to BQ
    refresh, which then will trigger the calculation DAG on completion."""

    logging.info(
        "Triggering the calc DAG with instance: [%s], state code filter: [%s], and sandbox_prefix: [%s].",
        ingest_instance.value,
        state_code_filter.value if state_code_filter else None,
        sandbox_prefix,
    )
    pubsub_helper.publish_message_to_topic(
        topic="v1.calculator.trigger_calculation_pipelines",
        message=json.dumps(
            {
                "state_code_filter": state_code_filter.value
                if state_code_filter
                else None,
                "ingest_instance": ingest_instance.value,
                "sandbox_prefix": sandbox_prefix,
            }
        ),
    )