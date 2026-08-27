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
"""Maps each source table update group to the Airflow DAG that owns it."""

from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.utils.airflow_dag import AirflowDag

# The DAG whose schema-update task owns each update group. Each SourceTableUpdateGroup
# must be owned by exactly one DAG, and no DAG may own more than one group.
_DAG_BY_UPDATE_GROUP: dict[SourceTableUpdateGroup, AirflowDag] = {
    SourceTableUpdateGroup.CALC: AirflowDag.CALCULATION,
    SourceTableUpdateGroup.LLM_DOCUMENT_EXTRACTION: AirflowDag.LLM_DOCUMENT_EXTRACTION,
    SourceTableUpdateGroup.IDENTITY_INGEST: AirflowDag.IDENTITY_INGEST,
    SourceTableUpdateGroup.RAW_DATA_IMPORT: AirflowDag.RAW_DATA_IMPORT,
}


def source_table_update_group_for_dag_id(
    dag_id: str, *, project_id: str
) -> SourceTableUpdateGroup:
    """Returns the source table update group owned by the DAG with this id, raising if
    the DAG does not own one."""
    owning_dag = AirflowDag.from_dag_id(dag_id, project_id=project_id)
    for group, dag in _DAG_BY_UPDATE_GROUP.items():
        if dag is owning_dag:
            return group
    raise ValueError(f"No SourceTableUpdateGroup is owned by DAG [{dag_id}].")
