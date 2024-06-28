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
"""Contains source table definitions for us_mi validation oneoffs."""
from recidiviz.common.constants.states import StateCode
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableCollectionValidationConfig,
)
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.external_data.regions.us_mi.cb_971_report_supervision_unified import (
    build_cb_971_supervision_report_schemas,
)
from recidiviz.validation.views.external_data.regions.us_mi.cb_971_report_unified import (
    build_cb_971_report_schemas,
)
from recidiviz.validation.views.external_data.regions.us_mi.incarceration_population_person_level import (
    build_orc_report_schemas,
)


def collect_duplicative_us_mi_validation_oneoffs() -> list[SourceTableCollection]:
    """There are some validation oneoffs that are made up of hundreds of source tables, so we define them in code,
    rather than duplicating the YAML files.
    """
    source_tables = [
        *build_cb_971_report_schemas(),
        *build_cb_971_supervision_report_schemas(),
    ]

    dataset_id = dataset_config.validation_oneoff_dataset_for_state(StateCode.US_MI)
    collection = SourceTableCollection(
        dataset_id=dataset_id,
        update_config=SourceTableCollectionUpdateConfig.unmanaged(),
        source_tables_by_address={
            source_table.address: source_table for source_table in source_tables
        },
    )

    orc_report_collection = SourceTableCollection(
        dataset_id=dataset_id,
        update_config=SourceTableCollectionUpdateConfig.unmanaged(),
        validation_config=SourceTableCollectionValidationConfig(
            only_check_required_columns=True
        ),
        source_tables_by_address={
            source_table.address: source_table
            for source_table in build_orc_report_schemas()
        },
    )

    return [
        collection,
        orc_report_collection,
    ]
