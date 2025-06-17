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
"""Code for building LookML dashboards for raw data tables and writing them to a file.
Used inside person_details_lookml_writer
"""
import os
from collections import defaultdict
from typing import Dict, List

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.looker.lookml_dashboard import LookMLDashboard
from recidiviz.looker.lookml_dashboard_element import (
    LookMLDashboardElement,
    LookMLElementType,
    LookMLListen,
    LookMLNoteDisplayType,
    LookMLSort,
)
from recidiviz.looker.lookml_dashboard_filter import (
    LookMLDashboardFilter,
    LookMLFilterType,
    LookMLFilterUIConfig,
    LookMLFilterUIDisplay,
    LookMLFilterUIType,
)
from recidiviz.looker.lookml_dashboard_parameter import (
    LookMLDashboardLayoutType,
    LookMLDashboardParameter,
)
from recidiviz.looker.lookml_explore import LookMLExplore
from recidiviz.looker.lookml_explore_parameter import ExploreParameterJoin
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    RAW_DATA_UP_TO_DATE_VIEWS_OPTION,
    VIEW_TYPE_PARAM_NAME,
    _generate_shared_fields_view,
    raw_field_name_for_column,
)
from recidiviz.tools.looker.script_helpers import remove_lookml_files_from

ELEMENT_WIDTH = 24
ELEMENT_HEIGHT = 6
VIEW_TYPE_FILTER_NAME = "View Type"


def _gen_real_dash_from_template(
    template: LookMLDashboard,
    state_code: StateCode,
    staging: bool,
) -> LookMLDashboard:
    """
    Return a copy of the given template dashboard
    converted to a prod or staging dashboard by stripping down the elements
    and filters to only have a name and model.
    """
    dashboard_suffix = "Staging" if staging else "Prod"
    state_abbrev = state_code.value.lower()
    state_name = state_code.get_state().name
    model_name = "recidiviz-staging" if staging else "recidiviz-123"
    return attr.evolve(
        template,
        parameters=[
            LookMLDashboardParameter.title(
                f"{state_name} Raw Data Person Details {dashboard_suffix}"
            ),
        ],
        load_configuration_wait=False,
        extension_required=False,
        extended_dashboard=f"{state_abbrev}_raw_data_person_details_template",
        dashboard_name=f"{state_abbrev}_raw_data_person_details_{dashboard_suffix.lower()}",
        elements=[
            LookMLDashboardElement(name=e.name, model=model_name)
            for e in template.elements
        ],
        filters=[
            LookMLDashboardFilter(name=f.name, model=model_name)
            for f in template.filters
        ],
    )


def _generate_dashboard_template(
    state_code: StateCode,
    region_config: DirectIngestRegionRawFileConfig,
    state_explore: LookMLExplore,
    views_by_file_tag: Dict[str, LookMLView],
) -> LookMLDashboard:
    """
    Return an Dashboard for the given state code that has basic information:
    an extension: required field, fitting description and group label,
    and the given name of the primary person table for that state,
    which is used as the base view name for the Dashboard.
    """
    state_abbrev = state_code.value.lower()
    dashboard_name = f"{state_abbrev}_raw_data_person_details_template"
    state_name = state_code.get_state().name

    all_tables = region_config.raw_file_configs
    filters = _generate_filters_for_state(
        state_abbrev,
        state_explore,
        sorted(list(all_tables.values()), key=lambda c: c.file_tag),
        views_by_file_tag,
    )

    elements = _generate_elements_for_state(
        state_explore,
        views_by_file_tag,
        region_config,
    )

    return LookMLDashboard(
        dashboard_name=dashboard_name,
        parameters=[
            LookMLDashboardParameter.title(
                f"{state_name} Latest Raw Data Person Details Template"
            ),
            LookMLDashboardParameter.description(
                f"For examining individuals in {state_code.value}'s raw data tables"
            ),
            LookMLDashboardParameter.layout(LookMLDashboardLayoutType.NEWSPAPER),
        ],
        load_configuration_wait=True,
        extension_required=True,
        filters=filters,
        elements=elements,
    )


def _generate_filters_for_state(
    state_code: str,
    state_explore: LookMLExplore,
    configs: List[DirectIngestRawFileConfig],
    views_by_file_tag: Dict[str, LookMLView],
) -> List[LookMLDashboardFilter]:
    """
    Return a list of the filters for a state raw data dashboard:
    one View Type filter and one filter per distinct external_id_type
    """
    filters = []

    if not state_explore.view_name:
        raise ValueError(
            f"State raw data explore for {state_code} should have a view name defined"
        )

    # "View Type" filter
    filters.append(
        LookMLDashboardFilter(
            name=VIEW_TYPE_FILTER_NAME,
            title=VIEW_TYPE_FILTER_NAME,
            type=LookMLFilterType.FIELD_FILTER,
            default_value=RAW_DATA_UP_TO_DATE_VIEWS_OPTION.replace("_", "^_"),
            allow_multiple_values=False,
            required=True,
            ui_config=LookMLFilterUIConfig(
                type=LookMLFilterUIType.DROPDOWN_MENU,
                display=LookMLFilterUIDisplay.INLINE,
            ),
            explore=state_explore.non_template_name,
            field=f"{state_explore.view_name}.{VIEW_TYPE_PARAM_NAME}",
        )
    )
    views_in_explore = [state_explore.view_name] + [
        p.view_name
        for p in state_explore.parameters
        if isinstance(p, ExploreParameterJoin)
    ]
    for config in configs:
        # Only add filters for fields that are included in the explore
        view = views_by_file_tag.get(config.file_tag)
        if not view or view.view_name not in views_in_explore:
            continue

        for col in config.get_primary_external_id_cols():
            if not col.external_id_type:
                raise ValueError(
                    f"Column {col.name} marked as primary does not have an external id type specified."
                )
            name = col.external_id_type
            filters.append(
                LookMLDashboardFilter(
                    name=name,
                    title=name,
                    type=LookMLFilterType.FIELD_FILTER,
                    default_value='""',
                    allow_multiple_values=True,
                    required=False,
                    ui_config=LookMLFilterUIConfig(
                        type=LookMLFilterUIType.TAG_LIST,
                        display=LookMLFilterUIDisplay.POPOVER,
                    ),
                    explore=state_explore.non_template_name,
                    field=view.qualified_name_for_field(col.name),
                )
            )

    return filters


def _get_sort_cols(file_config: DirectIngestRawFileConfig) -> List[str]:
    first_datetime_col: List[str] = []
    # Get the first datetime column with valid parsers
    for col in file_config.current_columns:
        if col.is_datetime and col.datetime_sql_parsers:
            # For datetime columns, there is no existing lookml field
            # with the same name as the column, so we need to use the raw
            # field name that we created in the view generator
            return [raw_field_name_for_column(col)]
        if not first_datetime_col and col.is_datetime:
            first_datetime_col = [raw_field_name_for_column(col)]

    # Otherwise, the first datetime column
    if first_datetime_col:
        return first_datetime_col

    # Otherwise the primary keys
    return [
        raw_field_name_for_column(file_config.get_column_info(c))
        for c in file_config.primary_key_cols
    ]


def _generate_elements_for_state(
    explore: LookMLExplore,
    views_by_file_tag: Dict[str, LookMLView],
    region_config: DirectIngestRegionRawFileConfig,
) -> List[LookMLDashboardElement]:
    """
    Return a list of the elements for a state raw data dashboard
    """
    elements = []
    if not explore.view_name:
        raise ValueError(
            f"LookML Explore for state {region_config.region_code} does not have a view name associated"
        )

    views_in_explore = [explore.view_name] + [
        p.view_name for p in explore.parameters if isinstance(p, ExploreParameterJoin)
    ]

    # Construct the listen where keys are filter names and values are id type
    shared_listen_dict = {}
    for config in sorted(
        region_config.raw_file_configs.values(), key=lambda c: c.file_tag
    ):
        view = views_by_file_tag.get(config.file_tag)
        if not view or view.view_name not in views_in_explore:
            continue

        for col in config.get_primary_external_id_cols():
            if not col.external_id_type:
                raise ValueError(
                    f"Column {col.name} marked as primary does not have an external id "
                    f"type specified."
                )
            shared_listen_dict[col.external_id_type] = view.qualified_name_for_field(
                col.name
            )

    view_name_to_file_tag = {
        view.view_name: file_tag for file_tag, view in views_by_file_tag.items()
    }
    shared_view = _generate_shared_fields_view(
        StateCode(region_config.region_code), explore.view_name
    )
    for i, view_name in enumerate(views_in_explore):
        file_tag = view_name_to_file_tag[view_name]
        file_config = region_config.raw_file_configs[file_tag]
        view = views_by_file_tag[file_tag]

        # Determine fields based on this table's columns and shared view
        fields = view.qualified_dimension_names()
        fields.extend(
            (
                dim.replace(shared_view.view_name, view_name)
                for dim in shared_view.qualified_dimension_names()
            )
        )

        sorts = [
            LookMLSort(view.qualified_name_for_field(col))
            for col in _get_sort_cols(file_config)
        ]
        listen_dict = {
            VIEW_TYPE_FILTER_NAME: f"{explore.view_name}.{VIEW_TYPE_PARAM_NAME}",
            **shared_listen_dict,
        }

        # Escape quote characters in the file description and replace newlines
        note_text = file_config.file_description.replace("\n", " ").replace('"', '\\"')
        elements.append(
            LookMLDashboardElement(
                name=file_config.file_tag,
                title=file_config.file_tag,
                explore=explore.non_template_name,
                type=LookMLElementType.LOOKER_GRID,
                note_display=LookMLNoteDisplayType.HOVER,
                note_text=f'"{note_text}"',
                fields=fields,
                sorts=sorts,
                listen=LookMLListen(listen_dict),
                col=0,
                width=ELEMENT_WIDTH,
                height=ELEMENT_HEIGHT,
                row=i * ELEMENT_HEIGHT,
            )
        )
    return elements


def _generate_all_state_dashboards(
    state_views: Dict[StateCode, Dict[str, LookMLView]],
    state_explores: Dict[StateCode, LookMLExplore],
) -> Dict[StateCode, List[LookMLDashboard]]:
    """
    Return a dictionary where keys are StateCodes for states with raw data
    and a primary person table defined, and values are three LookMLDashboards for each state
    """
    dashboards: Dict[StateCode, List[LookMLDashboard]] = defaultdict(list)
    # Only generate dashboards for states with an explore
    for state_code, explore in state_explores.items():
        region_config = DirectIngestRegionRawFileConfig(region_code=state_code.value)
        dashboard = _generate_dashboard_template(
            state_code,
            region_config,
            explore,
            state_views[state_code],
        )
        dashboards[state_code].append(dashboard)
        dashboards[state_code].append(
            _gen_real_dash_from_template(dashboard, state_code, staging=False)
        )
        dashboards[state_code].append(
            _gen_real_dash_from_template(dashboard, state_code, staging=True)
        )
    return dashboards


def generate_lookml_dashboards(
    looker_dir: str,
    state_views: Dict[StateCode, Dict[str, LookMLView]],
    state_explores: Dict[StateCode, LookMLExplore],
) -> None:
    """Produce LookML Dashboard files for all states, writing up-to-date
    .dashboard.lkml files in looker_dir/dashboards/raw_data/

    looker_dir: Local path to root directory of the Looker repo
    """

    dashboard_dir = os.path.join(looker_dir, "dashboards", "raw_data")
    remove_lookml_files_from(dashboard_dir)

    for state_code, dashboards in _generate_all_state_dashboards(
        state_views, state_explores
    ).items():
        state_dir = os.path.join(dashboard_dir, state_code.value.lower())
        for dash in dashboards:
            dash.write(state_dir, source_script_path=__file__)
