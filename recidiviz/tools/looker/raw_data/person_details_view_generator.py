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
"""Code for building LookML views for raw data tables and writing them to a file.
Used inside person_details_lookml_writer
"""

import os

from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    LookMLFieldParameter,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldType


def generate_shared_fields_view() -> LookMLView:
    """Produce state_raw_data_shared_fields.view.lkml,
    which contains configuration/dimensions shared among all raw data views

    Return: The LookMLView object corresponding to the file
    """

    view_type_param = ParameterLookMLViewField(
        field_name="view_type",
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            LookMLFieldParameter.description(
                "Used to select whether the view has the most recent version (raw data up to date views) or all data"
            ),
            LookMLFieldParameter.allowed_value("Raw Data", "raw_data"),
            LookMLFieldParameter.allowed_value(
                "Raw Data Up To Date Views", "raw_data_up_to_date_views"
            ),
            LookMLFieldParameter.default_value("raw_data_up_to_date_views"),
        ],
    )

    view = LookMLView(
        view_name="state_raw_data_shared_fields",
        fields=[view_type_param],
    )
    return view


def generate_lookml_views(looker_dir: str) -> None:
    """Produce LookML View files for a given state, writing to looker_dir/views/raw_data/

    looker_dir: Local path to root directory of the Looker repo"""

    view_dir = os.path.join(looker_dir, "views", "raw_data")

    shared_fields_view = generate_shared_fields_view()
    shared_fields_view.write(view_dir, source_script_path=__file__)
