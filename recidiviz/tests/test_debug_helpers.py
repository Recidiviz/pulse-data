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
"""Helpers for debugging test failures."""
import webbrowser
from typing import Any, Sequence

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    write_entity_tree_to_file,
)
from recidiviz.utils.log_helpers import write_html_diff_to_file


def launch_entity_tree_html_diff_comparison(
    found_root_entities: Sequence[Any],
    expected_root_entities: Sequence[Any],
    field_index: CoreEntityFieldIndex,
    region_code: str = StateCode.US_XX.value.lower(),
    print_tree_structure_only: bool = False,
) -> None:
    """Launches an HTML diff of the two root entity lists."""
    actual_output_filepath = write_entity_tree_to_file(
        operation_for_filename="actual_output_from_controller_test",
        region_code=region_code,
        print_tree_structure_only=print_tree_structure_only,
        field_index=field_index,
        root_entities=found_root_entities,
    )
    expected_output_filepath = write_entity_tree_to_file(
        operation_for_filename="expected_output_from_controller_test",
        region_code=region_code,
        print_tree_structure_only=print_tree_structure_only,
        field_index=field_index,
        root_entities=expected_root_entities,
    )

    html_filepath = write_html_diff_to_file(
        expected_output_filepath, actual_output_filepath, region_code=region_code
    )
    print(f"HTML diff located at {html_filepath}")
    webbrowser.get("chrome").open(f"file://{html_filepath}")
