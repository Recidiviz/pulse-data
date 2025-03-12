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
"""A script for writing a set of LookML views for the `state` dataset

Run the following to write files to the specified directory DIR:
python -m recidiviz.tools.looker.state.state_dataset_lookml_writer --looker_repo_root [DIR]

"""
import argparse
import os

from recidiviz.looker.lookml_explore import write_explores_to_file
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_classes_in_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tools.looker.constants import (
    DASHBOARDS_DIR,
    EXPLORES_DIR,
    LOOKER_REPO_NAME,
    VIEWS_DIR,
)
from recidiviz.tools.looker.entity.entity_dashboard_builder import (
    EntityLookMLDashboardBuilder,
)
from recidiviz.tools.looker.entity.entity_explore_builder import (
    EntityLookMLExploreBuilder,
)
from recidiviz.tools.looker.script_helpers import remove_lookml_files_from
from recidiviz.tools.looker.state.state_dataset_view_generator import (
    generate_state_views,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation

STATE_SUBDIR = "state"


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--looker_repo_root",
        dest="looker_dir",
        help="Specifies local path to the Looker repo, where all files will be saved to",
        type=str,
        required=True,
    )

    return parser.parse_args()


def write_lookml_files(looker_dir: str) -> None:
    """
    Write state LookML views, explores and dashboards to the given directory,
    which should be a path to the local copy of the looker repo
    """
    if os.path.basename(looker_dir).lower() != LOOKER_REPO_NAME:
        raise ValueError(
            f"Expected looker_dir to be at the root of [{LOOKER_REPO_NAME}] repo, but instead got [{looker_dir}]"
        )

    prompt_for_confirmation(
        f"Warning: .lkml files will be deleted/overwritten in {looker_dir}\nProceed?"
    )

    for subdir in [VIEWS_DIR, EXPLORES_DIR, DASHBOARDS_DIR]:
        remove_lookml_files_from(os.path.join(looker_dir, subdir, STATE_SUBDIR))

    # TODO(#23292): Generate normalized state views
    state_views = generate_state_views()
    for view in state_views:
        view.write(
            output_directory=os.path.join(looker_dir, VIEWS_DIR, STATE_SUBDIR),
            source_script_path=__file__,
        )

    module_context = entities_module_context_for_module(state_entities)

    for root_entity_cls in get_root_entity_classes_in_module(state_entities):
        if not issubclass(root_entity_cls, Entity):
            raise ValueError(
                f"Expected root entity class [{root_entity_cls}] to be a subclass of [{Entity}]"
            )
        write_explores_to_file(
            explores=EntityLookMLExploreBuilder(
                module_context=module_context,
                root_entity_cls=root_entity_cls,
            ).build(),
            top_level_explore_name=root_entity_cls.get_table_id(),
            output_directory=os.path.join(looker_dir, EXPLORES_DIR, STATE_SUBDIR),
            source_script_path=__file__,
        )

        dashboard = EntityLookMLDashboardBuilder(
            module_context=module_context,
            root_entity_cls=root_entity_cls,
            views=state_views,
        ).build_and_validate()
        dashboard.write(
            output_directory=os.path.join(looker_dir, DASHBOARDS_DIR, STATE_SUBDIR),
            source_script_path=__file__,
        )


if __name__ == "__main__":
    args = parse_arguments()
    write_lookml_files(args.looker_dir)
