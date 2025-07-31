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
"""A script for writing a set of LookML views for the `state` and `normalized_state` datasets

Example usage:
python -m recidiviz.tools.looker.top_level_generators.state_dataset_lookml_generator
"""
import os
from types import ModuleType

from recidiviz.ingest.views.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.looker.lookml_explore import write_explores_to_file
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_classes_in_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.looker.constants import (
    DASHBOARDS_DIR,
    EXPLORES_DIR,
    GENERATED_LOOKML_ROOT_PATH,
    VIEWS_DIR,
)
from recidiviz.tools.looker.entity.entity_dashboard_builder import (
    EntityLookMLDashboardBuilder,
)
from recidiviz.tools.looker.entity.entity_explore_builder import (
    generate_entity_explores,
)
from recidiviz.tools.looker.entity.entity_views_builder import (
    generate_entity_lookml_views,
)
from recidiviz.tools.looker.script_helpers import remove_lookml_files_from
from recidiviz.tools.looker.top_level_generators.base_lookml_generator import (
    LookMLGenerator,
)


def write_lookml_files(
    looker_dir: str, dataset_id: str, entities_module: ModuleType
) -> None:
    """
    Write state and normalized_state LookML views, explores and dashboards to the
    given directory, which should be a path to the local copy of the looker repo
    """
    dataset_subdir = dataset_id
    for subdir in [VIEWS_DIR, EXPLORES_DIR, DASHBOARDS_DIR]:
        remove_lookml_files_from(os.path.join(looker_dir, subdir, dataset_subdir))

    state_views = generate_entity_lookml_views(
        dataset_id=dataset_id, entities_module=entities_module
    )
    for view in state_views:
        view.write(
            output_directory=os.path.join(looker_dir, VIEWS_DIR, dataset_subdir),
            source_script_path=__file__,
        )

    module_context = entities_module_context_for_module(entities_module)
    for root_entity_cls in get_root_entity_classes_in_module(entities_module):
        if not issubclass(root_entity_cls, Entity):
            raise ValueError(
                f"Expected root entity class [{root_entity_cls}] to be a subclass of [{Entity}]"
            )
        write_explores_to_file(
            explores=generate_entity_explores(
                dataset_id=dataset_id,
                module_context=module_context,
                root_entity_cls=root_entity_cls,
            ),
            top_level_explore_name=root_entity_cls.get_entity_name(),
            output_directory=os.path.join(looker_dir, EXPLORES_DIR, dataset_subdir),
            source_script_path=__file__,
        )

        dashboard = EntityLookMLDashboardBuilder(
            module_context=module_context,
            root_entity_cls=root_entity_cls,
            dataset_id=dataset_id,
            views=state_views,
        ).build_and_validate()
        dashboard.write(
            output_directory=os.path.join(looker_dir, DASHBOARDS_DIR, dataset_subdir),
            source_script_path=__file__,
        )


class StateDatasetLookMLGenerator(LookMLGenerator):
    @staticmethod
    def generate_lookml(output_dir: str) -> None:
        """
        Generate LookML files for the state and normalized_state datasets.
        """
        for dataset, module in [
            (STATE_BASE_DATASET, state_entities),
            (NORMALIZED_STATE_DATASET, normalized_entities),
        ]:
            write_lookml_files(
                looker_dir=output_dir, dataset_id=dataset, entities_module=module
            )


if __name__ == "__main__":
    StateDatasetLookMLGenerator.generate_lookml(output_dir=GENERATED_LOOKML_ROOT_PATH)
