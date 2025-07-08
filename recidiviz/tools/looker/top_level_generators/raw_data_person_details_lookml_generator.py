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
"""A script for building a set of LookML views, explores and dashboards
for raw data tables and writing them to files.

Run the following to write files to the specified directory DIR:
python -m recidiviz.tools.looker.top_level_generators.raw_data_person_details_lookml_generator [--looker-repo-root [DIR]]

If you are running this for new states, you will also have to add the following lines
into the `models/recidiviz-123.model.lkml` and `models/recidiviz-staging.model.lkml`
files inside the Looker directory for every new state added:

explore: us_xx_raw_data {
  extends: [us_xx_raw_data_template]
}
"""


from recidiviz.tools.looker.raw_data.person_details_dashboard_generator import (
    generate_lookml_dashboards,
)
from recidiviz.tools.looker.raw_data.person_details_explore_generator import (
    generate_lookml_explores,
)
from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    generate_lookml_views,
)
from recidiviz.tools.looker.script_helpers import parse_and_validate_output_dir_arg
from recidiviz.tools.looker.top_level_generators.base_lookml_generator import (
    LookMLGenerator,
)


class RawDataPersonDetailsLookMLGenerator(LookMLGenerator):
    """Generates LookML files for person details raw data tables."""

    @staticmethod
    def generate_lookml(output_dir: str) -> None:
        """
        Write state raw data LookML views, explores and dashboards to the given directory,
        which should be a path to the local copy of the looker repo
        """
        all_state_views = generate_lookml_views(output_dir)
        all_state_explores = generate_lookml_explores(output_dir, all_state_views)
        generate_lookml_dashboards(output_dir, all_state_views, all_state_explores)


if __name__ == "__main__":
    RawDataPersonDetailsLookMLGenerator.generate_lookml(
        output_dir=parse_and_validate_output_dir_arg()
    )
