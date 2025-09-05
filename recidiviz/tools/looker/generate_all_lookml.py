# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
Generates LookML files using all available LookML generators. Results are saved to recidiviz/tools/looker/__generated__/

Usage:
    python -m recidiviz.tools.looker.generate_all_lookml [<modified_files>...] [--force-all]

If modified_files are provided, only the LookML files that depend on those files will be regenerated.
If --force-all is specified, all LookML files will be regenerated regardless of modified files.
Must supply one or the other, not both.
"""

import argparse
import logging
import os
import re
import sys

from recidiviz.tools.looker.constants import (
    GENERATED_LOOKML_ROOT_PATH,
    LOOKML_TOOLS_ROOT_PATH,
)
from recidiviz.tools.looker.top_level_generators.aggregated_metrics_lookml_generator import (
    AggregatedMetricsLookMLGenerator,
)
from recidiviz.tools.looker.top_level_generators.breadth_and_depth_lookml_generator import (
    BreadthAndDepthLookMLGenerator,
)
from recidiviz.tools.looker.top_level_generators.custom_metrics_lookml_generator import (
    CustomMetricsLookMLGenerator,
    LookMLGenerator,
)
from recidiviz.tools.looker.top_level_generators.observations_lookml_generator import (
    ObservationsLookMLGenerator,
)
from recidiviz.tools.looker.top_level_generators.raw_data_person_details_lookml_generator import (
    RawDataPersonDetailsLookMLGenerator,
)
from recidiviz.tools.looker.top_level_generators.state_dataset_lookml_generator import (
    StateDatasetLookMLGenerator,
)
from recidiviz.tools.looker.top_level_generators.usage_lookml_generator import (
    UsageLookMLGenerator,
)

# If you update this mapping, please also update the files regex for generate_lookml in .pre-commit-config.yaml
LOOKML_GENERATORS_TO_DEPENDENT_FILE_REGEX: dict[type[LookMLGenerator], str | None] = {
    StateDatasetLookMLGenerator: r"^recidiviz/persistence/entity/state/",
    RawDataPersonDetailsLookMLGenerator: r"^recidiviz/ingest/direct/regions/.+/raw_data/",
    ObservationsLookMLGenerator: r"^recidiviz/observations/views/",
    # Doesn't rely on any pulse-data defined views
    BreadthAndDepthLookMLGenerator: None,
    CustomMetricsLookMLGenerator: r"^recidiviz/aggregated_metrics/",
    AggregatedMetricsLookMLGenerator: r"^recidiviz/aggregated_metrics/",
    UsageLookMLGenerator: r"^recidiviz/segment/product_type/|^recidiviz/calculator/query/state/views/analyst_data/global_provisioned_user_sessions",
}


def _all_generators() -> list[type[LookMLGenerator]]:
    """Returns a list of all LookMLGenerator classes."""
    return list(LOOKML_GENERATORS_TO_DEPENDENT_FILE_REGEX.keys())


def _parse_and_validate_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "filenames",
        nargs="*",
        help="Modified files to indicate which subset of lookml needs to be regenerated."
        "Paths must be relative to the root of the repository. ",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        default=False,
        help="Generate docs for all regions, even if they are not modified.",
    )
    args = parser.parse_args()

    if not args.filenames and not args.force_all:
        logging.error(
            "Must provide modified files or use --force-all to generate all LookML."
        )
        sys.exit(1)
    if args.filenames and args.force_all:
        logging.error(
            "Cannot provide modified files and use --force-all at the same time."
        )
        sys.exit(1)

    return args


def _get_lookml_generators(modified_files: list[str]) -> list[type[LookMLGenerator]]:
    """Returns a list of LookMLGenerator classes that should be used based on modified files."""
    # If there are any modified files in the LookML tools directory, regenerate all files to be safe.
    lookml_tools_rel_path = os.path.relpath(LOOKML_TOOLS_ROOT_PATH)
    if any(file.startswith(lookml_tools_rel_path) for file in modified_files):
        return _all_generators()

    lookml_generators: list[type[LookMLGenerator]] = []
    for generator_class, pattern in LOOKML_GENERATORS_TO_DEPENDENT_FILE_REGEX.items():
        if not pattern:
            continue
        regex = re.compile(pattern)
        if any(regex.match(file) for file in modified_files):
            lookml_generators.append(generator_class)
    return lookml_generators


def main(output_dir: str) -> None:
    """Main function to generate all LookML files."""
    args = _parse_and_validate_arguments()

    generators = (
        _all_generators() if args.force_all else _get_lookml_generators(args.filenames)
    )
    if not generators:
        logging.error(
            "Didn't find any LookML generators to run."
            " Please ensure the LOOKML_GENERATORS_TO_DEPENDENT_FILE_REGEX mapping is correct."
        )
        sys.exit(1)

    for generator in generators:
        logging.info("Generating LookML using [%s]", generator.__name__)
        generator.generate_lookml(output_dir=output_dir)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main(output_dir=GENERATED_LOOKML_ROOT_PATH)
