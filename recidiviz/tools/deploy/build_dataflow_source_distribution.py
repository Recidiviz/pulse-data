# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Functions for building the `recidiviz-calculation-pipelines` source distribution.
 This package is installed onto our dataflow workers.

Run the following command to execute from the command-line:

    python -m recidiviz.tools.deploy.build_dataflow_source_distribution
"""
import logging
import os
import shutil
import subprocess
import sys

import recidiviz

RECIDIVIZ_ROOT = os.path.abspath(os.path.join(recidiviz.__file__, "../.."))
DEPLOY_ROOT = os.path.join(RECIDIVIZ_ROOT, "recidiviz/tools/deploy")
SETUP_FILE_SRC_PATH = os.path.join(DEPLOY_ROOT, "dataflow_setup.py")
SETUP_FILE_DEST_PATH = os.path.join(RECIDIVIZ_ROOT, "setup.py")
SOURCE_DISTRIBUTION_PATH = os.path.join(DEPLOY_ROOT, "dist")


def build_source_distribution() -> str:
    """Builds a `recidiviz-calculation-pipelines` source distribution for use inside the dataflow workers.
    Returns the path to the built distribution"""
    logging.info("Building `recidiviz-calculation-pipelines` source distribution")

    # Copy our setup file to the root of the repository so that it is included at the root of the source distribution
    shutil.copy(SETUP_FILE_SRC_PATH, SETUP_FILE_DEST_PATH)

    subprocess.run(
        [
            sys.executable,
            SETUP_FILE_DEST_PATH,
            "sdist",
            "--dist-dir",
            SOURCE_DISTRIBUTION_PATH,
        ],
        capture_output=True,
        check=True,
    )

    # Retrieves the full name of the package, for example `recidiviz-calculation-pipelines-1.0.7.6`
    package_name = subprocess.check_output(
        [sys.executable, SETUP_FILE_DEST_PATH, "--fullname"]
    ).splitlines()[0]

    os.remove(SETUP_FILE_DEST_PATH)

    return os.path.join(
        SOURCE_DISTRIBUTION_PATH, f"{package_name.decode('utf-8')}.tar.gz"
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Built source distribution to path: %s", build_source_distribution())
