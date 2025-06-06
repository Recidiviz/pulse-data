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
"""Script for manage view updates to occur - to be called only within the Airflow DAG's
KubernetesPodOperator."""
import argparse

from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.view_registry.execute_update_all_managed_views import (
    execute_update_all_managed_views,
)


class UpdateAllManagedViewsEntrypoint(EntrypointInterface):
    """Entrypoint for updating managed views"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        """Parses arguments for the managed views update process."""
        return argparse.ArgumentParser()

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        execute_update_all_managed_views()
