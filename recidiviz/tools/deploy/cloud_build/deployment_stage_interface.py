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
"""Interface for deployment stages"""
import abc
import argparse

from recidiviz.tools.deploy.cloud_build.build_configuration import (
    BuildConfiguration,
    DeploymentContext,
)


class DeploymentStageInterface(abc.ABC):
    """Interface for deployment stages"""

    @staticmethod
    @abc.abstractmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        return parser

    @abc.abstractmethod
    def configure_build(
        self,
        deployment_context: DeploymentContext,
        args: argparse.Namespace,
    ) -> BuildConfiguration:
        ...
