# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for testing Apache Beam pipelines."""
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.testing.test_pipeline import TestPipeline


def create_test_pipeline() -> TestPipeline:
    """Creates a TestPipeline with safe defaults for unit tests.

    This helper ensures consistent configuration across all Beam tests and avoids
    flaky serialization issues that can occur with default TestPipeline settings.

    Specifically, this sets save_main_session=False to avoid pickling the entire
    global namespace, which can lead to coder registry corruption and flaky tests.

    Returns:
        A TestPipeline configured with save_main_session=False
    """
    apache_beam_pipeline_options = PipelineOptions()
    apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
    return TestPipeline(options=apache_beam_pipeline_options)
