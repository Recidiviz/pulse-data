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
"""Unit tests for person details LookML View generation"""

import os
import tempfile
import unittest

from mock import Mock, patch

from recidiviz.tools.looker.raw_data.person_details_lookml_writer import (
    write_lookml_files,
)


class LookMLWriterTest(unittest.TestCase):
    """Tests raw data LookML generation script"""

    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_dashboards"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_explores"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_views"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.prompt_for_confirmation"
    )
    def test_generate_lookml_wrong_directory(
        self,
        mock_prompt: Mock,
        mock_generate_views: Mock,
        mock_generate_explores: Mock,
        mock_generate_dashboards: Mock,
    ) -> None:
        # Attempt to generate files into a wrong directory,
        # check that the user was prompted and files are not removed
        mock_prompt.return_value = False
        with tempfile.TemporaryDirectory() as tmp_dir:
            not_looker_dir = os.path.join(tmp_dir, "not_looker")
            write_lookml_files(not_looker_dir)
            mock_prompt.assert_called()
            mock_generate_views.assert_not_called()
            mock_generate_explores.assert_not_called()
            mock_generate_dashboards.assert_not_called()

    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_dashboards"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_explores"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_views"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.prompt_for_confirmation"
    )
    def test_generate_lookml_wrong_directory_still_proceed(
        self,
        mock_prompt: Mock,
        mock_generate_views: Mock,
        mock_generate_explores: Mock,
        mock_generate_dashboards: Mock,
    ) -> None:
        # Attempt to generate files into a wrong directory,
        # check that the user was prompted and we continue when they say to
        mock_prompt.return_value = True
        with tempfile.TemporaryDirectory() as tmp_dir:
            not_looker_dir = os.path.join(tmp_dir, "not_looker")
            write_lookml_files(not_looker_dir)
            mock_prompt.assert_called()
            mock_generate_views.assert_called()
            mock_generate_explores.assert_called()
            mock_generate_dashboards.assert_called()

    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_dashboards"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_explores"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.generate_lookml_views"
    )
    @patch(
        "recidiviz.tools.looker.raw_data.person_details_lookml_writer.prompt_for_confirmation"
    )
    def test_generate_lookml_right_directory(
        self,
        mock_prompt: Mock,
        mock_generate_views: Mock,
        mock_generate_explores: Mock,
        mock_generate_dashboards: Mock,
    ) -> None:
        # Attempt to generate files into the correct directory,
        # check that the user was not prompted, files are removed and generated
        with tempfile.TemporaryDirectory() as tmp_dir:
            looker_dir = os.path.join(tmp_dir, "looker")
            write_lookml_files(looker_dir)
            mock_prompt.assert_not_called()

            mock_generate_views.assert_called()
            mock_generate_explores.assert_called()
            mock_generate_dashboards.assert_called()
