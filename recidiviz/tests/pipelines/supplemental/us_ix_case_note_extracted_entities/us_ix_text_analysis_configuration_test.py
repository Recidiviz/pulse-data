# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the us_ix_text_analysis_configuration.py"""
import unittest

from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_content_text_analysis_configuration import (
    UsIxNoteContentTextEntity,
)
from recidiviz.pipelines.supplemental.us_ix_case_note_extracted_entities.us_ix_note_title_text_analysis_configuration import (
    UsIxNoteTitleTextEntity,
)


# TODO(#16661) Rename US_IX -> US_ID in this file/code when we are ready to migrate the
# new ATLAS pipeline to run for US_ID
class UsIxTextAnalysisConfiguration(unittest.TestCase):
    """Unit test that the text entities have all of the same enum values."""

    def test_enum_classes_share_same_values(self) -> None:
        self.assertListEqual(
            [str(enum.name) for enum in UsIxNoteContentTextEntity],
            [str(enum.name) for enum in UsIxNoteTitleTextEntity],
        )
