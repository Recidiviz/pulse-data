# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""A reusable singular/plural noun descriptor, used to name a thing specifically
(rather than generically) in programmatically generated text.
"""
import attr

from recidiviz.common import attr_validators
from recidiviz.utils.yaml_dict import YAMLDict


@attr.define(frozen=True, kw_only=True)
class Descriptor:
    """Human-readable singular and plural noun forms of a thing (e.g.
    "employer"/"employers" or "place of residence"/"places of residence"), so
    generated prompt can name it in each grammatical slot instead of speaking
    generically.
    """

    singular: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The singular noun (e.g. "employer", "place of residence")."""

    plural: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The plural noun (e.g. "employers", "places of residence")."""

    @property
    def indefinite(self) -> str:
        """Returns the singular with its indefinite article (e.g. "an employer",
        "a place of residence"), choosing "a"/"an" from the leading letter.
        """
        article = "an" if self.singular[:1].lower() in "aeiou" else "a"
        return f"{article} {self.singular}"

    @classmethod
    def from_yaml_dict(cls, yaml_dict: YAMLDict) -> "Descriptor":
        """Returns the descriptor parsed from a `{singular, plural}` sub-dict."""
        singular = yaml_dict.pop("singular", str)
        plural = yaml_dict.pop("plural", str)
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values in descriptor: "
                f"{repr(yaml_dict.get())}"
            )
        return cls(singular=singular, plural=plural)
