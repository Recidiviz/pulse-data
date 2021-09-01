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

"""Contains logic related to EntityEnums."""

import re
from typing import Dict, Optional, Type, TypeVar

from aenum import Enum, EnumMeta
from opencensus.stats import aggregation, measure, view

from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.enum_parser import EnumParsingError
from recidiviz.utils import monitoring
from recidiviz.utils.types import ClsT

m_enum_errors = measure.MeasureInt(
    "converter/enum_error_count", "The number of enum errors", "1"
)
enum_errors_view = view.View(
    "recidiviz/converter/enum_error_count",
    "The sum of enum errors",
    [monitoring.TagKey.REGION, monitoring.TagKey.ENTITY_TYPE],
    m_enum_errors,
    aggregation.SumAggregation(),
)
monitoring.register_views([enum_errors_view])


class EntityEnumMeta(EnumMeta):
    """Metaclass for mappable enums."""

    # pylint doesn't understand |cls| as |self|:
    # https://stackoverflow.com/questions/47615318/
    # what-is-the-best-practice-for-metaclass-methods-that-call-each-other
    # pylint: disable=no-value-for-parameter, not-an-iterable
    def parse(
        cls: Type[ClsT], label: str, enum_overrides: "EnumOverrides"
    ) -> Optional[ClsT]:
        try:
            return cls._parse_to_enum(label, enum_overrides)
        except EnumParsingError:
            with monitoring.measurements(
                {monitoring.TagKey.ENTITY_TYPE: cls.__name__}
            ) as m:
                m.measure_int_put(m_enum_errors, 1)
            raise

    def can_parse(cls: Type[ClsT], label: str, enum_overrides: "EnumOverrides") -> bool:
        """Checks if the given string will parse into this enum.

        Convenience method to be used by a child scraper to tell if a given
        string should be used for this field.
        """
        try:
            cls._parse_to_enum(label, enum_overrides)
            return True
        except EnumParsingError:
            return False

    def find_in_string(cls: Type[ClsT], text: Optional[str]) -> Optional[ClsT]:
        if not text:
            return None
        for inst in cls:
            if re.search(inst.value.replace("_", " "), text, re.I):
                return inst
        return None

    def _parse_to_enum(
        cls: Type[ClsT],
        label: str,
        enum_overrides: "EnumOverrides",
    ) -> Optional["EntityEnum"]:
        """Attempts to parse |label| using the default map of |cls| and the
        provided |override_map|. Ignores punctuation by treating punctuation as
        a separator, e.g. `(N/A)` will map to the same value as `N A`."""

        return DefaultingAndNormalizingEnumParser(
            raw_text=label, enum_cls=cls, enum_overrides=enum_overrides
        ).parse()

    def parse_from_canonical_string(
        cls: Type[ClsT], label: Optional[str]
    ) -> Optional[ClsT]:
        """Attempts to parse |label| using the enum canonical strings.
        Only accepts exact, case-sensitive matches. Returns `None` if
        |label| is empty."""

        if label is None:
            return None

        try:
            return cls._value2member_map_[label]
        except KeyError as e:
            raise EnumParsingError(cls, label) from e


class EntityEnum(Enum, metaclass=EntityEnumMeta):
    """Enum class that can be mapped from a string.

    When extending this class, you must override: _get_default_map
    """

    @staticmethod
    def _get_default_map() -> Dict[str, "EntityEnum"]:
        raise NotImplementedError

    @classmethod
    def _missing_value_(cls: Type[ClsT], name: str) -> Optional[ClsT]:
        """DO NOT DELETE THIS. It is a very hacky override of an inner function in the
        aenum library which makes it so we raise an EnumParsingError and not a
        ValueError when we try to instantiate an enum value with an unknown string.
        """
        return cls.parse_from_canonical_string(name.upper())


EntityEnumT = TypeVar("EntityEnumT", bound=EntityEnum)
