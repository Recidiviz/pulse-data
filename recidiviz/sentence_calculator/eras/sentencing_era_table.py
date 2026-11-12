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
"""Table of a state's sentencing eras: the windows of offense dates and the law
regime that governs each one.

The engine never reads this table. The input layer looks up a sentence's
offense date here before it builds the sentence, and hands the engine the
regime name it finds.
"""

import os
from datetime import date
from pathlib import Path

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator import eras as eras_package
from recidiviz.sentence_calculator.models.rules import (
    yaml_dict_from_path_with_unique_keys,
)
from recidiviz.sentence_calculator.rules.loader import (
    list_law_regime_names,
    load_law_regime,
)
from recidiviz.utils.types import assert_type


@attr.define(frozen=True, kw_only=True)
class SentencingEra:
    """A window of offense dates that one law regime governs. The window runs
    from offenses_on_or_after up to the next era's start in the table."""

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier for the era, e.g. NEW_CODE_A. Several eras can share one
    regime when the rules that differ between them are not encoded yet."""

    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What changed in sentencing law when this era began."""

    offenses_on_or_after: date | None = attr.ib(validator=attr_validators.is_opt_date)
    """First offense date in the era, or None for the first era in a table,
    which covers every offense before the second era began."""

    regime_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the law regime that governs sentences for offenses in this era,
    as list_law_regime_names returns it."""


@attr.define(frozen=True, kw_only=True)
class SentencingEraTable:
    """Table of a state's sentencing eras: the windows of offense dates and the
    law regime that governs each one.

    The engine never reads this table. The input layer looks up a sentence's
    offense date here before it builds the sentence, and hands the engine the
    regime name it finds.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state whose eras these are."""

    eras: list[SentencingEra] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(SentencingEra),
        ]
    )
    """The eras in date order. Only the first has no start date, and each
    later era starts after the one before it, so the windows have no gaps and
    do not overlap."""

    def __attrs_post_init__(self) -> None:
        first_era, *later_eras = self.eras
        if first_era.offenses_on_or_after is not None:
            raise ValueError(
                f"The first era [{first_era.name}] for state "
                f"[{self.state_code.value}] has a start date "
                f"[{first_era.offenses_on_or_after}]. The first era covers every "
                f"offense before the second era, so leave offenses_on_or_after "
                f"out."
            )
        previous_era = first_era
        for era in later_eras:
            if era.offenses_on_or_after is None:
                raise ValueError(
                    f"Era [{era.name}] for state [{self.state_code.value}] has no "
                    f"start date. Only the first era may leave "
                    f"offenses_on_or_after out."
                )
            if (
                previous_era.offenses_on_or_after is not None
                and era.offenses_on_or_after <= previous_era.offenses_on_or_after
            ):
                raise ValueError(
                    f"Era [{era.name}] for state [{self.state_code.value}] starts "
                    f"on [{era.offenses_on_or_after}], which is not after the start "
                    f"of the era before it, [{previous_era.name}] on "
                    f"[{previous_era.offenses_on_or_after}]. List the eras in date "
                    f"order."
                )
            previous_era = era
        names = [era.name for era in self.eras]
        duplicate_names = sorted({n for n in names if names.count(n) > 1})
        if duplicate_names:
            raise ValueError(
                f"The era table for state [{self.state_code.value}] has duplicate "
                f"era names: {duplicate_names}."
            )

    def era_for_offense_date(self, offense_date: date) -> SentencingEra:
        """Returns the era whose window holds offense_date."""
        era_found = self.eras[0]
        for era in self.eras[1:]:
            if offense_date < assert_type(era.offenses_on_or_after, date):
                break
            era_found = era
        return era_found

    def regime_name_for_offense_date(self, offense_date: date) -> str:
        """Returns the name of the law regime that governs a sentence for an
        offense committed on offense_date."""
        return self.era_for_offense_date(offense_date).regime_name

    @staticmethod
    def yaml_path(state_code: StateCode) -> str:
        """Returns the path of the YAML file holding state_code's era table,
        e.g. .../eras/us_az_eras.yaml for US_AZ."""
        return os.path.join(
            os.path.dirname(eras_package.__file__),
            f"{state_code.value.lower()}_eras.yaml",
        )

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str | Path,
        *,
        state_code: StateCode,
        regime_effective_dates: dict[str, date],
    ) -> "SentencingEraTable":
        """Returns the era table defined in the YAML file at yaml_path.

        Args:
            yaml_path: Path of the table's YAML file.
            state_code: The state whose eras the file holds.
            regime_effective_dates: The effective date of every law regime
                defined for the state, by regime name. An era that names any
                other regime is an error, and so is a regime whose first era
                starts on a different date than the regime's effective date,
                so the boundary cannot drift between the two files.
        """
        yaml_dict = yaml_dict_from_path_with_unique_keys(yaml_path)
        eras = []
        for era_dict in yaml_dict.pop_dicts("eras"):
            era = SentencingEra(
                name=era_dict.pop("name", str),
                description=era_dict.pop("description", str),
                offenses_on_or_after=era_dict.pop_optional(
                    "offenses_on_or_after", date
                ),
                regime_name=era_dict.pop("regime", str),
            )
            if era_dict:
                raise ValueError(
                    f"Found unexpected keys in era [{era.name}] in [{yaml_path}]: "
                    f"{repr(era_dict.get())}"
                )
            eras.append(era)
        if yaml_dict:
            raise ValueError(
                f"Found unexpected keys in the era table file [{yaml_path}]: "
                f"{repr(yaml_dict.get())}"
            )
        known_regime_names = sorted(regime_effective_dates)
        for era in eras:
            if era.regime_name not in regime_effective_dates:
                raise ValueError(
                    f"Era [{era.name}] in [{yaml_path}] names the law regime "
                    f"[{era.regime_name}], which is not defined for state "
                    f"[{state_code.value}]. Defined regimes: {known_regime_names}."
                )
        first_era_by_regime: dict[str, SentencingEra] = {}
        for era in eras:
            first_era_by_regime.setdefault(era.regime_name, era)
        for regime_name, era in first_era_by_regime.items():
            effective_date = regime_effective_dates[regime_name]
            if (
                era.offenses_on_or_after is not None
                and era.offenses_on_or_after != effective_date
            ):
                raise ValueError(
                    f"Era [{era.name}] in [{yaml_path}] starts on "
                    f"[{era.offenses_on_or_after}], but the law regime it routes "
                    f"to, [{regime_name}], has effective_date [{effective_date}]. "
                    f"The first era that routes to a regime must start on the "
                    f"regime's effective date; change one file so they agree."
                )
        return cls(state_code=state_code, eras=eras)


def load_sentencing_eras(state_code: StateCode) -> SentencingEraTable:
    """Returns the sentencing era table for state_code, raising if the state has
    none."""
    yaml_path = SentencingEraTable.yaml_path(state_code)
    if not os.path.exists(yaml_path):
        raise ValueError(
            f"No sentencing era table defined for state [{state_code.value}]. "
            f"Expected a file at [{yaml_path}]."
        )
    return SentencingEraTable.from_yaml(
        yaml_path,
        state_code=state_code,
        regime_effective_dates={
            regime_name: load_law_regime(
                state_code=state_code, regime_name=regime_name
            ).effective_date
            for regime_name in list_law_regime_names(state_code)
        },
    )
