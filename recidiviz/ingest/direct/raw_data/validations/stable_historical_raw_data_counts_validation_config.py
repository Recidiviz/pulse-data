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
"""Reads and provides access to the validation configuration used in the StableHistoricalRawDataCountsValidation."""
import datetime
from typing import Any, Dict, List, Optional

import attr
import yaml

from recidiviz.common.constants.states import StateCode
from recidiviz.common.local_file_paths import filepath_relative_to_caller

STABLE_HISTORICAL_COUNTS_TABLE_VALIDATION_CONFIG_YAML = (
    "stable_historical_counts_table_validation_config.yaml"
)
CONFIGS_DIRECTORY = "configs"


@attr.define
class StableHistoricalCountsDateRangeExclusion:
    """Represents a date range to exclude from being used in calculating the
    historical median number of raw rows imported for that file tag."""

    datetime_start_inclusive: datetime.datetime
    datetime_end_exclusive: datetime.datetime

    @staticmethod
    def format_for_query(dt: datetime.datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    @classmethod
    def from_dict(
        cls, exclusion_dict: Dict[str, str]
    ) -> "StableHistoricalCountsDateRangeExclusion":
        return StableHistoricalCountsDateRangeExclusion(
            datetime_start_inclusive=datetime.datetime.fromisoformat(
                exclusion_dict["datetime_start_inclusive"]
            ),
            datetime_end_exclusive=(
                datetime.datetime.fromisoformat(
                    exclusion_dict["datetime_end_exclusive"]
                )
                if exclusion_dict.get("datetime_end_exclusive")
                else datetime.datetime.now(tz=datetime.UTC)
            ),
        )


@attr.define
class StableHistoricalCountsToleranceOverride:
    """Represents the tolerance override for a file tag in the StableHistoricalRawDataCountsValidation.
    percent_change_tolerance is the percent change tolerance used to compare the current row count to the historical median.
    """

    percent_change_tolerance: float

    @classmethod
    def from_dict(
        cls, tolerance_override_dict: Dict[str, Any]
    ) -> "StableHistoricalCountsToleranceOverride":

        return StableHistoricalCountsToleranceOverride(
            percent_change_tolerance=tolerance_override_dict["percent_change_tolerance"]
        )


@attr.define
class StableHistoricalCountsFileTagConfig:
    """Represents the configuration for a file tag in the StableHistoricalRawDataCountsValidation.
    tolerance_override is optional and can be used to override the default percent_change_tolerance.
    date_range_exclusions is optional and can be used to exclude imports within certain date ranges
    from being used in calculating the historical median number of raw rows imported for that file tag.
    """

    tolerance_override: Optional[StableHistoricalCountsToleranceOverride]
    date_range_exclusions: Optional[List[StableHistoricalCountsDateRangeExclusion]]

    @classmethod
    def from_dict(
        cls, file_tag_dict: Dict[str, Any]
    ) -> "StableHistoricalCountsFileTagConfig":
        return StableHistoricalCountsFileTagConfig(
            tolerance_override=(
                StableHistoricalCountsToleranceOverride.from_dict(
                    file_tag_dict["tolerance_override"]
                )
                if file_tag_dict.get("tolerance_override")
                else None
            ),
            date_range_exclusions=(
                [
                    StableHistoricalCountsDateRangeExclusion.from_dict(exclusion)
                    for exclusion in file_tag_dict["date_range_exclusions"]
                ]
                if file_tag_dict.get("date_range_exclusions")
                else None
            ),
        )


@attr.define
class StableHistoricalCountsDefaultConfig:
    """Represents the default configuration for the StableHistoricalRawDataCountsValidation.
    time_window_lookback_days is the number of days to look back when querying historical counts.
    percent_change_tolerance is the percent change tolerance used to compare the current row count to the historical median.
    """

    time_window_lookback_days: int
    percent_change_tolerance: float

    @classmethod
    def from_dict(
        cls, default_config_dict: Dict[str, Any]
    ) -> "StableHistoricalCountsDefaultConfig":
        return StableHistoricalCountsDefaultConfig(
            time_window_lookback_days=default_config_dict["time_window_lookback_days"],
            percent_change_tolerance=default_config_dict["percent_change_tolerance"],
        )


@attr.define
class StableHistoricalCountsRegionConfig:
    """Represents the configuration for all of the file tags present in the configuration yaml for a region,
    with the file_tags dictionary's key being the file tag.
    """

    file_tags: Dict[str, StableHistoricalCountsFileTagConfig]

    @classmethod
    def from_dict(
        cls, region_config_dict: Dict[str, Any]
    ) -> "StableHistoricalCountsRegionConfig":
        return StableHistoricalCountsRegionConfig(
            file_tags={
                key: StableHistoricalCountsFileTagConfig.from_dict(value)
                for key, value in region_config_dict["file_tags"].items()
            }
        )


@attr.define
class StableHistoricalCountsValidationConfig:
    """Represents the configuration for the StableHistoricalRawDataCountsValidation."""

    defaults: StableHistoricalCountsDefaultConfig
    custom: Optional[Dict[StateCode, StableHistoricalCountsRegionConfig]]

    @classmethod
    def from_dict(
        cls, validation_config_dict: Dict[str, Any]
    ) -> "StableHistoricalCountsValidationConfig":
        custom_config_dict = {
            StateCode(key): StableHistoricalCountsRegionConfig.from_dict(value)
            for key, value in validation_config_dict.get("custom", {}).items()
        } or None

        return StableHistoricalCountsValidationConfig(
            defaults=StableHistoricalCountsDefaultConfig.from_dict(
                validation_config_dict["defaults"]
            ),
            custom=custom_config_dict,
        )


@attr.define
class StableHistoricalCountsValidationConfigLoader:
    """Loads and provides access to the validation configuration
    used in the StableHistoricalRawDataCountsValidation."""

    config_file_name: str = attr.ib(
        default=STABLE_HISTORICAL_COUNTS_TABLE_VALIDATION_CONFIG_YAML
    )
    config_directory: str = attr.ib(default=CONFIGS_DIRECTORY)
    config_path: str = attr.ib(default=None)
    validation_config: StableHistoricalCountsValidationConfig = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.config_path = self.config_path or filepath_relative_to_caller(
            self.config_file_name, self.config_directory
        )
        self.validation_config = self._load_config()

    def _load_config(self) -> StableHistoricalCountsValidationConfig:
        with open(self.config_path, "r", encoding="utf-8") as f:
            validation_yaml = yaml.safe_load(f)
        return StableHistoricalCountsValidationConfig.from_dict(validation_yaml)

    def _get_region_specific_config(
        self, state_code: StateCode
    ) -> Optional[StableHistoricalCountsRegionConfig]:
        if custom_config := self.validation_config.custom:
            return custom_config.get(state_code)
        return None

    def get_file_tag_specific_config(
        self, state_code: StateCode, file_tag: str
    ) -> Optional[StableHistoricalCountsFileTagConfig]:
        if region_config := self._get_region_specific_config(state_code):
            return region_config.file_tags.get(file_tag)
        return None

    def get_default_config(self) -> StableHistoricalCountsDefaultConfig:
        return self.validation_config.defaults


@attr.define
class StableHistoricalRawDataCountsValidationConfig:
    """Retrives configuration values for the StableHistoricalRawDataCountsValidation."""

    config_loader: StableHistoricalCountsValidationConfigLoader = attr.ib(
        factory=StableHistoricalCountsValidationConfigLoader
    )

    def get_custom_percent_change_tolerance(
        self, state_code: StateCode, file_tag: str
    ) -> float:
        if (
            file_tag_config := self.config_loader.get_file_tag_specific_config(
                state_code,
                file_tag,
            )
        ) and file_tag_config.tolerance_override:
            return file_tag_config.tolerance_override.percent_change_tolerance
        return self.config_loader.get_default_config().percent_change_tolerance

    def get_date_range_exclusions(
        self, state_code: StateCode, file_tag: str
    ) -> List[StableHistoricalCountsDateRangeExclusion]:
        if (
            file_tag_config := self.config_loader.get_file_tag_specific_config(
                state_code,
                file_tag,
            )
        ) and file_tag_config.date_range_exclusions:
            return file_tag_config.date_range_exclusions
        return []

    def datetime_is_excluded(
        self, state_code: StateCode, file_tag: str, datetime_to_check: datetime.datetime
    ) -> bool:
        for exclusion in self.get_date_range_exclusions(state_code, file_tag):
            if (
                exclusion.datetime_start_inclusive
                <= datetime_to_check
                < exclusion.datetime_end_exclusive
            ):
                return True
        return False

    def get_time_window_lookback_days(self) -> int:
        return self.config_loader.get_default_config().time_window_lookback_days
