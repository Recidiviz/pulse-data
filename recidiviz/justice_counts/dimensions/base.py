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
"""Contains base Dimension classes."""

import enum
import re
from abc import ABCMeta, abstractmethod

import attr


class DimensionBase:
    """Base class to unify dimensions defined as part of Phase 0 State Data Scan and
    dimensions later defined for the official Justice Counts metrics. Both sets of
    dimension classes should extend from both Enum and DimensionBase (and therefore
    define a `dimension_identifier`).
    """

    @classmethod
    @abstractmethod
    def dimension_identifier(cls) -> str:
        """The globally unique dimension_identifier of this dimension, used when storing it in the database.
        Currently, there are a few categories of dimensions:
            - Global, raw dimensions: these apply across many metrics, sources, and locations,
                but the same set of values is not necessarily shared across them.
                Example: race
                Naming convention: global/<dimension name>/raw`, e.g. `global/facility/raw`.
            - Global, normalized dimensions: these are like global raw dimensions, but are normalized
                to a set of values.
                Example: race, but normalized
                Naming convention: 'global/<dimension name>`.
            - Metric-specific dimensions: these have a pre-defined set of values and are crucial for
                the understanding of the data reported to a metric.
                Example: population type.
                Naming convention: 'metric/<metric name>/<dimension name>', e.g. 'metric/population/type'.
                    If the metric is specific to a system, use 'metric/<system name>/<metric name>/<dimension name>'
            - Location or source-specific dimensions: these may or may not have a pre-defined set of values,
                but are created to understand something unique to a source location.
                Example: facilities specific to a particular state
        """

    @classmethod
    def display_name(cls) -> str:
        # Text displayed as label for aggregated dimension breakdowns in control panel (right of toggle)
        return cls.human_readable_name() + "s"

    def to_enum(self) -> enum.Enum:
        if not isinstance(self, enum.Enum):
            raise ValueError("Subclasses of DimensionBase must also subclass Enum.")
        return self

    @classmethod
    def human_readable_name(cls) -> str:
        """The name of the dimension class

        E.g. 'OffenseType' --> 'Offense Type
        """
        class_name = cls.__name__
        class_name = re.sub(
            r"([A-Z][a-z]+)", r"\1 ", class_name
        )  # Add spaces between each word of class name. StaffBudgetType -> Staff Budget Type
        return class_name.rstrip()

    @property
    def dimension_value(self) -> str:
        """The value of this dimension instance.

        E.g. 'Female' is a potential value for an instance of the 'global/raw/gender' dimension.
        """
        return self.to_enum().value

    @property
    def dimension_name(self) -> str:
        """The value of this dimension instance.

        E.g. 'FEMALE' is a potential name for an instance of the 'global/raw/gender' dimension.
        """
        return self.to_enum().name


@attr.s(frozen=True)
class RawDimension(DimensionBase, metaclass=ABCMeta):
    """Base class to use to create a raw version of a normalized dimension.

    Child classes are typically created by passing a normalized dimension class to `raw_type_for_dimension`, which will
    create a raw, or not normalized, copy version of the dimension.
    """

    value: str = attr.ib(converter=str)

    @property
    def dimension_value(self) -> str:
        return self.value
