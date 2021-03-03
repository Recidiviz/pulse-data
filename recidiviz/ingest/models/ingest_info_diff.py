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
"""Human-readable diffs for IngestInfo."""

from deepdiff import DeepDiff


def diff_ingest_infos(expected, actual):
    """Returns a list of formatted strings describing the differences between
    two IngestInfo objects."""
    ddiff = DeepDiff(expected, actual, exclude_paths={"root._people_by_id"})
    differences = []

    for diff_type, diffs in ddiff.items():
        if diff_type in ("values_changed", "type_changes"):
            # DeepDiff's equality is stricter than ours, so ignore differences
            # where the values are ==.
            differences.extend(
                _format_change(
                    location, repr(change["old_value"]), repr(change["new_value"])
                )
                for location, change in diffs.items()
                if change["old_value"] != change["new_value"]
            )
        elif diff_type == "iterable_item_removed":
            differences.extend(
                _format_remove(location, item) for location, item in diffs.items()
            )
        elif diff_type == "iterable_item_added":
            differences.extend(
                _format_add(location, item) for location, item in diffs.items()
            )
        else:
            raise ValueError("Unexpected diff value: {}".format(diff_type))

    return differences


def _format_change(location, old, new):
    return "{}: expected {} but got {}".format(location, old, new)


def _format_remove(location, item):
    return "{}: expected the following object, but none was found:\n{}".format(
        location, item
    )


def _format_add(location, item):
    return "{}: got the following unexpected item:\n{}".format(location, item)
