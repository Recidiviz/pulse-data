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
"""Gating helpers for rolling out normalization in ingest pipelines."""
from recidiviz.common.constants.states import StateCode
from recidiviz.utils import environment


# TODO(#31741): Delete this function once we've shipped combined ingest and
#  normalization
def should_run_normalization_in_ingest(state_code: StateCode) -> bool:
    """This gate determines whether we should output normalized entities from the ingest
    pipeline but does not determine whether we read that data downstream.
    """
    # TODO(#29517): Add states here as we launch combined pipelines to prod
    prod_launched_states: set[StateCode] = {
        StateCode.US_IA,
        StateCode.US_ID,
        StateCode.US_MA,
        StateCode.US_NC,
        StateCode.US_OZ,
        StateCode.US_TX,
        StateCode.US_UT,
    }

    staging_only_launched_states: set[StateCode] = {
        StateCode.US_AR,
        StateCode.US_AZ,
        StateCode.US_CA,
        StateCode.US_CO,
        StateCode.US_IX,
        StateCode.US_MI,
        StateCode.US_ND,
        StateCode.US_NE,
        StateCode.US_OR,
        StateCode.US_TN,
    }

    unlaunched_states: set[StateCode] = {
        # TODO(#32760): Ungate in staging once null start_date values have been removed
        #  from NormalizedStateStaffRolePeriod
        # TODO(#29517): Ungate in staging once test staff_external_ids are fixed
        StateCode.US_MO,
        # TODO(#29517): Ungate in staging once test staff_external_ids are fixed
        StateCode.US_ME,
        # TODO(#29517): Ungate in staging once test staff_external_ids are fixed
        StateCode.US_PA,
    }

    if intersection := staging_only_launched_states.intersection(prod_launched_states):
        raise ValueError(
            f"Found states in both staging_only_launched_states and "
            f"prod_launched_states: {[s.value for s in intersection]}"
        )

    if intersection := unlaunched_states.intersection(prod_launched_states):
        raise ValueError(
            f"Found states in both unlaunched_states and prod_launched_states: "
            f"{[s.value for s in intersection]}"
        )

    if intersection := unlaunched_states.intersection(staging_only_launched_states):
        raise ValueError(
            f"Found states in both unlaunched_states and staging_only_launched_states: "
            f"{[s.value for s in intersection]}"
        )

    if environment.in_gcp_production():
        return state_code in prod_launched_states

    if state_code in {
        *prod_launched_states,
        *staging_only_launched_states,
    }:
        return True

    if state_code in unlaunched_states:
        return False

    raise ValueError(f"Uncategorized state code: {state_code.value}")


# TODO(#31741): Delete this function once we've shipped combined ingest and
#  normalization
def is_combined_ingest_and_normalization_launched_in_env(state_code: StateCode) -> bool:
    """This gate determines whether downstream processes read from the ingest pipeline
    normalization output.
    """
    # TODO(#29517): Add states here as we launch combined pipelines to prod
    prod_launched_states: set[StateCode] = {
        StateCode.US_IA,
        StateCode.US_ID,
        StateCode.US_MA,
        StateCode.US_NC,
        StateCode.US_OZ,
        StateCode.US_TX,
        StateCode.US_UT,
    }

    staging_only_launched_states: set[StateCode] = {
        StateCode.US_AZ,
        StateCode.US_CA,
        # TODO(#29517): Add states here as we launch combined pipelines to staging
    }

    # There are no downstream processes reading normalized entities produced by the
    # ingest pipeline for these states.
    unlaunched_states: set[StateCode] = {
        StateCode.US_AR,
        StateCode.US_CO,
        StateCode.US_IX,
        StateCode.US_MO,
        StateCode.US_MI,
        StateCode.US_ME,
        StateCode.US_ND,
        StateCode.US_NE,
        StateCode.US_OR,
        StateCode.US_PA,
        StateCode.US_TN,
    }

    if intersection := staging_only_launched_states.intersection(prod_launched_states):
        raise ValueError(
            f"Found states in both staging_only_launched_states and "
            f"prod_launched_states: {[s.value for s in intersection]}"
        )

    if intersection := unlaunched_states.intersection(prod_launched_states):
        raise ValueError(
            f"Found states in both unlaunched_states and prod_launched_states: "
            f"{[s.value for s in intersection]}"
        )

    if intersection := unlaunched_states.intersection(staging_only_launched_states):
        raise ValueError(
            f"Found states in both unlaunched_states and staging_only_launched_states: "
            f"{[s.value for s in intersection]}"
        )

    if environment.in_gcp_production():
        return state_code in prod_launched_states

    if state_code in {
        *prod_launched_states,
        *staging_only_launched_states,
    }:
        return True

    if state_code in unlaunched_states:
        return False

    raise ValueError(f"Uncategorized state code: {state_code.value}")
