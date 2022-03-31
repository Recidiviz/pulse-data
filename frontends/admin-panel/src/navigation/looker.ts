// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================
export const LOOKER_DASHBOARDS_BASE =
  "https://recidiviz.cloud.looker.com/dashboards";

export const LOOKER_PERSON_DETAILS_STAGING = `${LOOKER_DASHBOARDS_BASE}/state_staging::person_details_staging`;
export const LOOKER_PERSON_DETAILS_PROD = `${LOOKER_DASHBOARDS_BASE}/state_prod::person_details_prod`;
