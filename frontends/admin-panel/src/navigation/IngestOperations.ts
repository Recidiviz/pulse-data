// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
export const INGEST_OPERATIONS_BASE = "/admin/ingest_operations";
export const INGEST_ACTIONS_ROUTE = `${INGEST_OPERATIONS_BASE}/key_actions`;
export const INGEST_ACTIONS_WITH_STATE_CODE_ROUTE = `${INGEST_OPERATIONS_BASE}/key_actions/:stateCode`;
export const INGEST_ACTIONS_INGEST_QUEUES_ROUTE = `${INGEST_ACTIONS_WITH_STATE_CODE_ROUTE}/ingest_queues`;
export const INGEST_ACTIONS_INSTANCE_ROUTE = `${INGEST_ACTIONS_WITH_STATE_CODE_ROUTE}/instance/:instance`;
export const INGEST_ACTIONS_PRIMARY_ROUTE = `${INGEST_ACTIONS_WITH_STATE_CODE_ROUTE}/instance/PRIMARY`;
export const INGEST_ACTIONS_SECONDARY_ROUTE = `${INGEST_ACTIONS_WITH_STATE_CODE_ROUTE}/instance/SECONDARY`;
export const FLASH_DB_CHECKLIST_ROUTE = `${INGEST_OPERATIONS_BASE}/flash_primary_db`;
export const DIRECT_SANDBOX_RAW_IMPORT = `${INGEST_OPERATIONS_BASE}/direct_import`;
