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
export const ADMIN_BASE = "/admin/";
export const INGEST_OPERATIONS_BASE = `${ADMIN_BASE}ingest_operations`;
export const FLASH_DB_CHECKLIST_ROUTE = `${INGEST_OPERATIONS_BASE}/flash_primary_db`;
export const INGEST_DATAFLOW_ROUTE = `${INGEST_OPERATIONS_BASE}/ingest_pipeline_summary`;
export const INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE = `${INGEST_DATAFLOW_ROUTE}/:stateCode`;
export const INGEST_DATAFLOW_INGEST_QUEUES_ROUTE = `${INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE}/raw_data_queues`;
export const INGEST_DATAFLOW_PRIMARY_ROUTE = `${INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE}/instance/PRIMARY`;
export const INGEST_DATAFLOW_SECONDARY_ROUTE = `${INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE}/instance/SECONDARY`;
export const INGEST_DATAFLOW_INSTANCE_ROUTE = `${INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE}/instance/:instance`;
