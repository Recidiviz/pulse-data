// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
export type LogsType = "direct_ingest" | "app_engine" | "cloud_run";

export type RequestTrace = {
  timestamp: string;
  trace: string;
};

export type ErrorInstance = {
  method: string;
  status: string;
  url: string;
  errorLogs: string[];
  latestResponse: string;
  earliestResponse: string;
  sinceSucceededTimestamp: string;
  traces: RequestTrace[];
  resource: {
    type: "gae_app" | "cloud_run_revision";
    labels: Record<string, string>;
  };
};
