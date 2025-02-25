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
import status, { HttpStatus } from "http-status";
import moment from "moment-timezone";

import { gcpEnvironment } from "../Utilities/EnvironmentUtilities";
import { ErrorInstance, RequestTrace } from "./types";

const specialStatusCodes = {
  499: "CLIENT_CLOSED_REQUEST",
};

export const getIssueTitle = (instance: ErrorInstance): string => {
  return `${instance.status} ${instance.method} \`${instance.url}\``;
};

export const formatTimestamp = (date: string): string => {
  return moment(date).tz(moment.tz.guess()).format("LLL z");
};

export const getStatusCodeTitle = (code: number): string => {
  /* Converts names like INTERNAL_SERVER_ERROR to Internal Server Error */
  let name;
  if (Object.keys(specialStatusCodes).includes(String(code))) {
    name = specialStatusCodes[code as keyof typeof specialStatusCodes];
  } else {
    name = status[`${code}_NAME` as keyof HttpStatus] as string;
  }

  if (!name) {
    return "";
  }
  name = name
    .toLowerCase()
    .replace(/_(.)/g, (match, letter) => ` ${letter.toUpperCase()}`);

  return `${name[0].toUpperCase()}${name.substr(1)}`;
};

export const getTraceQuery = (trace: RequestTrace): string => {
  return `trace = "${trace.trace}"`;
};

export const getRequestsQuery = (instance: ErrorInstance): string => {
  return `resource.type = "${instance.resource.type}" httpRequest.requestUrl = "${instance.url}"`;
};

export const buildLogsExplorerUrl = (query: string): string => {
  const projectId = gcpEnvironment.isProduction
    ? "recidiviz-123"
    : "recidiviz-staging";

  return `https://console.cloud.google.com/logs/query;query=${encodeURIComponent(
    query
  )};duration=P7D?project=${projectId}`;
};
