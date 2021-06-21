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
import { Card, Descriptions } from "antd";
import * as React from "react";
import NewTabLink from "../NewTabLink";

interface IngestLogsCardProps {
  stateCode: string;
  env: string;
}

const IngestLogsCard: React.FC<IngestLogsCardProps> = ({ stateCode, env }) => {
  let logsUrl = "";
  let non200Url = "";
  if (env === "staging" || env === "development") {
    logsUrl = `http://go/staging-ingest-logs/${stateCode.toLowerCase()}/`;
    non200Url = `http://go/staging-non-200-ingest-responses/${stateCode.toLowerCase()}/`;
  } else if (env === "production") {
    logsUrl = `http://go/prod-ingest-logs/${stateCode.toLowerCase()}/`;
    non200Url = `http://go/prod-non-200-ingest-responses/${stateCode.toLowerCase()}/`;
  }

  return (
    <Card title="Summary" style={{ width: 1000 }}>
      <Descriptions>
        <Descriptions.Item label="Logs Explorer" span={3}>
          <NewTabLink href={logsUrl}>{logsUrl}</NewTabLink>
        </Descriptions.Item>
        <Descriptions.Item label="Non-200 Ingest Responses" span={3}>
          <NewTabLink href={non200Url}>{non200Url}</NewTabLink>
        </Descriptions.Item>
      </Descriptions>
    </Card>
  );
};

export default IngestLogsCard;
