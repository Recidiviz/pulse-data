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

import { Divider, Alert } from "antd";
import { useParams } from "react-router-dom";
import { DirectIngestInstance } from "./constants";
import IngestInstanceCard from "./IngestInstanceCard";

const instances = [
  DirectIngestInstance.PRIMARY,
  DirectIngestInstance.SECONDARY,
];

const IngestStateSpecificInstanceMetadata = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const { stateCode, instance } =
    useParams<{ stateCode: string; instance: string }>();

  const directInstance = getInstance(instance);
  if (!directInstance) {
    return <Alert message="Invalid instance" type="error" />;
  }

  return (
    <>
      <Divider orientation="left" style={{ marginTop: 24 }}>
        Ingest Instances
      </Divider>
      {env === "development" ? (
        <Alert
          message="The Operations Database information is inaccurate. Users are unable to hit a live database
          from a local machine"
          type="warning"
          showIcon
        />
      ) : null}
      <br />

      <div className="site-card-wrapper">
        <IngestInstanceCard
          instance={directInstance}
          env={env}
          stateCode={stateCode}
        />
      </div>
    </>
  );
};

function getInstance(instance: string): DirectIngestInstance | undefined {
  return instances.find((i) => instance.includes(i.toString()));
}

export default IngestStateSpecificInstanceMetadata;
