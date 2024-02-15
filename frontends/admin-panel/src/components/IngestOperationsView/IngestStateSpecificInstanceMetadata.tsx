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

import { Alert, Spin } from "antd";
import { useCallback } from "react";
import { useParams } from "react-router-dom";

import { isIngestInDataflowEnabled } from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON } from "../../hooks";
import { DirectIngestInstance } from "../IngestDataflow/constants";
import IngestInstanceActionsPageHeader from "../IngestDataflow/IngestInstanceActionsPageHeader";
import IngestInstanceCard from "./IngestInstanceCard";

const instances = [
  DirectIngestInstance.PRIMARY,
  DirectIngestInstance.SECONDARY,
];

// TODO(#24652): delete once dataflow is fully enabled
const IngestStateSpecificInstanceMetadata = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const { stateCode, instance } = useParams<{
    stateCode: string;
    instance: string;
  }>();

  const directInstance = getInstance(instance);
  const fetchDataflowEnabled = useCallback(async () => {
    if (directInstance === undefined) {
      throw new Error("Invalid instance");
    }
    return isIngestInDataflowEnabled(stateCode, directInstance);
  }, [stateCode, directInstance]);
  const { loading, data: dataflowEnabled } =
    useFetchedDataJSON<boolean>(fetchDataflowEnabled);

  if (!directInstance) {
    return <Alert message="Invalid instance" type="error" />;
  }

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  if (dataflowEnabled === undefined) {
    return <Alert message="Unable to load dataflow gating info" type="error" />;
  }

  return (
    <>
      <IngestInstanceActionsPageHeader
        instance={directInstance}
        stateCode={stateCode}
        dataflowEnabled={dataflowEnabled}
      />
      {env === "development" ? (
        <Alert
          style={{ margin: "6px 24px" }}
          message="The Operations Database information is inaccurate. Users are unable to hit a live database
          from a local machine"
          type="warning"
          showIcon
        />
      ) : null}

      <div
        style={{ height: "95%" }}
        className="main-content content-side-padding"
      >
        <IngestInstanceCard
          instance={directInstance}
          env={env}
          stateCode={stateCode}
          dataflowEnabled={dataflowEnabled}
        />
      </div>
    </>
  );
};

function getInstance(instance: string): DirectIngestInstance | undefined {
  return instances.find((i) => instance.includes(i.toString()));
}

export default IngestStateSpecificInstanceMetadata;
