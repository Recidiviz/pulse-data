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

import { Alert, PageHeader, Spin } from "antd";
import { useCallback } from "react";
import { useParams } from "react-router-dom";
import { isIngestInDataflowEnabled } from "../../AdminPanelAPI/IngestOperations";
import { useFetchedDataJSON } from "../../hooks";
import IngestInstanceCard from "../IngestOperationsView/IngestInstanceCard";
import IngestDataflowInstanceCard from "./IngestDataflowInstanceCard";
import { DirectIngestInstance } from "./constants";

const instances = [
  DirectIngestInstance.PRIMARY,
  DirectIngestInstance.SECONDARY,
];

const IngestStateSpecificInstanceMetadata = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const { stateCode, instance } =
    useParams<{ stateCode: string; instance: string }>();

  const directInstance = getInstance(instance);

  // TODO(#24652): delete once dataflow is fully enabled
  const fetchDataflowEnabled = useCallback(async () => {
    if (directInstance === undefined) {
      throw new Error("Invalid instance");
    }
    return isIngestInDataflowEnabled(stateCode, directInstance);
  }, [stateCode, directInstance]);
  const { data: dataflowEnabled } =
    useFetchedDataJSON<boolean>(fetchDataflowEnabled);

  if (!directInstance) {
    return <Alert message="Invalid instance" type="error" />;
  }

  if (dataflowEnabled === undefined) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  return (
    <>
      <PageHeader title={instance} />
      <div
        style={{ height: "95%" }}
        className="main-content content-side-padding"
      >
        <IngestDataflowInstanceCard
          instance={directInstance}
          env={env}
          stateCode={stateCode}
        />
        {/* TODO(#20930): Delete the the IngestInstanceCard and move all relevant 
           post-IID components into the IngestDataflowInstanceCard above. */}
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
