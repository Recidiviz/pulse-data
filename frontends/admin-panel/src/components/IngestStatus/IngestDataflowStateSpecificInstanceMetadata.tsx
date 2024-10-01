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
import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import { isRawDataImportDagEnabled } from "../../AdminPanelAPI/IngestOperations";
import { DirectIngestInstance } from "./constants";
import IngestDataflowInstanceCard from "./IngestDataflowInstanceCard";
import IngestInstanceActionsPageHeader from "./IngestInstanceActionsPageHeader";
import RawDataActionsPageHeader from "./RawDataActionsPageHeader";

const instances = [
  DirectIngestInstance.PRIMARY,
  DirectIngestInstance.SECONDARY,
];

const IngestStateSpecificInstanceMetadata = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const { stateCode, instance } = useParams<{
    stateCode: string;
    instance: string;
  }>();

  // TODO(#28239) remove once raw data import dag is rolled out
  const [rawDataImportDagEnabled, setRawDataImportDagEnabled] =
    useState<boolean>(false);
  const [rawDataImportDagEnabledLoading, setRawDataImportDagEnabledLoading] =
    useState<boolean>(false);

  const getRawDataDagEnabled = useCallback(async () => {
    if (!stateCode || !instance) {
      return;
    }
    setRawDataImportDagEnabledLoading(true);
    const isEnabledResponse = await isRawDataImportDagEnabled(
      stateCode,
      instance
    );
    const isEnabledResult = await isEnabledResponse.json();
    setRawDataImportDagEnabled(isEnabledResult);
    setRawDataImportDagEnabledLoading(false);
  }, [stateCode, instance]);

  useEffect(() => {
    getRawDataDagEnabled();
  }, [getRawDataDagEnabled, instance, stateCode]);

  const directInstance = getInstance(instance);

  if (!directInstance) {
    return <Alert message="Invalid instance" type="error" />;
  }

  if (rawDataImportDagEnabledLoading) {
    return <Spin />;
  }

  const pageHeader = rawDataImportDagEnabled ? (
    <RawDataActionsPageHeader instance={directInstance} stateCode={stateCode} />
  ) : (
    <IngestInstanceActionsPageHeader
      instance={directInstance}
      stateCode={stateCode}
    />
  );

  return (
    <>
      {pageHeader}
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
        <IngestDataflowInstanceCard
          instance={directInstance}
          env={env}
          stateCode={stateCode}
          rawDataImportDagEnabled={rawDataImportDagEnabled}
        />
      </div>
    </>
  );
};

function getInstance(instance: string): DirectIngestInstance | undefined {
  return instances.find((i) => instance.includes(i.toString()));
}

export default IngestStateSpecificInstanceMetadata;
