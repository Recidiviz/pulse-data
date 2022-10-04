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

import { Alert } from "antd";
import { useCallback, useEffect, useRef, useState } from "react";
import { useParams } from "react-router-dom";
import { getIngestInstanceSummary } from "../../AdminPanelAPI";
import { isAbortException } from "../Utilities/exceptions";
import { DirectIngestInstance, IngestInstanceSummary } from "./constants";
import IngestInstanceCard from "./IngestInstanceCard";
import IngestInstanceActionsPageHeader from "./IngestOperationsComponents/IngestInstanceActionsPageHeader";

const instances = [
  DirectIngestInstance.PRIMARY,
  DirectIngestInstance.SECONDARY,
];

const IngestStateSpecificInstanceMetadata = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const { stateCode, instance } =
    useParams<{ stateCode: string; instance: string }>();

  const directInstance = getInstance(instance);
  // Uses useRef so abort controller not re-initialized every render cycle.
  const abortControllerRef = useRef<AbortController | undefined>(undefined);

  const [ingestInstanceSummaryLoading, setIngestInstanceSummaryLoading] =
    useState<boolean>(true);
  const [ingestInstanceSummary, setIngestInstanceSummary] =
    useState<IngestInstanceSummary | undefined>(undefined);

  const fetchIngestInstanceSummary = useCallback(async () => {
    if (!directInstance) {
      return;
    }
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    abortControllerRef.current = new AbortController();
    setIngestInstanceSummaryLoading(true);
    try {
      const primaryResponse = await getIngestInstanceSummary(
        stateCode,
        directInstance,
        abortControllerRef.current
      );
      const result: IngestInstanceSummary = await primaryResponse.json();
      setIngestInstanceSummary(result);
    } catch (err) {
      if (!isAbortException(err)) {
        throw err;
      }
    }
    setIngestInstanceSummaryLoading(false);
  }, [directInstance, stateCode]);

  useEffect(() => {
    fetchIngestInstanceSummary();
  }, [fetchIngestInstanceSummary]);

  if (!directInstance) {
    return <Alert message="Invalid instance" type="error" />;
  }

  return (
    <>
      <IngestInstanceActionsPageHeader
        instance={directInstance}
        stateCode={stateCode}
        ingestInstanceSummary={ingestInstanceSummary}
        onRefreshIngestSummary={fetchIngestInstanceSummary}
      />
      {env === "development" ? (
        <>
          <Alert
            style={{ margin: "6px 24px" }}
            message="The Operations Database information is inaccurate. Users are unable to hit a live database
          from a local machine"
            type="warning"
            showIcon
          />
        </>
      ) : null}

      <div
        style={{ height: "95%", padding: "0 24px" }}
        className="main-content"
      >
        <IngestInstanceCard
          instance={directInstance}
          env={env}
          stateCode={stateCode}
          ingestInstanceSummary={ingestInstanceSummary}
          ingestInstanceSummaryLoading={ingestInstanceSummaryLoading}
        />
      </div>
    </>
  );
};

function getInstance(instance: string): DirectIngestInstance | undefined {
  return instances.find((i) => instance.includes(i.toString()));
}

export default IngestStateSpecificInstanceMetadata;
