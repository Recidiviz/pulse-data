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

import { SyncOutlined } from "@ant-design/icons";
import { Button, Divider, PageHeader } from "antd";
import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { getIngestQueuesState } from "../../AdminPanelAPI";
import {
  RegionAction,
  regionActionNames,
} from "../Utilities/ActionRegionConfirmationForm";
import { QueueMetadata } from "./constants";
import IngestActionButton from "./IngestActionButton";
import IngestQueuesTable from "./IngestQueuesTable";

const StateSpecificIngestQueues = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const projectId =
    env === "production" ? "recidiviz-123" : "recidiviz-staging";
  const { stateCode } = useParams<{ stateCode: string }>();
  const [queueStates, setQueueStates] = useState<QueueMetadata[]>([]);
  const [queueStatesLoading, setQueueStatesLoading] = useState<boolean>(true);

  const ingestQueueActions = [
    RegionAction.PauseIngestQueues,
    RegionAction.ResumeIngestQueues,
  ];

  const getData = useCallback(async () => {
    setQueueStatesLoading(true);
    await fetchQueueStates(stateCode);
    setQueueStatesLoading(false);
  }, [stateCode]);

  useEffect(() => {
    getData();
  }, [getData, stateCode]);

  async function fetchQueueStates(regionCodeInput: string) {
    const response = await getIngestQueuesState(regionCodeInput);
    const result: QueueMetadata[] = await response.json();
    setQueueStates(result);
  }

  return (
    <>
      <PageHeader
        extra={[
          <Button
            type="primary"
            shape="circle"
            icon={<SyncOutlined />}
            onClick={() => getData()}
          />,
        ]}
      />
      <Divider orientation="left">Ingest Queues</Divider>
      <div className="site-card-wrapper" />
      <IngestQueuesTable
        projectId={projectId}
        queueStates={queueStates}
        loading={queueStatesLoading}
      />
      <br />
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-evenly",
        }}
      >
        {ingestQueueActions.map((action) => {
          return (
            <IngestActionButton
              style={{ display: "block", textAlign: "center", width: "auto" }}
              action={action}
              stateCode={stateCode}
              buttonText={regionActionNames[action]}
              onActionConfirmed={() => {
                getData();
              }}
              block
              key={action}
            />
          );
        })}
      </div>
    </>
  );
};
export default StateSpecificIngestQueues;
