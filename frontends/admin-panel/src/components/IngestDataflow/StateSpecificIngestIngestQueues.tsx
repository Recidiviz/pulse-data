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

import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import { getIngestQueuesState } from "../../AdminPanelAPI";
import { QueueMetadata } from "./constants";
import IngestQueuesActionsPageHeader from "./IngestQueuesActionsPageHeader";
import IngestQueuesTable from "./IngestQueuesTable";

const StateSpecificIngestQueues = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const projectId =
    env === "production" ? "recidiviz-123" : "recidiviz-staging";
  const { stateCode } = useParams<{ stateCode: string }>();
  const [queueStates, setQueueStates] = useState<QueueMetadata[]>([]);
  const [queueStatesLoading, setQueueStatesLoading] = useState<boolean>(true);

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
      <IngestQueuesActionsPageHeader
        stateCode={stateCode}
        queueStates={queueStates}
        onRefreshQueuesData={getData}
      />
      <div
        style={{ height: "90%" }}
        className="main-content content-side-padding"
      >
        <IngestQueuesTable
          projectId={projectId}
          queueStates={queueStates}
          loading={queueStatesLoading}
        />
      </div>
    </>
  );
};
export default StateSpecificIngestQueues;
