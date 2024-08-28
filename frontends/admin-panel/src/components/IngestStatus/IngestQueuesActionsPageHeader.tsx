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
import { Button, PageHeader } from "antd";
import classNames from "classnames";

import {
  RegionAction,
  regionActionNames,
} from "../Utilities/ActionRegionConfirmationForm";
import { QueueMetadata } from "./constants";
import IngestActionButton from "./IngestActionButton";
import {
  getIngestQueuesCumulativeState,
  getQueueColor,
  removeUnderscore,
} from "./ingestStatusUtils";

interface IngestQueuesActionsPageHeaderProps {
  stateCode: string;
  queueStates: QueueMetadata[];
  onRefreshQueuesData: () => void;
}

const ingestQueueActions = [
  RegionAction.PauseIngestQueues,
  RegionAction.ResumeIngestQueues,
];

const IngestQueuesActionsPageHeader: React.FC<
  IngestQueuesActionsPageHeaderProps
> = ({ stateCode, queueStates, onRefreshQueuesData }) => {
  return (
    <PageHeader
      title="Ingest Queues"
      tags={createIngestQueueStatusTag(queueStates)}
      extra={ingestQueueActions
        .map((action) => {
          return (
            <IngestActionButton
              style={{ display: "block", textAlign: "center", width: "auto" }}
              action={action}
              stateCode={stateCode}
              buttonText={regionActionNames[action]}
              onActionConfirmed={() => {
                onRefreshQueuesData();
              }}
              block
              key={action}
            />
          );
        })
        .concat(
          ...[
            <Button
              type="primary"
              shape="circle"
              icon={<SyncOutlined />}
              onClick={() => onRefreshQueuesData()}
            />,
          ]
        )}
    />
  );
};

export default IngestQueuesActionsPageHeader;

function createIngestQueueStatusTag(queueStates: QueueMetadata[]): JSX.Element {
  const queueState = getIngestQueuesCumulativeState(queueStates).toString();
  return (
    <div
      className={classNames("tag", "tag-with-color", getQueueColor(queueState))}
    >
      {removeUnderscore(queueState)}
    </div>
  );
}
