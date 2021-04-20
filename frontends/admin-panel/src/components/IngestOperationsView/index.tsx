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
import * as React from "react";
import {
  Alert,
  Button,
  Col,
  Divider,
  PageHeader,
  Row,
  Select,
  Spin,
} from "antd";
import { useEffect, useState } from "react";
import {
  fetchIngestRegionCodes,
  getIngestQueuesState,
  startIngestRun,
  updateIngestQueuesState,
} from "../../AdminPanelAPI";
import useFetchedData from "../../hooks";
import IngestActionConfirmationForm from "./IngestActionConfirmationForm";
import {
  actionNames,
  IngestActions,
  QueueMetadata,
  QueueState,
} from "./constants";
import IngestQueueStatusCard from "./IngestQueueStatusCard";

const IngestOperationsView = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const projectId =
    env === "production" ? "recidiviz-123" : "recidiviz-staging";

  const { loading, data } = useFetchedData<string[]>(fetchIngestRegionCodes);
  const [regionCode, setRegionCode] = React.useState<string | undefined>(
    undefined
  );
  const [
    isConfirmationModalVisible,
    setIsConfirmationModalVisible,
  ] = React.useState(false);
  const [ingestAction, setIngestAction] = React.useState<
    IngestActions | undefined
  >(undefined);
  const [queueStates, setQueueStates] = useState<QueueMetadata[]>([]);
  const [queueStatesLoading, setQueueStatesLoading] = useState<boolean>(true);

  const ingestQueueActions = [
    IngestActions.PauseIngestQueues,
    IngestActions.ResumeIngestQueues,
  ];

  useEffect(() => {
    if (regionCode) {
      fetchQueueStates(regionCode);
    }
  }, [regionCode]);

  const handleRegionChange = (value: string) => {
    setRegionCode(value);
  };

  async function fetchQueueStates(regionCodeInput: string) {
    setQueueStatesLoading(true);
    const response = await getIngestQueuesState(regionCodeInput);
    const result: QueueMetadata[] = await response.json();
    setQueueStates(result);
    setQueueStatesLoading(false);
  }

  const onIngestActionConfirmation = async (
    ingestActionToExecute: IngestActions | undefined
  ) => {
    setIsConfirmationModalVisible(false);
    if (regionCode) {
      if (ingestActionToExecute === IngestActions.StartIngestRun) {
        await startIngestRun(regionCode);
      } else if (ingestActionToExecute === IngestActions.PauseIngestQueues) {
        setQueueStatesLoading(true);
        await updateIngestQueuesState(regionCode, QueueState.PAUSED);
        await fetchQueueStates(regionCode);
      } else if (ingestActionToExecute === IngestActions.ResumeIngestQueues) {
        setQueueStatesLoading(true);
        await updateIngestQueuesState(regionCode, QueueState.RUNNING);
        await fetchQueueStates(regionCode);
      }
    }
  };

  const showConfirmationModal = () => {
    setIsConfirmationModalVisible(true);
  };

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  return (
    <>
      <PageHeader
        title="Ingest Operations"
        subTitle={regionCode ? `${regionCode}, ${env}` : "Select a region"}
        extra={[
          <Select
            style={{ width: 200 }}
            placeholder="Select a region"
            onChange={handleRegionChange}
          >
            {data?.sort().map((code: string) => {
              return (
                <Select.Option key={code} value={code}>
                  {code}
                </Select.Option>
              );
            })}
          </Select>,
        ]}
      />
      {regionCode ? null : (
        <Alert
          message="A region must be selected to view the below information"
          type="warning"
          showIcon
        />
      )}

      <Divider orientation="left">Ingest Queues</Divider>
      <div className="site-card-wrapper">
        {regionCode ? (
          <IngestQueueStatusCard
            projectId={projectId}
            queueStates={queueStates}
            loading={queueStatesLoading}
          />
        ) : null}
        <Row gutter={[16, 16]}>
          {ingestQueueActions.map((action) => {
            return (
              <Col span={12} key={action}>
                <Button
                  onClick={() => {
                    setIngestAction(action);
                    showConfirmationModal();
                  }}
                  disabled={!regionCode}
                  block
                >
                  {/* TODO(#6072): Remove development label when we migrate to state-specific queues */}
                  {actionNames[action]?.concat(" (in development)")}
                </Button>
              </Col>
            );
          })}
        </Row>
      </div>
      {regionCode && ingestAction ? (
        <IngestActionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onIngestActionConfirmation}
          onCancel={() => {
            setIsConfirmationModalVisible(false);
          }}
          ingestAction={ingestAction}
          regionCode={regionCode}
        />
      ) : null}
    </>
  );
};

export default IngestOperationsView;
