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
  Card,
  Col,
  Divider,
  PageHeader,
  Row,
  Select,
  Spin,
} from "antd";
import { SyncOutlined } from "@ant-design/icons";
import { useCallback, useEffect, useState } from "react";
import {
  fetchIngestRegionCodes,
  getIngestInstanceSummaries,
  getIngestQueuesState,
  startIngestRun,
  updateIngestQueuesState,
} from "../../AdminPanelAPI";
import useFetchedData from "../../hooks";
import IngestActionConfirmationForm from "./IngestActionConfirmationForm";
import {
  actionNames,
  DirectIngestInstance,
  IngestActions,
  IngestInstanceSummary,
  QueueMetadata,
  QueueState,
} from "./constants";
import IngestQueuesTable from "./IngestQueuesTable";
import IngestInstanceCard from "./IngestInstanceCard";

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
  const [ingestInstanceSummaries, setIngestInstanceSummaries] = useState<
    IngestInstanceSummary[]
  >([]);
  const [
    ingestInstanceSummariesLoading,
    setIngestInstanceSummariesLoading,
  ] = useState<boolean>(true);
  const [ingestInstance, setIngestInstance] = useState<
    DirectIngestInstance | undefined
  >(undefined);

  const ingestQueueActions = [
    IngestActions.PauseIngestQueues,
    IngestActions.ResumeIngestQueues,
  ];

  const getData = useCallback(async () => {
    if (regionCode) {
      setQueueStatesLoading(true);
      setIngestInstanceSummariesLoading(true);
      await fetchQueueStates(regionCode);
      await fetchIngestInstanceSummaries(regionCode);
    }
  }, [regionCode]);

  useEffect(() => {
    getData();
  }, [getData]);

  const handleRegionChange = (value: string) => {
    setRegionCode(value);
  };

  const handleIngestActionOnClick = (
    action: IngestActions,
    instance: DirectIngestInstance | undefined = undefined
  ) => {
    setIngestAction(action);
    setIngestInstance(instance);
    showConfirmationModal();
  };

  async function fetchQueueStates(regionCodeInput: string) {
    const response = await getIngestQueuesState(regionCodeInput);
    const result: QueueMetadata[] = await response.json();
    setQueueStates(result);
    setQueueStatesLoading(false);
  }

  async function fetchIngestInstanceSummaries(regionCodeInput: string) {
    const response = await getIngestInstanceSummaries(regionCodeInput);
    const result: IngestInstanceSummary[] = await response.json();
    setIngestInstanceSummaries(result);
    setIngestInstanceSummariesLoading(false);
  }

  const onIngestActionConfirmation = async (
    ingestActionToExecute: IngestActions | undefined
  ) => {
    setIsConfirmationModalVisible(false);
    if (regionCode) {
      if (
        ingestActionToExecute === IngestActions.StartIngestRun &&
        ingestInstance
      ) {
        await startIngestRun(regionCode, ingestInstance);
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
          <Button
            type="primary"
            shape="circle"
            icon={<SyncOutlined />}
            disabled={!regionCode}
            onClick={() => getData()}
          />,
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
              <Button
                onClick={() => {
                  handleIngestActionOnClick(action);
                }}
                disabled={!regionCode}
                block
                key={action}
                style={{ display: "block", textAlign: "center", width: "auto" }}
              >
                {/* TODO(#6072): Remove development label when we migrate to state-specific queues */}
                {actionNames[action]?.concat(" (in dev)")}
              </Button>
            );
          })}
        </div>
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
          ingestInstance={ingestInstance}
        />
      ) : null}

      <Divider orientation="left" style={{ marginTop: 24 }}>
        Ingest Instances
      </Divider>
      <div className="site-card-wrapper">
        <Row gutter={[16, 16]}>
          {ingestInstanceSummariesLoading
            ? Object.keys(DirectIngestInstance).map((instance) => {
                return (
                  <Col span={12} key={instance}>
                    <Card title={instance} loading />
                  </Col>
                );
              })
            : ingestInstanceSummaries.map((summary: IngestInstanceSummary) => {
                return (
                  <Col span={12} key={summary.instance}>
                    <IngestInstanceCard
                      data={summary}
                      handleOnClick={handleIngestActionOnClick}
                    />
                  </Col>
                );
              })}
        </Row>
      </div>
    </>
  );
};

export default IngestOperationsView;
