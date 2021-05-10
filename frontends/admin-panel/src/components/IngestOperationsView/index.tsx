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
  Empty,
  PageHeader,
  Row,
  Table,
} from "antd";
import { SyncOutlined } from "@ant-design/icons";
import { useCallback, useEffect, useState } from "react";
import { useHistory, useLocation } from "react-router-dom";
import {
  getIngestInstanceSummaries,
  getIngestQueuesState,
  startIngestRun,
  updateIngestQueuesState,
} from "../../AdminPanelAPI";
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
import IngestStateSelector from "../IngestStateSelector";
import IngestLogsCard from "./IngestLogsCard";

const IngestOperationsView = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const projectId =
    env === "production" ? "recidiviz-123" : "recidiviz-staging";

  const history = useHistory();

  const location = useLocation();
  const queryString = location.search;
  const params = new URLSearchParams(queryString);
  const initialStateCode = params.get("stateCode");

  const [stateCode, setStateCode] =
    React.useState<string | null>(initialStateCode);
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    React.useState(false);
  const [ingestAction, setIngestAction] =
    React.useState<IngestActions | undefined>(undefined);
  const [queueStates, setQueueStates] = useState<QueueMetadata[]>([]);
  const [queueStatesLoading, setQueueStatesLoading] = useState<boolean>(true);
  const [ingestInstanceSummaries, setIngestInstanceSummaries] = useState<
    IngestInstanceSummary[]
  >([]);
  const [ingestInstanceSummariesLoading, setIngestInstanceSummariesLoading] =
    useState<boolean>(true);
  const [ingestInstance, setIngestInstance] =
    useState<DirectIngestInstance | undefined>(undefined);

  const ingestQueueActions = [
    IngestActions.PauseIngestQueues,
    IngestActions.ResumeIngestQueues,
  ];

  const getData = useCallback(async () => {
    if (stateCode) {
      setQueueStatesLoading(true);
      setIngestInstanceSummariesLoading(true);
      await fetchQueueStates(stateCode);
      await fetchIngestInstanceSummaries(stateCode);
    }
  }, [stateCode]);

  useEffect(() => {
    getData();
  }, [getData]);

  const handleStateCodeChange = (value: string) => {
    setStateCode(value);
    updateQueryParams(value);
  };

  const updateQueryParams = (stateCodeInput: string) => {
    const locationObj = {
      search: `?stateCode=${stateCodeInput}`,
    };
    history.push(locationObj);
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
    if (stateCode) {
      if (
        ingestActionToExecute === IngestActions.StartIngestRun &&
        ingestInstance
      ) {
        await startIngestRun(stateCode, ingestInstance);
      } else if (ingestActionToExecute === IngestActions.PauseIngestQueues) {
        setQueueStatesLoading(true);
        await updateIngestQueuesState(stateCode, QueueState.PAUSED);
        await fetchQueueStates(stateCode);
      } else if (ingestActionToExecute === IngestActions.ResumeIngestQueues) {
        setQueueStatesLoading(true);
        await updateIngestQueuesState(stateCode, QueueState.RUNNING);
        await fetchQueueStates(stateCode);
      }
    }
  };

  const showConfirmationModal = () => {
    setIsConfirmationModalVisible(true);
  };

  return (
    <>
      <PageHeader
        title="Ingest Operations"
        extra={[
          <Button
            type="primary"
            shape="circle"
            icon={<SyncOutlined />}
            disabled={!stateCode}
            onClick={() => getData()}
          />,
          <IngestStateSelector
            handleStateCodeChange={handleStateCodeChange}
            initialValue={initialStateCode}
          />,
        ]}
      />
      {stateCode ? null : (
        <Alert
          message="A region must be selected to view the below information"
          type="warning"
          showIcon
        />
      )}
      <Divider orientation="left">Ingest Queues</Divider>
      <div className="site-card-wrapper">
        {stateCode ? (
          <IngestQueuesTable
            projectId={projectId}
            queueStates={queueStates}
            loading={queueStatesLoading}
          />
        ) : (
          <Table>
            <Empty />
          </Table>
        )}
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
                disabled={!stateCode}
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

      {stateCode && ingestAction ? (
        <IngestActionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onIngestActionConfirmation}
          onCancel={() => {
            setIsConfirmationModalVisible(false);
          }}
          ingestAction={ingestAction}
          regionCode={stateCode}
          ingestInstance={ingestInstance}
        />
      ) : null}

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

      <Divider orientation="left" style={{ marginTop: 24 }}>
        Ingest Logs
      </Divider>
      <br />
      <div
        className="site-card-wrapper"
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-evenly",
        }}
      >
        {stateCode ? (
          <IngestLogsCard stateCode={stateCode} env={env} />
        ) : (
          <Card title="Summary" loading style={{ width: 1000 }} />
        )}
      </div>
    </>
  );
};

export default IngestOperationsView;
