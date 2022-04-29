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
import { SyncOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Card,
  Col,
  Divider,
  Empty,
  message,
  PageHeader,
  Row,
  Table,
} from "antd";
import * as React from "react";
import { useCallback, useEffect, useState } from "react";
import { useHistory, useLocation } from "react-router-dom";
import {
  exportDatabaseToGCS,
  fetchIngestStateCodes,
  getIngestInstanceSummary,
  getIngestQueuesState,
  pauseDirectIngestInstance,
  startIngestRun,
  unpauseDirectIngestInstance,
  updateIngestQueuesState,
} from "../../AdminPanelAPI";
import ActionRegionConfirmationForm from "../Utilities/ActionRegionConfirmationForm";
import StateSelector from "../Utilities/StateSelector";
import {
  actionNames,
  DirectIngestInstance,
  IngestActions,
  IngestInstanceSummary,
  QueueMetadata,
  QueueState,
  StateCodeInfo,
} from "./constants";
import IngestInstanceCard from "./IngestInstanceCard";
import IngestQueuesTable from "./IngestQueuesTable";

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
  const [actionConfirmed, setActionConfirmed] = useState<boolean>(false);

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
      setQueueStatesLoading(false);
    }
  }, [stateCode]);

  useEffect(() => {
    getData();
  }, [getData, actionConfirmed]);

  const stateCodeChange = (value: StateCodeInfo) => {
    setStateCode(value.code);
    updateQueryParams(value.code);
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
  }

  async function fetchIngestInstanceSummaries(regionCodeInput: string) {
    const primaryResponse = await getIngestInstanceSummary(
      regionCodeInput,
      DirectIngestInstance.PRIMARY
    );
    const secondaryResponse = await getIngestInstanceSummary(
      regionCodeInput,
      DirectIngestInstance.SECONDARY
    );
    const result: IngestInstanceSummary[] = [
      await primaryResponse.json(),
      await secondaryResponse.json(),
    ];
    setIngestInstanceSummaries(result);
    setIngestInstanceSummariesLoading(false);
  }

  const onIngestActionConfirmation = async (
    ingestActionToExecute: string | undefined
  ) => {
    setIsConfirmationModalVisible(false);
    if (stateCode) {
      setQueueStatesLoading(true);
      setActionConfirmed(false);
      const unsupportedIngestAction = "Unsupported ingest action";
      switch (ingestActionToExecute) {
        case IngestActions.StartIngestRun:
          if (ingestInstance) {
            await startIngestRun(stateCode, ingestInstance);
            setActionConfirmed(true);
          }
          break;
        case IngestActions.PauseIngestQueues:
          await updateIngestQueuesState(stateCode, QueueState.PAUSED);
          await fetchQueueStates(stateCode);
          setActionConfirmed(true);
          break;
        case IngestActions.ResumeIngestQueues:
          await updateIngestQueuesState(stateCode, QueueState.RUNNING);
          await fetchQueueStates(stateCode);
          setActionConfirmed(true);
          break;
        case IngestActions.PauseIngestInstance:
          if (ingestInstance) {
            await pauseDirectIngestInstance(stateCode, ingestInstance);
            await fetchIngestInstanceSummaries(stateCode);
            setActionConfirmed(true);
          }
          break;
        case IngestActions.UnpauseIngestInstance:
          if (ingestInstance) {
            await unpauseDirectIngestInstance(stateCode, ingestInstance);
            await fetchIngestInstanceSummaries(stateCode);
            setActionConfirmed(true);
          }
          break;
        case IngestActions.ExportToGCS:
          if (ingestInstance) {
            message.info("Exporting database...");
            const r = await exportDatabaseToGCS(stateCode, ingestInstance);
            if (r.status >= 400) {
              const text = await r.text();
              message.error(`Export to GCS failed: ${text}`);
            } else {
              message.success("GCS Export succeeded!");
            }
            setActionConfirmed(true);
          }
          break;
        default:
          throw unsupportedIngestAction;
      }
      setQueueStatesLoading(false);
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
          <StateSelector
            fetchStateList={fetchIngestStateCodes}
            onChange={stateCodeChange}
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
                {actionNames[action]}
              </Button>
            );
          })}
        </div>
      </div>

      {stateCode && ingestAction ? (
        <ActionRegionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onIngestActionConfirmation}
          onCancel={() => {
            setIsConfirmationModalVisible(false);
          }}
          action={ingestAction}
          actionName={actionNames[ingestAction]}
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
      {stateCode ? (
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
              : ingestInstanceSummaries.map(
                  (summary: IngestInstanceSummary) => {
                    return (
                      <Col span={12} key={summary.instance}>
                        <IngestInstanceCard
                          data={summary}
                          env={env}
                          stateCode={stateCode}
                          handleOnClick={handleIngestActionOnClick}
                        />
                      </Col>
                    );
                  }
                )}
          </Row>
        </div>
      ) : null}
    </>
  );
};

export default IngestOperationsView;
