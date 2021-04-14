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
import { fetchIngestRegionCodes } from "../AdminPanelAPI";
import useFetchedData from "../hooks";
import IngestActionConfirmationForm from "./IngestActionConfirmationForm";
import IngestActions from "../constants/ingestActions";

const ActionsView = (): JSX.Element => {
  const env = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const actionsData = [
    IngestActions.StartIngestRun,
    IngestActions.PauseIngestQueues,
    IngestActions.ResumeIngestQueues,
  ];

  const { loading, data } = useFetchedData<string[]>(fetchIngestRegionCodes);
  const [regionCode, setRegionCode] = React.useState<string | undefined>(
    undefined
  );
  const [isModalVisible, setIsModalVisible] = React.useState(false);
  const [ingestAction, setIngestAction] = React.useState<
    IngestActions | undefined
  >(undefined);

  const showModal = () => {
    setIsModalVisible(true);
  };

  const onIngestActionConfirmation = (
    regionCodeForIngestAction: string,
    ingestActionToExecute: IngestActions | undefined
  ) => {
    setIsModalVisible(false);
  };

  const handleRegionChange = (value: string) => {
    setRegionCode(value);
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
                  {code.toUpperCase()}
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
      <Divider orientation="left">Key Actions</Divider>

      <div className="site-card-wrapper">
        <Row gutter={16}>
          {actionsData.map((action) => {
            return (
              <Col span={8}>
                <Button
                  onClick={() => {
                    setIngestAction(action);
                    showModal();
                  }}
                  disabled={!regionCode}
                  block
                >
                  {action}
                </Button>
              </Col>
            );
          })}
        </Row>
      </div>
      <IngestActionConfirmationForm
        visible={isModalVisible}
        onConfirm={onIngestActionConfirmation}
        onCancel={() => {
          setIsModalVisible(false);
        }}
        ingestAction={ingestAction}
        regionCode={regionCode}
      />
    </>
  );
};

export default ActionsView;
