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
import { Alert, Card, Col, Empty, PageHeader, Row } from "antd";
import * as React from "react";
import { fetchEmailStateCodes } from "../AdminPanelAPI/LineStaffTools";
import useFetchedData from "../hooks";
import { StateCodeInfo } from "./IngestOperationsView/constants";
import GenerateEmails from "./POEmails/GenerateEmails";
import ListBatches from "./POEmails/ListBatches";
import SendEmails from "./POEmails/SendEmails";
import StateSelector from "./Utilities/StateSelector";

const POEmailsView = (): JSX.Element => {
  const { loading, data } =
    useFetchedData<StateCodeInfo[]>(fetchEmailStateCodes);
  const [stateCode, setStateCode] =
    React.useState<StateCodeInfo | undefined>(undefined);

  return (
    <>
      <PageHeader title="PO Emails" />
      <Alert
        message="Caution!"
        description={
          <div>
            You should only use this form if you are a member of the Line Staff
            Tools team, and you absolutely know what you are doing. Link to{" "}
            <a href="https://paper.dropbox.com/doc/PO-Monthly-Report-Admin-Panel-Edition--BQN7eVWzcMfonW2WEsL5H7GdAg-54kVLhYwGCDulQDx9hyNI">
              documentation
            </a>
            .
          </div>
        }
        type="warning"
        showIcon
      />
      <br />
      <StateSelector
        loading={loading}
        data={data}
        onChange={(state) => setStateCode(state)}
      />
      <Row gutter={[8, 8]}>
        <Col span={12}>
          {stateCode ? (
            <GenerateEmails stateInfo={stateCode} />
          ) : (
            <Card title="Generate Emails" style={{ margin: 10, height: "95%" }}>
              <Empty />
            </Card>
          )}
        </Col>
        <Col span={12}>
          {stateCode ? (
            <SendEmails stateInfo={stateCode} />
          ) : (
            <Card title="Send Emails" style={{ margin: 10, height: "95%" }}>
              <Empty />
            </Card>
          )}
        </Col>
      </Row>
      <Row justify="center">
        {stateCode ? (
          <ListBatches stateInfo={stateCode} />
        ) : (
          <Card
            title="Previously Generated Batches"
            style={{ margin: 10, height: "95%" }}
          >
            <Empty />
          </Card>
        )}
      </Row>
    </>
  );
};

export default POEmailsView;
