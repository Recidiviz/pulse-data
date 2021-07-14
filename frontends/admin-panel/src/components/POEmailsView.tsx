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
import { WarningFilled } from "@ant-design/icons";
import { Alert, Col, PageHeader, Row } from "antd";
import * as React from "react";
import { fetchEmailStateCodes } from "../AdminPanelAPI/LineStaffTools";
import useFetchedData from "../hooks";
import { StateCodeInfo } from "./IngestOperationsView/constants";
import GenerateEmails from "./POEmails/GenerateEmails";
import SendEmails from "./POEmails/SendEmails";
import ListBatches from "./POEmails/ListBatches";

const POEmailsView = (): JSX.Element => {
  const { data } = useFetchedData<StateCodeInfo[]>(fetchEmailStateCodes);

  return (
    <>
      <PageHeader title="PO Emails" />
      <Alert
        message={
          <>
            <WarningFilled /> Caution!
          </>
        }
        description="You should only use this form if you are a member of the Line Staff Tools team, and you absolutely know what you are doing."
        type="warning"
      />
      <Row gutter={[16, 16]}>
        <Col span={12}>
          <GenerateEmails />
        </Col>
        <Col span={12}>
          <SendEmails />
        </Col>
      </Row>
      <Row gutter={[16, 16]} justify="center">
        {data
          ?.sort((a, b) => a.name.localeCompare(b.name))
          .map((state: StateCodeInfo) => {
            return (
              <Col key={state.code} span={7}>
                <ListBatches stateInfo={state} />
              </Col>
            );
          })}
      </Row>
    </>
  );
};

export default POEmailsView;
