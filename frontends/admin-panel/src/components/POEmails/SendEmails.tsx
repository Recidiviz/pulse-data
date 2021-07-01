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
import { Button, Card, Form, Input, message, Select, Spin } from "antd";
import * as React from "react";
import {
  fetchEmailStateCodes,
  sendEmails,
} from "../../AdminPanelAPI/LineStaffTools";
import useFetchedData from "../../hooks";
import { StateCodeInfo } from "../IngestOperationsView/constants";
import ActionRegionConfirmationForm from "../Utilities/ActionRegionConfirmationForm";
import { layout, tailLayout } from "./constants";
import StateSelector from "../Utilities/StateSelector";

interface SendFormData {
  state: string;
  batchId: string;
  redirectAddress: string;
  ccAddresses: string[];
  subjectOverride: string;
  emailAllowlist: string[];
}

const SendEmails = (): JSX.Element => {
  const [formData, setFormData] =
    React.useState<SendFormData | undefined>(undefined);
  const [showSpinner, setShowSpinner] = React.useState(false);
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    React.useState(false);

  const { loading, data } =
    useFetchedData<StateCodeInfo[]>(fetchEmailStateCodes);

  const onFinish = (values?: SendFormData | undefined) => {
    setFormData(values);
    showConfirmationModal();
  };

  const onEmailActionConfirmation = async () => {
    setIsConfirmationModalVisible(false);
    setShowSpinner(true);
    message.info("Sending emails...");
    if (formData?.state) {
      const r = await sendEmails(
        formData.state,
        formData.batchId,
        formData.redirectAddress,
        formData.ccAddresses,
        formData.subjectOverride
      );
      const text = await r.text();
      if (r.status >= 400) {
        message.error(`Send emails... failed: ${text}`);
      } else {
        message.success(`Send emails... succeeded! ${text}`);
      }
    }
    setShowSpinner(false);
  };

  const onConfirmationCancel = () => {
    setIsConfirmationModalVisible(false);
    setFormData(undefined);
  };

  const showConfirmationModal = () => {
    setIsConfirmationModalVisible(true);
  };

  return (
    <>
      <Card title="Send Emails" style={{ margin: 10, height: "95%" }}>
        <Form
          {...layout}
          className="buffer"
          onFinish={(values) => {
            onFinish(values);
          }}
        >
          <Form.Item label="State" name="state" rules={[{ required: true }]}>
            <StateSelector loading={loading} data={data} />
          </Form.Item>
          <Form.Item
            label="Batch ID"
            name="batchId"
            rules={[{ required: true }]}
          >
            <Input placeholder="ex. 20210609161306" />
          </Form.Item>
          <Form.Item
            label="Redirect Address"
            name="redirectAddress"
            rules={[{ type: "email" }]}
          >
            <Input placeholder="xxx@idoc.idaho.gov" />
          </Form.Item>
          <Form.Item label="cc" name="ccAddresses" rules={[{ type: "array" }]}>
            <Select
              mode="tags"
              tokenSeparators={[",", " "]}
              placeholder="cc1@domain.gov, cc2@domain.gov"
            >
              <option value="user@recidiviz.org">user@recidiviz.org</option>
            </Select>
          </Form.Item>
          <Form.Item label="Subject Override" name="subjectOverride">
            <Input />
          </Form.Item>
          <Form.Item
            label="Email Allowlist"
            name="emailAllowlist"
            rules={[{ type: "array" }]}
          >
            <Select
              mode="tags"
              tokenSeparators={[",", " "]}
              placeholder="cc1@domain.gov, cc2@domain.gov"
            >
              <option value="user@recidiviz.org">user@recidiviz.org</option>
            </Select>
          </Form.Item>
          <Form.Item {...tailLayout}>
            <Button type="primary" htmlType="submit">
              Send Emails
            </Button>
          </Form.Item>
          {showSpinner ? <Spin /> : null}
        </Form>
      </Card>
      {formData?.state ? (
        <ActionRegionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onEmailActionConfirmation}
          onCancel={onConfirmationCancel}
          action="send"
          actionName="Send Emails"
          regionCode={formData.state}
        />
      ) : null}
    </>
  );
};

export default SendEmails;
