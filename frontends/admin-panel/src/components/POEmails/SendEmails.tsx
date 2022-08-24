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
import { Button, Card, Empty, Form, Input, message, Select, Spin } from "antd";
import * as React from "react";
import {
  getListBatchInfo,
  sendEmails,
} from "../../AdminPanelAPI/LineStaffTools";
import ActionRegionConfirmationForm, {
  RegionAction,
  regionActionNames,
} from "../Utilities/ActionRegionConfirmationForm";
import {
  BatchInfoType,
  layout,
  POEmailsFormProps,
  tailLayout,
} from "./constants";

interface SendFormData {
  state: string;
  batchId: string;
  redirectAddress: string;
  ccAddresses: string[];
  subjectOverride: string;
  emailAllowlist: string[];
}

const SendEmails: React.FC<POEmailsFormProps> = ({ stateInfo, reportType }) => {
  const [form] = Form.useForm();
  const [formData, setFormData] =
    React.useState<SendFormData | undefined>(undefined);
  const [showSpinner, setShowSpinner] = React.useState(false);
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    React.useState(false);
  const [labeledBatchList, setLabeledBatchList] =
    React.useState<string[] | undefined>(undefined);

  const onFinish = (values?: SendFormData | undefined) => {
    setFormData(values);
    showConfirmationModal();
  };

  const onEmailActionConfirmation = async () => {
    if (!stateInfo || !reportType) return;

    setIsConfirmationModalVisible(false);
    setShowSpinner(true);
    message.info("Sending emails...");
    if (formData?.batchId) {
      const r = await sendEmails(
        stateInfo.code,
        reportType,
        formData.batchId,
        formData.redirectAddress,
        formData.ccAddresses,
        formData.subjectOverride,
        formData.emailAllowlist
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

  const getBatches = React.useCallback(() => {
    if (!stateInfo?.code || !reportType) return;

    setLabeledBatchList(undefined);
    const getBatchInfo = async () => {
      form.setFieldsValue({
        batchId: undefined,
      });
      const r = await getListBatchInfo(stateInfo.code, reportType);
      const json = await r.json();
      labelBatchesInSelect(json.batchInfo);
    };
    getBatchInfo();
  }, [stateInfo?.code, reportType, form]);

  const labelBatchesInSelect = (batchInfo: BatchInfoType[]) => {
    const labeledList: string[] = batchInfo.map((x) => {
      if (x.sendResults.length > 0) {
        return `${x.batchId} (Sent)`;
      }
      return `${x.batchId}`;
    });
    setLabeledBatchList(labeledList);
  };

  React.useEffect(() => {
    getBatches();
  }, [getBatches]);

  return (
    <>
      <Card
        title={`Send ${stateInfo?.name || ""} ${reportType || ""} Emails`}
        style={{ margin: 10, height: "95%" }}
        extra={
          <Button type="primary" size="small" onClick={getBatches}>
            Update Batch IDs{" "}
          </Button>
        }
      >
        {stateInfo && reportType ? (
          <>
            <Form
              form={form}
              {...layout}
              className="buffer"
              onFinish={(values) => {
                onFinish(values);
              }}
            >
              {labeledBatchList ? (
                <Form.Item
                  label="Batch ID"
                  name="batchId"
                  rules={[{ required: true }]}
                  initialValue={labeledBatchList[0]}
                >
                  <Select
                    placeholder={labeledBatchList[0]}
                    defaultValue={labeledBatchList[0]}
                  >
                    {labeledBatchList?.map((item) => (
                      <Select.Option
                        key={item}
                        label={item}
                        value={item.substring(0, 14)}
                      >
                        {item}
                      </Select.Option>
                    ))}
                  </Select>
                </Form.Item>
              ) : null}

              <Form.Item
                label="Redirect Address"
                name="redirectAddress"
                rules={[{ type: "email" }]}
              >
                <Input placeholder="xxx@idoc.idaho.gov" />
              </Form.Item>
              <Form.Item
                label="cc"
                name="ccAddresses"
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
          </>
        ) : (
          <Empty />
        )}
      </Card>
      {stateInfo && formData?.batchId ? (
        <ActionRegionConfirmationForm
          visible={isConfirmationModalVisible}
          onConfirm={onEmailActionConfirmation}
          onCancel={onConfirmationCancel}
          action={RegionAction.SendEmails}
          actionName={regionActionNames[RegionAction.SendEmails]}
          regionCode={stateInfo.code}
        />
      ) : null}
    </>
  );
};

export default SendEmails;
