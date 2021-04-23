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
import { Form, Input, Modal } from "antd";
import { actionNames, DirectIngestInstance, IngestActions } from "./constants";

interface IngestActionConfirmationFormProps {
  visible: boolean;
  onConfirm: (ingestAction: IngestActions) => void;
  onCancel: () => void;
  ingestAction: IngestActions;
  regionCode: string;
  ingestInstance: DirectIngestInstance | undefined;
}

const IngestActionConfirmationForm: React.FC<IngestActionConfirmationFormProps> = ({
  visible,
  onConfirm,
  onCancel,
  ingestAction,
  regionCode,
  ingestInstance,
}) => {
  const [form] = Form.useForm();
  const actionName = actionNames[ingestAction];
  const confirmationRegEx = ingestInstance
    ? regionCode
        .toUpperCase()
        .concat("_", ingestAction.toUpperCase(), "_", ingestInstance)
    : regionCode.toUpperCase().concat("_", ingestAction.toUpperCase());

  return (
    <Modal
      visible={visible}
      title={actionName || ""}
      okText="Ok"
      cancelText="Cancel"
      onCancel={onCancel}
      onOk={() => {
        form
          .validateFields()
          .then((values) => {
            form.resetFields();
            onConfirm(ingestAction);
          })
          .catch((info) => {
            form.resetFields();
          });
      }}
    >
      <p>
        Are you sure you want to
        <b> {actionName?.toLowerCase()} </b>
        for
        <b>{ingestInstance ? ` ${ingestInstance}` : ""}</b>
        {ingestInstance ? " ingest instance in " : " "}
        <b>{regionCode.toUpperCase()}</b>?
      </p>
      <p>
        Type <b>{confirmationRegEx}</b> below to confirm.
      </p>
      <Form form={form} layout="vertical" name="form_in_modal">
        <Form.Item
          name="region_code"
          rules={[
            {
              required: true,
              message: "Please input the region code",
              pattern: RegExp(confirmationRegEx),
            },
          ]}
        >
          <Input />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default IngestActionConfirmationForm;
