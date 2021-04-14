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
import IngestActions from "../constants/ingestActions";

interface IngestActionConfirmationFormProps {
  visible: boolean;
  onConfirm: (
    regionCode: string,
    ingestAction: IngestActions | undefined
  ) => void;
  onCancel: () => void;
  ingestAction: IngestActions | undefined;
  regionCode: string | undefined;
}

const IngestActionConfirmationForm: React.FC<IngestActionConfirmationFormProps> = ({
  visible,
  onConfirm,
  onCancel,
  ingestAction,
  regionCode,
}) => {
  const [form] = Form.useForm();
  const action = ingestAction ? ingestAction?.split(" ")[0].toUpperCase() : "";
  const confirmationRegEx = regionCode
    ? regionCode.toUpperCase().concat("_", action)
    : "";
  return (
    <Modal
      visible={visible}
      title={ingestAction}
      okText="Ok"
      cancelText="Cancel"
      onCancel={onCancel}
      onOk={() => {
        form
          .validateFields()
          .then((values) => {
            form.resetFields();
            onConfirm(values.region_code, ingestAction);
          })
          .catch((info) => {
            form.resetFields();
          });
      }}
    >
      <p>
        Are you sure you want to <b>{ingestAction?.toLowerCase()}</b> for{" "}
        <b>{regionCode?.toUpperCase()}</b>?
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
