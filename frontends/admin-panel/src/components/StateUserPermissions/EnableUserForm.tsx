// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
import { Form } from "antd";

import { DraggableModal } from "../Utilities/DraggableModal";
import ReasonInput from "./ReasonInput";
import { validateAndFocus } from "./utils";

type reasonOnlyType = { reason: string };

export const CreateEnableUserForm = ({
  enableVisible,
  enableOnCreate,
  enableOnCancel,
}: {
  enableVisible: boolean;
  enableOnCreate: (reason: string) => Promise<void>;
  enableOnCancel: () => void;
}): JSX.Element => {
  const [form] = Form.useForm();

  return (
    <DraggableModal
      visible={enableVisible}
      title="Enable User"
      onCancel={enableOnCancel}
      okText="Enable"
      onOk={() => {
        validateAndFocus<reasonOnlyType>(form, (values) => {
          form.resetFields();
          enableOnCreate(values.reason);
        });
      }}
    >
      <br />
      <Form
        form={form}
        layout="horizontal"
        onFinish={enableOnCreate}
        labelCol={{ span: 8 }}
      >
        <ReasonInput label="Reason for enabling" />
      </Form>
    </DraggableModal>
  );
};
