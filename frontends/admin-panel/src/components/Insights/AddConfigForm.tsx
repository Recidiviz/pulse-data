// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { Form, Input } from "antd";
import { observer } from "mobx-react-lite";

import { InsightsConfiguration } from "../../InsightsStore/models/InsightsConfiguration";
import ConfigurationPresenter from "../../InsightsStore/presenters/ConfigurationPresenter";
import { DraggableModal } from "../Utilities/DraggableModal";

const AddConfigForm = ({
  visible,
  setVisible,
  presenter,
}: {
  visible: boolean;
  setVisible: (arg0: boolean) => void;
  presenter: ConfigurationPresenter;
}): JSX.Element => {
  const { createNewVersion, stateCode } = presenter;
  const [form] = Form.useForm();

  const onAdd = async (request: InsightsConfiguration) => {
    const success = await createNewVersion(request);
    if (success) setVisible(false);
  };

  return (
    <DraggableModal
      visible={visible}
      title="Create new version"
      onCancel={() => setVisible(false)}
      onOk={() => {
        form
          .validateFields()
          .then((values: InsightsConfiguration) => {
            form.resetFields();
            onAdd(values);
          })
          .catch((errorInfo) => {
            // hypothetically setting `scrollToFirstError` on the form should do this (or at least
            // scroll so the error is visible), but it doesn't seem to, so instead put the cursor in the
            // input directly.
            document
              .getElementById(errorInfo.errorFields?.[0].name?.[0])
              ?.focus();
          });
      }}
    >
      <Form form={form}>
        <Form.Item name="featureVariant" label="Feature Variant">
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionOfficerLabel"
          label="Supervision Officer Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls its supervision staff member, e.g. "officer"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionDistrictLabel"
          label="Supervision District Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a location-based group of offices, e.g. "district"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionUnitLabel"
          label="Supervision Unit Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a group of supervision officers, e.g. "unit"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionSupervisorLabel"
          label="Supervision Supervisor Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a supervisor, e.g. "supervisor"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionDistrictManagerLabel"
          label="Supervision District Manager Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls someone who manages supervision supervisors, e.g. "district director"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisionJiiLabel"
          label="Supervision JII Label"
          rules={[
            {
              required: true,
              message: `Please input what ${stateCode} calls a justice-impacted individual on supervision, e.g. "client"`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="learnMoreUrl"
          label="Learn More URL"
          rules={[
            {
              required: true,
              message: `Please input the URL that methodology/FAQ links can be pointed to for ${stateCode}`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="noneAreOutliersLabel"
          label="None are Outliers Label"
          rules={[
            {
              required: true,
              message: `Please input the string that goes in "None of the X on Y's unit ______. Keep checking back" when there are no outliers`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="worseThanRateLabel"
          label="Worse than rate label"
          rules={[
            {
              required: true,
              message: `Please input the string that describes a metric that is far worse than the statewide rate`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="slightlyWorseThanRateLabel"
          label="Slightly Worse Than Rate Label"
          rules={[
            {
              required: true,
              message: `Please input the string that describes a metric that is slightly worse than the statewide rate`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="atOrBelowRateLabel"
          label="At or Below Rate Label"
          rules={[
            {
              required: true,
              message: `Please input the string that describes a metric that is at or below the statewide rate`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="exclusionReasonDescription"
          label="Exclusion reason description"
          rules={[
            {
              required: true,
              message: `Please input the description of why some officers may be excluded from the list`,
            },
          ]}
        >
          <Input />
        </Form.Item>
      </Form>
    </DraggableModal>
  );
};

export default observer(AddConfigForm);
