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

import { Form, Input, Select } from "antd";
import { observer } from "mobx-react-lite";
import styled from "styled-components/macro";

import { ACTION_STRATEGIES_DEFAULT_COPY } from "../../InsightsStore/models/fixtures/ConfigurationsFixture";
import { InsightsConfiguration } from "../../InsightsStore/models/InsightsConfiguration";
import ConfigurationPresenter from "../../InsightsStore/presenters/ConfigurationPresenter";
import { DraggableModal } from "../Utilities/DraggableModal";
import ActionStrategyFormItem from "./ActionStrategyFormItem";

const Heading = styled.h3`
  margin-top: 1rem;
`;

const AddConfigForm = ({
  visible,
  setVisible,
  presenter,
}: {
  visible: boolean;
  setVisible: (arg0: boolean) => void;
  presenter: ConfigurationPresenter;
}): JSX.Element => {
  const {
    createNewVersion,
    stateCode,
    setSelectedFeatureVariant,
    selectedFeatureVariant,
    configs,
    allFeatureVariants,
    priorityConfigForSelectedFeatureVariant: baseConfig,
  } = presenter;
  const [form] = Form.useForm();

  const onAdd = async (request: InsightsConfiguration) => {
    const success = await createNewVersion(request);
    if (success) setVisible(false);
    setSelectedFeatureVariant(undefined);
  };

  const fields = baseConfig
    ? Object.entries(baseConfig).map(([field, value]) => {
        return {
          name: [field],
          value,
        };
      })
    : [];

  const actionStrategies = Object.entries(
    baseConfig?.actionStrategyCopy ?? ACTION_STRATEGIES_DEFAULT_COPY
  );

  return (
    <DraggableModal
      visible={visible}
      title="Create new version"
      width="50%"
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
      <Form form={form} key={selectedFeatureVariant} fields={fields}>
        <div>
          Select an existing feature variant to use as default values for the
          new version. Or, enter a new feature variant and start blank.
        </div>
        <br />

        <Form.Item label="Feature Variant" labelCol={{ span: 24 }}>
          <Form.Item
            style={{ display: "inline-block", width: "calc(50% - 8px)" }}
          >
            <Select
              style={{ width: 200 }}
              placeholder="Select feature variant"
              optionFilterProp="children"
              loading={!configs}
              defaultValue={undefined}
              onChange={(value) => setSelectedFeatureVariant(value)}
              value={selectedFeatureVariant}
            >
              {allFeatureVariants
                ?.sort((a, b) => (a ? a.localeCompare(b ?? "") : -1))
                .map((fv) => {
                  return (
                    <Select.Option key={fv} value={fv}>
                      {fv || "(None)"}
                    </Select.Option>
                  );
                })}
            </Select>
          </Form.Item>
          <Form.Item
            name="featureVariant"
            style={{ display: "inline-block", width: "calc(50% - 8px)" }}
          >
            <Input placeholder="Enter new feature variant" />
          </Form.Item>
        </Form.Item>
        <Heading>Vitals Configuration</Heading>
        <div>
          Vitals metrics provide insights into how well officers and locations
          adhere to key standards, such as timely discharge and risk
          assessments. This section allows you to configure the display and
          functionality of vitals metrics in the application.
        </div>
        <Form.Item
          name="vitalsMetricsMethodologyUrl"
          label="Vitals Metrics Methodology URL"
          rules={[
            {
              required: true,
              message: `Please input the vitals metrics methodology/FAQ URL for ${stateCode}`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Heading>Labels</Heading>
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
          name="supervisorHasNoOutlierOfficersLabel"
          label="Supervisor Has No Outlier Officers Label"
          rules={[
            {
              required: true,
              message: `Please input the string indicating the supervisor has no outlier officers`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="officerHasNoOutlierMetricsLabel"
          label="Officer Has No Outlier Metrics Label"
          rules={[
            {
              required: true,
              message: `Please input the string indicating the officer has no outlier metrics`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="supervisorHasNoOfficersWithEligibleClientsLabel"
          label="Supervisor Has No Officers With Eligible Clients Label"
          rules={[
            {
              required: true,
              message: `Please input the string indicating the supervisor has no officers with clients eligible for opportunities`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="officerHasNoEligibleClientsLabel"
          label="Officer Has No Eligible Clients Label"
          rules={[
            {
              required: true,
              message: `Please input the string indicating the officer has no clients eligible for opportunities`,
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
        <Form.Item
          name="abscondersLabel"
          label="Absconders Label"
          rules={[
            {
              required: true,
              message: `Please input the string that describes what a state calls people who have absconded`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="atOrAboveRateLabel"
          label="At or Above Rate Label"
          rules={[
            {
              required: true,
              message: `Please input the string that describes a metric that is at or above the statewide rate`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="outliersHover"
          label="Outliers hover tooltip"
          rules={[
            {
              required: true,
              message: `Please input the string that describes what an outlier is`,
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Heading>Action Strategy Copy</Heading>
        <div>
          Both the prompt and body copy are in{" "}
          <a
            href="https://www.markdownguide.org/cheat-sheet/"
            target="_blank"
            rel="noopener noreferrer"
          >
            Markdown format
          </a>
          . You can edit the Markdown text on the left and see a live preview of
          the rendered Markdown on the right.
        </div>
        {actionStrategies.map((actionStrategy) => (
          <ActionStrategyFormItem
            key={actionStrategy[0]}
            data={actionStrategy}
          />
        ))}
      </Form>
    </DraggableModal>
  );
};

export default observer(AddConfigForm);
