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

import { Button, Checkbox, Form, Input } from "antd";
import { observer } from "mobx-react-lite";
import { useHistory } from "react-router";

import { OpportunityConfiguration } from "../../WorkflowsStore/models/OpportunityConfiguration";
import OpportunityConfigurationPresenter from "../../WorkflowsStore/presenters/OpportunityConfigurationPresenter";
import { useWorkflowsStore } from "../StoreProvider";
import HydrationWrapper from "./HydrationWrapper";

const OpportunityConfigurationForm = ({
  presenter,
}: {
  presenter: OpportunityConfigurationPresenter;
}) => {
  const template = presenter?.selectedOpportunityConfiguration;
  const history = useHistory();

  const initial = {
    ...template,
    denialReasons: template && JSON.stringify(template.denialReasons),
    eligibleCriteriaCopy:
      template && JSON.stringify(template.eligibleCriteriaCopy),
    ineligibleCriteriaCopy:
      template && JSON.stringify(template.ineligibleCriteriaCopy),
    sidebarComponents: template && JSON.stringify(template.sidebarComponents),
  };

  return (
    <Form
      onFinish={async (values) => {
        const config = {
          ...values,
          snooze: {},
          denialReasons: JSON.parse(values.denialReasons),
          eligibleCriteriaCopy: JSON.parse(values.eligibleCriteriaCopy),
          ineligibleCriteriaCopy: JSON.parse(values.ineligibleCriteriaCopy),
          sidebarComponents: JSON.parse(values.sidebarComponents),
        } as OpportunityConfiguration;
        const success = await presenter.createOpportunityConfiguration(config);
        if (success) history.push("..");
      }}
      autoComplete="off"
      initialValues={initial}
    >
      <Form.Item label="Name" name="displayName">
        <Input />
      </Form.Item>
      <Form.Item label="Description" name="description">
        <Input />
      </Form.Item>
      <Form.Item label="Feature Variant" name="featureVariant">
        <Input />
      </Form.Item>
      <Form.Item label="Dynamic Eligibility Text" name="dynamicEligibilityText">
        <Input />
      </Form.Item>
      <Form.Item label="Call To Action" name="callToAction">
        <Input />
      </Form.Item>
      <Form.Item label="Denial Reasons" name="denialReasons">
        <Input />
      </Form.Item>
      <Form.Item label="Denial Text" name="denialText">
        <Input />
      </Form.Item>
      <Form.Item label="Initial Header" name="initialHeader">
        <Input />
      </Form.Item>
      <Form.Item label="Eligible Criteria Copy" name="eligibleCriteriaCopy">
        <Input />
      </Form.Item>
      <Form.Item
        label="Ineligible Eligibility Text"
        name="ineligibleCriteriaCopy"
      >
        <Input />
      </Form.Item>
      <Form.Item label="Sidebar Components" name="sidebarComponents">
        <Input />
      </Form.Item>
      <Form.Item label="Methodology URL" name="methodologyUrl">
        <Input />
      </Form.Item>
      <Form.Item label="Alert?" name="isAlert" valuePropName="checked">
        <Checkbox />
      </Form.Item>
      <Button type="primary" htmlType="submit">
        Submit
      </Button>
    </Form>
  );
};

const OpportunityConfigurationFormView = (): JSX.Element => {
  const { opportunityConfigurationPresenter } = useWorkflowsStore();

  return (
    <HydrationWrapper
      presenter={opportunityConfigurationPresenter}
      component={OpportunityConfigurationForm}
    />
  );
};

export default observer(OpportunityConfigurationFormView);
