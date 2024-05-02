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

import { Form } from "antd";
import { observer } from "mobx-react-lite";

import OpportunityConfigurationPresenter from "../../WorkflowsStore/presenters/OpportunityConfigurationPresenter";
import { useWorkflowsStore } from "../StoreProvider";
import HydrationWrapper from "./HydrationWrapper";

const OpportunityConfigurationSettings = ({
  presenter,
}: {
  presenter: OpportunityConfigurationPresenter;
}) => {
  const config = presenter?.selectedOpportunityConfiguration;
  if (!config) return <div />;

  return (
    <Form>
      <Form.Item label="Name">
        <span className="ant-form-text">{config.displayName}</span>
      </Form.Item>
      <Form.Item label="Description">
        <span className="ant-form-text">{config.description}</span>
      </Form.Item>
      <Form.Item label="Feature Variant">
        <span className="ant-form-text">
          {config.featureVariant ?? <i>None</i>}
        </span>
      </Form.Item>
      <Form.Item label="Dynamic Eligibility Text">
        <span className="ant-form-text">{config.dynamicEligibilityText}</span>
      </Form.Item>
      <Form.Item label="Call To Action">
        <span className="ant-form-text">{config.callToAction}</span>
      </Form.Item>
      <Form.Item label="Denial Reasons">
        <span className="ant-form-text">
          {JSON.stringify(config.denialReasons)}
        </span>
      </Form.Item>
      <Form.Item label="Denial Text">
        <span className="ant-form-text">{config.denialText}</span>
      </Form.Item>
      <Form.Item label="Eligibile Criteria Copy">
        <span className="ant-form-text">
          {JSON.stringify(config.eligibleCriteriaCopy)}
        </span>
      </Form.Item>
      <Form.Item label="Ineligible Eligibility Text">
        <span className="ant-form-text">
          {JSON.stringify(config.ineligibleCriteriaCopy)}
        </span>
      </Form.Item>
      <Form.Item label="Sidebar Components">
        <span className="ant-form-text">{config.sidebarComponents}</span>
      </Form.Item>
      <Form.Item label="Methodology URL">
        <span className="ant-form-text">{config.methodologyUrl}</span>
      </Form.Item>
    </Form>
  );
};

const OpportunityConfigurationView = (): JSX.Element => {
  const { opportunityConfigurationPresenter } = useWorkflowsStore();

  return (
    <HydrationWrapper
      presenter={opportunityConfigurationPresenter}
      component={OpportunityConfigurationSettings}
    />
  );
};

export default observer(OpportunityConfigurationView);
