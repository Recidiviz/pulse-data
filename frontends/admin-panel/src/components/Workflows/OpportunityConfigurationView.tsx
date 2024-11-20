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

import { Button, Divider, Form, Space } from "antd";
import { observer } from "mobx-react-lite";
import { useHistory } from "react-router-dom";

import OpportunityConfigurationPresenter from "../../WorkflowsStore/presenters/OpportunityConfigurationPresenter";
import { useWorkflowsStore } from "../StoreProvider";
import HydrationWrapper from "./HydrationWrapper";
import { CompareByView } from "./OpportunityConfigurationForm/CompareBy";
import { CriteriaCopyView } from "./OpportunityConfigurationForm/CriteriaCopy";
import { DenialReasonsView } from "./OpportunityConfigurationForm/DenialReasons";
import { StaticValue } from "./OpportunityConfigurationForm/formSpec";
import { MultiEntry } from "./OpportunityConfigurationForm/MultiEntry";
import { NotificationsView } from "./OpportunityConfigurationForm/Notifications";
import { SidebarComponentsView } from "./OpportunityConfigurationForm/SidebarComponents";
import { TabGroupsView } from "./OpportunityConfigurationForm/TabGroups";

const OpportunityConfigurationSettings = ({
  presenter,
}: {
  presenter: OpportunityConfigurationPresenter;
}) => {
  const config = presenter?.selectedOpportunityConfiguration;
  const history = useHistory();

  if (!config) return <div />;

  return (
    <>
      <Space>
        <Button onClick={() => history.push(`new?from=${config.id}`)}>
          Edit
        </Button>
        {config.status === "ACTIVE" && (
          <>
            {config.featureVariant && (
              <Button
                onClick={() =>
                  presenter.deactivateOpportunityConfiguration(config.id)
                }
              >
                Deactivate
              </Button>
            )}
            <Button
              onClick={() =>
                // eslint-disable-next-line no-alert
                window.confirm(
                  "Are you sure you want to promote this configuration to production?"
                ) && presenter.promoteOpportunityConfiguration(config.id)
              }
            >
              Promote to Production
            </Button>
          </>
        )}
        {config.status === "INACTIVE" && (
          <Button
            onClick={() =>
              presenter.activateOpportunityConfiguration(config.id)
            }
          >
            Activate
          </Button>
        )}
      </Space>
      <Form
        labelCol={{ span: 4 }}
        wrapperCol={{ span: 10 }}
        labelWrap
        initialValues={config}
      >
        <Form.Item label="Feature Variant" name="featureVariant">
          <StaticValue />
        </Form.Item>
        <Form.Item name="revisionDescription" label="Revision Description">
          <StaticValue />
        </Form.Item>
        <Divider />
        <Form.Item name="displayName" label="Opportunity Name">
          <StaticValue />
        </Form.Item>
        <Form.Item
          name="dynamicEligibilityText"
          label="Dynamic Eligibility Text"
        >
          <StaticValue />
        </Form.Item>
        <Form.Item name="eligibilityDateText" label="Eligibility Date Text">
          <StaticValue />
        </Form.Item>
        <Form.Item name="hideDenialRevert" label="Hide Denial Revert?">
          <StaticValue />
        </Form.Item>
        <Form.Item
          name="tooltipEligibilityText"
          label="Tooltip Eligibility Text"
        >
          <StaticValue />
        </Form.Item>
        <Form.Item name="zeroGrantsTooltip" label="Zero Grants Tooltip">
          <StaticValue />
        </Form.Item>
        <Form.Item name="callToAction" label="Call To Action">
          <StaticValue />
        </Form.Item>
        <Form.Item name="subheading" label="Subheading">
          <StaticValue />
        </Form.Item>
        <MultiEntry
          label="Denial Reasons"
          name="denialReasons"
          readonly
          child={DenialReasonsView}
        />
        <Form.Item name="denialText" label="Denial Text">
          <StaticValue />
        </Form.Item>
        <Form.Item name="initialHeader" label="Initial Header">
          <StaticValue />
        </Form.Item>
        <MultiEntry
          label="Eligible Criteria Copy"
          name="eligibleCriteriaCopy"
          child={CriteriaCopyView}
          readonly
        />
        <MultiEntry
          label="Almost Eligible Criteria Copy"
          name="ineligibleCriteriaCopy"
          child={CriteriaCopyView}
          readonly
        />
        <Form.Item label="Snooze">
          <span className="ant-form-text">{JSON.stringify(config.snooze)}</span>
        </Form.Item>
        <MultiEntry
          label="Sidebar Components"
          name="sidebarComponents"
          child={SidebarComponentsView}
          readonly
        />
        <Form.Item name="methodologyUrl" label="Methodology URL">
          <StaticValue />
        </Form.Item>
        <Form.Item name="isAlert" label="Alert?">
          <StaticValue />
        </Form.Item>
        <Form.Item name="priority" label="Priority">
          <StaticValue />
        </Form.Item>
        <MultiEntry
          label="Tab Groups"
          name="tabGroups"
          child={TabGroupsView}
          readonly
        />
        <MultiEntry
          label="Compare By"
          name="compareBy"
          child={CompareByView}
          readonly
        />
        <MultiEntry
          label="Notifications"
          name="notifications"
          child={NotificationsView}
          readonly
        />
      </Form>
    </>
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
