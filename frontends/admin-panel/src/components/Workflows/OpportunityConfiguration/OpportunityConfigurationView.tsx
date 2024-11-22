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

import { Button, Form, Space } from "antd";
import { observer } from "mobx-react-lite";
import { useHistory } from "react-router-dom";

import OpportunityConfigurationPresenter from "../../../WorkflowsStore/presenters/OpportunityConfigurationPresenter";
import { useWorkflowsStore } from "../../StoreProvider";
import { FieldsFromSpec } from "../formUtils/FieldsFromSpec";
import { StaticValue } from "../formUtils/StaticValue";
import HydrationWrapper from "../HydrationWrapper";
import { opportunityConfigFormSpec } from "./opportunityConfigurationFormSpec";

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
        requiredMark={false}
      >
        <Form.Item label="Feature Variant" name="featureVariant">
          <StaticValue />
        </Form.Item>
        <Form.Item name="revisionDescription" label="Revision Description">
          <StaticValue />
        </Form.Item>
        <FieldsFromSpec spec={opportunityConfigFormSpec} mode="view" />
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
