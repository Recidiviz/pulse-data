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

import { Button, Divider, Form, Input } from "antd";
import { observer } from "mobx-react-lite";
import { useState } from "react";
import { useHistory } from "react-router-dom";
import { z } from "zod";

import {
  babyOpportunityConfigurationSchema,
  notificationsSchema,
} from "../../../WorkflowsStore/models/OpportunityConfiguration";
import OpportunityConfigurationPresenter from "../../../WorkflowsStore/presenters/OpportunityConfigurationPresenter";
import { useWorkflowsStore } from "../../StoreProvider";
import { FieldsFromSpec } from "../formUtils/FieldsFromSpec";
import HydrationWrapper from "../HydrationWrapper";
import { opportunityConfigFormSpec } from "./opportunityConfigurationFormSpec";

type OpportunityConfigurationField = keyof z.input<
  typeof babyOpportunityConfigurationSchema
>;

const OPTIONAL_FIELDS: OpportunityConfigurationField[] = [
  "featureVariant",
  "initialHeader",
  "denialText",
  "eligibilityDateText",
  "tooltipEligibilityText",
  "zeroGrantsTooltip",
  "denialAdjective",
  "denialNoun",
  "submittedTabTitle",
  "omsCriteriaHeader",
  "nonOmsCriteriaHeader",
  "highlightedCaseCtaCopy",
  "overdueOpportunityCalloutCopy",
  "snoozeCompanionOpportunityTypes",
  "caseNotesTitle",
];

const OpportunityConfigurationForm = ({
  presenter,
}: {
  presenter: OpportunityConfigurationPresenter;
}) => {
  const template = presenter?.selectedOpportunityConfiguration;
  const history = useHistory();

  const [isSubmitting, setIsSubmitting] = useState(false);

  const urlParams = new URLSearchParams(document.location.search);

  const freshVariant = !!urlParams.get("freshVariant");
  const freshNonDefault = !!(freshVariant && urlParams.get("from"));

  let variantDescription = template?.variantDescription;
  if (freshNonDefault) {
    variantDescription = undefined;
  } else if (freshVariant) {
    variantDescription = "Default Config";
  }
  const initial = {
    ...template,
    isAlert: template?.isAlert ?? false,
    hideDenialRevert: template?.hideDenialRevert ?? false,
    priority: template?.priority ?? "NORMAL",
    revisionDescription: undefined,
    variantDescription,
    featureVariant: freshVariant ? undefined : template?.featureVariant,
  };

  // Add notification UUIDs to newly created notifications
  const addNotificationIds = (
    notifications?: z.infer<typeof notificationsSchema>
  ) => {
    return notifications?.map((notification) => {
      if (!notification.id) {
        return {
          ...notification,
          id: crypto.randomUUID(),
        };
      }
      return notification;
    });
  };

  return (
    <Form
      labelCol={{ span: 4 }}
      wrapperCol={{ span: 12 }}
      labelWrap
      scrollToFirstError
      onFinish={async (values) => {
        setIsSubmitting(true);
        const config = babyOpportunityConfigurationSchema.parse({
          ...values,
          ...Object.fromEntries(
            OPTIONAL_FIELDS.map((f) => [
              f,
              values[f]?.length ? values[f] : undefined,
            ])
          ),
          notifications: addNotificationIds(values.notifications),
        });
        const success = await presenter.createOpportunityConfiguration(config);
        setIsSubmitting(false);
        if (success) history.push("..");
      }}
      autoComplete="off"
      initialValues={initial}
    >
      <Form.Item
        label="Variant Description"
        name="variantDescription"
        rules={[{ required: true }]}
      >
        <Input />
      </Form.Item>
      <Form.Item
        label="Feature Variant"
        name="featureVariant"
        rules={[{ required: freshNonDefault }]}
      >
        <Input disabled={!freshNonDefault} />
      </Form.Item>
      <FieldsFromSpec spec={opportunityConfigFormSpec} mode="edit" />
      <Divider />
      <Form.Item
        label="Revision Description"
        name="revisionDescription"
        rules={[{ required: true }]}
      >
        <Input />
      </Form.Item>
      <Button type="primary" htmlType="submit" disabled={isSubmitting}>
        Save And Apply Configuration
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
