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

import { Button, Checkbox, Form, Input, Select } from "antd";
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
import HydrationWrapper from "../HydrationWrapper";
import { CriteriaCopy } from "./CriteriaCopy";
import { MultiEntry } from "./MultiEntry";
import { SidebarComponents } from "./SidebarComponents";
import { SnoozeInput } from "./SnoozeInput";

const OPTIONAL_FIELDS: (keyof z.input<
  typeof babyOpportunityConfigurationSchema
>)[] = [
  "featureVariant",
  "initialHeader",
  "denialText",
  "eligibilityDateText",
  "tooltipEligibilityText",
];

const OpportunityConfigurationForm = ({
  presenter,
}: {
  presenter: OpportunityConfigurationPresenter;
}) => {
  const template = presenter?.selectedOpportunityConfiguration;
  const history = useHistory();

  const [isSubmitting, setIsSubmitting] = useState(false);

  const initial = {
    ...template,
    denialReasons: template && Object.entries(template.denialReasons),
    eligibleCriteriaCopy:
      template && Object.entries(template.eligibleCriteriaCopy),
    ineligibleCriteriaCopy:
      template && Object.entries(template.ineligibleCriteriaCopy),
    isAlert: template?.isAlert ?? false,
    hideDenialRevert: template?.hideDenialRevert ?? false,
    priority: template?.priority ?? "NORMAL",
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
          denialReasons: Object.fromEntries(values.denialReasons ?? []),
          eligibleCriteriaCopy: Object.fromEntries(
            values.eligibleCriteriaCopy ?? []
          ),
          ineligibleCriteriaCopy: Object.fromEntries(
            values.ineligibleCriteriaCopy ?? []
          ),
          sidebarComponents: values.sidebarComponents ?? [],
          notifications: addNotificationIds(values.notifications) ?? [],
        });
        const success = await presenter.createOpportunityConfiguration(config);
        setIsSubmitting(false);
        if (success) history.push("..");
      }}
      autoComplete="off"
      initialValues={initial}
    >
      <Form.Item label="Name" name="displayName" rules={[{ required: true }]}>
        <Input />
      </Form.Item>
      <Form.Item
        label="Configuration Description"
        name="description"
        rules={[{ required: true }]}
      >
        <Input />
      </Form.Item>
      <Form.Item label="Feature Variant" name="featureVariant">
        <Input />
      </Form.Item>
      <Form.Item
        label="Dynamic Eligibility Text"
        name="dynamicEligibilityText"
        rules={[{ required: true }]}
      >
        <Input />
      </Form.Item>
      <Form.Item label="Eligibility Date Text" name="eligibilityDateText">
        <Input />
      </Form.Item>
      <Form.Item
        label="Hide Denial Revert?"
        name="hideDenialRevert"
        valuePropName="checked"
      >
        <Checkbox />
      </Form.Item>
      <Form.Item label="Tooltip Eligibility Text" name="tooltipEligibilityText">
        <Input />
      </Form.Item>
      <Form.Item
        label="Call To Action"
        name="callToAction"
        rules={[{ required: true }]}
      >
        <Input />
      </Form.Item>
      <Form.Item label="Subheading" name="subheading">
        <Input />
      </Form.Item>
      <MultiEntry label="Denial Reasons" name="denialReasons">
        {({ name, ...field }) => (
          <>
            <Form.Item
              {...field}
              noStyle
              name={[name, 0]}
              rules={[{ required: true, message: "'code' is required" }]}
            >
              <Input placeholder="Code" />
            </Form.Item>
            :
            <Form.Item
              {...field}
              noStyle
              name={[name, 1]}
              rules={[{ required: true, message: "'text' is required" }]}
            >
              <Input placeholder="Text" />
            </Form.Item>
          </>
        )}
      </MultiEntry>
      <Form.Item label="Denial Text" name="denialText">
        <Input />
      </Form.Item>
      <Form.Item label="Initial Header" name="initialHeader">
        <Input />
      </Form.Item>
      <CriteriaCopy
        label="Eligible Criteria Copy"
        name="eligibleCriteriaCopy"
      />
      <CriteriaCopy
        label="Ineligible Criteria Copy"
        name="ineligibleCriteriaCopy"
      />
      <Form.Item label="Snooze" name="snooze">
        <SnoozeInput />
      </Form.Item>
      <SidebarComponents />
      <Form.Item
        label="Methodology URL"
        name="methodologyUrl"
        rules={[{ required: true }]}
      >
        <Input />
      </Form.Item>
      <Form.Item label="Alert?" name="isAlert" valuePropName="checked">
        <Checkbox />
      </Form.Item>
      <Form.Item label="Priority" name="priority" rules={[{ required: true }]}>
        <Select>
          <Select.Option value="NORMAL">NORMAL</Select.Option>
          <Select.Option value="HIGH">HIGH</Select.Option>
        </Select>
      </Form.Item>
      <Form.Item label="Tab Groups" name="tabGroups">
        <Input />
      </Form.Item>
      <Form.Item label="Compare By" name="compareBy">
        <Input />
      </Form.Item>
      <MultiEntry label="Notifications" name="notifications">
        {({ name, ...field }) => {
          return (
            <>
              <Form.Item
                {...field}
                noStyle
                name={[name, "title"]}
                rules={[{ required: false }]}
              >
                <Input placeholder="Title (optional)" />
              </Form.Item>
              :
              <Form.Item
                {...field}
                noStyle
                name={[name, "body"]}
                rules={[
                  { required: true, message: "Notification body is required" },
                ]}
              >
                <Input placeholder="Body" />
              </Form.Item>
              :
              <Form.Item
                {...field}
                noStyle
                name={[name, "cta"]}
                rules={[{ required: true }]}
              >
                <Input placeholder="CTA (optional)" />
              </Form.Item>
            </>
          );
        }}
      </MultiEntry>
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
