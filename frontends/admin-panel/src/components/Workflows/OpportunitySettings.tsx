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

import { Button, Form, InputNumber, Space } from "antd";
import Input from "antd/lib/input/Input";
import { useState } from "react";

import {
  babyOpportunitySchema,
  updatedStringForOpportunity,
} from "../../WorkflowsStore/models/Opportunity";
import OpportunityPresenter from "../../WorkflowsStore/presenters/OpportunityPresenter";

const layout = {
  labelCol: { span: 3 },
  wrapperCol: { span: 8 },
  labelWrap: true,
};

// Needs to be a separate component because initialValues doesn't seem to work if the form
// is created before it is mounted.
const EditFields = ({
  presenter,
  onDone,
}: {
  presenter: OpportunityPresenter;
  onDone: () => void;
}) => {
  const [isSubmitting, setIsSubmitting] = useState(false);

  const opportunity = presenter.selectedOpportunity;
  if (!opportunity) return <div />;

  return (
    <Form
      {...layout}
      initialValues={opportunity}
      onFinish={async ({ gatingFeatureVariant, homepagePosition }) => {
        setIsSubmitting(true);
        const opp = babyOpportunitySchema.parse({
          homepagePosition,
          gatingFeatureVariant: gatingFeatureVariant?.length
            ? gatingFeatureVariant
            : undefined,
        });
        const success = await presenter.postOpportunity(opp);
        setIsSubmitting(false);
        if (success) onDone();
      }}
    >
      <Form.Item label="Gating Feature Variant" name="gatingFeatureVariant">
        <Input />
      </Form.Item>
      <Form.Item
        label="Homepage Position"
        name="homepagePosition"
        rules={[{ required: true }]}
        extra="The higher this number, the lower on the workflows homepage this opportunity will appear. The first listed opportunity should have a Homepage Position of 1."
      >
        <InputNumber min={1} />
      </Form.Item>
      <Form.Item>
        <Space>
          <Button type="primary" htmlType="submit" disabled={isSubmitting}>
            Save
          </Button>
          <Button htmlType="button" onClick={onDone}>
            Cancel
          </Button>
        </Space>
      </Form.Item>
    </Form>
  );
};

const OpportunitySettings = ({
  presenter,
}: {
  presenter: OpportunityPresenter;
}) => {
  const [isEditing, setIsEditing] = useState(false);

  const opportunity = presenter.selectedOpportunity;
  if (!opportunity) return <div />;
  const isProvisioned = !!opportunity.lastUpdatedAt;

  const url = `https://dashboard-staging.recidiviz.org/workflows/${opportunity.urlSection}?tenantId=${opportunity.stateCode}`;

  const hardcodedFields = (
    <Form {...layout}>
      <Form.Item label="Dashboard URL">
        <span className="ant-form-text">
          <a href={url}>{url}</a>
        </span>
      </Form.Item>
      <Form.Item label="System">
        <span className="ant-form-text">{opportunity.systemType}</span>
      </Form.Item>
      <Form.Item label="Completion Event">
        <span className="ant-form-text">{opportunity.completionEvent}</span>
      </Form.Item>{" "}
      <Form.Item label="Experiment ID">
        <span className="ant-form-text">{opportunity.experimentId}</span>
      </Form.Item>
    </Form>
  );

  const viewFields = (
    <Form {...layout}>
      <Form.Item label="Gating Feature Variant">
        <span className="ant-form-text">
          {opportunity.gatingFeatureVariant ?? <i>None</i>}
        </span>
      </Form.Item>
      <Form.Item label="Homepage Position">
        <span className="ant-form-text">
          {opportunity.homepagePosition ?? <i>None</i>}
        </span>
      </Form.Item>
      <Form.Item label="Last Updated">
        <span className="ant-form-text">
          {updatedStringForOpportunity(opportunity)}
        </span>
      </Form.Item>
      <Button onClick={() => setIsEditing(true)}>Edit</Button>
    </Form>
  );

  let editableFields = (
    <Button onClick={() => setIsEditing(true)}>Set Up</Button>
  ); // By default, the non-provisioned case;
  if (isEditing) {
    editableFields = (
      <EditFields presenter={presenter} onDone={() => setIsEditing(false)} />
    );
  } else if (isProvisioned) {
    editableFields = viewFields;
  }

  return (
    <div style={{ marginBottom: "1em" }}>
      {hardcodedFields}
      <h2>Settings</h2>
      {editableFields}
    </div>
  );
};

export default OpportunitySettings;
