// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2026 Recidiviz, Inc.
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

import { Form, Select } from "antd";

import { MultiEntryChild } from "../../formUtils/MultiEntry";
import { StaticValue } from "../../formUtils/StaticValue";

export const ReasonsRequiringApprovalView: MultiEntryChild = ({ name }) => (
  <Form.Item noStyle name={[name]}>
    <StaticValue />
  </Form.Item>
);

/**
 * React functional component wrapper to safely use the useFormInstance hook as
 * Form.Item does not allow hooks to be called directly inside its child components
 */
export const ReasonsRequiringApprovalViewWrapper = (
  props: React.ComponentPropsWithoutRef<typeof ReasonsRequiringApprovalView>
) => {
  return <ReasonsRequiringApprovalView {...props} />;
};

export const ReasonsRequiringApprovalEdit: MultiEntryChild = ({ name }) => {
  const form = Form.useFormInstance();
  const denialReasons = Form.useWatch("denialReasons", form) || [];
  const selectedReasonsRequiringApproval =
    Form.useWatch("reasonsRequiringApproval", form) || [];
  const value = Form.useWatch(["reasonsRequiringApproval", name], form);

  // A selected reason is stale once its denial reason is removed, so display in red.
  const isStale =
    value != null &&
    !denialReasons.some((reason?: { key?: string }) => reason?.key === value);

  const options = denialReasons
    .map((reason?: { key?: string }) => reason?.key)
    .filter(
      (key?: string): key is string =>
        !!key &&
        (key === value || !selectedReasonsRequiringApproval.includes(key))
    )
    .map((key: string) => ({
      value: key,
      label: key,
    }));

  return (
    <Form.Item name={name} noStyle>
      <Select
        style={{ width: 300, color: isStale ? "red" : undefined }}
        options={options}
      />
    </Form.Item>
  );
};

/**
 * React functional component wrapper to safely use the useFormInstance hook as
 * Form.Item does not allow hooks to be called directly inside its child components
 */
export const ReasonsRequiringApprovalEditWrapper = (
  props: React.ComponentPropsWithoutRef<typeof ReasonsRequiringApprovalEdit>
) => {
  return <ReasonsRequiringApprovalEdit {...props} />;
};
