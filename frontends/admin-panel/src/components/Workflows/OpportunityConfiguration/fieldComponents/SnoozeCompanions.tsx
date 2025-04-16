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

import { Form, Select } from "antd";

import { useWorkflowsStore } from "../../../StoreProvider";
import { MultiEntryChild } from "../../formUtils/MultiEntry";
import { StaticValue } from "../../formUtils/StaticValue";

export const SnoozeCompanionsView: MultiEntryChild = ({ name }) => (
  <Form.Item noStyle name={[name]}>
    <StaticValue />
  </Form.Item>
);

export const SnoozeCompanionsEdit: MultiEntryChild = ({ name }) => {
  const { opportunities, selectedOpportunityType } = useWorkflowsStore();

  const form = Form.useFormInstance();
  const selectedCompanionOpportunityTypes =
    Form.useWatch("snoozeCompanionOpportunityTypes", form) || [];

  const options = opportunities
    ?.filter(
      ({ opportunityType }) =>
        opportunityType !== selectedOpportunityType &&
        !selectedCompanionOpportunityTypes.includes(opportunityType)
    )
    .map(({ opportunityType }) => ({
      value: opportunityType,
      label: opportunityType,
    }));

  return (
    <Form.Item name={name} noStyle>
      <Select
        style={{
          width: 300,
        }}
        options={options}
      />
    </Form.Item>
  );
};

/**
 * React functional component wrapper to safely use the useWorkflowsStore hook as
 * Form.Item does not allow hooks to be called directly inside its child components
 */
export const SnoozeCompanionsEditWrapper = (
  props: React.ComponentPropsWithoutRef<typeof SnoozeCompanionsEdit>
) => {
  return <SnoozeCompanionsEdit {...props} />;
};
