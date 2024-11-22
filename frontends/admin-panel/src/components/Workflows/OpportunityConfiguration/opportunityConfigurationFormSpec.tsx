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

import { Checkbox, Select } from "antd";
import TextArea from "antd/lib/input/TextArea";

import { OpportunityConfiguration } from "../../../WorkflowsStore/models/OpportunityConfiguration";
import { FormSpec } from "../formUtils/FieldsFromSpec";
import { CompareByEdit, CompareByView } from "./fieldComponents/CompareBy";
import {
  CriteriaCopyEdit,
  CriteriaCopyView,
} from "./fieldComponents/CriteriaCopy";
import {
  DenialReasonsEdit,
  DenialReasonsView,
} from "./fieldComponents/DenialReasons";
import {
  NotificationsEdit,
  NotificationsView,
} from "./fieldComponents/Notifications";
import {
  SidebarComponentsEdit,
  SidebarComponentsView,
} from "./fieldComponents/SidebarComponents";
import { SnoozeEdit, SnoozeView } from "./fieldComponents/Snooze";
import { TabGroupsEdit, TabGroupsView } from "./fieldComponents/TabGroups";

export const opportunityConfigFormSpec: FormSpec<OpportunityConfiguration> = [
  {
    sectionHeading: "Copy",
    fields: {
      displayName: {
        label: "Opportunity Name",
        required: true,
      },
      initialHeader: {},
      subheading: {
        Edit: TextArea,
      },
      callToAction: {
        required: true,
        Edit: TextArea,
      },
      dynamicEligibilityText: {
        required: true,
      },
      notifications: {
        multiple: true,
        Edit: NotificationsEdit,
        View: NotificationsView,
      },
      tooltipEligibilityText: {},
    },
  },
  {
    sectionHeading: "Criteria",
    fields: {
      eligibleCriteriaCopy: {
        multiple: true,
        Edit: CriteriaCopyEdit,
        View: CriteriaCopyView,
      },
      ineligibleCriteriaCopy: {
        label: "Almost Eligible Criteria Copy",
        multiple: true,
        Edit: CriteriaCopyEdit,
        View: CriteriaCopyView,
      },
    },
  },
  {
    sectionHeading: "Sidebar",
    fields: {
      isAlert: {
        label: "Alert?",
        Edit: Checkbox,
      },
      sidebarComponents: {
        multiple: true,
        Edit: SidebarComponentsEdit,
        View: SidebarComponentsView,
      },
      eligibilityDateText: {},
      methodologyUrl: {
        label: "Methodology URL",
        required: true,
      },
    },
  },
  {
    sectionHeading: "Denial/Snooze",
    fields: {
      denialReasons: {
        multiple: true,
        Edit: DenialReasonsEdit,
        View: DenialReasonsView,
      },
      snooze: {
        Edit: SnoozeEdit,
        View: SnoozeView,
      },
      denialText: {
        label: "Denial Text",
      },
      hideDenialRevert: {
        label: "Hide Denial Revert?",
        Edit: Checkbox,
      },
    },
  },
  {
    sectionHeading: "Supervisor Homepage",
    fields: {
      priority: {
        required: true,
        Edit: (props: object) => (
          <Select {...props}>
            <Select.Option value="NORMAL">NORMAL</Select.Option>
            <Select.Option value="HIGH">HIGH</Select.Option>
          </Select>
        ),
      },
      zeroGrantsTooltip: {},
    },
  },
  {
    sectionHeading: "Danger Zone",
    sectionSubhead: "Consult with Polaris before editing these fields.",
    fields: {
      tabGroups: {
        multiple: true,
        Edit: TabGroupsEdit,
        View: TabGroupsView,
      },
      compareBy: {
        label: "Caseload Sorting",
        multiple: true,
        Edit: CompareByEdit,
        View: CompareByView,
      },
    },
  },
];
