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
  KeylessCriteriaCopyEdit,
  KeylessCriteriaCopyView,
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
import {
  SnoozeCompanionsEditWrapper,
  SnoozeCompanionsView,
} from "./fieldComponents/SnoozeCompanions";
import {
  SubcategoryHeadingsEdit,
  SubcategoryHeadingsView,
} from "./fieldComponents/SubcategoryHeadings";
import { TabGroupsEdit, TabGroupsView } from "./fieldComponents/TabGroups";
import { TabTextEdit, TabTextView } from "./fieldComponents/TabText";
import {
  TabTextListEdit,
  TabTextListView,
} from "./fieldComponents/TabTextList";

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
    sectionHeading: "Opportunity Statuses",
    fields: {
      supportsSubmitted: {
        label: "Supports Submitted status?",
        Edit: Checkbox,
      },
      supportsIneligible: {
        label: `Include ineligible opportunities on the Client/Resident Profile page?`,
        Edit: Checkbox,
      },
    },
  },
  {
    sectionHeading: "Criteria",
    fields: {
      omsCriteriaHeader: {
        label: "OMS Criteria Header",
      },
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
      strictlyIneligibleCriteriaCopy: {
        label: "Ineligible Criteria Copy",
        multiple: true,
        Edit: CriteriaCopyEdit,
        View: CriteriaCopyView,
      },
      nonOmsCriteriaHeader: {
        label: "Non-OMS Criteria Header",
      },
      nonOmsCriteria: {
        label: "Non-OMS Criteria Copy",
        multiple: true,
        Edit: KeylessCriteriaCopyEdit,
        View: KeylessCriteriaCopyView,
      },
      methodologyUrl: {
        label: "Criteria Tooltip Icon Link",
        required: true,
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
      caseNotesTitle: {},
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
      denialAdjective: {},
      denialNoun: {},
      hideDenialRevert: {
        label: "Hide Denial Revert?",
        Edit: Checkbox,
      },
      snoozeCompanionOpportunityTypes: {
        multiple: true,
        Edit: SnoozeCompanionsEditWrapper,
        View: SnoozeCompanionsView,
      },
    },
  },
  {
    sectionHeading: "Tabs",
    fields: {
      deniedTabTitle: {},
      submittedTabTitle: {},
      emptyTabCopy: {
        multiple: true,
        Edit: TabTextEdit,
        View: TabTextView,
      },
      tabPrefaceCopy: {
        multiple: true,
        Edit: TabTextEdit,
        View: TabTextView,
      },
    },
  },
  {
    sectionHeading: "Subcategories",
    sectionSubhead:
      "Please consult Polaris before editing or adding all-caps labels in these fields",
    fields: {
      subcategoryHeadings: {
        multiple: true,
        Edit: SubcategoryHeadingsEdit,
        View: SubcategoryHeadingsView,
      },
      subcategoryOrderings: {
        multiple: true,
        Edit: TabTextListEdit,
        View: TabTextListView,
      },
      markSubmittedOptionsByTab: {
        multiple: true,
        Edit: TabTextListEdit,
        View: TabTextListView,
      },
    },
  },
  {
    sectionHeading: "Workflows Homepage Highlights",
    fields: {
      highlightCasesOnHomepage: {
        Edit: Checkbox,
      },
      highlightedCaseCtaCopy: {
        label: "Highlighted Case CTA Copy",
      },
      overdueOpportunityCalloutCopy: {},
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
