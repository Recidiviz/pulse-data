// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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

import {
  Dropdown,
  DropdownMenu,
  DropdownMenuLabel,
  DropdownToggle,
  Icon,
  palette,
} from "@recidiviz/design-system";
import React from "react";
import { useRootStore } from "../../stores";
import { Opportunity } from "../../stores/OpportunityStore";
import { ActionRow } from "../CaseCard/ActionRow";
import { ReminderMenuItems } from "../NeedsActionFlow/ReminderMenuItems";
import { ReviewContents } from "./OpportunityReview.styles";

type OpportunityReviewProps = {
  opportunity: Opportunity;
};

export const OpportunityReview = ({
  opportunity,
}: OpportunityReviewProps): JSX.Element => {
  const { opportunityStore } = useRootStore();
  return (
    <ActionRow
      // TODO(#7635): placeholder, not all opportunities will use this icon
      bullet={
        <Icon kind="StarCircled" color={palette.signal.highlight} size={16} />
      }
    >
      <ReviewContents>
        <strong>{opportunity.previewText}</strong>
      </ReviewContents>
      <Dropdown>
        <DropdownToggle kind="borderless" icon="TripleDot" shape="block" />
        <DropdownMenu alignment="right">
          <ReminderMenuItems
            onDeferred={(deferUntil) => {
              opportunityStore.createOpportunityDeferral(
                opportunity,
                deferUntil
              );
            }}
          />
          <DropdownMenuLabel>Other Actions</DropdownMenuLabel>
        </DropdownMenu>
      </Dropdown>
    </ActionRow>
  );
};
