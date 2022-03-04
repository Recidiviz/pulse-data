import * as React from "react";
import {
  Dropdown,
  DropdownMenu,
  DropdownToggle,
  TooltipTrigger,
} from "@recidiviz/design-system";
import { OpportunityDeferralDropdownProps } from "./types";
import { ReminderMenuItems } from "./ReminderMenuItems";

const OpportunityDeferralDropdown = ({
  onDeferred,
}: OpportunityDeferralDropdownProps): JSX.Element => {
  return (
    <Dropdown>
      <TooltipTrigger contents={<span>Remind Me Later</span>}>
        <DropdownToggle
          aria-label="Create a reminder"
          icon="Clock"
          iconSize={16}
        />
      </TooltipTrigger>
      <DropdownMenu alignment="right">
        <ReminderMenuItems onDeferred={onDeferred} />
      </DropdownMenu>
    </Dropdown>
  );
};

export { OpportunityDeferralDropdown };
