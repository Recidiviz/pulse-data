import * as React from "react";
import {
  Dropdown,
  DropdownMenu,
  DropdownToggle,
} from "@recidiviz/design-system";
import Tooltip from "../Tooltip";
import { OpportunityDeferralDropdownProps } from "./types";
import { ReminderMenuItems } from "./ReminderMenuItems";

const OpportunityDeferralDropdown = ({
  onDeferred,
}: OpportunityDeferralDropdownProps): JSX.Element => {
  return (
    <Dropdown>
      <Tooltip title="Remind Me Later">
        <DropdownToggle
          aria-label="Create a reminder"
          icon="Clock"
          iconSize={16}
        />
      </Tooltip>
      <DropdownMenu alignment="right">
        <ReminderMenuItems onDeferred={onDeferred} />
      </DropdownMenu>
    </Dropdown>
  );
};

export { OpportunityDeferralDropdown };
