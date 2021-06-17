import * as React from "react";
import {
  Dropdown,
  DropdownMenu,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownToggle,
} from "@recidiviz/design-system";
import moment from "moment";
import Tooltip from "../Tooltip";

interface OpportunityDeferralDropdownProps {
  onDeferred: (deferUntil: moment.Moment) => void;
}

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
        <DropdownMenuLabel>Remind Me In</DropdownMenuLabel>
        <DropdownMenuItem
          label="1 day"
          onClick={() => onDeferred(moment.utc().add(1, "day"))}
        />
        <DropdownMenuItem
          label="7 days"
          onClick={() => onDeferred(moment.utc().add(7, "days"))}
        />
        <DropdownMenuItem
          label="30 days"
          onClick={() => onDeferred(moment.utc().add(30, "days"))}
        />
      </DropdownMenu>
    </Dropdown>
  );
};

export { OpportunityDeferralDropdown };
