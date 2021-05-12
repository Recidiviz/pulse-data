import * as React from "react";
import { Dropdown, IconSVG } from "@recidiviz/design-system";
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
        <Dropdown.Toggle ariaLabel="Create a reminder">
          <Dropdown.ToggleIcon kind={IconSVG.Clock} size={16} />
        </Dropdown.Toggle>
      </Tooltip>
      <Dropdown.Menu alignment="right">
        <Dropdown.MenuLabel>Remind Me In</Dropdown.MenuLabel>
        <Dropdown.MenuItem
          label="1 day"
          onClick={() => onDeferred(moment.utc().add(1, "day"))}
        />
        <Dropdown.MenuItem
          label="7 days"
          onClick={() => onDeferred(moment.utc().add(7, "days"))}
        />
        <Dropdown.MenuItem
          label="30 days"
          onClick={() => onDeferred(moment.utc().add(30, "days"))}
        />
      </Dropdown.Menu>
    </Dropdown>
  );
};

export { OpportunityDeferralDropdown };
