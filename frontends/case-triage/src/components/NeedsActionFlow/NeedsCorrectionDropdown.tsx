import * as React from "react";
import { Dropdown } from "@recidiviz/design-system";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import FeedbackFormModal from "../FeedbackFormModal";
import { Client } from "../../stores/ClientsStore";
import { ACTION_TITLES } from "./NeedsActionFlow";

interface NeedsCorrectionDropdownProps {
  actions: CaseUpdateActionType[];
  borderless?: boolean;
  children?: React.ReactNode | React.ReactNode[] | null;
  className?: string;
  client: Client;
  alignment?: "left" | "right";
}

const NeedsCorrectionDropdown = ({
  actions,
  children,
  className,
  alignment,
  borderless = false,
  client,
}: NeedsCorrectionDropdownProps): JSX.Element => {
  const [feedbackModalActionType, setFeedbackModalActionType] =
    React.useState<CaseUpdateActionType>(actions[0]);
  const [feedbackModalIsOpen, setFeedbackModalIsOpen] = React.useState(false);

  const items = actions.map((action) => (
    <Dropdown.MenuItem
      label={ACTION_TITLES[action]}
      key={action}
      onClick={() => {
        setFeedbackModalActionType(action);
        setFeedbackModalIsOpen(true);
      }}
    />
  ));

  return (
    <>
      <Dropdown className={className}>
        <Dropdown.Toggle
          ariaLabel="Submit a correction"
          borderless={borderless}
        >
          {children}
        </Dropdown.Toggle>
        <Dropdown.Menu alignment={alignment}>{items}</Dropdown.Menu>
      </Dropdown>

      <FeedbackFormModal
        isOpen={feedbackModalIsOpen}
        onRequestClose={() => setFeedbackModalIsOpen(false)}
        actionType={feedbackModalActionType}
        client={client}
      />
    </>
  );
};

export default NeedsCorrectionDropdown;
