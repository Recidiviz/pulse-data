import * as React from "react";
import {
  Dropdown,
  DropdownMenu,
  DropdownMenuItem,
  DropdownToggle,
  DropdownToggleProps,
} from "@recidiviz/design-system";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import FeedbackFormModal from "../FeedbackFormModal";
import { Client } from "../../stores/ClientsStore";
import { ACTION_TITLES } from "../../stores/CaseUpdatesStore/CaseUpdates";

interface NeedsCorrectionDropdownProps {
  actions: CaseUpdateActionType[];
  children?: React.ReactNode | React.ReactNode[] | null;
  className?: string;
  client: Client;
  alignment?: "left" | "right";
  toggleProps?: DropdownToggleProps;
}

const NeedsCorrectionDropdown = ({
  actions,
  alignment,
  children,
  className,
  client,
  toggleProps,
}: NeedsCorrectionDropdownProps): JSX.Element => {
  const [feedbackModalActionType, setFeedbackModalActionType] =
    React.useState<CaseUpdateActionType>(actions[0]);
  const [feedbackModalIsOpen, setFeedbackModalIsOpen] = React.useState(false);

  const items = actions.map((action) => (
    <DropdownMenuItem
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
        <DropdownToggle aria-label="Submit a correction" {...toggleProps}>
          {children}
        </DropdownToggle>
        <DropdownMenu alignment={alignment}>{items}</DropdownMenu>
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
