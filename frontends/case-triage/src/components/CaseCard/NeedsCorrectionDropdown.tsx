import * as React from "react";
import { FC } from "react";
import styled from "styled-components/macro";
import { Dropdown, palette } from "@recidiviz/design-system";
import moment from "moment";
import { rem } from "polished";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import FeedbackFormModal from "../FeedbackFormModal";
import { DecoratedClient } from "../../stores/ClientsStore";
import { ButtonContainer } from "./CaseCard.styles";
import { NeedsCheckboxButton } from "./CaseCardButtons";
import { useRootStore } from "../../stores";

const ReportedText = styled.div`
  font-style: normal;
  font-size: ${rem(14)};
  line-height: ${rem(24)};
  letter-spacing: -0.01em;
  color: ${palette.slate70};
`;

interface NeedsCorrectionDropdownProps {
  actions: CaseUpdateActionType[];
  borderless?: boolean;
  className?: string;
  client: DecoratedClient;
  icon?: FC;
  met?: boolean;
}

const NeedsCorrectionDropdown = ({
  actions,
  className,
  borderless = false,
  met = false,
  client,
  icon,
}: NeedsCorrectionDropdownProps): JSX.Element => {
  const [
    feedbackModalActionType,
    setFeedbackModalActionType,
  ] = React.useState<CaseUpdateActionType>(actions[0]);
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
        <Dropdown.Toggle icon={icon} borderless={borderless}>
          {met ? "Submit a correction " : null}
        </Dropdown.Toggle>
        <Dropdown.Menu alignment={met ? "left" : "right"}>
          {items}
        </Dropdown.Menu>
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

const ACTION_TITLES: Record<CaseUpdateActionType, string> = {
  [CaseUpdateActionType.COMPLETED_ASSESSMENT]:
    "I completed their risk assessment",
  [CaseUpdateActionType.FOUND_EMPLOYMENT]: "I helped them find employment",
  [CaseUpdateActionType.SCHEDULED_FACE_TO_FACE]:
    "I scheduled our next face-to-face contact",
  [CaseUpdateActionType.DISCHARGE_INITIATED]: "DISCHARGE_INITIATED",
  [CaseUpdateActionType.DOWNGRADE_INITIATED]: "DOWNGRADE_INITIATED",

  [CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA]:
    "Incorrect assessment status",
  [CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA]:
    "Incorrect employment status",
  [CaseUpdateActionType.INCORRECT_CONTACT_DATA]: "Incorrect contact status",
  [CaseUpdateActionType.NOT_ON_CASELOAD]: "Not on Caseload",
  [CaseUpdateActionType.CURRENTLY_IN_CUSTODY]: "In Custody",
};

interface NeedsActionFlowProps {
  client: DecoratedClient;
  met: boolean;
  resolve: CaseUpdateActionType;
  dismiss: CaseUpdateActionType;
}

const NeedsActionFlow = ({
  client,
  dismiss,
  met,
  resolve,
}: NeedsActionFlowProps): JSX.Element => {
  const { caseUpdatesStore } = useRootStore();
  const [needChecked, setNeedChecked] = React.useState(false);

  const recordEvent = (
    eventType: CaseUpdateActionType,
    completedAction: boolean
  ): void => {
    caseUpdatesStore.toggleAction(client, eventType, completedAction);
  };

  React.useEffect(() => {
    setNeedChecked(client.hasInProgressUpdate(resolve));
  }, [client, resolve]);

  const onToggleCheck = (checked: boolean) => {
    setNeedChecked(checked);
    recordEvent(resolve, checked);
  };

  if (client.hasInProgressUpdate(dismiss)) {
    const caseUpdate = client.caseUpdates[dismiss];

    return (
      <>
        <ReportedText>
          Reported on {moment(caseUpdate?.actionTs).format("LL")}
        </ReportedText>

        <ButtonContainer>
          <NeedsCheckboxButton
            checked
            onToggleCheck={(completed: boolean) =>
              recordEvent(dismiss, completed)
            }
            title={ACTION_TITLES[dismiss]}
          />
        </ButtonContainer>
      </>
    );
  }

  return (
    <ButtonContainer>
      {met ? null : (
        <NeedsCheckboxButton
          checked={needChecked}
          onToggleCheck={onToggleCheck}
          title={ACTION_TITLES[resolve]}
        />
      )}

      {!needChecked ? (
        <NeedsCorrectionDropdown
          actions={[dismiss]}
          client={client}
          met={met}
        />
      ) : null}
    </ButtonContainer>
  );
};

export default NeedsCorrectionDropdown;
export { NeedsActionFlow };
