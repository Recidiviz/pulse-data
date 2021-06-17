import * as React from "react";
import moment from "moment";
import { Icon, IconSVG } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import {
  CaseUpdateActionType,
  CaseUpdateStatus,
} from "../../stores/CaseUpdatesStore";
import { Client } from "../../stores/ClientsStore";
import { useRootStore } from "../../stores";
import { ButtonContainer } from "../CaseCard/CaseCard.styles";
import { NeedsCheckboxButton } from "../CaseCard/CaseCardButtons";
import NeedsCorrectionDropdown from "./NeedsCorrectionDropdown";
import { OpportunityDeferralDropdown } from "./OpportunityDeferralDropdown";
import { Opportunity } from "../../stores/OpportunityStore";

export const ACTION_TITLES: Record<CaseUpdateActionType, string> = {
  [CaseUpdateActionType.COMPLETED_ASSESSMENT]:
    "I completed their risk assessment",
  [CaseUpdateActionType.FOUND_EMPLOYMENT]: "I helped them find employment",
  [CaseUpdateActionType.SCHEDULED_FACE_TO_FACE]:
    "I scheduled our next face-to-face contact",
  [CaseUpdateActionType.DISCHARGE_INITIATED]: "DISCHARGE_INITIATED",
  [CaseUpdateActionType.DOWNGRADE_INITIATED]:
    "I updated their supervision level",
  [CaseUpdateActionType.INCORRECT_SUPERVISION_LEVEL_DATA]:
    "Incorrect supervision level data",

  [CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA]:
    "Incorrect assessment status",
  [CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA]:
    "Incorrect employment status",
  [CaseUpdateActionType.INCORRECT_CONTACT_DATA]: "Incorrect contact status",
  [CaseUpdateActionType.NOT_ON_CASELOAD]: "Not on Caseload",
  [CaseUpdateActionType.CURRENTLY_IN_CUSTODY]: "In Custody",
};

interface NeedsActionFlowProps {
  actionable: boolean;
  client: Client;
  opportunity?: Opportunity;
  resolve: CaseUpdateActionType;
  dismiss: CaseUpdateActionType;
}

interface CaseUpdateProps {
  actionType: CaseUpdateActionType;
  client: Client;
  onCompleteClicked: CallableFunction;
}

const CaseUpdateComponent = observer(
  ({ actionType, client, onCompleteClicked }: CaseUpdateProps, ref) => {
    return (
      <NeedsCheckboxButton
        checked={client.hasInProgressUpdate(actionType)}
        onToggleCheck={() => onCompleteClicked()}
        title={ACTION_TITLES[actionType]}
      />
    );
  },
  { forwardRef: true }
);

const NeedsActionFlow = observer(
  ({
    actionable,
    client,
    opportunity,
    dismiss,
    resolve,
  }: NeedsActionFlowProps): JSX.Element => {
    const { caseUpdatesStore, clientsStore, opportunityStore } = useRootStore();

    const recordEvent = (
      eventType: CaseUpdateActionType,
      completedAction: boolean
    ): void => {
      caseUpdatesStore.toggleAction(client, eventType, completedAction);
    };

    const dismissedUpdate = client.caseUpdates[dismiss];
    const resolvedUpdate = client.caseUpdates[resolve];

    let inProgressPillProps;
    if (resolvedUpdate?.status === CaseUpdateStatus.IN_PROGRESS) {
      inProgressPillProps = {
        title: ACTION_TITLES[resolvedUpdate.actionType],
        tooltip: `Reported on ${moment(resolvedUpdate.actionTs).format("LL")}`,
        onToggleCheck: () => recordEvent(resolvedUpdate.actionType, false),
      };
    } else if (dismissedUpdate?.status === CaseUpdateStatus.IN_PROGRESS) {
      inProgressPillProps = {
        title: ACTION_TITLES[dismissedUpdate.actionType],
        tooltip: `Reported on ${moment(dismissedUpdate.actionTs).format("LL")}`,
        onToggleCheck: () => recordEvent(dismissedUpdate.actionType, false),
      };
    } else if (opportunity?.deferredUntil) {
      inProgressPillProps = {
        title: `Reminder on ${moment(opportunity.deferredUntil).format("LL")}`,
        onToggleCheck: () => {
          clientsStore.setClientPendingAnimation(true);
          opportunityStore.deleteOpportunityDeferral(opportunity);
        },
      };
    }

    if (inProgressPillProps) {
      return (
        <ButtonContainer>
          <NeedsCheckboxButton checked {...inProgressPillProps} />
        </ButtonContainer>
      );
    }

    return (
      <ButtonContainer>
        {actionable ? (
          <CaseUpdateComponent
            actionType={resolve}
            client={client}
            onCompleteClicked={() => {
              recordEvent(resolve, true);
            }}
          />
        ) : null}

        {actionable && opportunity ? (
          <OpportunityDeferralDropdown
            onDeferred={(deferUntil) => {
              clientsStore.setClientPendingAnimation(true);
              opportunityStore.createOpportunityDeferral(
                client,
                opportunity.opportunityType,
                deferUntil
              );
            }}
          />
        ) : null}

        <NeedsCorrectionDropdown
          actions={[dismiss]}
          client={client}
          alignment={!actionable ? "left" : "right"}
        >
          <span style={{ alignSelf: !actionable ? "auto" : "baseline" }}>
            {!actionable ? "Submit a correction " : null}
            <Icon kind={IconSVG.Caret} size={6} />
          </span>
        </NeedsCorrectionDropdown>
      </ButtonContainer>
    );
  }
);

export { NeedsActionFlow };
