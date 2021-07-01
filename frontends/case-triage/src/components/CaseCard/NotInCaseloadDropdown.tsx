import * as React from "react";
import { observer } from "mobx-react-lite";
import { Client } from "../../stores/ClientsStore";
import {
  CaseUpdateActionType,
  NotInCaseloadActions,
} from "../../stores/CaseUpdatesStore";
import { EllipsisDropdown } from "./CaseCard.styles";

export interface NotInCaseloadDropdownProps {
  client: Client;
}

export const NotInCaseloadDropdown: React.FC<NotInCaseloadDropdownProps> =
  observer(({ client }) => {
    const currentNotOnCaseloadAction =
      client.findInProgressUpdate(NotInCaseloadActions);
    let actions: CaseUpdateActionType[] = [];
    if (!currentNotOnCaseloadAction) {
      actions = actions.concat(NotInCaseloadActions);
    } else if (
      client.hasInProgressUpdate(CaseUpdateActionType.NOT_ON_CASELOAD)
    ) {
      actions.push(CaseUpdateActionType.NOT_ON_CASELOAD);
    }

    if (actions.length > 0) {
      return (
        <EllipsisDropdown actions={actions} client={client} alignment="right" />
      );
    }

    return null;
  });
