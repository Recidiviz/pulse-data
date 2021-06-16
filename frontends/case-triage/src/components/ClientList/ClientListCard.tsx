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
import * as React from "react";
import { observer } from "mobx-react-lite";
import { autorun } from "mobx";
import { useRootStore } from "../../stores";
import {
  ClientListCardElement,
  InProgressIndicator,
  StackingClientListCardElement,
} from "./ClientList.styles";
import Tooltip from "../Tooltip";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { trackPersonSelected } from "../../analytics";
import { ClientProps } from "./ClientList.types";
import { KNOWN_EXPERIMENTS } from "../../stores/UserStore";
import CardIconsLayout from "./CardIconsLayout";
import CardPillsLayout from "./CardPillsLayout";

const ClientComponent: React.FC<ClientProps> = observer(
  ({ client }: ClientProps) => {
    const { clientsStore, policyStore, userStore } = useRootStore();
    const cardRef = React.useRef<HTMLDivElement>(null);

    const viewClient = React.useCallback(() => {
      clientsStore.view(
        client,
        cardRef !== null && cardRef.current !== null
          ? cardRef.current.offsetTop
          : 0
      );
    }, [client, clientsStore, cardRef]);

    React.useLayoutEffect(() => {
      // When undoing a Case Update, we need to re-open the client card
      // Check if this card's client is pending a `view`, if so, re-open the Case Card
      return autorun(() => {
        if (client.isActive) {
          viewClient();
        }
      });
    }, [client, clientsStore, viewClient]);

    // Counts in-progress (non-deprecated) actions
    const numInProgressActions = Object.values(CaseUpdateActionType).reduce(
      (accumulator, actionType) =>
        accumulator +
        (client.hasInProgressUpdate(actionType as CaseUpdateActionType)
          ? 1
          : 0),
      0
    );

    const omsName = policyStore.policies?.omsName || "OMS";

    const active =
      clientsStore.activeClient?.personExternalId === client.personExternalId;

    const showNewLayout = userStore.isInExperiment(KNOWN_EXPERIMENTS.NewLayout);

    const CardElement = showNewLayout
      ? ClientListCardElement
      : StackingClientListCardElement;

    return (
      <CardElement
        className={active ? "client-card--active" : ""}
        ref={cardRef}
        onClick={() => {
          trackPersonSelected(client);
          viewClient();
        }}
      >
        {showNewLayout ? (
          <CardPillsLayout client={client} />
        ) : (
          <CardIconsLayout client={client} />
        )}

        {numInProgressActions > 0 ? (
          <Tooltip
            title={
              <>
                <strong>{numInProgressActions}</strong> action
                {numInProgressActions !== 1 ? "s" : ""} being confirmed with{" "}
                {omsName}
              </>
            }
          >
            <InProgressIndicator />
          </Tooltip>
        ) : null}
      </CardElement>
    );
  }
);

export default ClientComponent;
