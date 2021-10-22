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

import { DropdownMenuItem, Icon, palette } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import moment from "moment";
import React from "react";
import { useRootStore } from "../../../stores";
import {
  ACTION_TITLES,
  CaseUpdate,
  CaseUpdateActionType,
} from "../../../stores/CaseUpdatesStore/CaseUpdates";
import { Client } from "../../../stores/ClientsStore";
import { LONG_DATE_FORMAT } from "../../../utils";
import { Alert } from "./Alert";

export const CaseloadRemovalAlert = observer(
  ({
    client,
    pendingUpdate,
  }: {
    client: Client;
    pendingUpdate: CaseUpdate;
  }): JSX.Element => {
    const { caseUpdatesStore } = useRootStore();
    console.log(pendingUpdate.actionType);
    return (
      <Alert
        bullet={<Icon size={16} kind="Alert" color={palette.data.gold1} />}
        title={ACTION_TITLES[pendingUpdate.actionType]}
        body={
          <>
            Submitted on{" "}
            {moment(pendingUpdate.actionTs).format(LONG_DATE_FORMAT)}.{" "}
            {pendingUpdate.actionType ===
            CaseUpdateActionType.CURRENTLY_IN_CUSTODY
              ? `To remove this status, select the three dots and click "Undo"`
              : "Once processed, this client will be removed from your list."}
          </>
        }
        menuItems={
          <DropdownMenuItem
            onClick={() => {
              if (pendingUpdate.updateId) {
                caseUpdatesStore.removeAction(
                  client,
                  pendingUpdate.updateId,
                  pendingUpdate.actionType
                );
              }
            }}
          >
            Undo
          </DropdownMenuItem>
        }
      />
    );
  }
);
