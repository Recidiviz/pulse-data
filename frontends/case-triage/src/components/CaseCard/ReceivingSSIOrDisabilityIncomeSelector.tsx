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
import {
  Dropdown,
  DropdownMenu,
  DropdownMenuItem,
  DropdownToggle,
  Icon,
  IconSVG,
  useToasts,
} from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import * as React from "react";
import { useRootStore } from "../../stores";
import { Client } from "../../stores/ClientsStore/Client";

export interface ReceivingSSIOrDisabilityIncomeSelectorProps {
  client: Client;
}

export const ReceivingSSIOrDisabilityIncomeSelector: React.FC<ReceivingSSIOrDisabilityIncomeSelectorProps> =
  observer(({ client }) => {
    const { opportunityStore } = useRootStore();
    const { addToast } = useToasts();

    return (
      <Dropdown>
        <DropdownToggle kind="link">
          {client.receivingSSIOrDisabilityIncome
            ? "SSI or Disability"
            : "Status"}{" "}
          <Icon kind={IconSVG.Caret} size={6} />
        </DropdownToggle>
        <DropdownMenu>
          <DropdownMenuItem
            label={`${
              client.receivingSSIOrDisabilityIncome ? "Remove" : "Add"
            } SSI or Disability`}
            onClick={async () => {
              try {
                await client.updateReceivingSSIOrDisabilityIncome(
                  !client.receivingSSIOrDisabilityIncome
                );
              } catch (error) {
                addToast("Failed to update SSI/Disability income", {
                  appearance: "error",
                });
              }
              // We check to see if their opportunities have changed
              // now that we've changed SSI/Disability income information.
              await opportunityStore.fetchOpportunities();
            }}
          />
        </DropdownMenu>
      </Dropdown>
    );
  });
