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
import {
  Client,
  PreferredContactMethod,
} from "../../stores/ClientsStore/Client";
import { useRootStore } from "../../stores";

import { titleCase } from "../../utils";

export interface PreferredContactMethodSelectorProps {
  client: Client;
}

const DEFAULT_TITLE = "Method";

export const PreferredContactMethodSelector: React.FC<PreferredContactMethodSelectorProps> =
  observer(({ client }) => {
    const { clientsStore } = useRootStore();
    const { addToast } = useToasts();

    return (
      <Dropdown>
        <DropdownToggle kind="link">
          {client.preferredContactMethod
            ? titleCase(client.preferredContactMethod)
            : DEFAULT_TITLE}{" "}
          <Icon kind={IconSVG.Caret} size={6} />
        </DropdownToggle>
        <DropdownMenu>
          {Object.values(PreferredContactMethod).map((value) => (
            <DropdownMenuItem
              label={titleCase(value)}
              onClick={() =>
                clientsStore
                  .updateClientPreferredContactMethod(client, value)
                  .catch(() =>
                    addToast("Failed to update preferred contact method", {
                      appearance: "error",
                    })
                  )
              }
              key={value}
            />
          ))}
        </DropdownMenu>
      </Dropdown>
    );
  });
