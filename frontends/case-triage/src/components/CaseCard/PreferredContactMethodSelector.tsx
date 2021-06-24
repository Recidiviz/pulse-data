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
