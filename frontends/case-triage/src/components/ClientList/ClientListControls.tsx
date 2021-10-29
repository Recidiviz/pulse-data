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

import { Dropdown, DropdownMenuItem } from "@recidiviz/design-system";
import { action } from "mobx";
import { observer } from "mobx-react-lite";
import React from "react";
import { useRootStore } from "../../stores";
import { SupervisionLevel, SupervisionLevels } from "../../stores/ClientsStore";
import {
  SortOrder,
  SortOrderList,
} from "../../stores/ClientsStore/ClientsStore";
import { titleCase, useUuid } from "../../utils";
import { ToggleMenuProps } from "../ToggleMenu";
import {
  ControlLabel,
  ControlsWrapper,
  ControlWrapper,
  FilterControlMenu,
  FilterControlToggle,
  SortControlMenu,
  SortControlToggle,
  SortControlWrapper,
} from "./ClientList.styles";

const FilterControl = <OptionId extends string>({
  currentValue,
  labelText,
  options,
  setCurrentValue,
}: ToggleMenuProps<OptionId> & { labelText: string }) => {
  const id = useUuid();

  const selectedOption = options.find((opt) => opt.id === currentValue);
  const toggleLabel = selectedOption?.label || "All";

  return (
    <>
      <ControlLabel id={id}>{labelText}</ControlLabel>
      <Dropdown>
        <FilterControlToggle aria-labelledby={id}>
          {toggleLabel}
        </FilterControlToggle>
        <FilterControlMenu
          currentValue={currentValue}
          options={options}
          setCurrentValue={setCurrentValue}
        />
      </Dropdown>
    </>
  );
};

const SupervisionLevelControl = observer(() => {
  const { clientsStore, policyStore } = useRootStore();
  const { supervisionLevelFilter } = clientsStore;

  const options = [...SupervisionLevels]
    .reverse()
    .map((id) => ({ id, label: policyStore.getSupervisionLevelName(id) }));

  return (
    <FilterControl
      labelText="Supervision Level"
      currentValue={supervisionLevelFilter}
      setCurrentValue={action(
        "supervision level filter select",
        (newValue: SupervisionLevel | undefined) => {
          clientsStore.supervisionLevelFilter = newValue;
        }
      )}
      options={options}
    />
  );
});

const SupervisionTypeControl = observer(() => {
  const { clientsStore } = useRootStore();
  const { supervisionTypeFilter, supervisionTypes } = clientsStore;

  const options = supervisionTypes.map((id) => ({
    id,
    label: titleCase(id),
  }));

  return (
    <FilterControl
      labelText="Supervision Type"
      currentValue={supervisionTypeFilter}
      options={options}
      setCurrentValue={action(
        "supervision type filter select",
        (newValue: string | undefined) => {
          clientsStore.supervisionTypeFilter = newValue;
        }
      )}
    />
  );
});

const SORT_ORDER_LABELS: Record<SortOrder, string> = {
  RELEVANCE: "Relevance",
  ASSESSMENT_DATE: "Risk assessment due date",
  CONTACT_DATE: "Contact recommended date",
  DAYS_WITH_PO: "New to caseload",
  START_DATE: "New to supervision",
  HOME_VISIT_DATE: "Home visit recommended date",
};

const SORT_ORDER_OPTIONS = SortOrderList.map((id) => ({
  id,
  label: SORT_ORDER_LABELS[id],
}));

const SortControl = observer(() => {
  const { clientsStore, userStore } = useRootStore();
  const { sortOrder } = clientsStore;

  let options = SORT_ORDER_OPTIONS;

  if (!userStore.canSeeExtendedProfile) {
    options = options.filter(({ id }) => id !== "DAYS_WITH_PO");
  }
  // TODO(#9807) remove feature flag when ready to release home visit
  if (!userStore.canSeeHomeVisit) {
    options = options.filter(({ id }) => id !== "HOME_VISIT_DATE");
  }

  return (
    <Dropdown>
      <SortControlToggle>
        Sort by {SORT_ORDER_LABELS[sortOrder]}
      </SortControlToggle>
      <SortControlMenu>
        {options.map(({ id, label }) => (
          <DropdownMenuItem
            key={id}
            label={label}
            onClick={action("sort order select", () => {
              clientsStore.sortOrder = id;
            })}
          />
        ))}
      </SortControlMenu>
    </Dropdown>
  );
});

export const ClientListControls = (): JSX.Element => {
  return (
    <ControlsWrapper>
      <ControlWrapper>
        <SupervisionLevelControl />
      </ControlWrapper>
      <ControlWrapper>
        <SupervisionTypeControl />
      </ControlWrapper>
      <SortControlWrapper>
        <SortControl />
      </SortControlWrapper>
    </ControlsWrapper>
  );
};
