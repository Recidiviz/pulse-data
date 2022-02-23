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
import { Card, Modal } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import { rem } from "polished";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

const UnpaddedModal = styled(Modal)`
  /* need extra specificity to override base */
  && .ReactModal__Content {
    padding: 0;
  }

  ${Card} {
    box-shadow: none;
  }
`;

const DRAWER_MARGIN = 24;
export const FORM_WIDTH = 742;
export const PROFILE_WIDTH = 555;

const DrawerModal = styled(UnpaddedModal)<{ includeForm?: boolean }>`
  --drawer-width: ${(props) =>
    props.includeForm ? FORM_WIDTH + PROFILE_WIDTH : PROFILE_WIDTH}px;

  /* need extra specificity to override base */
  && .ReactModal__Content {
    height: calc(100vh - ${rem(DRAWER_MARGIN * 2)});
    max-height: unset;
    max-width: calc(100vw - ${rem(DRAWER_MARGIN * 2)});
    width: var(--drawer-width);

    /* transition: slide out from side instead of zooming from center */
    left: unset;
    right: ${rem(DRAWER_MARGIN)};
    transform: translate(var(--drawer-width), -50%) !important;

    &.ReactModal__Content--after-open {
      transform: translate(0, -50%) !important;
    }

    &.ReactModal__Content--before-close {
      transform: translate(var(--drawer-width), -50%) !important;
    }
  }
`;

export const ProfileDrawer: React.FC<{ includeForm: boolean }> = observer(
  ({ children, includeForm }) => {
    const { caseStore } = useDataStore();

    return (
      <DrawerModal
        isOpen={Boolean(caseStore.activeClientId)}
        onRequestClose={() => caseStore.setActiveClient()}
        includeForm={includeForm}
      >
        {children}
      </DrawerModal>
    );
  }
);
