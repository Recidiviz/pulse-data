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
import { action, autorun } from "mobx";
import { observer } from "mobx-react-lite";
import React, { useEffect, useState } from "react";
import styled from "styled-components/macro";
import { useRootStore } from "../../stores";

const UnpaddedModal = styled(Modal)`
  /* need extra specificity to override base */
  && .ReactModal__Content {
    padding: 0;
  }

  ${Card} {
    box-shadow: none;
  }
`;

export const CaseCardModal: React.FC<{ isOpen: boolean }> = ({
  children,
  isOpen,
}) => {
  const { clientsStore } = useRootStore();

  return (
    <UnpaddedModal
      isOpen={isOpen}
      onRequestClose={action("close client modal", () => {
        clientsStore.activeClient = undefined;
      })}
    >
      {children}
    </UnpaddedModal>
  );
};

const PopoutWrapper = styled(Card).attrs({ stacked: true })`
  position: relative;
  transition-property: "transform";

  /* need extra specificity to override Card shadow */
  && {
    box-shadow: 0px 15px 40px rgba(53, 83, 98, 0.3),
      inset 0px -1px 1px rgba(19, 44, 82, 0.2);
  }

  &:before {
    position: absolute;
    display: block;
    content: "";
    width: 0;
    height: 0;
    border-top: 16px solid transparent;
    border-bottom: 16px solid transparent;
    border-right: 16px solid white;
    left: -15px;
    top: 38px;
  }

  ${Card} {
    box-shadow: none;
  }
`;

export const CaseCardPopout: React.FC = observer(({ children }) => {
  const { clientsStore } = useRootStore();

  const [transitionDuration, setTransitionDuration] = useState("0");
  const [translateY, setTranslateY] = useState(clientsStore.activeClientOffset);

  useEffect(() => {
    return autorun(() => {
      if (clientsStore.activeClientOffset !== translateY) {
        if (clientsStore.clientPendingAnimation) {
          clientsStore.setClientPendingAnimation(false);
          setTransitionDuration("1s");
        } else {
          setTransitionDuration("0s");
        }

        setTranslateY(clientsStore.activeClientOffset);
      }
    });
  });

  return (
    <PopoutWrapper
      style={{
        transitionDuration,
        transform: `translateY(${translateY}px)`,
      }}
    >
      {children}
    </PopoutWrapper>
  );
});
