// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

import React, { useEffect, useState } from "react";
import styled, { css, keyframes } from "styled-components/macro";

import { HEADER_BAR_HEIGHT, palette } from "../GlobalStyles";

const ModalContainer = styled.div`
  width: 100vw;
  height: 100vh;
  position: absolute;
  top: 0;
  left: 0;
  z-index: 10;
  background: ${palette.highlight.grey4};
`;

const fromBottomToTop = keyframes`
  0% { transform: translateY(100%); }
  100%   { transform: translateY(0); }   
`;

const fromTopToBottom = keyframes`
  0%   { transform: translateY(0); }   
  100% { transform: translateY(100%); }
`;

const animateBottomToTop = css`
  animation: ${fromBottomToTop} 0.5s forwards ease;
`;

const animateTopToBottom = css`
  animation: ${fromTopToBottom} 0.3s forwards ease-in-out;
`;

const MODAL_TOP_GAP = `${HEADER_BAR_HEIGHT}px`;

const ModalElement = styled.div<{ showUnmountAnimation?: boolean }>`
  height: calc(100% - ${MODAL_TOP_GAP});
  margin-top: ${MODAL_TOP_GAP};
  background: ${palette.solid.white};
  position: relative;
  border-top: 1px solid ${palette.solid.darkgrey};
  ${({ showUnmountAnimation }) =>
    showUnmountAnimation ? animateTopToBottom : animateBottomToTop}
`;

const CloseButton = styled.div`
  width: 64px;
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: ${palette.solid.red};
  border-radius: 0.5px;
  position: absolute;
  top: 0;
  right: 0;
  z-index: 4;

  &:before {
    content: "";
    position: absolute;
    width: 32px;
    height: 2px;
    background: ${palette.solid.white};
    transform: rotate(135deg);
  }

  &:after {
    content: "";
    position: absolute;
    width: 32px;
    height: 2px;
    background: ${palette.solid.white};
    transform: rotate(45deg);
  }

  &:hover {
    cursor: pointer;
    opacity: 0.9;
  }
`;

type ModalProps = {
  isOpen: boolean;
  handleClose: () => void;
};

export const Modal: React.FC<ModalProps> = ({
  isOpen,
  handleClose,
  children,
}) => {
  const [showUnmountAnimation, setShowUnmountAnimation] = useState(false);

  const closeModal = () => {
    if (showUnmountAnimation) {
      setShowUnmountAnimation(false);
      handleClose();
    }
  };

  const prepareModalToClose = (e: React.MouseEvent) => {
    if (e.target !== e.currentTarget) return;
    setShowUnmountAnimation(true);
  };

  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "unset";
    }

    return () => {
      document.body.style.overflow = "unset";
    };
  }, [isOpen]);

  if (!isOpen) return null;

  return (
    <ModalContainer onClick={prepareModalToClose}>
      <ModalElement
        showUnmountAnimation={showUnmountAnimation}
        onAnimationEnd={closeModal}
      >
        <CloseButton onClick={prepareModalToClose} /> {children}
      </ModalElement>
    </ModalContainer>
  );
};
