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

import React from "react";
import styled, { keyframes } from "styled-components/macro";

import sprite from "../assets/loader-sprite-horizontal.svg";

const loaderWidth = 144;
const spriteFrames = 104;

const LoadingWrapper = styled.div`
  height: 100vh;
  width: 100vw;
  display: flex;
  justify-content: center;
  align-items: center;
  position: absolute;
  top: 0;
  left: 0;
`;

const loadingSpriteAnimation = keyframes`
  to {
    background-position: -${loaderWidth * spriteFrames}px 0px;
  }
`;

const Loader = styled.div`
  height: ${loaderWidth}px;
  width: ${loaderWidth}px;
  background-image: url(${sprite});
  background-repeat: no-repeat;
  background-size: ${loaderWidth * spriteFrames}px ${loaderWidth}px;
  background-position: 0px 0px;
  animation: ${loadingSpriteAnimation} 1.5s steps(${spriteFrames}) infinite
    alternate;
`;

export const Loading = () => {
  return (
    <LoadingWrapper>
      <Loader data-testid="loading" />
    </LoadingWrapper>
  );
};
