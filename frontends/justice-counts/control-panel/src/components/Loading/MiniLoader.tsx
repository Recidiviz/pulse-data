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

const dotLoopAnimation = keyframes`
    0% {
        transform: rotate(0);
    }
    100% {
        transform: rotate(360deg); 
    }
`;

export const SVGContainer = styled.div`
  height: 100%;
  margin-left: 5px;
`;

export const AnimatedSVGGroup = styled.g`
  transform-origin: 8px 8px;
  animation: ${dotLoopAnimation} 0.8s infinite steps(8, start);
`;

export const MiniLoader: React.FC = () => {
  return (
    <SVGContainer>
      <svg
        xmlns="http://www.w3.org/2000/svg"
        height="16"
        width="16"
        viewBox="0 0 16 16"
      >
        <title>dots anim</title>
        <g fill="#ffffff">
          <AnimatedSVGGroup>
            <circle cx="8" cy="1.5" fill="#ffffff" r="1.5" opacity="0.6" />
            <circle
              cx="12.596"
              cy="3.404"
              fill="#ffffff"
              r="1.5"
              opacity="0.8"
            />
            <circle cx="14.5" cy="8" fill="#ffffff" r="1.5" />
            <circle
              cx="12.596"
              cy="12.596"
              fill="#ffffff"
              r="1.5"
              opacity="0.4"
            />
            <circle cx="8" cy="14.5" fill="#ffffff" r="1.5" opacity="0.4" />
            <circle
              cx="3.404"
              cy="12.596"
              fill="#ffffff"
              r="1.5"
              opacity="0.4"
            />
            <circle cx="1.5" cy="8" fill="#ffffff" r="1.5" opacity="0.4" />
            <circle
              cx="3.404"
              cy="3.404"
              fill="#ffffff"
              r="1.5"
              opacity="0.4"
            />
          </AnimatedSVGGroup>
        </g>
      </svg>
    </SVGContainer>
  );
};
