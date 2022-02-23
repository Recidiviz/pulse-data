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
import { rem } from "polished";
import React from "react";
import styled from "styled-components/macro";

import Seal from "./Seal.png";

interface FormHeaderProps {
  title: string;
}

const FormHeaderContainer = styled.div`
  margin-bottom: ${rem(10)};
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
`;

const FormHeaderHeading = styled.h1`
  margin-left: ${rem(50)};
  font-family: "Arial", sans-serif;
  font-size: 9px;
  margin-bottom: 0;
  margin-top: 0;
`;

const FormHeader: React.FC<FormHeaderProps> = ({ title }) => (
  <FormHeaderContainer>
    <img src={Seal} style={{ height: rem(50), width: rem(50) }} alt="TN Seal" />
    <FormHeaderHeading>
      TENNESSEE DEPARTMENT OF CORRECTION
      <br /> COMMUNITY SUPERVISION
      <br />
      <strong>{title}</strong>
    </FormHeaderHeading>
  </FormHeaderContainer>
);

export default FormHeader;
