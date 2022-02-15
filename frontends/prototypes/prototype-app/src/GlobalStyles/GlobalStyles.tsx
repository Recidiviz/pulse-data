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

import { fonts, palette } from "@recidiviz/design-system";
import React from "react";
import { createGlobalStyle } from "styled-components/macro";
import reset from "styled-reset";

const BaseStyles = createGlobalStyle`
  ${reset}

  html {
    box-sizing: border-box;
    font-family: ${fonts.sans};
    
    *, *:before, *:after {
      box-sizing: inherit;
      font-family: inherit;
    }
  }

  body {
    background-color: ${palette.marble2};
    color: ${palette.text.normal};
  }

  strong {
    font-weight: 600;
  }
`;

const GlobalStyles: React.FC = () => {
  return <BaseStyles />;
};

export default GlobalStyles;
