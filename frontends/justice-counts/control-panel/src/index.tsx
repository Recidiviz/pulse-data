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
import ReactDOM from "react-dom";
import { BrowserRouter } from "react-router-dom";
import { createGlobalStyle } from "styled-components/macro";

import App from "./App";
import AuthWall from "./components/Auth";
import { palette } from "./components/GlobalStyles";
import { StoreProvider } from "./stores";

const GlobalStyle = createGlobalStyle`
  * {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }

  html, body, #root {
    height: 100%;
  }

  body {
    width: 100%;
    font-family: "Inter", sans-serif;
    font-weight: 500;
    font-size: 16px;
    background-color: ${palette.white};
    color: ${palette.text.darkgrey};
  }
`;

ReactDOM.render(
  <React.StrictMode>
    <StoreProvider>
      <GlobalStyle />
      <AuthWall>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </AuthWall>
    </StoreProvider>
  </React.StrictMode>,
  document.getElementById("root")
);
