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
import "./window.d";
import "react-app-polyfill/ie11";
import "core-js";
import React from "react";
import ReactDOM from "react-dom";
import * as Sentry from "@sentry/react";
import { Integrations } from "@sentry/tracing";
import styled from "styled-components/macro";
import { Router } from "@reach/router";
import ReactModal from "react-modal";

import { GlobalStyle } from "@recidiviz/design-system";

import Home from "./routes/Home";
import Verify from "./routes/Verify";

import { trackScrolledToBottom } from "./analytics";
import StoreProvider from "./stores";

if (process.env.NODE_ENV !== "development") {
  Sentry.init({
    dsn:
      "https://1aa10e823cad49d9a662d71cedb3365b@o432474.ingest.sentry.io/5623757",
    integrations: [new Integrations.BrowserTracing()],

    // This value may need to be adjusted over time as usage increases.
    tracesSampleRate: 1.0,
  });
}

// Implement scrollToBottom listener
let isCurrentlyScrolledToBottom = false;
window.onscroll = function () {
  const scrolledToBottom =
    window.innerHeight + window.scrollY >= document.body.offsetHeight;
  if (scrolledToBottom) {
    if (!isCurrentlyScrolledToBottom) {
      isCurrentlyScrolledToBottom = true;
      trackScrolledToBottom();
    }
  } else {
    isCurrentlyScrolledToBottom = false;
  }
};

const RoutingContainer = styled(Router)`
  margin: 0 auto;
  max-width: 1288px;
  height: 100%;
`;

ReactDOM.render(
  <StoreProvider>
    <GlobalStyle />
    <RoutingContainer>
      <Verify path="verify" />
      <Home path="/" />
    </RoutingContainer>
  </StoreProvider>,
  document.getElementById("root"),
  () => {
    ReactModal.setAppElement("#root");
  }
);
