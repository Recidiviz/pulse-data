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

// load analytics
window.analytics = window.analytics || [];
const { analytics } = window;
if (!analytics.initialize) {
  if (analytics.invoked) {
    console.error("Segment snippet included twice.");
  } else {
    analytics.invoked = true;
    analytics.methods = [
      "trackSubmit",
      "trackClick",
      "trackLink",
      "trackForm",
      "pageview",
      "identify",
      "reset",
      "group",
      "track",
      "ready",
      "alias",
      "debug",
      "page",
      "once",
      "off",
      "on",
      "addSourceMiddleware",
      "addIntegrationMiddleware",
      "setAnonymousId",
      "addDestinationMiddleware",
    ];
    analytics.factory = function (e: any) {
      return function () {
        const t = Array.prototype.slice.call(arguments); // eslint-disable-line prefer-rest-params
        t.unshift(e);
        analytics.push(t);
        return analytics;
      };
    };
    for (let e = 0; e < analytics.methods.length; e += 1) {
      const key = analytics.methods[e];
      analytics[key] = analytics.factory(key);
    }
    analytics.load = function (key: any, e: any) {
      const t = document.createElement("script");
      t.type = "text/javascript";
      t.async = !0;
      t.src = `https://cdn.segment.com/analytics.js/v1/${key}/analytics.min.js`;
      const n = document.getElementsByTagName("script")[0];
      n.parentNode?.insertBefore(t, n);
      analytics._loadOptions = e; // eslint-disable-line no-underscore-dangle
    };
    analytics._writeKey = window.SEGMENT_KEY; // eslint-disable-line no-underscore-dangle
    analytics.SNIPPET_VERSION = "4.15.3";
    analytics.load(window.SEGMENT_KEY);
    analytics.page();
  }
}

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
    background-color: ${palette.solid.white};
    color: ${palette.solid.darkgrey};
  }

  input, textarea {
    font-family: "Inter", sans-serif;
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
