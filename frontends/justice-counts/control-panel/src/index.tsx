import { fonts, palette } from "@recidiviz/design-system";
import React from "react";
import ReactDOM from "react-dom";
import { createGlobalStyle } from "styled-components/macro";

import App from "./App";
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
    font-family: ${fonts.sans};
    background-color: ${palette.marble2};
    color: ${palette.text.normal};
  }
`;

ReactDOM.render(
  <React.StrictMode>
    <StoreProvider>
      <GlobalStyle />
      <App />
    </StoreProvider>
  </React.StrictMode>,
  document.getElementById("root")
);
