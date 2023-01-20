import { StrictMode } from "react";
import ReactDOM from "react-dom";
import ReactModal from "react-modal";

import App from "./App";

ReactDOM.render(
  <StrictMode>
    <App />
  </StrictMode>,
  document.getElementById("root"),
  () => {
    ReactModal.setAppElement("#root");
  }
);
