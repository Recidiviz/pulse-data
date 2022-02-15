import { Assets, Header } from "@recidiviz/design-system";
import React from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";

import AuthWall from "./AuthWall";
import GlobalStyles from "./GlobalStyles";
import Logout from "./Logout";
import CompliantReporting from "./pages/CompliantReporting";
import StoreProvider from "./StoreProvider";

const App: React.FC = () => {
  return (
    <BrowserRouter>
      <StoreProvider>
        <GlobalStyles />
        <AuthWall>
          <Header
            left={<img src={Assets.LOGO} alt="Recidiviz" />}
            right={<Logout />}
          />
          <Routes>
            <Route path="/" element={<CompliantReporting />} />
          </Routes>
        </AuthWall>
      </StoreProvider>
    </BrowserRouter>
  );
};

export default App;
