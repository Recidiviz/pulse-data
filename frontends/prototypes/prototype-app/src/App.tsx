import { Assets, Header } from "@recidiviz/design-system";
import React from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";

import AuthWall from "./AuthWall";
import GlobalStyles from "./GlobalStyles";
import Logout from "./Logout";
import CompliantReporting from "./pages/CompliantReporting";
import StoreProvider from "./StoreProvider";
import UpcomingDischarge from "./pages/UpcomingDischarge";

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
            <Route path="/discharge" element={<UpcomingDischarge />} />
          </Routes>
        </AuthWall>
      </StoreProvider>
    </BrowserRouter>
  );
};

export default App;
