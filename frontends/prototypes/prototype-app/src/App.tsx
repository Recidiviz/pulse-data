import { Assets, Header } from "@recidiviz/design-system";
import React from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";

import AuthWall from "./AuthWall";
import GlobalStyles from "./GlobalStyles";
import Logout from "./Logout";
import StoreProvider from "./StoreProvider";

const Home: React.FC = () => {
  return (
    <div className="App">
      <Header
        left={<img src={Assets.LOGO} alt="Recidiviz" />}
        right={<Logout />}
      />
    </div>
  );
};

const App: React.FC = () => {
  return (
    <BrowserRouter>
      <StoreProvider>
        <GlobalStyles />
        <AuthWall>
          <Routes>
            <Route path="" element={<Home />} />
          </Routes>
        </AuthWall>
      </StoreProvider>
    </BrowserRouter>
  );
};

export default App;
