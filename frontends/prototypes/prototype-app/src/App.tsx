import { Assets, Header } from "@recidiviz/design-system";
import { FC } from "react";
import { BrowserRouter, Route, Routes } from "react-router-dom";

import AuthWall from "./AuthWall";
import GlobalStyles from "./GlobalStyles";
import Logout from "./Logout";
import StoreProvider from "./StoreProvider";

const App: FC = () => {
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
            <Route
              path="/"
              element={
                <div style={{ textAlign: "center" }}>
                  This page intentionally left blank
                </div>
              }
            />
          </Routes>
        </AuthWall>
      </StoreProvider>
    </BrowserRouter>
  );
};

export default App;
