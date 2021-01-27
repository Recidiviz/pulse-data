// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
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
import * as React from "react";
import { useEffect, useState } from "react";
import * as ReactDOM from "react-dom";
import { BrowserRouter } from "react-router-dom";
import { Auth0Provider, useAuth0 } from "@auth0/auth0-react";

const ClientList: React.FC = () => {
  const {
    getAccessTokenSilently,
    isAuthenticated,
    isLoading,
    user,
  } = useAuth0();
  const [data, setData] = useState({});

  useEffect(() => {
    const callSecureApi = async (url: string) => {
      try {
        const token = await getAccessTokenSilently({
          audience: "https://case-triage.recidiviz.org/api",
          scope: "email",
        });

        const response = await fetch(url, {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        });

        const responseData = await response.json();
        setData(responseData);
      } catch (error) {
        setData(error);
      }
    };

    if (!isLoading) {
      callSecureApi("/api/clients");
    }
  }, [isLoading, isAuthenticated, getAccessTokenSilently]);

  return (
    <div>
      <h1>User</h1>
      <h3>
        {isLoading ? "Loading..." : (user && user.name) || "Unauthenticated"}
      </h3>
      <h1>Protected API</h1>
      <pre>{data ? JSON.stringify(data, null, 4) : "Loading..."}</pre>
    </div>
  );
};

const LoginButton: React.FC = () => {
  const { loginWithRedirect, logout, isAuthenticated } = useAuth0();

  if (isAuthenticated) {
    return (
      <button type="button" onClick={() => logout()}>
        Log Out
      </button>
    );
  }

  return (
    <button type="button" onClick={() => loginWithRedirect()}>
      Log In
    </button>
  );
};

export default LoginButton;

ReactDOM.render(
  <Auth0Provider
    domain={window.AUTH0_CONFIG.domain}
    clientId={window.AUTH0_CONFIG.clientId}
    redirectUri={window.location.origin}
    audience={window.AUTH0_CONFIG.audience}
  >
    <BrowserRouter>
      <LoginButton />
      <ClientList />
    </BrowserRouter>
  </Auth0Provider>,
  document.getElementById("root")
);
