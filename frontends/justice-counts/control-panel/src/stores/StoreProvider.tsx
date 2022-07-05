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

import React, { useContext } from "react";

import NoAuthConfigErrorPage from "../components/Error/NoAuthConfigErrorPage";
import rootStore from "./RootStore";

const StoreContext = React.createContext<typeof rootStore | undefined>(
  undefined
);

export const StoreProvider: React.FC<React.ReactNode> = ({
  children,
}): React.ReactElement => {
  if (window.APP_CONFIG) {
    return (
      <StoreContext.Provider value={rootStore}>
        {children}
      </StoreContext.Provider>
    );
  }
  return <NoAuthConfigErrorPage />;
};

export function useStore(): typeof rootStore {
  const context = useContext(StoreContext);

  if (context === undefined) {
    throw new Error("useStore must be used within a StoreProvider");
  }

  return context;
}
