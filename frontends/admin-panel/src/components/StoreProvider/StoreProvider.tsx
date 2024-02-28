// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { observer } from "mobx-react-lite";
import PropTypes from "prop-types";
import React, { useContext } from "react";

import { InsightsStore } from "../../InsightsStore/InsightsStore";

const StoreContext = React.createContext<undefined | InsightsStore>(undefined);

const StoreProvider: React.FC = observer(function StoreProvider({ children }) {
  return (
    <StoreContext.Provider value={new InsightsStore()}>
      {children}
    </StoreContext.Provider>
  );
});

StoreProvider.propTypes = {
  children: PropTypes.node.isRequired,
};

export default StoreProvider;

export function useInsightsStore(): InsightsStore {
  const context = useContext(StoreContext);
  if (context === undefined) {
    throw new Error("useInsightsStore must be used within a StoreProvider");
  }
  return context;
}
