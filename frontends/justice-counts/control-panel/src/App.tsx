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

import { observer } from "mobx-react-lite";
import React, { ReactElement, useEffect } from "react";
import { Route, Routes, useLocation } from "react-router-dom";

import { trackNavigation } from "./analytics";
import { PageWrapper } from "./components/Forms";
import Header from "./components/Header";
import CreateReports from "./components/Reports/CreateReport";
import ReportDataEntry from "./components/Reports/ReportDataEntry";
import AccountSettings from "./pages/AccountSettings";
import Reports from "./pages/Reports";

const App: React.FC = (): ReactElement => {
  const location = useLocation();
  useEffect(() => {
    trackNavigation(location.pathname + location.search);
  }, [location]);

  return (
    <>
      <PageWrapper>
        <Header />

        <Routes>
          <Route path="/" element={<Reports />} />
          <Route path="/reports/create" element={<CreateReports />} />
          <Route path="/reports/:id" element={<ReportDataEntry />} />
          <Route path="/settings" element={<AccountSettings />} />
        </Routes>
      </PageWrapper>
    </>
  );
};

export default observer(App);
