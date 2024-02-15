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

import { Breadcrumb, Layout } from "antd";
import { FC, ReactNode } from "react";
import { useHistory, useLocation } from "react-router-dom";

import { StateCodeInfo } from "../general/constants";
import StateSelectorPageHeader from "../general/StateSelectorPageHeader";

interface DatasetsHeaderProps {
  title: string;
  stateCode: string | null;
  breadCrumbLinks: ReactNode[];
  backAction?: () => void;
}

const DatasetsHeader: FC<DatasetsHeaderProps> = ({
  title,
  stateCode,
  breadCrumbLinks,
  backAction,
}) => {
  const history = useHistory();
  const { search } = useLocation();

  const queryParams = new URLSearchParams(search);

  const stateCodeChange = (value: StateCodeInfo) => {
    queryParams.set("stateCode", value.code);
    history.push({ search: queryParams.toString() });
  };

  return (
    <>
      <StateSelectorPageHeader
        title={title}
        stateCode={stateCode}
        onChange={stateCodeChange}
        onBack={backAction}
      />
      <Layout className="content-side-padding">
        <Breadcrumb>
          {breadCrumbLinks.map((x) => (
            <Breadcrumb.Item key={x?.toString()}>{x}</Breadcrumb.Item>
          ))}
        </Breadcrumb>
      </Layout>
    </>
  );
};

export default DatasetsHeader;
