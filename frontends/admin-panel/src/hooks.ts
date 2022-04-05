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
import { message } from "antd";
import * as React from "react";

interface FetchedDataResponse<T> {
  loading: boolean;
  data?: T;
  setData: React.Dispatch<React.SetStateAction<T | undefined>>;
}

function useFetchedData<T>(
  fetchReq: () => Promise<Response>,
  responseHandler: (r: Response) => Promise<T>
): FetchedDataResponse<T> {
  const [loading, setLoading] = React.useState<boolean>(true);
  const [data, setData] = React.useState<T>();

  React.useEffect(() => {
    const fetchData = async (req: () => Promise<Response>): Promise<void> => {
      const r = await req();
      if (r.status >= 400) {
        const text = await r.text();
        message.error(`Error loading data: ${text}`);
        setLoading(false);
        return;
      }
      setData(await responseHandler(r));
      setLoading(false);
    };
    fetchData(fetchReq);
  }, [fetchReq]);

  return { loading, data, setData };
}

export const useFetchedDataJSON = <T>(
  fetchReq: () => Promise<Response>
): FetchedDataResponse<T> => {
  return useFetchedData(fetchReq, (r) => r.json());
};

export const useFetchedDataProtobuf = <T>(
  fetchReq: () => Promise<Response>,
  parseFunction: (data: Uint8Array) => T
): FetchedDataResponse<T> => {
  return useFetchedData(fetchReq, async (r: Response) => {
    return parseFunction(new Uint8Array(await r.arrayBuffer()));
  });
};
