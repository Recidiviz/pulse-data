// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { postWithURLAndBody } from "./utils";

export const fetchRegionCodeFiles = async (
  regionCode: string
): Promise<Response> => {
  return postWithURLAndBody("/data_discovery/files", {
    region_code: regionCode,
  });
};

export interface Message {
  cursor: number;
  data: string;
  kind: string;
}

export const fetchDiscoveryStatus = async (
  discoveryId: string,
  messageCursor: number
): Promise<Response> => {
  return postWithURLAndBody("/data_discovery/discovery_status", {
    message_cursor: messageCursor,
    discovery_id: discoveryId,
  });
};

export const pollDiscoveryStatus = async (
  discoveryId: string,
  latestMessage: Message | null,
  onMessageReceived: CallableFunction
): Promise<void> => {
  const response = await fetchDiscoveryStatus(
    discoveryId,
    latestMessage ? latestMessage.cursor : 0
  );

  if (response.status === 200) {
    // Get and show the message
    const receivedMessage = await response.json();

    onMessageReceived(receivedMessage);

    if (receivedMessage.kind === "close") {
      return;
    }

    await pollDiscoveryStatus(discoveryId, receivedMessage, onMessageReceived);
  }

  if (response.status === 500) {
    onMessageReceived(null);
  }
};

export const createDiscovery = async (
  body: Record<string, unknown>
): Promise<Response> => {
  return postWithURLAndBody("/data_discovery/create_discovery", body);
};
