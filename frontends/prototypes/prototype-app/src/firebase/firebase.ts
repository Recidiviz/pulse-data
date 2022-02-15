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

import { initializeApp } from "firebase/app";
import { getAuth, signInWithCustomToken } from "firebase/auth";

const firebaseConfig = {
  projectId: "recidiviz-prototypes",
  apiKey: "AIzaSyChvCNsdiXuTjTucRcez74IlyQeB_Y5UKk",
  authDomain: "recidiviz-prototypes.firebaseapp.com",
  storageBucket: "recidiviz-prototypes.appspot.com",
  messagingSenderId: "776749136174",
  appId: "1:776749136174:web:a45fe1f54aedf0f81762bf",
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);

const tokenExchangeEndpoint = import.meta.env.DEV
  ? // requires local function emulator to be running
    `http://localhost:5001/${firebaseConfig.projectId}/us-central1/getFirebaseToken`
  : `https://us-central1-${firebaseConfig.projectId}.cloudfunctions.net/getFirebaseToken`;
export const authenticate = async (
  auth0Token: string
): ReturnType<typeof signInWithCustomToken> => {
  const tokenExchangeResponse = await fetch(tokenExchangeEndpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${auth0Token}`,
    },
  });

  const { firebaseToken } = await tokenExchangeResponse.json();
  const auth = getAuth(app);
  return signInWithCustomToken(auth, firebaseToken);
};
