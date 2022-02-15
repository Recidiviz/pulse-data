import * as functions from "firebase-functions";
import getFirebaseTokenFn from "./getFirebaseToken";

// // Start writing Firebase Functions
// // https://firebase.google.com/docs/functions/typescript

export const getFirebaseToken = functions.https.onRequest(getFirebaseTokenFn);
