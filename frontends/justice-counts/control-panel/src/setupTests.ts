// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import "@testing-library/jest-dom";

import { enableFetchMocks } from "jest-fetch-mock";

enableFetchMocks();

window.APP_CONFIG = {
  domain: "",
  client_id: "",
  audience: "",
};

// polyfill for when running jest tests
/* eslint-disable no-extend-native */
if (typeof String.prototype.replaceAll === "undefined") {
  String.prototype.replaceAll = function (match, replace) {
    return this.replace(new RegExp(match, "g"), () => replace as string);
  };
}
/* eslint-enable no-extend-native */
