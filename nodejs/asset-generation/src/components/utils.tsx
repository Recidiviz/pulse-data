// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

import { ComponentType } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ServerStyleSheet } from "styled-components";

/**
 * Given a string containing CSS rules, identifies any rem units and converts them to px,
 * using 1rem=16px by default
 */
export function convertRemToPx(styles: string, remSize = 16) {
  return styles.replace(
    /(\d*\.?\d+)rem/g,
    (match, num) => `${num * remSize}px`
  );
}

/**
 * Renders the given component (which must render a root <svg>) to a markup string.
 * Any styles collected from Styled Components within the component tree will be injected
 * into the markup as a <style> tag.
 */
export function renderToStaticSvg(Cmp: ComponentType): string {
  const sheet = new ServerStyleSheet();
  // render the component to collect styles from Styled Components
  const svgString = renderToStaticMarkup(sheet.collectStyles(<Cmp />));

  // verify that this is an SVG before proceeding
  const openingTagPattern = /^<svg.*?>/;

  let newOpeningTag = svgString.match(openingTagPattern)?.[0];

  if (!newOpeningTag) {
    throw new Error("Expected component to render an SVG");
  }

  // make sure the namespace is included for proper display
  const namespaceAttr = `xmlns="http://www.w3.org/2000/svg"`;
  if (!newOpeningTag.includes(namespaceAttr)) {
    newOpeningTag = newOpeningTag.replace(/^<svg/, `$& ${namespaceAttr}`);
  }

  // inject the styles into the final output
  newOpeningTag += `<defs>${
    // most of our design-system styles use rem units but these are not supported by the image library
    convertRemToPx(sheet.getStyleTags())
  }</defs>`;

  return svgString.replace(openingTagPattern, newOpeningTag);
}
