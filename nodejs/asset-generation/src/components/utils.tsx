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

import {
  letterMapKern,
  letterMapSingle,
  SIZE_BASIS,
} from "./characterWidths/PublicSans-Medium";

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

/**
 * Calculates width of given text based on a lookup table of relative character widths
 * Adapted from https://chrishewett.com/blog/calculating-text-width-programmatically/
 *
 * NOTE: Assumes you are using the typography.SansXX styles from the design system,
 * as this is the only typeface currently supported!
 */
export function computeTextWidth(text: string, fontSizePx: number) {
  // this will get mutated as we process the string
  let letterWidth = 0;
  // this will store all the intermediate values for later reference
  const cumulativeWidths = [];

  // split the string up using the spread operator (to handle UTF-8)
  const letterSplit = [...text];

  // this should match the design system for the most accurate result
  const letterSpacing = (fontSizePx < 18 ? -0.01 : -0.02) * SIZE_BASIS;

  // this syntax is fine, our node environment supports it
  // eslint-disable-next-line no-restricted-syntax
  for (const [key, letter] of letterSplit.entries()) {
    // add on the width of this letter to the sum
    letterWidth +=
      letterMapSingle.get(letter) || letterMapSingle.get("_median") || 0;

    if (key !== letterSplit.length - 1) {
      // add/remove the kerning modifier of this letter and the next one
      letterWidth += letterMapKern.get(`${letter}${letterSplit[key + 1]}`) || 0;
    }

    letterWidth += letterSpacing;

    // sizes must be scaled from the reference value to the desired font size
    cumulativeWidths.push(letterWidth * (fontSizePx / SIZE_BASIS));
  }

  const totalWidth = cumulativeWidths[cumulativeWidths.length - 1];
  return { totalWidth, cumulativeWidths };
}

/**
 * Regex to match the last occurrence of a word boundary in a string.
 * word boundaries here defined as spaces or hyphens
 */
const lastWordBoundary = /[\s-](?!.*[\s-])/g;

/**
 * Wraps the provided string to the provided width, given the specified font size.
 * Returns an array of strings representing separate lines.
 * 
 * NOTE: Assumes you are using the typography.SansXX styles from the design system,
 * as this is the only typeface currently supported!
 
 */
export function wrapText(
  text: string,
  width: number,
  fontSizePx: number
): string[] {
  const trimmed = text.trim();
  const { totalWidth, cumulativeWidths } = computeTextWidth(
    trimmed,
    fontSizePx
  );
  if (totalWidth <= width) return [text];

  // the point at which text overflows.
  // add 1 in case the last character is a word boundary
  const overflowIndex = cumulativeWidths.findIndex((v) => v > width);
  // the last word break before overflow;
  // add 1 to include the break character in the first group
  // (important if it's a hyphen, and will be trimmed if it's a space)
  const breakIndex =
    trimmed.substring(0, overflowIndex).search(lastWordBoundary) + 1;

  const firstLine = trimmed.substring(0, breakIndex).trim();

  return [
    firstLine,
    ...wrapText(trimmed.substring(breakIndex), width, fontSizePx).flat(),
  ];
}
