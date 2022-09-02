export function optionalStringSort(
  a: string | null | undefined,
  b: string | null | undefined
): number {
  return a?.localeCompare(b || "") || -1;
}

export function optionalNumberSort(
  a: number | null | undefined,
  b: number | null | undefined
): number {
  return (a || 0) - (b || 0);
}

export function booleanSort(a: boolean, b: boolean): number {
  const aNumber = a ? 1 : 0;
  const bNumber = b ? 1 : 0;
  return aNumber - bNumber;
}

export function optionalBooleanSort(
  a: boolean | null | undefined,
  b: boolean | null | undefined
): number {
  const aIsBoolean = typeof a === "boolean";
  const bIsBoolean = typeof b === "boolean";
  return aIsBoolean && bIsBoolean
    ? booleanSort(a as boolean, b as boolean)
    : booleanSort(aIsBoolean, bIsBoolean);
}

export function getUniqueValues<T>(values: T[]): T[] {
  return Array.from(new Set(values));
}
