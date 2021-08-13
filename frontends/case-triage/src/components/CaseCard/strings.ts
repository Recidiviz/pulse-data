import {
  parseContactFrequency,
  SupervisionContactFrequency,
} from "../../stores/PolicyStore/Policy";

export const getContactFrequencyText = (
  contactFrequency: SupervisionContactFrequency | undefined
): string | null => {
  if (!contactFrequency) {
    return null;
  }

  const [contacts, days] = parseContactFrequency(contactFrequency);
  return `Policy: ${contacts} every ${days}`;
};
