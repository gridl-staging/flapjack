export const SOURCE_REQUIRED_MESSAGE = 'Source is required.';

export function formatSecuritySourceDescription(description: string): string {
  return description.trim().length > 0 ? description : 'No description';
}
