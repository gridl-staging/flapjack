import { describe, expect, it } from 'vitest';
import { formatSecuritySourceDescription } from './shared';

describe('formatSecuritySourceDescription', () => {
  it('returns fallback text when description is blank', () => {
    expect(formatSecuritySourceDescription('')).toBe('No description');
    expect(formatSecuritySourceDescription('   ')).toBe('No description');
  });

  it('returns the original description when it has content', () => {
    expect(formatSecuritySourceDescription('office network')).toBe('office network');
  });
});
