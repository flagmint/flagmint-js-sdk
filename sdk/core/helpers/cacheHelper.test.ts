import {
  clearCachedFlags,
  clearCachedRulesSnapshot,
  loadCachedFlags,
  loadCachedRulesSnapshot,
  saveCachedFlags,
  saveCachedRulesSnapshot,
  setCacheStorage,
} from './cacheHelper';
import type { RulesCacheSnapshot } from '@/core/config-sync/types';

describe('evaluated flags localCache helpers', () => {
  const mem = new Map<string, string>();

  beforeEach(() => {
    mem.clear();
    setCacheStorage({
      getItem: (key) => mem.get(key) ?? null,
      setItem: (key, value) => {
        mem.set(key, value);
      },
      removeItem: (key) => {
        mem.delete(key);
      },
    });
  });

  it('clears the evaluated flags entry', () => {
    saveCachedFlags('ff_test', { demo: true });
    expect(loadCachedFlags('ff_test', 60_000)).toEqual({ demo: true });
    clearCachedFlags('ff_test');
    expect(loadCachedFlags('ff_test', 60_000)).toBeNull();
  });

  it('invalidates evaluated flags when the adapter has no removeItem', () => {
    setCacheStorage({
      getItem: (key) => mem.get(key) ?? null,
      setItem: (key, value) => {
        mem.set(key, value);
      },
    });
    saveCachedFlags('ff_test', { demo: true });
    expect(loadCachedFlags('ff_test', 60_000)).toEqual({ demo: true });
    clearCachedFlags('ff_test');
    expect(loadCachedFlags('ff_test', 60_000)).toBeNull();
  });
});

describe('rules localCache helpers', () => {
  const mem = new Map<string, string>();

  beforeEach(() => {
    mem.clear();
    setCacheStorage({
      getItem: (key) => mem.get(key) ?? null,
      setItem: (key, value) => {
        mem.set(key, value);
      },
      removeItem: (key) => {
        mem.delete(key);
      },
    });
  });

  it('round-trips a rules snapshot', () => {
    const snapshot: RulesCacheSnapshot = {
      version: 7,
      expiresAt: Date.now() + 60_000,
      flags: [
        {
          key: 'demo',
          type: 'boolean',
          is_active: true,
          default_value: false,
          targeting_rules: [],
          variations: [{ id: 'on', value: true }],
          rollouts: {},
          analytics_enabled: true,
        },
      ],
      segments: { s1: { id: 's1', rules: [] } },
    };
    saveCachedRulesSnapshot('ff_test', snapshot);
    expect(loadCachedRulesSnapshot('ff_test')).toEqual(snapshot);
  });

  it('round-trips PII wire fields on a rules snapshot', () => {
    const snapshot: RulesCacheSnapshot = {
      version: 8,
      expiresAt: Date.now() + 60_000,
      flags: [
        {
          key: 'demo',
          type: 'boolean',
          is_active: true,
          default_value: false,
          targeting_rules: [],
          variations: [{ id: 'on', value: true }],
          rollouts: {},
          analytics_enabled: true,
        },
      ],
      segments: {},
      piiAttributes: ['email'],
      piiSalt: 'client-salt',
      piiValueEncoding: 'hmac-v1',
    };
    saveCachedRulesSnapshot('ff_test', snapshot);
    expect(loadCachedRulesSnapshot('ff_test')).toEqual(snapshot);
  });

  it('preserves piiWireBroken on load', () => {
    const snapshot: RulesCacheSnapshot = {
      version: 9,
      expiresAt: Date.now() + 60_000,
      flags: [
        {
          key: 'demo',
          type: 'boolean',
          is_active: true,
          default_value: false,
          targeting_rules: [],
          variations: [],
          rollouts: {},
          analytics_enabled: false,
        },
      ],
      segments: {},
      piiWireBroken: true,
    };
    saveCachedRulesSnapshot('ff_test', snapshot);
    expect(loadCachedRulesSnapshot('ff_test')).toMatchObject({
      piiWireBroken: true,
    });
  });

  it('rejects malformed snapshots', () => {
    mem.set('flagmint_ff_test_rules', JSON.stringify({ version: 1 }));
    expect(loadCachedRulesSnapshot('ff_test')).toBeNull();
  });

  it('clears the rules snapshot', () => {
    saveCachedRulesSnapshot('ff_test', {
      version: 1,
      expiresAt: Date.now() + 1,
      flags: [
        {
          key: 'a',
          type: 'boolean',
          is_active: true,
          default_value: true,
          targeting_rules: [],
          variations: [],
          rollouts: {},
          analytics_enabled: false,
        },
      ],
      segments: {},
    });
    clearCachedRulesSnapshot('ff_test');
    expect(loadCachedRulesSnapshot('ff_test')).toBeNull();
  });
});
