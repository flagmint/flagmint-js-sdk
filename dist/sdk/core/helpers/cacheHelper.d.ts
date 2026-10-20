import { FeatureFlags } from '../../core/helpers/types';
import type { RulesCacheSnapshot } from '../../core/config-sync/types';
export interface StorageAdapter {
    getItem(key: string): string | null;
    setItem(key: string, value: string): void;
    removeItem?(key: string): void;
}
export declare function setCacheStorage(customStorage: StorageAdapter): void;
export declare function loadCachedFlags<T>(apiKey: string, ttl: number): FeatureFlags<T> | null;
export declare function saveCachedFlags<T>(apiKey: string, data: FeatureFlags<T>): void;
/**
 * Drop the evaluated-flag localCache entry for an API key.
 * Used when config-sync is on so a prior tab’s answers cannot poison another tab.
 * Prefer `removeItem`; if the adapter omits it, overwrite with an immediately
 * expired empty blob so {@link loadCachedFlags} returns null.
 *
 * @param apiKey SDK API key used as the storage key suffix
 */
export declare function clearCachedFlags(apiKey: string): void;
export declare function loadCachedContext<C>(apiKey: string): C | null;
export declare function saveCachedContext<C>(apiKey: string, context: C): void;
/** Config-sync rules localCache — no wall-clock TTL; lease `expiresAt` gates reuse. */
export declare function loadCachedRulesSnapshot(apiKey: string): RulesCacheSnapshot | null;
export declare function saveCachedRulesSnapshot(apiKey: string, snapshot: RulesCacheSnapshot): void;
export declare function clearCachedRulesSnapshot(apiKey: string): void;
