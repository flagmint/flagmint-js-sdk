import { FeatureFlags } from '../../core/helpers/types';
import type { RulesCacheSnapshot } from '../../core/config-sync/types';
export interface AsyncStorageAdapter {
    getItem(key: string): Promise<string | null>;
    setItem(key: string, value: string): Promise<void>;
    removeItem?(key: string): Promise<void>;
}
export declare function setAsyncCacheStorage(adapter: AsyncStorageAdapter): void;
export declare function loadCachedFlags<T>(apiKey: string, ttl: number): Promise<FeatureFlags<T> | null>;
export declare function saveCachedFlags<T>(apiKey: string, data: FeatureFlags<T>): Promise<void>;
/**
 * Drop the evaluated-flag localCache entry for an API key (async adapter).
 * Prefer `removeItem`; if the adapter omits it, overwrite with an immediately
 * expired empty blob so {@link loadCachedFlags} returns null.
 *
 * @param apiKey SDK API key used as the storage key suffix
 */
export declare function clearCachedFlags(apiKey: string): Promise<void>;
export declare function loadCachedContext<C>(apiKey: string): Promise<C | null>;
export declare function saveCachedContext<C>(apiKey: string, context: C): Promise<void>;
export declare function loadCachedRulesSnapshot(apiKey: string): Promise<RulesCacheSnapshot | null>;
export declare function saveCachedRulesSnapshot(apiKey: string, snapshot: RulesCacheSnapshot): Promise<void>;
export declare function clearCachedRulesSnapshot(apiKey: string): Promise<void>;
