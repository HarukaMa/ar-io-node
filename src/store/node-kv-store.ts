/**
 * AR.IO Gateway
 * Copyright (C) 2022-2025 Permanent Data Solutions, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
import { LRUCache } from 'lru-cache';
import { KVBufferStore } from '../types.js';

export class NodeKvStore implements KVBufferStore {
  private cache: LRUCache<string, Buffer>;

  constructor({
    ttlSeconds,
    maxKeys,
  }: {
    ttlSeconds: number;
    maxKeys: number;
  }) {
    this.cache = new LRUCache({
      max: maxKeys,
      ttl: ttlSeconds * 1000,
    });
  }

  async get(key: string): Promise<Buffer | undefined> {
    const value = this.cache.get(key);
    if (value === undefined) {
      return undefined;
    }
    return value as Buffer;
  }

  async set(key: string, buffer: Buffer): Promise<void> {
    this.cache.set(key, buffer);
  }

  async del(key: string): Promise<void> {
    this.cache.delete(key);
  }

  async has(key: string): Promise<boolean> {
    return this.cache.has(key);
  }

  async close(): Promise<void> {
    this.cache.clear();
  }
}
