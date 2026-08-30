/**
 * AR.IO Gateway
 * Copyright (C) 2022-2025 Permanent Data Solutions, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
import { LRUCache } from 'lru-cache';
import winston from 'winston';

import { Ans104OffsetSource } from '../data/ans104-offset-source.js';
import {
  DataItemRootIndex,
  GetRootTxOptions,
  RootTxLookupResult,
} from '../types.js';

export class VerifiedDataItemRootIndex implements DataItemRootIndex {
  private readonly log: winston.Logger;
  private readonly candidates: DataItemRootIndex[];
  private readonly offsetSource: Ans104OffsetSource;
  private readonly trustedSelfCandidate?: DataItemRootIndex;
  private readonly cache?: LRUCache<string, RootTxLookupResult>;

  constructor({
    log,
    candidates,
    offsetSource,
    trustedSelfCandidate,
    cache,
  }: {
    log: winston.Logger;
    candidates: DataItemRootIndex[];
    offsetSource: Ans104OffsetSource;
    trustedSelfCandidate?: DataItemRootIndex;
    cache?: LRUCache<string, RootTxLookupResult>;
  }) {
    this.log = log.child({ class: this.constructor.name });
    this.candidates = candidates;
    this.offsetSource = offsetSource;
    this.trustedSelfCandidate = trustedSelfCandidate;
    this.cache = cache;
  }

  async getRootTx(
    id: string,
    _opts?: GetRootTxOptions,
  ): Promise<RootTxLookupResult | undefined> {
    const cached = this.cache?.get(id);
    if (cached !== undefined) return cached;

    for (const [candidateIndex, candidateSource] of this.candidates.entries()) {
      try {
        const candidate = await candidateSource.getRootTx(id);
        if (candidate === undefined) continue;
        if (candidate.rootTxId === id) {
          if (candidateSource === this.trustedSelfCandidate) {
            return candidate;
          }
          continue;
        }

        const verified = await this.verifyCandidate(id, candidate);
        this.cache?.set(id, verified);
        return verified;
      } catch (error: unknown) {
        this.log.warn('Rejected root transaction candidate', {
          id,
          candidateIndex,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    return undefined;
  }

  private async verifyCandidate(
    id: string,
    candidate: RootTxLookupResult,
  ): Promise<RootTxLookupResult> {
    const { rootTxId, path } = candidate;
    if (path !== undefined && path[0] !== rootTxId) {
      throw new Error('Bundle path does not start at the candidate root');
    }

    if (path !== undefined) {
      for (let index = 1; index < path.length; index++) {
        const parentId = path[index];
        const parentOffset = await this.offsetSource.getDataItemOffsetWithPath(
          parentId,
          path.slice(0, index),
        );
        if (parentOffset === null) {
          throw new Error(`Bundle path item ${parentId} was not found`);
        }
        await this.offsetSource.verifyDataItem({
          dataItemId: parentId,
          rootBundleId: rootTxId,
          itemOffset: parentOffset.itemOffset,
          itemSize: parentOffset.itemSize,
        });
      }
    }

    const offset =
      candidate.rootDataOffset !== undefined
        ? await this.offsetSource.getDataItemByOffset(
            id,
            rootTxId,
            candidate.rootDataOffset,
          )
        : path !== undefined && path.length > 0
          ? await this.offsetSource.getDataItemOffsetWithPath(id, path)
          : await this.offsetSource.getDataItemOffset(id, rootTxId);
    if (offset === null) {
      throw new Error('Data item was not found in the candidate root');
    }

    const item = await this.offsetSource.verifyDataItem({
      dataItemId: id,
      rootBundleId: rootTxId,
      itemOffset: offset.itemOffset,
      itemSize: offset.itemSize,
    });
    if (
      item.dataOffset !== offset.dataOffset ||
      item.dataSize !== offset.dataSize
    ) {
      throw new Error('Verified data item offsets do not match bundle headers');
    }

    return {
      rootTxId,
      path,
      rootOffset: item.offset,
      rootDataOffset: item.dataOffset,
      size: item.size,
      dataSize: item.dataSize,
      contentType: item.tags.find(
        (tag) => tag.name.toLowerCase() === 'content-type',
      )?.value,
    };
  }
}
