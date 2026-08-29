/**
 * AR.IO Gateway
 * Copyright (C) 2022-2025 Permanent Data Solutions, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
import {
  ArweaveSigner,
  bundleAndSignData,
  createData,
} from '@dha-team/arbundles';
import Arweave from 'arweave';
import { strict as assert } from 'node:assert';
import { Readable } from 'node:stream';
import { describe, it } from 'node:test';
import winston from 'winston';

import { Ans104OffsetSource } from '../data/ans104-offset-source.js';
import { ContiguousDataSource, DataItemRootIndex } from '../types.js';
import { VerifiedDataItemRootIndex } from './verified-data-item-root-tx-index.js';

const log = winston.createLogger({ silent: true });

class MemoryDataSource implements ContiguousDataSource {
  constructor(private readonly roots: Map<string, Buffer>) {}

  async getData({
    id,
    region,
  }: Parameters<ContiguousDataSource['getData']>[0]) {
    const root = this.roots.get(id);
    if (root === undefined) {
      throw new Error(`Unknown root ${id}`);
    }

    const offset = region?.offset ?? 0;
    const size = region?.size ?? root.length - offset;
    if (offset < 0 || size < 0 || offset + size > root.length) {
      throw new Error('Requested region is outside the root data');
    }

    const data = root.subarray(offset, offset + size);
    return {
      stream: Readable.from([data]),
      size: data.length,
      verified: true,
      trusted: true,
      cached: false,
    };
  }
}

describe('VerifiedDataItemRootIndex', async () => {
  const wallet = await Arweave.init({}).wallets.generate();
  const signer = new ArweaveSigner(wallet);
  const dataItem = createData('verified payload', signer, {
    tags: [{ name: 'Content-Type', value: 'text/plain' }],
  });
  const bundle = await bundleAndSignData([dataItem], signer);
  const goodRootId = 'good-root';
  const badRootId = 'bad-root';
  const goodRoot = Buffer.from(bundle.getRaw());
  const badRoot = Buffer.from(goodRoot);
  badRoot[badRoot.length - 1] ^= 1;
  const offsetSource = new Ans104OffsetSource({
    log,
    dataSource: new MemoryDataSource(
      new Map([
        [goodRootId, goodRoot],
        [badRootId, badRoot],
      ]),
    ),
  });

  it('rejects a bad signature and accepts the next candidate', async () => {
    const candidates: DataItemRootIndex[] = [
      {
        async getRootTx() {
          return { rootTxId: badRootId, path: [badRootId] };
        },
      },
      {
        async getRootTx() {
          return { rootTxId: goodRootId, path: [goodRootId] };
        },
      },
    ];
    const index = new VerifiedDataItemRootIndex({
      log,
      candidates,
      offsetSource,
    });

    const result = await index.getRootTx(dataItem.id);

    assert.equal(result?.rootTxId, goodRootId);
    assert.equal(result?.dataSize, Buffer.byteLength('verified payload'));
    assert.equal(result?.contentType, 'text/plain');
  });

  it('returns no result when every candidate fails verification', async () => {
    const index = new VerifiedDataItemRootIndex({
      log,
      candidates: [
        {
          async getRootTx() {
            return { rootTxId: badRootId, path: [badRootId] };
          },
        },
      ],
      offsetSource,
    });

    assert.equal(await index.getRootTx(dataItem.id), undefined);
  });
});
