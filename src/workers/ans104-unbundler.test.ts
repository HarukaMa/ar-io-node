/**
 * AR.IO Gateway
 * Copyright (C) 2022-2025 Permanent Data Solutions, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
import { strict as assert } from 'node:assert';
import { describe, it, beforeEach, mock, afterEach } from 'node:test';
import { Ans104Unbundler, UnbundleableItem } from './ans104-unbundler.js';
import { NormalizedBundleDataItem } from '../types.js';
import { EventEmitter } from 'node:events';
import { createTestLogger } from '../../test/test-logger.js';
import * as metrics from '../metrics.js';

async function getSkipValue(reason: string): Promise<number> {
  const out = await metrics.bundlesUnbundleSkippedCounter.get();
  const sample = out.values.find((v) => v.labels.reason === reason);
  return sample?.value ?? 0;
}

describe('Ans104Unbundler', () => {
  let log: ReturnType<typeof createTestLogger>;
  let eventEmitter: EventEmitter;
  let filter: any;
  let contiguousDataSource: any;
  let ans104Unbundler: Ans104Unbundler;
  let shouldUnbundleMock: any;
  let mockAns104Parser: any;

  beforeEach(() => {
    log = createTestLogger({ suite: 'Ans104Unbundler' });
    eventEmitter = new EventEmitter();
    filter = { match: () => true };
    contiguousDataSource = {};
    shouldUnbundleMock = mock.fn(() => true);
    mockAns104Parser = {
      stop: mock.fn(),
      parseBundle: () => Promise.resolve(),
    };

    ans104Unbundler = new Ans104Unbundler({
      log,
      eventEmitter,
      filter,
      contiguousDataSource,
      dataItemIndexFilterString: '',
      workerCount: 1,
      maxQueueSize: 2,
      shouldUnbundle: shouldUnbundleMock,
      ans104Parser: mockAns104Parser,
    });
  });

  afterEach(async () => {
    await ans104Unbundler.stop();
    mock.restoreAll();
  });

  describe('queueItem', () => {
    const mockItem = {
      id: 'test-id',
    } as UnbundleableItem;

    it('should not queue item when shouldUnbundle returns false', async () => {
      shouldUnbundleMock.mock.mockImplementation(() => false);
      const before = await getSkipValue('high_queue_depth');

      for (let i = 0; i < 10; i++) {
        ans104Unbundler.queueItem(mockItem, false);
      }

      assert.equal(shouldUnbundleMock.mock.calls.length, 10);
      assert.equal(ans104Unbundler.queueDepth(), 0);
      assert.equal(await getSkipValue('high_queue_depth'), before + 10);
    });

    it('returns whether the item was queued', () => {
      shouldUnbundleMock.mock.mockImplementation(() => false);
      assert.equal(ans104Unbundler.queueItem(mockItem, false), false);
      shouldUnbundleMock.mock.mockImplementation(() => true);
      assert.equal(ans104Unbundler.queueItem(mockItem, false), true);
    });

    it('should queue item when shouldUnbundle returns true', async () => {
      const before = await getSkipValue('queue_full');
      for (let i = 0; i < 10; i++) {
        ans104Unbundler.queueItem(mockItem, false);
      }

      assert.equal(shouldUnbundleMock.mock.calls.length, 10);
      assert.equal(ans104Unbundler.queueDepth(), 2);
      // First push pulls a worker immediately (fastq increments _running
      // and calls the worker synchronously when below concurrency), so
      // queue length stays 0 after that push. The next two pushes land
      // in the queue, bringing queueDepth to maxQueueSize=2. The
      // remaining 7 hit the queue_full skip branch.
      assert.equal(await getSkipValue('queue_full'), before + 7);
    });

    it('should queue prioritized item even when shouldUnbundle returns false', async () => {
      shouldUnbundleMock.mock.mockImplementation(() => false);

      for (let i = 0; i < 10; i++) {
        ans104Unbundler.queueItem(mockItem, true);
      }

      assert.equal(shouldUnbundleMock.mock.calls.length, 10);
      assert.equal(ans104Unbundler.queueDepth(), 9);
    });

    it('should not call shouldUnbundle when workerCount is 0', async () => {
      ans104Unbundler['workerCount'] = 0;
      const before = await getSkipValue('no_workers');

      for (let i = 0; i < 10; i++) {
        ans104Unbundler.queueItem(mockItem, false);
      }

      assert.equal(shouldUnbundleMock.mock.calls.length, 0);
      assert.equal(ans104Unbundler.queueDepth(), 0);
      assert.equal(await getSkipValue('no_workers'), before + 10);
    });

    it("should parse bundle even if filter doesn't match if bypassFilter is true", async () => {
      mock.method(mockAns104Parser, 'parseBundle');
      (ans104Unbundler as any).filter = { match: () => false };

      const mockItem = {
        id: 'test-id',
        root_tx_id: 'root_tx_id',
      } as UnbundleableItem;

      await ans104Unbundler.queueItem(mockItem, false, true);

      assert.deepEqual(
        (mockAns104Parser.parseBundle as any).mock.calls[0].arguments[0],
        {
          parentId: 'test-id',
          parentIndex: undefined,
          rootParentOffset: 0,
          rootTxId: 'root_tx_id',
        },
      );
    });

    it('passes the verified nested root range to the parser', async () => {
      const nestedItem = {
        anchor: '',
        data_hash: null,
        data_offset: 250,
        data_size: 400,
        id: 'nested-id',
        index: 1,
        offset: 200,
        owner: '',
        owner_address: '',
        owner_offset: 2,
        owner_size: 32,
        parent_id: 'parent-id',
        parent_index: 0,
        root_parent_offset: 1000,
        root_tx_id: 'root-id',
        signature: null,
        signature_offset: 2,
        signature_size: 64,
        signature_type: 1,
        size: 450,
        tags: [],
        target: '',
      } satisfies NormalizedBundleDataItem;
      let parseArgs:
        | {
            rootTxId: string;
            parentId: string;
            parentIndex: number;
            rootParentOffset: number;
            parentSize?: number;
          }
        | undefined;
      mockAns104Parser.parseBundle = (args: NonNullable<typeof parseArgs>) => {
        parseArgs = args;
        return Promise.resolve();
      };

      await ans104Unbundler.unbundle({
        item: nestedItem,
        bypassFilter: true,
      });

      assert.deepEqual(parseArgs, {
        rootTxId: 'root-id',
        parentId: 'nested-id',
        parentIndex: 1,
        rootParentOffset: 1250,
        parentSize: 400,
      });
    });
  });
});
