import * as express from 'express';
import { addAsync, RouterWithAsync } from '@awaitjs/express';
import { DataStore, DbBlock } from '../../../datastore/common';
import {
  NetworkIdentifier,
  RosettaAccount,
  RosettaBlockIdentifier,
  RosettaAccountBalanceResponse,
} from '@blockstack/stacks-blockchain-api-types';
import { RosettaErrors, RosettaConstants } from '../../rosetta-constants';
import { has0xPrefix, FoundOrNot } from '../../../helpers';
import { rosettaValidateRequest, ValidSchema, makeRosettaError } from '../../rosetta-validate';

async function getBalance(db: DataStore, address: string, blockHeight?: number): Promise<string> {
  let result;
  if (blockHeight) {
    result = await db.getStxBalanceAtBlock(address, blockHeight);
  } else {
    result = await db.getStxBalance(address);
  }

  return result.balance.toString();
}

export function createRosettaAccountRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());
  router.use(express.json());

  router.postAsync('/balance', async (req, res) => {
    const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    if (!valid.valid) {
      res.status(400).json(makeRosettaError(valid));
      return;
    }

    const accountIdentifier: RosettaAccount = req.body.account_identifier;
    const blockIdentifier: RosettaBlockIdentifier = req.body.block_identifier;
    let balance: string = '';
    let index: number = 0;
    let hash: string = '';

    if (blockIdentifier == null) {
      balance = await getBalance(db, accountIdentifier.address);
      const block: FoundOrNot<DbBlock> = await db.getCurrentBlock();
      if (block.found) {
        index = block.result.block_height;
        hash = block.result.block_hash;
      } else {
        res.status(400).json(RosettaErrors.blockNotFound);
      }
    } else if (blockIdentifier.index) {
      balance = await getBalance(db, accountIdentifier.address, blockIdentifier.index);
      index = blockIdentifier.index;
      const block = await db.getBlockByHeight(index);
      if (block.found) {
        hash = block.result.block_hash;
      } else {
        res.status(400).json(RosettaErrors.blockNotFound);
      }
    } else if (blockIdentifier.hash) {
      let blockHash = blockIdentifier.hash;
      if (!has0xPrefix(blockHash)) {
        blockHash = '0x' + blockHash;
      }
      const block = await db.getBlock(blockHash);
      if (block.found) {
        balance = await getBalance(db, accountIdentifier.address, block.result.block_height);
        index = block.result.block_height;
        hash = block.result.block_hash;
      } else {
        res.status(400).json(RosettaErrors.blockNotFound);
      }
    } else {
      res.status(400).json(RosettaErrors.invalidBlockIdentifier);
    }

    const response: RosettaAccountBalanceResponse = {
      block_identifier: {
        index,
        hash,
      },
      balances: [
        {
          value: balance,
          currency: {
            symbol: RosettaConstants.symbol,
            decimals: RosettaConstants.decimals,
          },
        },
      ],
      coins: [],
      metadata: {
        sequence_number: 0,
      },
    };

    res.json(response);
  });

  return router;
}
