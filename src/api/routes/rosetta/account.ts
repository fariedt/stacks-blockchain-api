import * as express from 'express';
import { addAsync, RouterWithAsync } from '@awaitjs/express';
import { DataStore } from '../../../datastore/common';
import {
  RosettaError,
  NetworkIdentifier,
  RosettaAccount,
} from '@blockstack/stacks-blockchain-api-types';
import { RosettaErrors, RosettaConstants } from '../../rosetta-constants';
import { isValidC32Address, has0xPrefix } from '../../../helpers';

function isValidNetworkIdentifier(networkIdentifier: NetworkIdentifier): RosettaError | true {
  if (!networkIdentifier) {
    return RosettaErrors.emptyNetworkIdentifier;
  }

  if (!networkIdentifier.blockchain) {
    return RosettaErrors.emptyBlockchain;
  }

  if (!networkIdentifier.network) {
    return RosettaErrors.emptyBlockchain;
  }

  if (networkIdentifier.blockchain != RosettaConstants.blockchain) {
    return RosettaErrors.invalidBlockchain;
  }

  if (networkIdentifier.network != RosettaConstants.network) {
    return RosettaErrors.invalidNetwork;
  }

  return true;
}

function isValidAccountIdentifier(accountIdentifier: RosettaAccount): RosettaError | true {
  if (!accountIdentifier) {
    return RosettaErrors.emptyAccountIdentifier;
  }

  const stxAddress = accountIdentifier.address;
  if (!isValidC32Address(stxAddress)) {
    return RosettaErrors.invalidAccount;
  }

  return true;
}

export function createRosettaAccountRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());
  router.use(express.json());

  router.postAsync('/balance', async (req, res) => {
    const networkIdentifier = req.body.network_identifier;
    const validNetworkIdentifier = isValidNetworkIdentifier(networkIdentifier);
    if (validNetworkIdentifier !== true) {
      res.status(400).json(validNetworkIdentifier);
    }

    const accountIdentifier = req.body.account_identifier;
    const validAccountIdentifier = isValidAccountIdentifier(accountIdentifier);
    if (validAccountIdentifier !== true) {
      res.status(400).json(validAccountIdentifier);
    }

    const blockIdentifier = req.body.block_identifier;
    let balance: bigint = BigInt(0);
    let index: number = 0;
    let hash: string = '';

    if (blockIdentifier == null) {
      const result = await db.getStxBalance(accountIdentifier.address);
      balance = result.balance;
      const block = await db.getCurrentBlock();
      if (block.found) {
        index = block.result.block_height;
        hash = block.result.block_hash;
      } else {
        res.status(400).json(RosettaErrors.blockNotFound);
      }
    } else if (blockIdentifier.index) {
      const result = await db.getStxBalanceAtBlock(
        accountIdentifier.address,
        blockIdentifier.index
      );
      balance = result.balance;
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
        const result = await db.getStxBalanceAtBlock(
          accountIdentifier.address,
          block.result.block_height
        );
        balance = result.balance;
        index = block.result.block_height;
        hash = block.result.block_hash;
      } else {
        res.status(400).json(RosettaErrors.blockNotFound);
      }
    } else {
      res.status(400).json(RosettaErrors.invalidBlockIdentifier);
    }

    res.json({ status: 'ready' });
  });

  return router;
}
