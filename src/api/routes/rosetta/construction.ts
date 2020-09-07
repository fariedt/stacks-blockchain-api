import * as express from 'express';
import { addAsync, RouterWithAsync } from '@awaitjs/express';
import { DataStore, DbBlock } from '../../../datastore/common';
import {
  NetworkIdentifier,
  RosettaAccount,
  RosettaBlockIdentifier,
  RosettaAccountBalanceResponse,
  RosettaPublicKey,
  RosettaConstructionDeriveResponse,
  RosettaConstructionPreprocessResponse,
  RosettaOperation,
} from '@blockstack/stacks-blockchain-api-types';
import { RosettaErrors, RosettaConstants } from '../../rosetta-constants';
import { has0xPrefix, FoundOrNot } from '../../../helpers';
import { rosettaValidateRequest, ValidSchema, makeRosettaError } from '../../rosetta-validate';
import { publicKeyToAddress, convertToSTXAddress } from './../../../rosetta-helpers';
import { StacksCoreRpcClient } from '../../../core-rpc/client';

export function createRosettaConstructionRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());
  router.use(express.json());

  router.postAsync('/derive', async (req, res) => {
    const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    if (!valid.valid) {
      res.status(400).json(makeRosettaError(valid));
      return;
    }

    const publicKey: RosettaPublicKey = req.body.public_key;
    const btcAddress = publicKeyToAddress(publicKey.hex_bytes);
    const stxAddress = convertToSTXAddress(btcAddress);

    const response: RosettaConstructionDeriveResponse = {
      address: stxAddress,
    };

    res.json(response);
  });

  router.postAsync('/preprocess', async (req, res) => {
    const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    if (!valid.valid) {
      res.status(400).json(makeRosettaError(valid));
      return;
    }

    const operations = req.body.operations;

    const options: any = {
      sender_address: operations[0].account.address,
      type: operations[0].type,
      status: operations[0].status,
      token_transffer_recipient_address: operations[1].account.address,
      amount: operations[1].amount.value,
      symbol: operations[1].amount.symbol,
      decimals: operations[1].amount.decimals,
    };

    if (req.body.metadata.gas_limit) {
      options['gas_limit'] = req.body.metadata.gas_limit;
    }

    if (req.body.metadata.gas_limit) {
      options['gas_price'] = req.body.metadata.gas_price;
    }

    if (req.body.suggested_fee_multiplier) {
      options['suggested_fee_multiplier'] = req.body.suggested_fee_multiplier;
    }

    //todo we need to check our configuration
    if (req.body.max_fee) {
      options['max_fee'] = req.body.max_fee[0].value;
    }
  });

  router.postAsync('/metadata', async (req, res) => {
    // const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    // if (!valid.valid) {
    //   res.status(400).json(makeRosettaError(valid));
    //   return;
    // }

    ///const options = req.body.options;

    const client = new StacksCoreRpcClient();
    const result = await client.getFees();
    res.json(result);
    // return result;
  });

  router.postAsync('/payloads', async (req, res) => {});

  router.postAsync('/parse', async (req, res) => {});

  router.postAsync('/combine', async (req, res) => {});

  router.postAsync('/hash', async (req, res) => {});

  router.postAsync('/submit', async (req, res) => {});

  return router;
}
