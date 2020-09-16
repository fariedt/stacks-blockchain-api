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
  RosettaMaxFeeAmount,
  RosettaConstructionPreprocessRequest,
  RosettaOptions,
  RosettaConstructionMetadataResponse,
  RosettaConstructionHashResponse,
  RosettaConstructionPayloadsRequest,
} from '@blockstack/stacks-blockchain-api-types';
import { RosettaErrors, RosettaConstants } from './../../rosetta-constants';
import { rosettaValidateRequest, ValidSchema, makeRosettaError } from './../../rosetta-validate';
import {
  publicKeyToAddress,
  convertToSTXAddress,
  getOptionsFromOperations,
} from './../../../rosetta-helpers';
import {
  makeUnsignedSTXTokenTransfer,
  UnsignedTokenTransferOptions,
} from '@blockstack/stacks-transactions';
import { type } from 'os';
import { isValidC32Address } from '../../../helpers';
import BN = require('bn.js');

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

    const operations: RosettaOperation[] = req.body.operations;
    let feeOperation: RosettaOperation | null = null;
    let transferToOperation: RosettaOperation | null = null;
    let transferFromOperation: RosettaOperation | null = null;

    for (const operation of operations) {
      switch (operation.type) {
        case 'fee':
          feeOperation = operation;
          break;
        case 'token_transfer':
          if (operation.amount) {
            if (BigInt(operation.amount.value) < 0) {
              transferFromOperation = operation;
            } else {
              transferToOperation = operation;
            }
          }
          break;
        default:
          break;
      }
    }

    const options: RosettaOptions = {
      sender_address: transferFromOperation?.account?.address,
      type: transferFromOperation?.type,
      status: transferFromOperation?.status,
      token_transfer_recipient_address: transferToOperation?.account?.address,
      amount: transferToOperation?.amount?.value,
      symbol: transferToOperation?.amount?.currency.symbol,
      decimals: transferToOperation?.amount?.currency.decimals,
      fee: feeOperation?.amount?.value,
    };

    if (req.body.metadata.gas_limit) {
      options.gas_limit = req.body.metadata.gas_limit;
    }

    if (req.body.metadata.gas_price) {
      options.gas_price = req.body.metadata.gas_price;
    }

    if (req.body.suggested_fee_multiplier) {
      options.suggested_fee_multiplier = req.body.suggested_fee_multiplier;
    }

    if (req.body.max_fee) {
      const max_fee: RosettaMaxFeeAmount = req.body.max_fee[0];
      if (
        max_fee.currency.symbol === RosettaConstants.symbol &&
        max_fee.currency.decimals === RosettaConstants.decimals
      ) {
        options.max_fee = max_fee.value;
      }
    }

    const rosettaPreprocessResponse: RosettaConstructionPreprocessResponse = {
      options,
    };

    res.json(rosettaPreprocessResponse);
  });

  router.postAsync('/metadata', async (req, res) => {
    const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    if (!valid.valid) {
      res.status(400).json(makeRosettaError(valid));
      return;
    }

    const options: RosettaOptions = req.body.options;
    if (options.type != 'token_transfer') {
      res.status(400).json(RosettaErrors.invalidTransactionType);
    }

    if (options?.sender_address && !isValidC32Address(options.sender_address)) {
      res.status(400).json(RosettaErrors.invalidSender);
    }

    if (
      options?.token_transfer_recipient_address &&
      !isValidC32Address(options.token_transfer_recipient_address)
    ) {
      res.status(400).json(RosettaErrors.invalidRecipient);
    }

    const response: RosettaConstructionMetadataResponse = {
      metadata: { ...req.body.options },
    };

    res.json(response);
  });

  router.postAsync('/payloads', async (req, res) => {
    const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    if (!valid.valid) {
      res.status(400).json(makeRosettaError(valid));
      return;
    }

    const options = getOptionsFromOperations(req.body.operations);
    const recipientAddress = options.token_transfer_recipient_address
      ? options.token_transfer_recipient_address
      : '';
    const senderAddress = options.sender_address ? options.sender_address : '';

    const tokenTransferOptions: UnsignedTokenTransferOptions = {
      recipient: recipientAddress,
      amount: options.amount ? new BN(options.amount) : new BN(0),
      fee: options.fee ? new BN(options.fee) : new BN(0),
      publicKey: senderAddress,
    };

    const transaction = await makeUnsignedSTXTokenTransfer(tokenTransferOptions);
    res.json(transaction);
  });

  router.postAsync('/parse', async (req, res) => {});

  router.postAsync('/combine', async (req, res) => {});

  router.postAsync('/hash', async (req, res) => {
    const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    if (!valid.valid) {
      res.status(400).json(makeRosettaError(valid));
      return;
    }

    const signedTransaction = req.body.signed_transaction;
    const hash = signedTransaction.serialize().toString('hex');

    const response: RosettaConstructionHashResponse = {
      transaction_identifier: {
        hash: hash,
      },
    };
    return res.json(response);
  });

  router.postAsync('/submit', async (req, res) => {});

  return router;
}
