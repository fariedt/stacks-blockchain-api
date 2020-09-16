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
} from '@blockstack/stacks-blockchain-api-types';
import { RosettaErrors, RosettaConstants } from './../../rosetta-constants';
import { rosettaValidateRequest, ValidSchema, makeRosettaError } from './../../rosetta-validate';
import { publicKeyToAddress, convertToSTXAddress } from './../../../rosetta-helpers';
import {
  makeUnsignedSTXTokenTransfer,
  UnsignedTokenTransferOptions,
} from '@blockstack/stacks-transactions';
import { type } from 'os';
import { isValidC32Address } from '../../../helpers';

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
    var feeOperation: RosettaOperation | null = null;
    var transferToOperation: RosettaOperation | null = null;
    var transferFromOperation: RosettaOperation | null = null;

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
        case 'contract_call':
          break;
        // case 'coinbase':
        //   break;
        case 'smart_contract':
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

    // if (operations && operations.length < 2) {
    //   res.status(500).json(RosettaErrors.invalidParams);
    // }

    // const options: RosettaOptions = {
    //   sender_address: operations[0].account?.address,
    //   type: operations[0].type,
    //   status: operations[0].status,
    //   token_transfer_recipient_address: operations[1].account?.address,
    //   amount: operations[1].amount?.value,
    //   symbol: operations[1].amount?.currency.symbol,
    //   decimals: operations[1].amount?.currency.decimals,
    // };

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
    const options: any = req.body;

    const tokenTransferOptions: UnsignedTokenTransferOptions = {
      recipient: options.token_transfer_recipient_address,
      amount: options.amount,
      fee: options.fee,
      publicKey: options.sender_address,
      nonce: options.nonce,
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

    return res.json(hash);
  });

  router.postAsync('/submit', async (req, res) => {});

  return router;
}
