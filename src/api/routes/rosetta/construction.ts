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
  GetStacksTestnetNetwork,
  isSymbolSupported,
  isDecimalsSupported,
} from './../../../rosetta-helpers';
import {
  createStacksPrivateKey,
  getPublicKey,
  makeUnsignedSTXTokenTransfer,
  publicKeyToString,
  UnsignedTokenTransferOptions,
} from '@blockstack/stacks-transactions';
import { type } from 'os';
import { isValidC32Address } from '../../../helpers';
import BN = require('bn.js');
import { getCoreNodeEndpoint, StacksCoreRpcClient } from '../../../core-rpc/client';
import { has0xPrefix, FoundOrNot } from '../../../helpers';

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

    // We are only supporting transfer, we should have operations length = 3
    if (operations.length != 3) {
      res.status(400).json(RosettaErrors.invalidOperation);
      return;
    }

    const options = getOptionsFromOperations(req.body.operations);
    if (options == null) {
      res.status(400).json(RosettaErrors.invalidOperation);
      return;
    }

    if (isSymbolSupported(req.body.operations)) {
      res.status(400).json(RosettaErrors.invalidCurrencySymbol);
      return;
    }

    if (isDecimalsSupported(req.body.operations)) {
      res.status(400).json(RosettaErrors.invalidCurrencyDecimals);
      return;
    }

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
      } else {
        res.status(400).json(RosettaErrors.invalidFee);
        return;
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
      return;
    }

    if (options?.sender_address && !isValidC32Address(options.sender_address)) {
      res.status(400).json(RosettaErrors.invalidSender);
      return;
    }
    if (options?.symbol !== RosettaConstants.symbol) {
      res.status(400).json(RosettaErrors.invalidCurrencySymbol);
      return;
    }

    const recipientAddress = options.token_transfer_recipient_address;
    if (options?.decimals !== RosettaConstants.decimals) {
      res.status(400).json(RosettaErrors.invalidCurrencyDecimals);
      return;
    }

    if (recipientAddress == null || !isValidC32Address(recipientAddress)) {
      res.status(400).json(RosettaErrors.invalidRecipient);
      return;
    }

    const accountInfo = await new StacksCoreRpcClient().getAccount(recipientAddress);
    const nonce = accountInfo.nonce;

    let recentBlockHash = '';
    const blockQuery: FoundOrNot<DbBlock> = await db.getCurrentBlock();
    if (blockQuery.found) {
      recentBlockHash = blockQuery.result.block_hash;
    }

    const response: RosettaConstructionMetadataResponse = {
      metadata: {
        ...req.body.options,
        account_sequence: nonce,
        recent_block_hash: recentBlockHash,
      },
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
    if (options == null) {
      res.status(400).json(RosettaErrors.invalidOperation);
      return;
    }

    //    const publicKey = publicKeyToString(getPublicKey(createStacksPrivateKey(txOptions.senderKey)));

    const recipientAddress = options.token_transfer_recipient_address
      ? options.token_transfer_recipient_address
      : '';
    const senderAddress = options.sender_address ? options.sender_address : '';

    //    const publicKey = publicKeyToString(getPublicKey(createStacksPrivateKey(senderAddress)));

    res.json(createStacksPrivateKey('STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0'));
    // const tokenTransferOptions: UnsignedTokenTransferOptions = {
    //   recipient: recipientAddress,
    //   amount: options.amount ? new BN(options.amount) : new BN(0),
    //   fee: options.fee ? new BN(options.fee) : new BN(0),
    //   publicKey: senderAddress,
    //   network: GetStacksTestnetNetwork(),
    // };

    // const transaction = await makeUnsignedSTXTokenTransfer(tokenTransferOptions);
    // res.json(transaction.serialize().toString('hex'));
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
