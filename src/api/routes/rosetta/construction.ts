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
  RosettaConstructionPayloadResponse,
  RosettaConstructionCombineRequest,
  RosettaConstructionCombineResponse,
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
  verifySignature,
  makePresignHash,
} from './../../../rosetta-helpers';
import {
  createStacksPrivateKey,
  createStacksPublicKey,
  getPublicKey,
  makeUnsignedSTXTokenTransfer,
  publicKeyToString,
  UnsignedTokenTransferOptions,
  signWithKey,
  TransactionSigner,
  makeSTXTokenTransfer,
} from '@blockstack/stacks-transactions';
import { type } from 'os';
import { digestSha512_256, hexToBuffer, isValidC32Address } from '../../../helpers';
import * as BN from 'bn.js';
import { getCoreNodeEndpoint, StacksCoreRpcClient } from '../../../core-rpc/client';
import { has0xPrefix, FoundOrNot } from '../../../helpers';
import * as crypto from 'crypto';
import { RESTClient } from 'rpc-bitcoin';
import {
  deserializeTransaction,
  StacksTransaction,
} from '@blockstack/stacks-transactions/lib/transaction';
import { MessageSignature, nextSignature } from '@blockstack/stacks-transactions/lib/authorization';
import { BufferReader } from '@blockstack/stacks-transactions/lib/bufferReader';
import {
  createMessageSignature,
  createTransactionAuthField,
  isSingleSig,
} from '@blockstack/stacks-transactions/lib/authorization';
import * as BigNum from 'bn.js';
import { Signature } from 'typescript';

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

    let recentBlockHash = undefined;
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

    const amount = options.amount;
    if (!amount) {
      res.status(400).json(RosettaErrors.invalidAmount);
      return;
    }

    const fees = options.fee;
    if (!fees) {
      res.status(400).json(RosettaErrors.invalidFees);
      return;
    }

    const publicKeys: RosettaPublicKey[] = req.body.public_keys;
    if (!publicKeys) {
      res.status(400).json(RosettaErrors.emptyPublicKey);
      return;
    }

    if (publicKeys[0].curve_type !== 'secp256k1') {
      res.status(400).json(RosettaErrors.invalidCurveType);
      return;
    }

    const recipientAddress = options.token_transfer_recipient_address
      ? options.token_transfer_recipient_address
      : '';
    const senderAddress = options.sender_address ? options.sender_address : '';

    const accountInfo = await new StacksCoreRpcClient().getAccount(senderAddress);

    const tokenTransferOptions: UnsignedTokenTransferOptions = {
      recipient: recipientAddress,
      amount: new BN(amount),
      fee: new BN(fees),
      publicKey: publicKeys[0].hex_bytes,
      network: GetStacksTestnetNetwork(),
      nonce: accountInfo.nonce ? new BN(accountInfo.nonce) : new BN(0),
    };

    const transaction = await makeUnsignedSTXTokenTransfer(tokenTransferOptions);
    const unsignedTransaction = transaction.serialize();
    const hexBytes = digestSha512_256(unsignedTransaction).toString('hex');
    const response: RosettaConstructionPayloadResponse = {
      unsigned_transaction: unsignedTransaction.toString('hex'),
      payloads: [
        {
          address: senderAddress,
          hex_bytes: '0x' + hexBytes,
          signature_type: 'ecdsa',
        },
      ],
    };
    res.json(response);
  });

  router.postAsync('/parse', async (req, res) => {});

  router.postAsync('/combine', async (req, res) => {
    const valid: ValidSchema = await rosettaValidateRequest(req.originalUrl, req.body);
    if (!valid.valid) {
      res.status(400).json(makeRosettaError(valid));
      return;
    }

    const combineRequest: RosettaConstructionCombineRequest = req.body;
    const signatures = combineRequest.signatures;

    if (has0xPrefix(combineRequest.unsigned_transaction)) {
      res.status(400).json(RosettaErrors.invalidTransactionString);
      return;
    }

    if (signatures.length === 0) {
      res.status(400).json(RosettaErrors.noSignatures);
      return;
    }

    let unsigned_transaction_buffer: Buffer;
    let transaction: StacksTransaction;

    try {
      unsigned_transaction_buffer = hexToBuffer('0x' + combineRequest.unsigned_transaction);
      transaction = deserializeTransaction(BufferReader.fromBuffer(unsigned_transaction_buffer));
    } catch (e) {
      res.status(400).json(RosettaErrors.invalidTransactionString);
      return;
    }

    for (const signature of signatures) {
      if (signature.public_key.curve_type !== 'secp256k1') {
        res.status(400).json(RosettaErrors.invalidCurveType);
        return;
      }
      const preSignHash = makePresignHash(transaction);
      if (!preSignHash) {
        res.status(400).json(RosettaErrors.invalidTransactionString);
        return;
      }

      let newSignature: MessageSignature;

      try {
        newSignature = createMessageSignature(signature.signing_payload.hex_bytes);
      } catch (error) {
        res.status(400).json(RosettaErrors.invalidSignature);
        return;
      }

      if (!verifySignature(preSignHash, signature.public_key.hex_bytes, newSignature)) {
        res.status(400).json(RosettaErrors.signatureNotVerified);
      }

      if (transaction.auth.spendingCondition && isSingleSig(transaction.auth.spendingCondition)) {
        transaction.auth.spendingCondition.signature = newSignature;
      } else {
        const authField = createTransactionAuthField(newSignature);
        transaction.auth.spendingCondition?.fields.push(authField);
      }
    }

    const serializedTx = transaction.serialize().toString('hex');

    const combineResponse: RosettaConstructionCombineResponse = {
      signed_transaction: serializedTx,
    };

    res.status(200).json(combineResponse);
  });

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
