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
import { deserializeTransaction } from '@blockstack/stacks-transactions/lib/transaction';
import { nextSignature } from '@blockstack/stacks-transactions/lib/authorization';
import { BufferReader } from '@blockstack/stacks-transactions/lib/bufferReader';
import {
  createMessageSignature,
  createTransactionAuthField,
  isSingleSig,
  makeSigHashPreSign,
} from '@blockstack/stacks-transactions/lib/authorization';
import * as BigNum from 'bn.js';

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

    const privKey = createStacksPrivateKey(
      '7a1da04ca6fbf4adcb09acc15ff0f9b8bc158d7bd6698a2fa1a7e2c18906e02601'
    );
    const publicKey = '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51';
    const recipient = 'ST2VHM28V9E5QCRD6C73215KAPSBKQGPWTEE5CMQT';
    const amount = new BigNum(500000);
    const fee = new BigNum(100);
    const nonce = new BigNum(0);
    const memo = 'test transaction';

    const txpromise = makeUnsignedSTXTokenTransfer({
      recipient,
      amount,
      fee,
      nonce,
      memo,
      numSignatures: 1, // number of signature required
      publicKey, // the participants public keys
    });

    // const singedTxPromise = makeSTXTokenTransfer({
    //   recipient,
    //   amount,
    //   fee,
    //   nonce,
    //   memo,
    //   numSignatures: 1, // number of signature required,
    //   senderKey: '7a1da04ca6fbf4adcb09acc15ff0f9b8bc158d7bd6698a2fa1a7e2c18906e02601',
    // });

    // const signedTx = await singedTxPromise;

    // res.status(200).json({ signedTX: (await singedTxPromise).serialize().toString('hex') });

    const unsignedtx = await txpromise;

    const combineRequest: RosettaConstructionCombineRequest = req.body;
    const unsigned_transaction_buffer = hexToBuffer('0x' + combineRequest.unsigned_transaction);
    const signed_hex_bytes = combineRequest.signatures[0].signing_payload.hex_bytes;

    const signer = new TransactionSigner(unsignedtx);
    signer.signOrigin(privKey);

    // const signed_hex = signer.transaction.serialize().toString('hex');

    const unsignedSerialized = unsignedtx.serialize().toString('hex');

    // const messageSignature = signWithKey(privKey, unsignedSerialized);

    if (!unsignedtx.auth.authType || !unsignedtx.auth.spendingCondition?.fee) return;
    console.log(
      'signature',
      JSON.stringify(
        nextSignature(
          unsignedtx.txid(),
          unsignedtx.auth.authType,
          unsignedtx.auth.spendingCondition?.fee,
          unsignedtx.auth.spendingCondition?.nonce,
          privKey
        )
      )
    );

    // if (
    //   signer.transaction.auth.spendingCondition &&
    //   isSingleSig(signer.transaction.auth.spendingCondition)
    // ) {
    //   console.log(
    //     'comparison',
    //     signer.transaction.auth.spendingCondition.signature.data + ' ',
    //     messageSignature.data
    //   );
    // }

    // const unsignedBuffer = hexToBuffer('0x' + unsignedSerialized);

    // if (verifySignature(unsignedSerialized, publicKey, messageSignature.data)) {
    //   // res.status(200).json({ singnature_verification: 'verified' });
    //   console.log('Signed Verified');
    // } else {
    //   // res.status(200).json({ singnature_verification: 'not verified' });
    //   console.log('Signed not verified');
    // }

    // const obj = {
    //   publicKey: publicKey,
    //   unsigned: unsignedSerialized,
    //   // signed: signed_hex,
    //   signature: messageSignature.data,
    // };

    // // save it
    // const jobj = JSON.stringify(obj, null, 2);
    // // // fs.writeFileSync('/tmp/sig.json', jobj);

    // console.log(`private key is ${privKey.data.toString('hex')}`);
    // console.log(jobj);
    // return;

    //** public key verification */

    // if (!transaction.auth.authType || !transaction.auth.spendingCondition?.nonce) {
    //   return;
    // }

    // const preSignHash = makeSigHashPreSign(
    //   transaction.txid(),
    //   transaction.auth.authType,
    //   transaction.auth.spendingCondition?.fee,
    //   transaction.auth.spendingCondition?.nonce
    // );

    const obj1 = {
      publicKey: combineRequest.signatures[0].public_key.hex_bytes,
      unsigned: combineRequest.unsigned_transaction,
      // signed: signed_hex,
      signature: combineRequest.signatures[0].hex_bytes,
    };

    // // save it
    const jobj1 = JSON.stringify(obj1, null, 2);
    // // fs.writeFileSync('/tmp/sig.json', jobj);

    console.log(`request params${privKey.data.toString('hex')}`);
    console.log(jobj1);

    // if (
    //   verifySignature(
    //     combineRequest.unsigned_transaction,
    //     combineRequest.signatures[0].public_key.hex_bytes,
    //     combineRequest.signatures[0].hex_bytes
    //   )
    // ) {
    //   res.status(200).json({ singnature_verification: 'verified' });
    // } else {
    //   res.status(200).json({ singnature_verification: 'not verified' });
    // }

    const transaction = deserializeTransaction(
      BufferReader.fromBuffer(unsigned_transaction_buffer)
    );

    const signature = createMessageSignature(combineRequest.signatures[0].hex_bytes);

    if (transaction.auth.spendingCondition && isSingleSig(transaction.auth.spendingCondition)) {
      transaction.auth.spendingCondition.signature = signature;
    } else {
      const authField = createTransactionAuthField(signature);
      transaction.auth.spendingCondition?.fields.push(authField);
    }
    const serializedTx = transaction.serialize().toString('hex');
    try {
      const submitResult = await new StacksCoreRpcClient().sendTransaction(
        hexToBuffer('0x' + serializedTx)
      );
      console.log('Transaction submited', JSON.stringify(submitResult));
      res.status(200).json({ signed_transaction: submitResult });
    } catch (e) {
      res.status(400).json(e.message);
    }

    // res.status(200).json({ signed_transaction: serializedTx });
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
