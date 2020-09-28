import { BaseTx, DbTxStatus, DbTxTypeId } from './datastore/common';
import { getTxTypeString, getTxStatusString } from './api/controllers/db-controller';
import * as btc from 'bitcoinjs-lib';
import * as c32check from 'c32check';
import {
  assertNotNullish as unwrapOptional,
  bufferToHexPrefixString,
  hexToBuffer,
} from './helpers';
import { RosettaOperation, RosettaOptions } from '@blockstack/stacks-blockchain-api-types';
import { StacksTestnet, StacksTransaction } from '@blockstack/stacks-transactions';
import { txidFromData } from '@blockstack/stacks-transactions/lib/utils';
import { deserializeTransaction } from '@blockstack/stacks-transactions/lib/transaction';
import {
  isSingleSig,
  emptyMessageSignature,
} from '@blockstack/stacks-transactions/lib/authorization';
import { BufferReader } from '@blockstack/stacks-transactions/lib/bufferReader';
import { getCoreNodeEndpoint } from './core-rpc/client';
import { getTxSenderAddress, getTxSponsorAddress } from './event-stream/reader';
import { readTransaction, TransactionPayloadTypeID } from './p2p/tx';
import { RosettaConstants } from './api/rosetta-constants';
import { addressToString } from '@blockstack/stacks-transactions/lib/types';

enum CoinAction {
  CoinSpent = 'coin_spent',
  CoinCreated = 'coin_created',
}

export function publicKeyToAddress(publicKey: string): string {
  const publicKeyBuffer = Buffer.from(publicKey, 'hex');

  const address = btc.payments.p2pkh({
    pubkey: publicKeyBuffer,
    network: btc.networks.regtest,
  });
  return address.address ? address.address : '';
}

export function convertToSTXAddress(btcAddress: string): string {
  return c32check.b58ToC32(btcAddress);
}

export function rawTxToStacksTransaction(raw_tx: string): StacksTransaction {
  const buffer = hexToBuffer(raw_tx);
  let transaction: StacksTransaction = deserializeTransaction(BufferReader.fromBuffer(buffer));
  return transaction;
}

export function isSignedTransaction(transaction: StacksTransaction): Boolean {
  if (!transaction.auth.spendingCondition) {
    return false;
  }
  if (isSingleSig(transaction.auth.spendingCondition)) {
    /**Single signature Transaction has an empty signature, so the transaction is not signed */
    if (
      !transaction.auth.spendingCondition.signature.data ||
      emptyMessageSignature().data === transaction.auth.spendingCondition.signature.data
    ) {
      return false;
    }
  } else {
    /**Multi-signature transaction does not have signature fields thus the transaction not signed */
    if (transaction.auth.spendingCondition.fields.length === 0) {
      return false;
    }
  }
  return true;
}

export function rawTxToBaseTx(raw_tx: string): BaseTx {
  const txBuffer = Buffer.from(raw_tx.substring(2), 'hex');
  const txId = '0x' + txidFromData(txBuffer);
  const bufferReader = BufferReader.fromBuffer(txBuffer);
  const transaction = readTransaction(bufferReader);
  const txSender = getTxSenderAddress(transaction);
  const sponsorAddress = getTxSponsorAddress(transaction);
  const payload: any = transaction.payload;
  const fee = transaction.auth.originCondition.feeRate;
  const amount = payload.amount;
  transaction.auth.originCondition;
  const recipientAddr =
    payload.recipient && payload.recipient.address
      ? addressToString({
          type: payload.recipient.typeId,
          version: payload.recipient.address.version,
          hash160: payload.recipient.address.bytes.toString('hex'),
        })
      : '';
  const sponsored = sponsorAddress ? true : false;

  let transactionType = DbTxTypeId.TokenTransfer;
  switch (transaction.payload.typeId) {
    case TransactionPayloadTypeID.TokenTransfer:
      transactionType = DbTxTypeId.TokenTransfer;
      break;
    case TransactionPayloadTypeID.SmartContract:
      transactionType = DbTxTypeId.SmartContract;
      break;
    case TransactionPayloadTypeID.ContractCall:
      transactionType = DbTxTypeId.ContractCall;
      break;
    case TransactionPayloadTypeID.Coinbase:
      transactionType = DbTxTypeId.Coinbase;
      break;
    case TransactionPayloadTypeID.PoisonMicroblock:
      transactionType = DbTxTypeId.PoisonMicroblock;
      break;
  }
  const dbtx: BaseTx = {
    token_transfer_recipient_address: recipientAddr,
    tx_id: txId,
    type_id: transactionType,
    status: DbTxStatus.Pending,
    fee_rate: fee,
    sender_address: txSender,
    token_transfer_amount: amount,
    sponsored: sponsored,
    sponsor_address: sponsorAddress,
  };

  return dbtx;
}

export function getOperations(tx: BaseTx): RosettaOperation[] {
  const operations: RosettaOperation[] = [];
  const txType = getTxTypeString(tx.type_id);
  switch (txType) {
    case 'token_transfer':
      operations.push(makeFeeOperation(tx));
      operations.push(makeSenderOperation(tx, operations.length));
      operations.push(makeReceiverOperation(tx, operations.length));
      break;
    case 'contract_call':
      operations.push(makeFeeOperation(tx));
      operations.push(makeCallContractOperation(tx, operations.length));
      break;
    case 'smart_contract':
      operations.push(makeFeeOperation(tx));
      operations.push(makeDeployContractOperation(tx, operations.length));
      break;
    case 'coinbase':
      operations.push(makeCoinbaseOperation(tx, 0));
      break;
    case 'poison_microblock':
      operations.push(makePoisonMicroblockOperation(tx, 0));
      break;
    default:
      throw new Error(`Unexpected tx type: ${JSON.stringify(txType)}`);
  }
  return operations;
}

function makeFeeOperation(tx: BaseTx): RosettaOperation {
  const address = tx.sponsored
    ? unwrapOptional(tx.sponsor_address, () => 'Unexpected nullish sponsor signer')
    : tx.sender_address;
  const fee: RosettaOperation = {
    operation_identifier: { index: 0 },
    type: 'fee',
    status: getTxStatusString(tx.status),
    account: { address: address },
    amount: {
      value: (BigInt(0) - tx.fee_rate).toString(10),
      currency: { symbol: 'STX', decimals: 6 },
    },
  };

  return fee;
}

function makeSenderOperation(tx: BaseTx, index: number): RosettaOperation {
  const sender: RosettaOperation = {
    operation_identifier: { index: index },
    type: getTxTypeString(tx.type_id),
    status: getTxStatusString(tx.status),
    account: {
      address: unwrapOptional(tx.sender_address, () => 'Unexpected nullish sender_address'),
    },
    amount: {
      value:
        '-' +
        unwrapOptional(
          tx.token_transfer_amount,
          () => 'Unexpected nullish token_transfer_amount'
        ).toString(10),
      currency: { symbol: 'STX', decimals: 6 },
    },
    coin_change: {
      coin_action: CoinAction.CoinSpent,
      coin_identifier: { identifier: tx.tx_id + ':' + index },
    },
  };

  return sender;
}

function makeReceiverOperation(tx: BaseTx, index: number): RosettaOperation {
  const receiver: RosettaOperation = {
    operation_identifier: { index: index },
    related_operations: [{ index: 0, operation_identifier: { index: 1 } }],
    type: getTxTypeString(tx.type_id),
    status: getTxStatusString(tx.status),
    account: {
      address: unwrapOptional(
        tx.token_transfer_recipient_address,
        () => 'Unexpected nullish token_transfer_recipient_address'
      ),
    },
    amount: {
      value: unwrapOptional(
        tx.token_transfer_amount,
        () => 'Unexpected nullish token_transfer_amount'
      ).toString(10),
      currency: { symbol: 'STX', decimals: 6 },
    },
    coin_change: {
      coin_action: CoinAction.CoinCreated,
      coin_identifier: { identifier: tx.tx_id + ':' + index },
    },
  };

  return receiver;
}

function makeDeployContractOperation(tx: BaseTx, index: number): RosettaOperation {
  const deployer: RosettaOperation = {
    operation_identifier: { index: index },
    type: getTxTypeString(tx.type_id),
    status: getTxStatusString(tx.status),
    account: {
      address: unwrapOptional(tx.sender_address, () => 'Unexpected nullish sender_address'),
    },
  };

  return deployer;
}

function makeCallContractOperation(tx: BaseTx, index: number): RosettaOperation {
  const caller: RosettaOperation = {
    operation_identifier: { index: index },
    type: getTxTypeString(tx.type_id),
    status: getTxStatusString(tx.status),
    account: {
      address: unwrapOptional(tx.sender_address, () => 'Unexpected nullish sender_address'),
      sub_account: {
        address: tx.contract_call_contract_id ? tx.contract_call_contract_id : '',
        metadata: {
          contract_call_function_name: tx.contract_call_function_name,
          contract_call_function_args: bufferToHexPrefixString(
            unwrapOptional(tx.contract_call_function_args, () => '')
          ),
          raw_result: tx.raw_result,
        },
      },
    },
  };

  return caller;
}
function makeCoinbaseOperation(tx: BaseTx, index: number): RosettaOperation {
  // TODO : Add more mappings in operations for coinbase
  const sender: RosettaOperation = {
    operation_identifier: { index: index },
    type: getTxTypeString(tx.type_id),
    status: getTxStatusString(tx.status),
    account: {
      address: unwrapOptional(tx.sender_address, () => 'Unexpected nullish sender_address'),
    },
  };

  return sender;
}

function makePoisonMicroblockOperation(tx: BaseTx, index: number): RosettaOperation {
  // TODO : add more mappings in operations for poison-microblock
  const sender: RosettaOperation = {
    operation_identifier: { index: index },
    type: getTxTypeString(tx.type_id),
    status: getTxStatusString(tx.status),
    account: {
      address: unwrapOptional(tx.sender_address, () => 'Unexpected nullish sender_address'),
    },
  };

  return sender;
}

export function getOptionsFromOperations(operations: RosettaOperation[]): RosettaOptions | null {
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
        return null;
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

  return options;
}

export function isSymbolSupported(operations: RosettaOperation[]): boolean {
  for (const operation of operations) {
    if (operation.amount?.currency.symbol !== RosettaConstants.symbol) {
      return false;
    }
  }

  return true;
}

export function isDecimalsSupported(operations: RosettaOperation[]): boolean {
  for (const operation of operations) {
    if (operation.amount?.currency.decimals !== RosettaConstants.decimals) {
      return false;
    }
  }

  return true;
}

export function GetStacksTestnetNetwork() {
  const stacksNetwork = new StacksTestnet();
  stacksNetwork.coreApiUrl = `http://${getCoreNodeEndpoint()}`;
  return stacksNetwork;
}
