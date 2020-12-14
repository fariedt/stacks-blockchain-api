import { inspect } from 'util';
import * as net from 'net';
import { Server } from 'http';
import * as express from 'express';
import * as bodyParser from 'body-parser';
import { addAsync } from '@awaitjs/express';
import PQueue from 'p-queue';

import { hexToBuffer, logError, logger, digestSha512_256, jsonStringify } from '../helpers';
import {
  CoreNodeBlockMessage,
  CoreNodeEventType,
  CoreNodeBurnBlockMessage,
  CoreNodeParsedTxMessage,
} from './core-node-message';
import {
  DataStore,
  createDbTxFromCoreMsg,
  DbEventBase,
  DbSmartContractEvent,
  DbStxEvent,
  DbEventTypeId,
  DbFtEvent,
  DbAssetEventTypeId,
  DbNftEvent,
  DbBlock,
  DataStoreUpdateData,
  createDbMempoolTxFromCoreMsg,
  DbStxLockEvent,
  DbMinerReward,
  DbBurnchainReward,
  DbBNSNamespace,
  DbBNSName,
} from '../datastore/common';
import { parseMessageTransactions, getTxSenderAddress, getTxSponsorAddress } from './reader';
import { TransactionPayloadTypeID, readTransaction } from '../p2p/tx';
import {
  deserializeCV,
  ClarityType,
  ClarityValue,
  BufferCV,
  StandardPrincipalCV,
  TupleCV,
  BufferReader,
  Address,
  IntCV,
  addressToString,
  StringAsciiCV,
  SomeCV,
  UIntCV,
  ListCV,
} from '@stacks/transactions';

import { StacksCoreRpcClient, getCoreNodeEndpoint } from './../core-rpc/client';
import { Name } from 'node-pg-migrate';
import BN = require('bn.js');

interface Attachment {
  attachment: {
    hash: string;
    metadata: {
      name: string;
      namespace: string;
      tx_sender: Address;
    };
    page_index: number;
    position_in_page: string;
  };
}

export function parseNameRawValue(rawValue: string): Attachment {
  const cl_val: ClarityValue = deserializeCV(hexToBuffer(rawValue));
  console.log('metadat CV: ', jsonStringify(cl_val));
  if (cl_val.type == ClarityType.Tuple) {
    const attachment = cl_val.data['attachment'] as TupleCV;

    const hash: BufferCV = attachment.data['hash'] as BufferCV;
    const contentHash = hash.buffer.toString('hex');

    const metadataCV: TupleCV = attachment.data['metadata'] as TupleCV;

    const nameCV: BufferCV = metadataCV.data['name'] as BufferCV;
    const name = nameCV.buffer.toString();
    const namespaceCV: BufferCV = metadataCV.data['namespace'] as BufferCV;
    const namespace = namespaceCV.buffer.toString();
    const addressCV: StandardPrincipalCV = metadataCV.data['tx-sender'] as StandardPrincipalCV;
    const address = addressCV.address;

    const page_indexCV: IntCV = metadataCV.data['namespace'] as IntCV;
    const page_index = page_indexCV.value;
    const position_in_pageCV: IntCV = metadataCV.data['namespace'] as IntCV;
    const position_in_page = position_in_pageCV.value;

    const result: Attachment = {
      attachment: {
        hash: contentHash,
        metadata: {
          name: name,
          namespace: namespace,
          tx_sender: address,
        },
        page_index: page_index,
        position_in_page: position_in_page,
      },
    };

    console.log('metadat: ', metadataCV);
    console.log('hash: ', contentHash);
    console.log('metadat name: ', name);
    console.log('metadat namespace: ', namespace);
    console.log('metadat address: ', address);
    return result;
  }
  throw Error('Value can not be parsed');
}

export function parseNamespaceRawValue(rawValue: string): DbBNSNamespace | undefined {
  const cl_val: ClarityValue = deserializeCV(hexToBuffer(rawValue));
  if (cl_val.type == ClarityType.Tuple) {
    const namespaceCV: BufferCV = cl_val.data['namespace'] as BufferCV;
    const namespace = namespaceCV.buffer.toString();
    const statusCV: StringAsciiCV = cl_val.data['status'] as StringAsciiCV;
    const status = statusCV.data;

    const properties = cl_val.data['properties'] as TupleCV;

    const launched_atCV = properties.data['launched-at'] as SomeCV;
    const launch_atintCV = launched_atCV.value as UIntCV;
    const launched_at = parseInt(launch_atintCV.value.toString());
    const lifetimeCV = properties.data['lifetime'] as IntCV;
    const lifetime: BN = lifetimeCV.value;
    const revealed_atCV = properties.data['revealed-at'] as IntCV;
    const revealed_at: BN = revealed_atCV.value;
    const addressCV: StandardPrincipalCV = properties.data[
      'namespace-import'
    ] as StandardPrincipalCV;
    const address = addressCV.address;

    const price_function = properties.data['price-function'] as TupleCV;

    const baseCV = price_function.data['base'] as IntCV;
    const base: BN = baseCV.value;
    const coeffCV = price_function.data['coeff'] as IntCV;
    const coeff: BN = coeffCV.value;
    const no_vowel_discountCV = price_function.data['no-vowel-discount'] as IntCV;
    const no_vowel_discount: BN = no_vowel_discountCV.value;
    const nonalpha_discountCV = price_function.data['nonalpha-discount'] as IntCV;
    const nonalpha_discount: BN = nonalpha_discountCV.value;
    const bucketsCV = price_function.data['buckets'] as ListCV;

    const buckets: number[] = [];
    const listCV = bucketsCV.list;
    for (let i = 0; i < listCV.length; i++) {
      const cv = listCV[i];
      if (cv.type === ClarityType.UInt) {
        buckets.push(cv.value);
      }
    }

    const namespaceBNS: DbBNSNamespace = {
      namespace_id: namespace,
      address: addressToString(address),
      base: base.toNumber(),
      coeff: coeff.toNumber(),
      launched_at: launched_at,
      lifetime: lifetime.toNumber(),
      no_vowel_discount: no_vowel_discount.toNumber(),
      nonalpha_discount: nonalpha_discount.toNumber(),
      ready_block: 0,
      reveal_block: revealed_at.toNumber(),
      status: status,
      latest: true,
      buckets: buckets.toString(),
      tx_id: Buffer.from('0x00'),
    };
    return namespaceBNS;
  }
}

interface AttachmentValue {
  attachment: {
    content: number[];
  };
}

export async function parseContentHash(contentHash: string): Promise<string> {
  // { host: '127.0.0.1', port: '20443' }
  let result: AttachmentValue | undefined = undefined;
  try {
    result = await new StacksCoreRpcClient().fetchJson<AttachmentValue>(
      `v2/attachments/${contentHash}`,
      {
        method: 'GET',
        timeout: 10 * 1000, //10 seconds
      }
    );
  } catch (error) {}

  // const attachment: Attachment = {
  //   attachment: {
  //     content: [250, 202, 222, 1],
  //   },
  // };
  if (result === undefined) {
    // result = {
    //   attachment: {
    //     content: [250, 202, 222, 1],
    //   },
    // };
    throw Error('Error: can not get content hash');
  }
  let content = '';
  for (const char of result.attachment.content) {
    content = content + char.toString(16);
  }
  console.log('attachment result', content);
  return content;
}

function getFunctionName(tx_id: string, transactions: CoreNodeParsedTxMessage[]): string {
  let parsed_tx: CoreNodeParsedTxMessage | null = null;
  const contract_function_name: string = '';
  for (const tx of transactions) {
    if (tx.core_tx.txid === tx_id) parsed_tx = tx;
  }
  if (parsed_tx && parsed_tx.parsed_tx.payload.typeId === TransactionPayloadTypeID.ContractCall) {
    return parsed_tx.parsed_tx.payload.functionName;
  }
  return contract_function_name;
}

async function handleBurnBlockMessage(
  burnBlockMsg: CoreNodeBurnBlockMessage,
  db: DataStore
): Promise<void> {
  logger.verbose(
    `Received burn block message hash ${burnBlockMsg.burn_block_hash}, height: ${burnBlockMsg.burn_block_height}`
  );
  logger.verbose(
    `Received burn block rewards for ${burnBlockMsg.reward_recipients.length} recipients`
  );
  const rewards = burnBlockMsg.reward_recipients.map((r, index) => {
    const dbReward: DbBurnchainReward = {
      canonical: true,
      burn_block_hash: burnBlockMsg.burn_block_hash,
      burn_block_height: burnBlockMsg.burn_block_height,
      burn_amount: BigInt(burnBlockMsg.burn_amount),
      reward_recipient: r.recipient,
      reward_amount: BigInt(r.amount),
      reward_index: index,
    };
    return dbReward;
  });
  await db.updateBurnchainRewards({
    burnchainBlockHash: burnBlockMsg.burn_block_hash,
    burnchainBlockHeight: burnBlockMsg.burn_block_height,
    rewards: rewards,
  });
}

async function handleMempoolTxsMessage(rawTxs: string[], db: DataStore): Promise<void> {
  logger.verbose(`Received ${rawTxs.length} mempool transactions`);
  // TODO: mempool-tx receipt date should be sent from the core-node
  const receiptDate = Math.round(Date.now() / 1000);
  const rawTxBuffers = rawTxs.map(str => hexToBuffer(str));
  const decodedTxs = rawTxBuffers.map(buffer => {
    const txId = '0x' + digestSha512_256(buffer).toString('hex');
    const bufferReader = BufferReader.fromBuffer(buffer);
    const parsedTx = readTransaction(bufferReader);
    const txSender = getTxSenderAddress(parsedTx);
    const sponsorAddress = getTxSponsorAddress(parsedTx);
    return {
      txId: txId,
      sender: txSender,
      sponsorAddress,
      txData: parsedTx,
      rawTx: buffer,
    };
  });
  const dbMempoolTxs = decodedTxs.map(tx => {
    logger.verbose(`Received mempool tx: ${tx.txId}`);
    const dbMempoolTx = createDbMempoolTxFromCoreMsg({
      txId: tx.txId,
      txData: tx.txData,
      sender: tx.sender,
      sponsorAddress: tx.sponsorAddress,
      rawTx: tx.rawTx,
      receiptDate: receiptDate,
    });
    return dbMempoolTx;
  });
  await db.updateMempoolTxs({ mempoolTxs: dbMempoolTxs });
}

async function handleClientMessage(msg: CoreNodeBlockMessage, db: DataStore): Promise<void> {
  const parsedMsg = parseMessageTransactions(msg);

  const dbBlock: DbBlock = {
    canonical: true,
    block_hash: parsedMsg.block_hash,
    index_block_hash: parsedMsg.index_block_hash,
    parent_index_block_hash: parsedMsg.parent_index_block_hash,
    parent_block_hash: parsedMsg.parent_block_hash,
    parent_microblock: parsedMsg.parent_microblock,
    block_height: parsedMsg.block_height,
    burn_block_time: parsedMsg.burn_block_time,
    burn_block_hash: parsedMsg.burn_block_hash,
    burn_block_height: parsedMsg.burn_block_height,
    miner_txid: parsedMsg.miner_txid,
  };

  logger.verbose(
    `Received block ${parsedMsg.block_hash} (${parsedMsg.block_height}) from node`,
    dbBlock
  );

  const dbMinerRewards: DbMinerReward[] = [];
  for (const minerReward of msg.matured_miner_rewards ?? []) {
    const dbMinerReward: DbMinerReward = {
      canonical: true,
      block_hash: minerReward.from_stacks_block_hash,
      index_block_hash: minerReward.from_index_consensus_hash,
      mature_block_height: parsedMsg.block_height,
      recipient: minerReward.recipient,
      coinbase_amount: BigInt(minerReward.coinbase_amount),
      tx_fees_anchored_shared: BigInt(minerReward.tx_fees_anchored_shared),
      tx_fees_anchored_exclusive: BigInt(minerReward.tx_fees_anchored_exclusive),
      tx_fees_streamed_confirmed: BigInt(minerReward.tx_fees_streamed_confirmed),
    };
    dbMinerRewards.push(dbMinerReward);
  }
  logger.verbose(`Received ${dbMinerRewards.length} matured miner rewards`);

  const dbData: DataStoreUpdateData = {
    block: dbBlock,
    minerRewards: dbMinerRewards,
    txs: new Array(parsedMsg.transactions.length),
  };

  for (let i = 0; i < parsedMsg.transactions.length; i++) {
    const tx = parsedMsg.parsed_transactions[i];
    logger.verbose(`Received mined tx: ${tx.core_tx.txid}`);
    dbData.txs[i] = {
      tx: createDbTxFromCoreMsg(tx),
      stxEvents: [],
      stxLockEvents: [],
      ftEvents: [],
      nftEvents: [],
      contractLogEvents: [],
      smartContracts: [],
    };
    if (tx.parsed_tx.payload.typeId === TransactionPayloadTypeID.SmartContract) {
      const contractId = `${tx.sender_address}.${tx.parsed_tx.payload.name}`;
      dbData.txs[i].smartContracts.push({
        tx_id: tx.core_tx.txid,
        contract_id: contractId,
        block_height: parsedMsg.block_height,
        source_code: tx.parsed_tx.payload.codeBody,
        abi: JSON.stringify(tx.core_tx.contract_abi),
        canonical: true,
      });
    }
  }

  for (const event of parsedMsg.events) {
    const dbTx = dbData.txs.find(entry => entry.tx.tx_id === event.txid);
    if (!dbTx) {
      throw new Error(`Unexpected missing tx during event parsing by tx_id ${event.txid}`);
    }

    const dbEvent: DbEventBase = {
      event_index: event.event_index,
      tx_id: event.txid,
      tx_index: dbTx.tx.tx_index,
      block_height: parsedMsg.block_height,
      canonical: true,
    };

    switch (event.type) {
      case CoreNodeEventType.ContractEvent: {
        logger.verbose(`------Contract Event received  ${JSON.stringify(event)}`);
        const entry: DbSmartContractEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.SmartContractLog,
          contract_identifier: event.contract_event.contract_identifier,
          topic: event.contract_event.topic,
          value: hexToBuffer(event.contract_event.raw_value),
        };
        dbTx.contractLogEvents.push(entry);
        if (
          event.contract_event.topic === 'print' &&
          event.contract_event.contract_identifier === 'ST000000000000000000002AMW42H.bns'
        ) {
          console.log(
            `tx_id:  ${event.txid} parse transaction: ${getFunctionName(
              event.txid,
              parsedMsg.parsed_transactions
            )}`
          );
          if (getFunctionName(event.txid, parsedMsg.parsed_transactions) === 'name-import') {
            const attachment = parseNameRawValue(event.contract_event.raw_value);
            const attachmentValue = await parseContentHash(attachment.attachment.hash);
            const names: DbBNSName = {
              name: attachment.attachment.metadata.name,
              namespace_id: attachment.attachment.metadata.namespace,
              address: addressToString(attachment.attachment.metadata.tx_sender),
              expire_block: 0,
              registered_at: parsedMsg.burn_block_time,
              zonefile_hash: attachment.attachment.hash,
              zonefile: Buffer.from(event.txid),
              latest: true,
              tx_id: hexToBuffer(event.txid),
            };
            console.log('update names ', JSON.stringify(names));
            await db.updateNames(names);
          } else if (
            getFunctionName(event.txid, parsedMsg.parsed_transactions) === 'namespace-ready'
          ) {
            //event received for namespaces
            const namespace: DbBNSNamespace | undefined = parseNamespaceRawValue(
              event.contract_event.raw_value
            );
            if (namespace != undefined) {
              namespace.ready_block = parsedMsg.burn_block_time;
              namespace.tx_id = hexToBuffer(event.txid);
              console.log('update namespaces ', JSON.stringify(namespace));
              await db.updateNamespaces(namespace);
            }
          }
        }
        break;
      }
      case CoreNodeEventType.StxLockEvent: {
        const entry: DbStxLockEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.StxLock,
          locked_amount: BigInt(event.stx_lock_event.locked_amount),
          unlock_height: Number(event.stx_lock_event.unlock_height),
          locked_address: event.stx_lock_event.locked_address,
        };
        dbTx.stxLockEvents.push(entry);
        break;
      }
      case CoreNodeEventType.StxTransferEvent: {
        const entry: DbStxEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.StxAsset,
          asset_event_type_id: DbAssetEventTypeId.Transfer,
          sender: event.stx_transfer_event.sender,
          recipient: event.stx_transfer_event.recipient,
          amount: BigInt(event.stx_transfer_event.amount),
        };
        dbTx.stxEvents.push(entry);
        break;
      }
      case CoreNodeEventType.StxMintEvent: {
        const entry: DbStxEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.StxAsset,
          asset_event_type_id: DbAssetEventTypeId.Mint,
          recipient: event.stx_mint_event.recipient,
          amount: BigInt(event.stx_mint_event.amount),
        };
        dbTx.stxEvents.push(entry);
        break;
      }
      case CoreNodeEventType.StxBurnEvent: {
        const entry: DbStxEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.StxAsset,
          asset_event_type_id: DbAssetEventTypeId.Burn,
          sender: event.stx_burn_event.sender,
          amount: BigInt(event.stx_burn_event.amount),
        };
        dbTx.stxEvents.push(entry);
        break;
      }
      case CoreNodeEventType.FtTransferEvent: {
        const entry: DbFtEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.FungibleTokenAsset,
          asset_event_type_id: DbAssetEventTypeId.Transfer,
          sender: event.ft_transfer_event.sender,
          recipient: event.ft_transfer_event.recipient,
          asset_identifier: event.ft_transfer_event.asset_identifier,
          amount: BigInt(event.ft_transfer_event.amount),
        };
        dbTx.ftEvents.push(entry);
        break;
      }
      case CoreNodeEventType.FtMintEvent: {
        const entry: DbFtEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.FungibleTokenAsset,
          asset_event_type_id: DbAssetEventTypeId.Mint,
          recipient: event.ft_mint_event.recipient,
          asset_identifier: event.ft_mint_event.asset_identifier,
          amount: BigInt(event.ft_mint_event.amount),
        };
        dbTx.ftEvents.push(entry);
        break;
      }
      case CoreNodeEventType.NftTransferEvent: {
        const entry: DbNftEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.NonFungibleTokenAsset,
          asset_event_type_id: DbAssetEventTypeId.Transfer,
          recipient: event.nft_transfer_event.recipient,
          sender: event.nft_transfer_event.sender,
          asset_identifier: event.nft_transfer_event.asset_identifier,
          value: hexToBuffer(event.nft_transfer_event.raw_value),
        };
        dbTx.nftEvents.push(entry);
        break;
      }
      case CoreNodeEventType.NftMintEvent: {
        const entry: DbNftEvent = {
          ...dbEvent,
          event_type: DbEventTypeId.NonFungibleTokenAsset,
          asset_event_type_id: DbAssetEventTypeId.Mint,
          recipient: event.nft_mint_event.recipient,
          asset_identifier: event.nft_mint_event.asset_identifier,
          value: hexToBuffer(event.nft_mint_event.raw_value),
        };
        dbTx.nftEvents.push(entry);
        break;
      }
      default: {
        throw new Error(`Unexpected CoreNodeEventType: ${inspect(event)}`);
      }
    }
  }

  await db.update(dbData);
}

interface EventMessageHandler {
  handleBlockMessage(msg: CoreNodeBlockMessage, db: DataStore): Promise<void> | void;
  handleMempoolTxs(rawTxs: string[], db: DataStore): Promise<void> | void;
  handleBurnBlock(msg: CoreNodeBurnBlockMessage, db: DataStore): Promise<void> | void;
}

function createMessageProcessorQueue(): EventMessageHandler {
  // Create a promise queue so that only one message is handled at a time.
  const processorQueue = new PQueue({ concurrency: 1 });
  const handler: EventMessageHandler = {
    handleBlockMessage: (msg: CoreNodeBlockMessage, db: DataStore) => {
      return processorQueue
        .add(() => handleClientMessage(msg, db))
        .catch(e => {
          logError(`Error processing core node block message`, e);
        });
    },
    handleBurnBlock: (msg: CoreNodeBurnBlockMessage, db: DataStore) => {
      return processorQueue
        .add(() => handleBurnBlockMessage(msg, db))
        .catch(e => {
          logError(`Error processing core node burn block message`, e);
        });
    },
    handleMempoolTxs: (rawTxs: string[], db: DataStore) => {
      return processorQueue
        .add(() => handleMempoolTxsMessage(rawTxs, db))
        .catch(e => {
          logError(`Error processing core node mempool message`, e);
        });
    },
  };

  return handler;
}

export async function startEventServer(opts: {
  db: DataStore;
  messageHandler?: EventMessageHandler;
  promMiddleware?: express.Handler;
}): Promise<net.Server> {
  const db = opts.db;
  const messageHandler = opts.messageHandler ?? createMessageProcessorQueue();

  let eventHost = process.env['STACKS_CORE_EVENT_HOST'];
  const eventPort = parseInt(process.env['STACKS_CORE_EVENT_PORT'] ?? '', 10);
  if (!eventHost) {
    throw new Error(
      `STACKS_CORE_EVENT_HOST must be specified, e.g. "STACKS_CORE_EVENT_HOST=127.0.0.1"`
    );
  }
  if (!eventPort) {
    throw new Error(`STACKS_CORE_EVENT_PORT must be specified, e.g. "STACKS_CORE_EVENT_PORT=3700"`);
  }

  if (eventHost.startsWith('http:')) {
    const { hostname } = new URL(eventHost);
    eventHost = hostname;
  }

  const app = addAsync(express());

  if (opts.promMiddleware) {
    app.use(opts.promMiddleware);
  }

  app.use(bodyParser.json({ type: 'application/json', limit: '25MB' }));
  app.getAsync('/', (req, res) => {
    res
      .status(200)
      .json({ status: 'ready', msg: 'API event server listening for core-node POST messages' });
  });

  app.postAsync('/new_block', async (req, res) => {
    console.log(JSON.stringify(req.body, null, 2));
    try {
      const msg: CoreNodeBlockMessage = req.body;
      await messageHandler.handleBlockMessage(msg, db);
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /new_block: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/attachments/new', (req, res) => {
    console.log('---- new_attachment');
    console.log(JSON.stringify(req.body, null, 2));
    console.log('---- new_attachment');
    res.status(200).json({ result: 'ok' });
  });

  app.postAsync('/new_burn_block', async (req, res) => {
    try {
      const msg: CoreNodeBurnBlockMessage = req.body;
      await messageHandler.handleBurnBlock(msg, db);
      res.status(200).json({ result: 'ok' });
    } catch (error) {
      logError(`error processing core-node /new_burn_block: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  app.postAsync('/new_mempool_tx', async (req, res) => {
    try {
      const rawTxs: string[] = req.body;
      await messageHandler.handleMempoolTxs(rawTxs, db);
      res.status(200).json({ result: 'ok' });
      await Promise.resolve();
    } catch (error) {
      logError(`error processing core-node /new_mempool_tx: ${error}`, error);
      res.status(500).json({ error: error });
    }
  });

  const server = await new Promise<Server>(resolve => {
    const server = app.listen(eventPort, eventHost as string, () => resolve(server));
  });

  const addr = server.address();
  if (addr === null) {
    throw new Error('server missing address');
  }
  const addrStr = typeof addr === 'string' ? addr : `${addr.address}:${addr.port}`;
  logger.info(`Event observer listening at: http://${addrStr}`);

  return server;
}
