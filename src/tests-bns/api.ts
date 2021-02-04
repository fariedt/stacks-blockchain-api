import { PgDataStore, cycleMigrations, runMigrations } from '../datastore/postgres-store';
import { PoolClient } from 'pg';
import { ApiServer, startApiServer } from '../api/init';
import * as supertest from 'supertest';
import { startEventServer } from '../event-stream/event-server';
import { Server } from 'net';
import { resolveModuleName } from 'typescript';
import { BNSGetAllNamespacesResponse } from '@blockstack/stacks-blockchain-api-types';
import * as Ajv from 'ajv';
import { validate } from '../api/rosetta-validate';
import { DbBNSName, DbBNSNamespace, DbMempoolTx, DbTx, DbTxStatus } from '../datastore/common';
import { StacksMocknet, StacksTestnet } from '@stacks/network';

import {
  broadcastTransaction,
  bufferCV,
  bufferCVFromString,
  callReadOnlyFunction,
  FungibleConditionCode,
  getAddressFromPrivateKey,
  makeContractCall,
  makeStandardSTXPostCondition,
  makeSTXTokenTransfer,
  standardPrincipalCV,
  TransactionVersion,
  uintCV,
  deserializeCV,
  cvToString,
  ContractCallOptions,
  SignedContractCallOptions,
} from '@stacks/transactions';
import ripemd160 = require('ripemd160');
import shajs = require('sha.js');
import BigNum = require('bn.js');
import { logger } from '../../src/helpers';

// import { StacksCoreRpcClient } from 'src/core-rpc/client';

function hash160(bfr: Buffer): Buffer {
  const hash160 = new ripemd160().update(new shajs.sha256().update(bfr).digest()).digest('hex');
  return Buffer.from(hash160, 'hex');
}

const network = new StacksMocknet();

const deployedTo = 'ST000000000000000000002AMW42H';
const deployedName = 'bns';

const pkey = 'cb3df38053d132895220b9ce471f6b676db5b9bf0b4adefb55f2118ece2478df01';
const address = getAddressFromPrivateKey(pkey, TransactionVersion.Testnet);

const salt = Buffer.from('60104ad42ed976f5b8cfd6341496476aa72d1101', 'hex'); // salt and pepper
const namespaceBuffer = Buffer.from('alice');
const namespaceHash = hash160(Buffer.concat([namespaceBuffer, salt]));
const postConditions = [
  makeStandardSTXPostCondition(address, FungibleConditionCode.GreaterEqual, new BigNum(1)),
];

describe('BNS API', () => {
  let db: PgDataStore;
  let client: PoolClient;
  let eventServer: Server;
  let api: ApiServer;

  function standByForTx(expectedTxId: string): Promise<DbTx> {
    const broadcastTx = new Promise<DbTx>(resolve => {
      const listener: (info: DbTx | DbMempoolTx) => void = info => {
        if (
          info.tx_id === expectedTxId &&
          (info.status === DbTxStatus.Success ||
            info.status === DbTxStatus.AbortByResponse ||
            info.status === DbTxStatus.AbortByPostCondition)
        ) {
          api.datastore.removeListener('txUpdate', listener);
          resolve(info as DbTx);
        }
      };
      api.datastore.addListener('txUpdate', listener);
    });

    return broadcastTx;
  }

  beforeAll(async () => {
    process.env.PG_DATABASE = 'postgres';
    await cycleMigrations();
    db = await PgDataStore.connect();
    client = await db.pool.connect();
    eventServer = await startEventServer({ db });
    api = await startApiServer(db);
    const namespace: DbBNSNamespace = {
      namespace_id: 'abc',
      address: 'ST2ZRX0K27GW0SP3GJCEMHD95TQGJMKB7G9Y0X1MH',
      base: 1,
      coeff: 1,
      launched_at: 14,
      lifetime: 1,
      no_vowel_discount: 1,
      nonalpha_discount: 1,
      ready_block: 2,
      reveal_block: 6,
      status: 'ready',
      latest: true,
      buckets: '1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1',
      canonical: true,
    };
    await db.updateNamespaces(client, namespace);

    const name: DbBNSName = {
      name: 'xyz',
      address: 'ST5RRX0K27GW0SP3GJCEMHD95TQGJMKB7G9Y0X1ZA',
      namespace_id: 'abc',
      registered_at: 1,
      expire_block: 14,
      zonefile:
        '$ORIGIN muneeb.id\n$TTL 3600\n_http._tcp IN URI 10 1 "https://blockstack.s3.amazonaws.com/muneeb.id"\n',
      zonefile_hash: 'b100a68235244b012854a95f9114695679002af9',
      latest: true,
      canonical: true,
    };
    await db.updateNames(client, name);

    //preorder and reveal namespace to the bns network
    while (true) {
      try {
        const txOptions: SignedContractCallOptions = {
          contractAddress: deployedTo,
          contractName: deployedName,
          functionName: 'namespace-preorder',
          functionArgs: [bufferCV(namespaceHash), uintCV(64000000000)],
          senderKey: pkey,
          validateWithAbi: true,
          postConditions: postConditions,
          network,
        };

        const transaction = await makeContractCall(txOptions);
        const submitResult = await broadcastTransaction(transaction, network);
        const preorder = await standByForTx('0x' + transaction.txid());
        if (preorder.status != 1) logger.error('Namespace preorder error');

        const revealTxOptions: SignedContractCallOptions = {
          contractAddress: deployedTo,
          contractName: deployedName,
          functionName: 'namespace-reveal',
          functionArgs: [
            bufferCV(namespaceBuffer),
            bufferCV(salt),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(1),
            uintCV(0),
            standardPrincipalCV(address),
          ],
          senderKey: pkey,
          validateWithAbi: true,
          network,
        };

        const revealTransaction = await makeContractCall(revealTxOptions);
        const revealSubmitResult = await broadcastTransaction(revealTransaction, network);
        const reveal = await standByForTx('0x' + revealTransaction.txid());
        if (reveal.status != 1) logger.error('Namespace Reveal Error');

        break;
      } catch (e) {
        console.log('error connection', e);
      }
    }
  });

  test('name-import contract call', async () => {
    const txOptions = {
      contractAddress: deployedTo,
      contractName: deployedName,
      functionName: 'name-import',
      functionArgs: [
        bufferCV(namespaceBuffer),
        bufferCVFromString('bob'),
        standardPrincipalCV(address),
        bufferCV(hash160(Buffer.from('hello', 'hex'))),
      ],
      senderKey: pkey,
      validateWithAbi: true,
      network,
    };

    const transaction = await makeContractCall(txOptions);
    // const transaction = await nameImport('abcdef', 'hello01');

    // const expectedTxId = '0x' + transaction.txid();
    const submitResult = await broadcastTransaction(transaction, network);
    console.log('name import: ', submitResult);

    const result = await standByForTx('0x' + transaction.txid());
    // if (reveal.status != 1) logger.error('Namespace Reveal Error');
    console.log('name import: ', submitResult);
    // expect(true).toBe(true); //check api call for name
    const query1 = await supertest(api.server).get(`/v1/names/bob`);
    expect(query1.status).toBe(200);
    expect(query1.type).toBe('application/json');
    expect(query1.status).toBe(200);
  });

  // test('namespace-ready contract call', async () => {
  //   // const transaction = await nameImport('abcdef', 'hello01');

  //   // const expectedTxId = '0x' + transaction.txid();
  //   // const submitResult = await broadcastTransaction(transaction, network);
  //   // console.log('preprder: ', submitResult);

  //   // await standByForTx(expectedTxId);
  //   expect(true).toBe(true); //check api call for namespace
  // });

  // test('name-update contract call', async () => {
  //   // const transaction = await nameImport('abcdef', 'hello01');

  //   // const expectedTxId = '0x' + transaction.txid();
  //   // const submitResult = await broadcastTransaction(transaction, network);
  //   // console.log('preprder: ', submitResult);

  //   // await standByForTx(expectedTxId);
  //   expect(true).toBe(true); //check api call for subdomain
  // });

  test('Success: namespaces', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces`);
    expect(query1.status).toBe(200);
    expect(query1.type).toBe('application/json');
  });

  test('Validate: namespace response schema', async () => {
    const query1 = await supertest(api.server).get('/v1/namespaces');
    const result = JSON.parse(query1.text);
    const path = require.resolve(
      '@blockstack/stacks-blockchain-api-types/api/bns/namespace-operations/bns-get-all-namespaces-response.schema.json'
    );
    const valid = await validate(path, result);
    expect(valid.valid).toBe(true);
  });

  test('Validate: namespaces returned length', async () => {
    const query1 = await supertest(api.server).get('/v1/namespaces');
    const result = JSON.parse(query1.text);
    expect(result.namespaces.length).toBe(1);
  });

  test('Validate: namespace id returned correct', async () => {
    const query1 = await supertest(api.server).get('/v1/namespaces');
    const result = JSON.parse(query1.text);
    expect(result.namespaces[0]).toBe('abc');
  });

  test('Success: fetching names from namespace', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces/abc/names`);
    expect(query1.status).toBe(200);
    expect(query1.type).toBe('application/json');
  });

  test('Namespace not found', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces/def/names`);
    expect(query1.status).toBe(404);
  });

  test('Validate: names returned length', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces/abc/names`);
    expect(query1.status).toBe(200);
    const result = JSON.parse(query1.text);
    expect(result.length).toBe(1);
  });

  test('Validate: name returned for namespace', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces/abc/names`);
    expect(query1.status).toBe(200);
    const result = JSON.parse(query1.text);
    expect(result[0]).toBe('xyz');
  });

  test('Success: namespaces/{namespace}/name schema', async () => {
    const query1 = await supertest(api.server).get('/v1/namespaces/abc/names');
    const result = JSON.parse(query1.text);
    const path = require.resolve(
      '@blockstack/stacks-blockchain-api-types/api/bns/namespace-operations/bns-get-all-namespaces-names-response.schema.json'
    );
    const valid = await validate(path, result);
    expect(valid.valid).toBe(true);
  });

  test('Invalid page for names', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces/abc/names?page=1`);
    expect(query1.status).toBe(400);
  });

  // TODO: implement schema validation test
  // TODO: implement price check successful test
  test('Success: names returned with page number in namespaces/{namespace}/names', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces/abc/names?page=0`);
    expect(query1.status).toBe(200);
  });

  test('Fail namespace price', async () => {
    // if namespace length greater than 20 chars
    const query1 = await supertest(api.server).get(`/v2/prices/namespaces/someLongIdString12345`);
    expect(query1.status).toBe(400);
    expect(query1.type).toBe('application/json');
  });

  test('Fail names price invalid name', async () => {
    // if name is without dot
    const query1 = await supertest(api.server).get(`/v2/prices/names/withoutdot`);
    expect(query1.status).toBe(400);
    expect(query1.type).toBe('application/json');
  });

  test('Fail names price invalid name multi dots', async () => {
    const query1 = await supertest(api.server).get(`/v2/prices/names/name.test.id`);
    expect(query1.status).toBe(400);
    expect(query1.type).toBe('application/json');
  });

  test('Success zonefile by name and hash', async () => {
    const name = 'test';
    const zonefileHash = 'test-hash';
    const zonefile = 'test-zone-file';

    const dbName: DbBNSName = {
      name: name,
      address: 'STRYYQQ9M8KAF4NS7WNZQYY59X93XEKR31JP64CP',
      namespace_id: '',
      expire_block: 10000,
      zonefile: zonefile,
      zonefile_hash: zonefileHash,
      latest: true,
      registered_at: 1000,
      canonical: true,
    };
    await db.updateNames(client, dbName);

    const query1 = await supertest(api.server).get(`/v1/names/${name}/zonefile/${zonefileHash}`);
    expect(query1.status).toBe(200);
    expect(query1.body.zonefile).toBe('test-zone-file');
    expect(query1.type).toBe('application/json');
  });

  test('Fail zonefile by name - Invalid name', async () => {
    const name = 'test';
    const zonefileHash = 'test-hash';
    const zonefile = 'test-zone-file';

    const dbName: DbBNSName = {
      name: name,
      address: 'STRYYQQ9M8KAF4NS7WNZQYY59X93XEKR31JP64CP',
      namespace_id: '',
      expire_block: 10000,
      zonefile: zonefile,
      zonefile_hash: zonefileHash,
      latest: true,
      registered_at: 1000,
      canonical: true,
    };
    await db.updateNames(client, dbName);

    const query1 = await supertest(api.server).get(`/v1/names/invalid/zonefile/${zonefileHash}`);
    expect(query1.status).toBe(400);
    expect(query1.body.error).toBe('Invalid name or subdomain');
    expect(query1.type).toBe('application/json');
  });

  test('Fail zonefile by name - No zonefile found', async () => {
    const name = 'test';
    const zonefileHash = 'test-hash';
    const zonefile = 'test-zone-file';

    const dbName: DbBNSName = {
      name: name,
      address: 'STRYYQQ9M8KAF4NS7WNZQYY59X93XEKR31JP64CP',
      namespace_id: '',
      expire_block: 10000,
      zonefile: zonefile,
      zonefile_hash: zonefileHash,
      latest: true,
      registered_at: 1000,
      canonical: true,
    };
    await db.updateNames(client, dbName);

    const query1 = await supertest(api.server).get(`/v1/names/${name}/zonefile/invalidHash`);
    expect(query1.status).toBe(404);
    expect(query1.body.error).toBe('No such zonefile');
    expect(query1.type).toBe('application/json');
  });

  test('Success names by address', async () => {
    const blockchain = 'stacks';
    const address = 'ST1HB1T8WRNBYB0Y3T7WXZS38NKKPTBR3EG9EPJKR';
    const name = 'test-name';

    const dbName: DbBNSName = {
      name: name,
      address: address,
      namespace_id: '',
      expire_block: 10000,
      zonefile: 'test-zone-file',
      zonefile_hash: 'zonefileHash',
      latest: true,
      registered_at: 1000,
      canonical: true,
    };
    await db.updateNames(client, dbName);

    const query1 = await supertest(api.server).get(`/v1/addresses/${blockchain}/${address}`);
    expect(query1.status).toBe(200);
    expect(query1.body.names[0]).toBe(name);
    expect(query1.type).toBe('application/json');
  });

  test('Fail names by address - Blockchain not support', async () => {
    const query1 = await supertest(api.server).get(`/v1/addresses/invalid/test`);
    expect(query1.status).toBe(404);
    expect(query1.body.error).toBe('Unsupported blockchain');
    expect(query1.type).toBe('application/json');
  });

  test('Success get zonefile by name', async () => {
    const zonefile = 'test-zone-file';
    const address = 'ST1HB1T8WRNBYB0Y3T7WXZS38NKKPTBR3EG9EPJKR';
    const name = 'zonefile-test-name';

    const dbName: DbBNSName = {
      name: name,
      address: address,
      namespace_id: '',
      expire_block: 10000,
      zonefile: 'test-zone-file',
      zonefile_hash: 'zonefileHash',
      latest: true,
      registered_at: 1000,
      canonical: true,
    };
    await db.updateNames(client, dbName);

    const query1 = await supertest(api.server).get(`/v1/names/${name}/zonefile`);
    expect(query1.status).toBe(200);
    expect(query1.body.zonefile).toBe(zonefile);
    expect(query1.type).toBe('application/json');
  });

  test('Fail get zonefile by name - invalid name', async () => {
    const query1 = await supertest(api.server).get(`/v1/names/invalidName/zonefile`);
    expect(query1.status).toBe(400);
    expect(query1.body.error).toBe('Invalid name or subdomain');
    expect(query1.type).toBe('application/json');
  });

  test('Success: names', async () => {
    const query1 = await supertest(api.server).get(`/v1/names`);
    expect(query1.status).toBe(200);
    expect(query1.type).toBe('application/json');
  });

  test('Validate: names response schema', async () => {
    const query1 = await supertest(api.server).get('/v1/names');
    const result = JSON.parse(query1.text);
    const path = require.resolve(
      '@blockstack/stacks-blockchain-api-types/api/bns/name-querying/bns-get-all-names-response.schema.json'
    );
    const valid = await validate(path, result);
    expect(valid.valid).toBe(true);
  });

  test('Invalid page from /v1/names', async () => {
    const query1 = await supertest(api.server).get('/v1/names?page=1');
    expect(query1.status).toBe(400);
  });

  test('Success: name info', async () => {
    const query1 = await supertest(api.server).get(`/v1/names/xyz`);
    expect(query1.status).toBe(200);
    expect(query1.type).toBe('application/json');
  });

  test('Validate: name info response schema', async () => {
    const query1 = await supertest(api.server).get('/v1/names/xyz');
    const result = JSON.parse(query1.text);
    console.log('Result', result);
    const path = require.resolve(
      '@blockstack/stacks-blockchain-api-types/api/bns/name-querying/bns-get-name-info.response.schema.json'
    );
    const valid = await validate(path, result);
    expect(valid.valid).toBe(true);
  });

  test('Failure: name info', async () => {
    const query1 = await supertest(api.server).get(`/v1/names/testname`);
    expect(query1.status).toBe(404);
  });

  test('Success: fetching name info', async () => {
    const query1 = await supertest(api.server).get(`/v1/names/xyz`);
    expect(query1.status).toBe(200);
    expect(query1.body.address).toBe('ST5RRX0K27GW0SP3GJCEMHD95TQGJMKB7G9Y0X1ZA');
    expect(query1.body.expire_block).toBe(14);
    expect(query1.body.zonefile).toBe(
      '$ORIGIN muneeb.id\n$TTL 3600\n_http._tcp IN URI 10 1 "https://blockstack.s3.amazonaws.com/muneeb.id"\n'
    );
    expect(query1.body.zonefile_hash).toBe('b100a68235244b012854a95f9114695679002af9');
  });

  afterAll(async () => {
    await new Promise(resolve => eventServer.close(() => resolve(true)));
    await api.terminate();
    client.release();
    await db?.close();
    await runMigrations(undefined, 'down');
  });
});
