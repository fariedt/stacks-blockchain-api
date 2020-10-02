import { PgDataStore, cycleMigrations, runMigrations } from '../datastore/postgres-store';
import { PoolClient } from 'pg';
import { ApiServer, startApiServer } from '../api/init';
import * as supertest from 'supertest';
import {
  RosettaConstructionCombineRequest,
  RosettaConstructionCombineResponse,
  RosettaConstructionDeriveRequest,
  RosettaConstructionDeriveResponse,
  RosettaConstructionMetadataRequest,
  RosettaConstructionMetadataResponse,
  RosettaConstructionPayloadsRequest,
  RosettaConstructionPreprocessRequest,
  RosettaConstructionPreprocessResponse,
} from '@blockstack/stacks-blockchain-api-types';

import { startEventServer } from '../event-stream/event-server';
import { Server } from 'net';
import { DbBlock, DbTx, DbMempoolTx, DbTxStatus } from '../datastore/common';
import * as assert from 'assert';
import { makeSTXTokenTransfer, StacksTestnet } from '@blockstack/stacks-transactions';
import * as BN from 'bn.js';
import { getCoreNodeEndpoint, StacksCoreRpcClient } from '../core-rpc/client';
import { timeout } from '../helpers';
import { RosettaConstants, RosettaErrors } from './../api/rosetta-constants';

describe('Rosetta API', () => {
  let db: PgDataStore;
  let client: PoolClient;
  let eventServer: Server;
  let api: ApiServer;

  beforeAll(async () => {
    process.env.PG_DATABASE = 'postgres';
    await cycleMigrations();
    db = await PgDataStore.connect();
    client = await db.pool.connect();
    eventServer = await startEventServer(db);
    api = await startApiServer(db);
  });

  test('derive api', async () => {
    const request: RosettaConstructionDeriveRequest = {
      network_identifier: {
        blockchain: RosettaConstants.blockchain,
        network: RosettaConstants.network,
      },
      public_key: {
        curve_type: 'secp256k1',
        hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
      },
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/derive`)
      .send(request);

    expect(result.status).toBe(200);
    expect(result.type).toBe('application/json');

    const expectResponse: RosettaConstructionDeriveResponse = {
      address: 'ST19SH1QSCR8VMEX6SVWP33WCF08RPDY5QVHX94BM',
    };

    expect(JSON.parse(result.text)).toEqual(expectResponse);

    const request2 = {
      network_identifier: {
        blockchain: RosettaConstants.blockchain,
        network: RosettaConstants.network,
      },
      public_key: {
        curve_type: 'this is an invalid curve type',
        hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
      },
    };

    const result2 = await supertest(api.server)
      .post(`/rosetta/v1/construction/derive`)
      .send(request2);
    expect(result2.status).toBe(400);

    const expectedResponse2 = RosettaErrors.invalidCurveType;

    expect(JSON.parse(result2.text)).toEqual(expectedResponse2);

    const request3 = {
      network_identifier: {
        blockchain: RosettaConstants.blockchain,
        network: RosettaConstants.network,
      },
      public_key: {
        curve_type: 'secp256k1',
        hex_bytes: 'this is an invalid public key',
      },
    };

    const result3 = await supertest(api.server)
      .post(`/rosetta/v1/construction/derive`)
      .send(request3);
    expect(result3.status).toBe(400);

    const expectedResponse3 = RosettaErrors.invalidPublicKey;

    expect(JSON.parse(result3.text)).toEqual(expectedResponse3);
  });

  test('preprocess api', async () => {
    const request: RosettaConstructionPreprocessRequest = {
      network_identifier: {
        blockchain: RosettaConstants.blockchain,
        network: RosettaConstants.network,
      },
      operations: [
        {
          operation_identifier: {
            index: 0,
            network_index: 0,
          },
          related_operations: [],
          type: 'fee',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-180',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 1,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 2,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
            metadata: {},
          },
          amount: {
            value: '500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
      ],
      metadata: {},
      max_fee: [
        {
          value: '12380898',
          currency: {
            symbol: 'STX',
            decimals: 6,
          },
          metadata: {},
        },
      ],
      suggested_fee_multiplier: 0,
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/preprocess`)
      .send(request);

    expect(result.status).toBe(200);
    expect(result.type).toBe('application/json');

    const expectResponse: RosettaConstructionPreprocessResponse = {
      options: {
        sender_address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
        type: 'token_transfer',
        status: 'success',
        token_transfer_recipient_address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
        amount: '500000',
        symbol: 'STX',
        decimals: 6,
        fee: '-180',
        max_fee: '12380898',
      },
    };

    expect(JSON.parse(result.text)).toEqual(expectResponse);

    const request2 = {
      network_identifier: {
        blockchain: RosettaConstants.blockchain,
        network: RosettaConstants.network,
      },
      operations: [
        {
          operation_identifier: {
            index: 0,
            network_index: 0,
          },
          related_operations: [],
          type: 'fee',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-180',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 1,
            network_index: 0,
          },
          related_operations: [],
          type: 'invalid operation type',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 2,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
            metadata: {},
          },
          amount: {
            value: '500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
      ],
      metadata: {},
      max_fee: [
        {
          value: '12380898',
          currency: {
            symbol: 'STX',
            decimals: 6,
          },
          metadata: {},
        },
      ],
      suggested_fee_multiplier: 0,
    };

    const result2 = await supertest(api.server)
      .post(`/rosetta/v1/construction/preprocess`)
      .send(request2);
    expect(result2.status).toBe(400);

    const expectedResponse2 = RosettaErrors.invalidOperation;

    expect(JSON.parse(result2.text)).toEqual(expectedResponse2);
  });

  test('metadata api', async () => {
    const request: RosettaConstructionMetadataRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      options: {
        sender_address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
        type: 'token_transfer',
        status: 'success',
        token_transfer_recipient_address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
        amount: '500000',
        symbol: 'STX',
        decimals: 6,
        fee: '-180',
        max_fee: '12380898',
      },
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/metadata`)
      .send(request);

    expect(result.status).toBe(200);
    expect(result.type).toBe('application/json');
    expect(JSON.parse(result.text)).toHaveProperty('metadata');
  });

  test('metadata api empty network identifier', async () => {
    const request = {
      options: {
        sender_address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
        type: 'token_transfer',
        status: 'success',
        token_transfer_recipient_address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
        amount: '500000',
        symbol: 'STX',
        decimals: 6,
        fee: '-180',
        max_fee: '12380898',
      },
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/metadata`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectResponse = {
      code: 613,
      message: 'Network identifier object is null.',
      retriable: true,
      details: {
        message: "should have required property 'network_identifier'",
      },
    };

    expect(JSON.parse(result.text)).toEqual(expectResponse);
  });

  test('metadata invalid transfer type', async () => {
    const request: RosettaConstructionMetadataRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      options: {
        sender_address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
        type: 'token',
        status: 'success',
        token_transfer_recipient_address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
        amount: '500000',
        symbol: 'STX',
        decimals: 6,
        fee: '-180',
        max_fee: '12380898',
      },
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/metadata`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectResponse = {
      code: 619,
      message: 'Invalid transaction type',
      retriable: false,
    };

    expect(JSON.parse(result.text)).toEqual(expectResponse);
  });

  test('metadata invalid sender address', async () => {
    const request: RosettaConstructionMetadataRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      options: {
        sender_address: 'abc',
        type: 'token_transfer',
        status: 'success',
        token_transfer_recipient_address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
        amount: '500000',
        symbol: 'STX',
        decimals: 6,
        fee: '-180',
        max_fee: '12380898',
      },
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/metadata`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectResponse = {
      code: 620,
      message: 'Invalid sender address',
      retriable: false,
    };

    expect(JSON.parse(result.text)).toEqual(expectResponse);
  });

  test('metadata invalid recipient address', async () => {
    const request: RosettaConstructionMetadataRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      options: {
        sender_address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
        type: 'token_transfer',
        status: 'success',
        token_transfer_recipient_address: 'xyz',
        amount: '500000',
        symbol: 'STX',
        decimals: 6,
        fee: '-180',
        max_fee: '12380898',
      },
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/metadata`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectResponse = {
      code: 621,
      message: 'Invalid recipient address',
      retriable: false,
    };

    expect(JSON.parse(result.text)).toEqual(expectResponse);
  });

  test('payloads success', async () => {
    const request: RosettaConstructionPayloadsRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      operations: [
        {
          operation_identifier: {
            index: 0,
            network_index: 0,
          },
          related_operations: [],
          type: 'fee',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-180',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 1,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 2,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
            metadata: {},
          },
          amount: {
            value: '500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
      ],
      metadata: {},
      public_keys: [
        {
          hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
          curve_type: 'secp256k1',
        },
      ],
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/payloads`)
      .send(request);

    expect(result.status).toBe(200);
    expect(result.type).toBe('application/json');

    const expectedResponse = {
      unsigned_transaction:
        '80800000000400539886f96611ba3ba6cef9618f8c78118b37c5be000000000000000000000000000000b400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003020000000000051a1ae3f911d8f1d46d7416bfbe4b593fd41eac19cb000000000007a12000000000000000000000000000000000000000000000000000000000000000000000',
      payloads: [
        {
          address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
          hex_bytes: '0xf1e432494d509577c5468a8cad70d957942e2671f299340a20f65992a4bfa221',
          signature_type: 'ecdsa',
        },
      ],
    };

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  test('payloads public key not added', async () => {
    const request: RosettaConstructionPayloadsRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      operations: [
        {
          operation_identifier: {
            index: 0,
            network_index: 0,
          },
          related_operations: [],
          type: 'fee',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-180',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 1,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 2,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
            metadata: {},
          },
          amount: {
            value: '500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
      ],
      metadata: {},
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/payloads`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectedResponse = {
      code: 640,
      message: 'Public key not available',
      retriable: false,
    };

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  test('payloads public key invalid curve type', async () => {
    const request: RosettaConstructionPayloadsRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      operations: [
        {
          operation_identifier: {
            index: 0,
            network_index: 0,
          },
          related_operations: [],
          type: 'fee',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-180',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 1,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STB44HYPYAT2BB2QE513NSP81HTMYWBJP02HPGK6',
            metadata: {},
          },
          amount: {
            value: '-500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
        {
          operation_identifier: {
            index: 2,
            network_index: 0,
          },
          related_operations: [],
          type: 'token_transfer',
          status: 'success',
          account: {
            address: 'STDE7Y8HV3RX8VBM2TZVWJTS7ZA1XB0SSC3NEVH0',
            metadata: {},
          },
          amount: {
            value: '500000',
            currency: {
              symbol: 'STX',
              decimals: 6,
            },
            metadata: {},
          },
        },
      ],
      metadata: {},
      public_keys: [
        {
          hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
          curve_type: 'edwards25519',
        },
      ],
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/payloads`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectedResponse = {
      code: 644,
      message: 'Invalid curve type',
      retriable: false,
    };

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  test('combine success', async () => {
    const request: RosettaConstructionCombineRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      unsigned_transaction:
        '80800000000400539886f96611ba3ba6cef9618f8c78118b37c5be000000000000000000000000000000b4000136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f03020000000000051ab71a091b4b8b7661a661c620966ab6573bc2dcd3000000000007a12074657374207472616e73616374696f6e000000000000000000000000000000000000',
      signatures: [
        {
          signing_payload: {
            hex_bytes:
              '0136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f',
            signature_type: 'ecdsa',
          },
          public_key: {
            hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
            curve_type: 'secp256k1',
          },
          signature_type: 'ecdsa',
          hex_bytes:
            '0136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f',
        },
      ],
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/combine`)
      .send(request);

    expect(result.status).toBe(200);
    expect(result.type).toBe('application/json');

    const expectedResponse: RosettaConstructionCombineResponse = {
      signed_transaction:
        '80800000000400539886f96611ba3ba6cef9618f8c78118b37c5be000000000000000000000000000000b4000136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f03020000000000051ab71a091b4b8b7661a661c620966ab6573bc2dcd3000000000007a12074657374207472616e73616374696f6e000000000000000000000000000000000000',
    };

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  test('combine invalid transaction', async () => {
    const request: RosettaConstructionCombineRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      unsigned_transaction: 'invalid transaction',
      signatures: [
        {
          signing_payload: {
            hex_bytes:
              '0136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f',
            signature_type: 'ecdsa',
          },
          public_key: {
            hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
            curve_type: 'secp256k1',
          },
          signature_type: 'ecdsa',
          hex_bytes:
            '0136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f',
        },
      ],
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/combine`)
      .send(request);

    expect(result.status).toBe(200);
    expect(result.type).toBe('application/json');

    const expectedResponse = RosettaErrors.invalidTransactionString;

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  test('combine invalid signature', async () => {
    const request: RosettaConstructionCombineRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      unsigned_transaction:
        '00000000010400539886f96611ba3ba6cef9618f8c78118b37c5be0000000000000000000000000000006400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003020000000000051ab71a091b4b8b7661a661c620966ab6573bc2dcd3000000000007a12074657374207472616e73616374696f6e000000000000000000000000000000000000',
      signatures: [
        {
          signing_payload: {
            hex_bytes: 'invalid signature',
            signature_type: 'ecdsa',
          },
          public_key: {
            hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
            curve_type: 'secp256k1',
          },
          signature_type: 'ecdsa',
          hex_bytes:
            '0136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f',
        },
      ],
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/combine`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectedResponse = RosettaErrors.invalidTransactionString;

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  test('combine signature not verified', async () => {
    const request: RosettaConstructionCombineRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      unsigned_transaction:
        '80800000000400539886f96611ba3ba6cef9618f8c78118b37c5be000000000000000000000000000000b4000136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f03020000000000051ab71a091b4b8b7661a661c620966ab6573bc2dcd3000000000007a12074657374207472616e73616374696f6e000000000000000000000000000000000000',
      signatures: [
        {
          signing_payload: {
            hex_bytes:
              '017a33a91515ef48608a99c6adecd2eb258e11534a1acf66348f5678c8e2c8f83d243555ed67a0019d3500df98563ca31321c1a675b43ef79f146e322fe08df751',
            signature_type: 'ecdsa',
          },
          public_key: {
            hex_bytes: '025c13b2fc2261956d8a4ad07d481b1a3b2cbf93a24f992249a61c3a1c4de79c51',
            curve_type: 'secp256k1',
          },
          signature_type: 'ecdsa',
          hex_bytes:
            '017a33a91515ef48608a99c6adecd2eb258e11534a1acf66348f5678c8e2c8f83d243555ed67a0019d3500df98563ca31321c1a675b43ef79f146e322fe08df751',
        },
      ],
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/combine`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectedResponse = RosettaErrors.signatureNotVerified;

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  test('combine invalid public key', async () => {
    const request: RosettaConstructionCombineRequest = {
      network_identifier: {
        blockchain: 'stacks',
        network: 'testnet',
      },
      unsigned_transaction:
        '80800000000400539886f96611ba3ba6cef9618f8c78118b37c5be000000000000000000000000000000b4000136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f03020000000000051ab71a091b4b8b7661a661c620966ab6573bc2dcd3000000000007a12074657374207472616e73616374696f6e000000000000000000000000000000000000',
      signatures: [
        {
          signing_payload: {
            hex_bytes:
              '0136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f',
            signature_type: 'ecdsa',
          },
          public_key: {
            hex_bytes: 'invalid  public key',
            curve_type: 'secp256k1',
          },
          signature_type: 'ecdsa',
          hex_bytes:
            '0136212600bf7463399a23c398f29ca7006b9986b4a01129dd7c6e89314607208e516b0b28c1d850fe6e164abea7b6cceb4aa09700a6d218d1b605d4a402d3038f',
        },
      ],
    };

    const result = await supertest(api.server)
      .post(`/rosetta/v1/construction/combine`)
      .send(request);

    expect(result.status).toBe(400);
    expect(result.type).toBe('application/json');

    const expectedResponse = RosettaErrors.signatureNotVerified;

    expect(JSON.parse(result.text)).toEqual(expectedResponse);
  });

  afterAll(async () => {
    await new Promise(resolve => eventServer.close(() => resolve()));
    await api.terminate();
    client.release();
    await db?.close();
    await runMigrations(undefined, 'down');
  });
});
