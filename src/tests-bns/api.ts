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

describe('BNS API', () => {
  let db: PgDataStore;
  let client: PoolClient;
  let eventServer: Server;
  let api: ApiServer;

  beforeAll(async () => {
    process.env.PG_DATABASE = 'postgres';
    await cycleMigrations();
    db = await PgDataStore.connect();
    client = await db.pool.connect();
    eventServer = await startEventServer({ db });
    api = await startApiServer(db);
  });

  test('Success namespaces', async () => {
    const query1 = await supertest(api.server).get(`/v1/namespaces`);
    expect(query1.status).toBe(200);
    expect(query1.type).toBe('application/json');
  });

  test('Namespace response schema', async () => {
    const query1 = await supertest(api.server).get('/v1/namespaces');
    const result = JSON.parse(query1.text);
    const path = require.resolve(
      '@blockstack/stacks-blockchain-api-types/api/bns/namespace-operations/bns-get-all-namespaces-response.schema.json'
    );
    const valid = await validate(path, result);
    expect(valid.valid).toBe(true);
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

  test('Validate: names length from /v1/names', async () => {
    const query1 = await supertest(api.server).get('/v1/names');
    const result = JSON.parse(query1.text);
    expect(result.length).toBe(0);
  });

  test('Invalid page from /v1/names', async () => {
    const query1 = await supertest(api.server).get('/v1/names?page=1');
    expect(query1.status).toBe(400);
  });

  test('Success Subdomain', async () => {
    const query1 = await supertest(api.server).get(`/v1/subdomains`);
    expect(query1.status).toBe(200);
    expect(query1.type).toBe('application/json');
  });

  afterAll(async () => {
    await new Promise(resolve => eventServer.close(() => resolve()));
    await api.terminate();
    client.release();
    await db?.close();
    await runMigrations(undefined, 'down');
  });
});
