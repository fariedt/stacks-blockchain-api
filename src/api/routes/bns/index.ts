import * as express from 'express';
import { RouterWithAsync, addAsync } from '@awaitjs/express';
import { DataStore } from '../../../datastore/common';
import { parsePagingQueryInput } from '../../../api/pagination';
import { computeNamespacePrice, computeNamePrice } from '../../../helpers';

export function createBNSNamespacesRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());

  router.getAsync('/', async (req, res) => {
    const { results } = await db.getNamespaceList();
    return res.json(results);
  });

  router.getAsync('/:tld/names', async (req, res) => {
    const { tld } = req.params;
    const page = parsePagingQueryInput(req.query.page ?? 0);

    const { results } = await db.getNamespaceNamesList({ namespace: tld, page });
    res.json(results);
  });

  return router;
}

export function createBNSNamesRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());

  router.getAsync('/', async (req, res) => {
    const page = parsePagingQueryInput(req.query.page ?? 0);

    const { results } = await db.getNamesList({ page });

    res.json(results);
  });

  router.getAsync('/:name', async (req, res) => {
    const { name } = req.params;

    const nameQuery = await db.getName({ name });
    if (!nameQuery.found) {
      return res.status(404).json({ error: `cannot find name ${name}` });
    }

    res.json(nameQuery.result);
  });

  router.getAsync('/:name/history', (req, res) => {
    const response = {
      '373821': [
        {
          address: '1QJQxDas5JhdiXhEbNS14iNjr8auFT96GP',
          block_number: 373821,
          consensus_hash: null,
          first_registered: 373821,
          importer: '76a9143e2b5fdd12db7580fb4d3434b31d4fe9124bd9f088ac',
          importer_address: '16firc3qZU97D1pWkyL6ZYwPX5UVnWc82V',
          last_creation_op: ';',
          last_renewed: 373821,
          name: 'muneeb.id',
          name_hash128: 'deb7fe99776122b77925cbf0a24ab6f8',
          namespace_block_number: 373601,
          namespace_id: 'id',
          op: ';',
          op_fee: 100000.0,
          opcode: 'NAME_IMPORT',
          preorder_block_number: 373821,
        },
      ],
    };
    res.json(response);
  });

  router.getAsync('/:name/zonefile/:zoneFileHash', (req, res) => {
    const response = {
      zonefile:
        '$ORIGIN muneeb.id\n$TTL 3600\n_http._tcp IN URI 10 1 "https://blockstack.s3.amazonaws.com/muneeb.id"\n',
    };

    res.json(response);
  });

  return router;
}

export function createBNSSubdomainsRouterRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());

  router.getAsync('/', async (req, res) => {
    const page = parsePagingQueryInput(req.query.page ?? 0);

    const { results } = await db.getSubdomainsList({ page });
    res.json(results);
  });

  router.getAsync('/:txid', (req, res) => {
    const response = [
      {
        accepted: 1,
        block_height: 546199,
        domain: 'id.blockstack',
        fully_qualified_subdomain: 'nturl345.id.blockstack',
        missing: '',
        owner: '17Q8hcsxRLCk3ypJiGeXQv9tFK9GnHr5Ea',
        parent_zonefile_hash: '58224144791919f6206251a9960a2dd5723b96b6',
        parent_zonefile_index: 95780,
        resolver: 'https://registrar.blockstack.org',
        sequence: 0,
        signature: 'None',
        txid: 'd04d708472ea3c147f50e43264efdb1535f71974053126dc4db67b3ac19d41fe',
        zonefile_hash: 'd3bdf1cf010aac3f21fac473e41450f5357e0817',
        zonefile_offset: 0,
      },
      {
        accepted: 1,
        block_height: 546199,
        domain: 'id.blockstack',
        fully_qualified_subdomain: 'dwerner1.id.blockstack',
        missing: '',
        owner: '17tFeKEBMUAAiHVsCgqKo8ccwYqq7aCn9X',
        parent_zonefile_hash: '58224144791919f6206251a9960a2dd5723b96b6',
        parent_zonefile_index: 95780,
        resolver: 'https://registrar.blockstack.org',
        sequence: 0,
        signature: 'None',
        txid: 'd04d708472ea3c147f50e43264efdb1535f71974053126dc4db67b3ac19d41fe',
        zonefile_hash: 'ab79b1774fa7a4c5709b6ad4e5892fb7c0f79765',
        zonefile_offset: 1,
      },
    ];
    res.json(response);
  });

  return router;
}

export function createBNSAddressesRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());

  router.getAsync('/:blockchain/:address', (req, res) => {
    const response = {
      names: ['muneeb.id'],
    };
    res.json(response);
  });

  return router;
}

export function createBNSPriceRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());

  router.getAsync('/namespaces/:namespace', async (req, res) => {
    const { namespace } = req.params;

    const namespaceQuery = await db.getNamespace({ namespace });
    if (!namespaceQuery.found) {
      return res.status(404).json({ error: `cannot find namespace ${namespace}` });
    }

    const response = {
      units: 'STX',
      amount: computeNamespacePrice(namespaceQuery.result).toString(),
    };

    res.json(response);
  });

  router.getAsync('/names/:name', async (req, res) => {
    const { name } = req.params;

    const nameQuery = await db.getName({ name });
    if (!nameQuery.found) {
      return res.status(404).json({ error: `cannot find name ${name}` });
    }

    const response = {
      units: 'STX',
      amount: computeNamePrice(nameQuery.result).toString(),
    };

    res.json(response);
  });

  return router;
}
