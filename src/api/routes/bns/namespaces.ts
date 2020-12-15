import * as express from 'express';
import { RouterWithAsync, addAsync } from '@awaitjs/express';
import { DataStore } from '../../../datastore/common';
import { parsePagingQueryInput } from '../../../api/pagination';
import { BNSErrors } from '../../bns-constants';
import { BNSGetAllNamespacesResponse } from '@blockstack/stacks-blockchain-api-types';

export function createBNSNamespacesRouter(db: DataStore): RouterWithAsync {
  const router = addAsync(express.Router());

  router.getAsync('/', async (req, res) => {
    const { results } = await db.getNamespaceList();
    const response: BNSGetAllNamespacesResponse = {
      namespaces: results,
    };
    return res.json(response);
  });

  router.getAsync('/:tld/names', async (req, res) => {
    const { tld } = req.params;
    const page = parsePagingQueryInput(req.query.page ?? 0);

    const response = await db.getNamespace({ namespace: tld });
    if (!response.found) {
      res.status(404).json(BNSErrors.NoSuchNamespace);
    } else {
      const { results } = await db.getNamespaceNamesList({ namespace: tld, page });
      if (results.length === 0 && req.query.page) {
        res.status(400).json(BNSErrors.InvalidPageNumber);
      }
      res.json(results);
    }
  });

  return router;
}
