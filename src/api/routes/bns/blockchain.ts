import * as express from 'express';
import { RouterWithAsync, addAsync } from '@awaitjs/express';
import { DataStore } from '../../../datastore/common';
import { parsePagingQueryInput } from '../../../api/pagination';
import { bnsBlockchain, BNSErrors } from '../../../bns-constants';
import { BNSGetNameInfoResponse } from '@blockstack/stacks-blockchain-api-types';

export function createBNSBlockchainsRouter(db: DataStore): RouterWithAsync {
    const router = addAsync(express.Router());

    router.getAsync ('/:blockchainName/name_count', async (req, res) => {
        const { blockchainName } = req.params;
        let all = req.query.all;
        let includeExpired = (all === "true") ? 0 : 1 // 0 = show all, 1 = exclude expired
        if ( blockchainName != "stacks") {
            res.status(404).json({"error": "Unsupported blockchain"})
            return
        }
        const countQuery = await db.getNameCount({includeExpired: includeExpired});

        res.json(countQuery.result)
    });
    
    return router;
}