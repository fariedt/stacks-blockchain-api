import * as express from 'express';
import { RouterWithAsync, addAsync } from '@awaitjs/express';
import { DataStore } from '../../../datastore/common';
import { parsePagingQueryInput } from '../../../api/pagination';
import { bnsBlockchain, BNSErrors } from '../../../bns-constants';
import { BNSGetNameInfoResponse } from '@blockstack/stacks-blockchain-api-types';
import { UnderscoreEscapedMap } from 'typescript';

export function createBNSBlockchainsRouter(db: DataStore): RouterWithAsync {
    const router = addAsync(express.Router());

    router.getAsync ('/:blockchainName/name_count', async (req, res) => {
        const { blockchainName } = req.params;
        if ( blockchainName != "stacks") {
            res.status(404).json({"error": "Unsupported blockchain"});
            return;
        }

        let ignoreBlocks = 0;
        let all = req.query.all;
        console.log("All query", all)
        if (all === "false") {
            const blockHeightQuery = await db.getCurrentBlock();
            console.log("block height received", blockHeightQuery);
            console.log("block height found ", blockHeightQuery.found );
            if (blockHeightQuery.found === true) {
                let recentBlockHeight = blockHeightQuery.result.block_height;
                ignoreBlocks = recentBlockHeight;
                console.log("Recent block height: ",recentBlockHeight );
            }
        }
        console.log("ignore blocks:", ignoreBlocks);
        const countQuery = await db.getNameCount({expired: ignoreBlocks});

        res.json(countQuery.result)
    });
    
    return router;
}