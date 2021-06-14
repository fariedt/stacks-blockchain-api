import { StacksMocknet } from '@stacks/network';
import {
  broadcastTransaction,
  bufferCVFromString,
  ChainID,
  contractPrincipalCV,
  makeContractCall,
} from '@stacks/transactions';
import * as express from 'express';
import {
  ChainlinkFulfillmentResponse,
  createOracleFulfillmentTx,
  parseOracleRequestValue,
} from '../../integration-helper';
import { logger } from '../../helpers';

export function createIntegrationRouter(chainId: ChainID) {
  const router = express.Router();
  router.use(express.json());

  // change route name to fulfil-chainlink-request
  router.post('/chainlink', async (req, res) => {
    try {
      console.log('Sajjad-> chainlink route');
      console.log('Sajjad-> ', req.body.data.encoded_data);
      const fulfillment = parseOracleRequestValue(req.body.data.encoded_data);
      const linkFulfillment: ChainlinkFulfillmentResponse = {
        result: req.body.result,
        fulfillment: fulfillment,
      };
      const response = await createOracleFulfillmentTx(linkFulfillment, chainId);
      const txid = response.txid();
      logger.verbose(`Chainlink request fulfillment txid: 0x${txid}`);
      res.status(200).json({ txid: txid });
    } catch (err) {
      res.status(500).json({ msg: err.message });
    }
  });

  // For testing purposes only, to be removed
  router.get('/consumer-get-eth-price', async (req, res) => {
    const network = new StacksMocknet();
    const txOptions = {
      contractAddress: 'ST248M2G9DF9G5CX42C31DG04B3H47VJK6W73JDNC',
      contractName: 'consumercontract',
      functionName: 'get-eth-price',
      functionArgs: [
        bufferCVFromString('0xde5b9eb9e7c5592930eb2e30a01369'),
        contractPrincipalCV('ST248M2G9DF9G5CX42C31DG04B3H47VJK6W73JDNC', 'consumercontract'),
      ],
      senderKey: '4773c54317d082ff5cce3976e6a2a1b691f65ab82ec59e98fe97460a922019ee01',
      validateWithAbi: true,
      network,
      postConditions: [],
    };
    try {
      const transaction = await makeContractCall(txOptions);
      const _ = broadcastTransaction(transaction, network);
      res.status(200).json({
        txid: transaction.txid(),
      });
    } catch (err) {
      res.status(400).json({ msg: err.message });
    }
  });

  return router;
}
