import url from 'url'
import { EthLogFactory } from './generated/EthLogFactory'
import {
  createProvider,
  getArgs,
  DEVNET_ADDRESS,
  credentials,
  registerPromiseHandler,
} from './common'
const request = require('request-promise').defaults({ jar: true })

async function main() {
  registerPromiseHandler()
  const args = getArgs(['CHAINLINK_URL', 'ETH_LOG_ADDRESS', 'ECHO_SERVER_URL'])

  await sendEthlogTransaction({
    ethLogAddress: args.ETH_LOG_ADDRESS,
    chainlinkUrl: args.CHAINLINK_URL,
    echoServerUrl: args.ECHO_SERVER_URL,
  })
}
main()

interface Options {
  ethLogAddress: string
  chainlinkUrl: string
  echoServerUrl: string
}
async function sendEthlogTransaction({
  ethLogAddress,
  chainlinkUrl,
  echoServerUrl,
}: Options) {
  const provider = createProvider()
  const signer = provider.getSigner(DEVNET_ADDRESS)
  const ethLog = new EthLogFactory(signer).attach(ethLogAddress)

  const sessionsUrl = url.resolve(chainlinkUrl, '/sessions')
  await request.post(sessionsUrl, { json: credentials })

  const job = {
    _comment: 'An ethlog with no address listens to all addresses.',
    initiators: [{ type: 'ethlog', params: { address: ethLog.address } }],
    tasks: [{ type: 'HttpPost', params: { url: echoServerUrl } }],
  }
  const specsUrl = url.resolve(chainlinkUrl, '/v2/specs')
  const Job = await request.post(specsUrl, { json: job }).catch((e: any) => {
    console.error(e)
    throw Error(`Error creating Job ${e}`)
  })

  console.log('Deployed Job at:', Job.data.id)

  await ethLog.logEvent({ gasLimit: 200000 }).catch(() => {
    throw Error('Error making EthLog entry')
  })

  console.log(`Made EthLog entry`)
}
