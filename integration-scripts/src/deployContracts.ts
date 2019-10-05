import { LinkTokenFactory } from './generated/LinkTokenFactory'
import { OracleFactory } from './generated/OracleFactory'
import {
  createProvider,
  DEVNET_ADDRESS,
  registerPromiseHandler,
  getArgs,
} from './common'
import { EthLogFactory } from './generated/EthLogFactory'
import { RunLogFactory } from './generated/RunLogFactory'

async function main() {
  registerPromiseHandler()
  const args = getArgs(['CHAINLINK_NODE_ADDRESS'])

  await deployContracts({ chainlinkNodeAddress: args.CHAINLINK_NODE_ADDRESS })
}
main()

interface Args {
  chainlinkNodeAddress: string
}
async function deployContracts({ chainlinkNodeAddress }: Args) {
  const provider = createProvider()
  const signer = provider.getSigner(DEVNET_ADDRESS)

  // deploy LINK token
  const linkTokenFactory = new LinkTokenFactory(signer)
  const linkToken = await linkTokenFactory.deploy()
  await linkToken.deployed()
  console.log(`Deployed LinkToken at: ${linkToken.address}`)

  // deploy Oracle
  const oracleFactory = new OracleFactory(signer)
  const oracle = await oracleFactory.deploy(linkToken.address)
  await oracle.setFulfillmentPermission(chainlinkNodeAddress, true)
  console.log(`Deployed Oracle at: ${oracle.address}`)

  // deploy EthLog contract
  const ethLogFactory = new EthLogFactory(signer)
  const ethLog = await ethLogFactory.deploy()
  await ethLog.deployed()
  console.log(`Deployed EthLog at: ${ethLog.address}`)

  // deploy runlog contract
  const runLogFactory = new RunLogFactory(signer)
  const runLog = await runLogFactory.deploy(linkToken.address, oracle.address)
  await runLog.deployed()
  console.log(`Deployed RunLog at: ${runLog.address}`)
}
