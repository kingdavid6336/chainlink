import { ethers } from 'ethers'
import chalk from 'chalk'

/**
 * Devnet miner address
 */
export const DEVNET_ADDRESS = '0x9CA9d2D5E04012C9Ed24C0e513C9bfAa4A2dD77f'

/**
 * Default credentials for testing node
 */
export const credentials = {
  email: 'notreal@fakeemail.ch',
  password: 'twochains',
}

export function createProvider(): ethers.providers.JsonRpcProvider {
  const port = process.env.ETH_HTTP_PORT || `18545`
  const providerURL = process.env.ETH_HTTP_URL || `http://localhost:${port}`

  return new ethers.providers.JsonRpcProvider(providerURL)
}

class MissingEnvVarError extends Error {
  constructor(envKey: string) {
    super()
    this.name = 'MissingEnvVarError'
    this.message = this.formErrorMsg(envKey)
  }

  private formErrorMsg(envKey: string) {
    const errMsg = `Not enough arguments supplied. 
      Expected "${envKey}" to be supplied as environment variable.`

    return errMsg
  }
}

export function getArgs<T extends string>(keys: T[]): { [K in T]: string } {
  return keys.reduce<{ [K in T]: string }>(
    (prev, next) => {
      const envVar = process.env[next]
      if (!envVar) {
        throw new MissingEnvVarError(next)
      }
      prev[next] = envVar
      return prev
    },
    {} as { [K in T]: string },
  )
}

/**
 * Registers a global promise handler that will exit the currently
 * running process if an unhandled promise rejection is caught
 */
export function registerPromiseHandler() {
  process.on('unhandledRejection', e => {
    console.error(chalk.red(e as any))
    console.error(chalk.red('Exiting due to promise rejection'))
    process.exit(1)
  })
}
