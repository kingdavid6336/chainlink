import http from 'http'
import httpStatus from 'http-status-codes'
import { Connection } from 'typeorm'
import { closeDbConnection, getDb } from '../../../database'
import { clearDb } from '../../testdatabase'
import { createAdmin } from '../../../support/admin'
import {
  createChainlinkNode,
  find as findNode,
} from '../../../entity/ChainlinkNode'
import { start as testServer } from '../../../support/server'
import {
  sendAdminAuthPost,
  sendAdminAuthDelete,
} from '../../../support/supertest'

const USERNAME = 'myadmin'
const PASSWORD = 'validpassword'
const ADMIN_PATH = '/api/v1/admin'
const adminNodesPath = `${ADMIN_PATH}/nodes`

let server: http.Server
let db: Connection

beforeAll(async () => {
  db = await getDb()
  server = await testServer()
})
afterAll(async done => {
  if (server) {
    server.close(done)
    await closeDbConnection()
  }
})
beforeEach(async () => {
  await clearDb()
  await createAdmin(db, USERNAME, PASSWORD)
})

describe('POST /nodes', () => {
  it('can create a node and returns the generated information', done => {
    const data = { name: 'nodeA', url: 'http://nodea.com' }

    sendAdminAuthPost(server, adminNodesPath, data, USERNAME, PASSWORD)
      .expect(httpStatus.CREATED)
      .expect(res => {
        expect(res.body.id).toBeDefined()
        expect(res.body.accessKey).toBeDefined()
        expect(res.body.secret).toBeDefined()
      })
      .end(done)
  })

  it('returns an error with invalid params', done => {
    const data = { url: 'http://nodea.com' }

    sendAdminAuthPost(server, adminNodesPath, data, USERNAME, PASSWORD)
      .expect(httpStatus.UNPROCESSABLE_ENTITY)
      .expect(res => {
        const errors = res.body.errors

        expect(errors).toBeDefined()
        expect(errors.name).toEqual({
          minLength: 'must be at least 3 characters',
        })
      })
      .end(done)
  })

  it('returns an error when the node already exists', async done => {
    const [node] = await createChainlinkNode(db, 'nodeA')
    const data = { name: node.name }

    sendAdminAuthPost(server, adminNodesPath, data, USERNAME, PASSWORD)
      .expect(httpStatus.CONFLICT)
      .end(done)
  })

  it('returns a 401 unauthorized with invalid admin credentials', done => {
    sendAdminAuthPost(server, adminNodesPath, {}, USERNAME, 'invalidpassword')
      .expect(httpStatus.UNAUTHORIZED)
      .end(done)
  })
})

describe('DELETE /nodes/:name', () => {
  function path(name: string): string {
    return `${adminNodesPath}/${name}`
  }

  it('can delete a node', async done => {
    const [node] = await createChainlinkNode(db, 'nodeA')

    sendAdminAuthDelete(server, path(node.name), {}, USERNAME, PASSWORD)
      .expect(httpStatus.OK)
      .expect(async () => {
        const nodeAfter = await findNode(db, node.id)
        expect(nodeAfter).not.toBeDefined()
      })
      .end(done)
  })

  it('returns a 401 unauthorized with invalid admin credentials', done => {
    sendAdminAuthDelete(
      server,
      path('idontexist'),
      {},
      USERNAME,
      'invalidpassword',
    )
      .expect(httpStatus.UNAUTHORIZED)
      .end(done)
  })
})
