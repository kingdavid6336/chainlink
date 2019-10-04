import http from 'http'
import { Connection } from 'typeorm'
import { closeDbConnection, getDb } from '../../../database'
import { clearDb } from '../../testdatabase'
import { createAdmin } from '../../../support/admin'
import { start as testServer } from '../../../support/server'
import { sendAdminAuthPost } from '../../../support/supertest'
import {
  ChainlinkNode,
  createChainlinkNode,
} from '../../../entity/ChainlinkNode'

const USERNAME = 'myadmin'
const PASSWORD = 'validpassword'
const adminLoginPath = '/api/v1/admin/login'

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

describe('#index', () => {
  beforeEach(async () => {
    await clearDb()
    await createAdmin(db, USERNAME, PASSWORD)
  })

  it('returns the operators attributes safe for admin consumption', async done => {
    await createChainlinkNode(db, 'nodeA')

    sendAdminAuthPost(server, adminLoginPath, {}, USERNAME, PASSWORD)
      .expect(200)
      .expect(res => {
        console.log('--- OPERATORS: %o', res.body)
        // expect(res.body.id).toBeDefined()
        // expect(res.body.accessKey).toBeDefined()
        // expect(res.body.secret).toBeDefined()
      })
      .end(done)
  })

  it('returns a 401 unauthorized with invalid admin credentials', done => {
    sendAdminAuthPost(server, adminLoginPath, {}, USERNAME, 'invalidpassword')
      .expect(401)
      .end(done)
  })
})
