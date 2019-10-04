import { Router, Request, Response } from 'express'
import { getDb } from '../../database'
import { all as allOperators } from '../../entity/ChainlinkNode'

const router = Router()

router.post('/operators', async (req: Request, res: Response) => {
  const db = await getDb()
  const operators = await allOperators(db)
  const data = {
    data: {
      operators,
    },
    meta: {
      count: 0,
    },
  }

  return res.status(200).send(operators)
})

export default router
