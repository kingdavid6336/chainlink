import http from 'http'
import request from 'supertest'
import {
  ADMIN_USERNAME_HEADER,
  ADMIN_PASSWORD_HEADER,
} from '../utils/constants'

interface HttpHeaders {
  [key: string]: string
}

const BASE_HEADERS: HttpHeaders = {
  Accept: 'application/json',
  'Content-Type': 'application/json',
}

function authHeaders(username: string, password: string): HttpHeaders {
  return {
    [ADMIN_USERNAME_HEADER]: username,
    [ADMIN_PASSWORD_HEADER]: password,
  }
}

export function sendGet(
  server: http.Server,
  path: string,
  extraHeaders: HttpHeaders = {},
) {
  const headers = { ...BASE_HEADERS, ...extraHeaders }
  const r = request(server).get(path)

  return Object.keys(headers).reduce((acc, k) => {
    return acc.set(k, headers[k])
  }, r)
}

export function sendAdminAuthGet(
  server: http.Server,
  path: string,
  username: string,
  password: string,
) {
  const headers = authHeaders(username, password)
  return sendGet(server, path, headers)
}

export function sendDelete(
  server: http.Server,
  path: string,
  data: object,
  extraHeaders: HttpHeaders = {},
) {
  const headers = { ...BASE_HEADERS, ...extraHeaders }
  const r = request(server)
    .delete(path)
    .send(data)

  return Object.keys(headers).reduce((acc, k) => {
    return acc.set(k, headers[k])
  }, r)
}

export function sendAdminAuthDelete(
  server: http.Server,
  path: string,
  data: object,
  username: string,
  password: string,
) {
  const headers = authHeaders(username, password)
  return sendDelete(server, path, data, headers)
}

export function sendPost(
  server: http.Server,
  path: string,
  data: object,
  extraHeaders: HttpHeaders = {},
) {
  const headers = { ...BASE_HEADERS, ...extraHeaders }
  const r = request(server)
    .post(path)
    .send(data)

  return Object.keys(headers).reduce((acc, k) => {
    return acc.set(k, headers[k])
  }, r)
}

export function sendAdminAuthPost(
  server: http.Server,
  path: string,
  data: object,
  username: string,
  password: string,
) {
  const headers = authHeaders(username, password)
  return sendPost(server, path, data, headers)
}
