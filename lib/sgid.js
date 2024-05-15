const fetch = require('node-fetch')
const jwt = require('jsonwebtoken')
const { JWE, JWK, JWS } = require('node-jose')

/**
 * Fetches the token from the oauth endpoint
 *
 * @param {string} baseUrl
 * @param {string} code
 * @param {string} hostname
 * @param {string} clientId
 * @param {string} clientSecret
 *
 * @returns {object} { accessToken }
 */
async function fetchToken(baseUrl, clientId, clientSecret, redirectUri, code) {
  for (const [key, value] of Object.entries({
    baseUrl,
    clientId,
    clientSecret,
    redirectUri,
    code,
  })) {
    if (!value) throw Error(`${key} cannot be empty`)
  }

  const response = await fetch(`${baseUrl}/v1/oauth/token`, {
    // make a POST request
    method: 'POST',
    cache: 'no-cache',
    // Set the content type header, so that we get the response in JSON
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify({
      client_id: clientId,
      client_secret: clientSecret,
      redirect_uri: redirectUri,
      code,
      grant_type: 'authorization_code',
    }),
  })

  const body = await response.json()
  if (response.ok) {
    const { access_token, id_token } = body
    const sub = await verifyAndDecodeIdToken(baseUrl, id_token)
    return { sub, accessToken: access_token }
  } else {
    handleResponseError(body)
  }
}

/**
 * Fetches the user info
 *
 * @param {string} baseUrl
 * @param {string} accessToken
 * @param {string} privateKeyPem in pem format
 * @return {object} { sub: string, data: array }
 */
async function fetchUserInfo(baseUrl, accessToken, privateKeyPem) {
  for (const [key, value] of Object.entries({ baseUrl, accessToken })) {
    if (!value) throw Error(`${key} cannot be empty`)
  }

  const response = await fetch(`${baseUrl}/v1/oauth/userinfo`, {
    method: 'GET',
    cache: 'no-cache',
    headers: {
      Accept: 'application/json',
      Authorization: `Bearer ${accessToken}`,
    },
  })

  const body = await response.json()
  if (response.ok) {
    const { sub, key, data } = body

    let decrypted = []
    if (data && key) {
      decrypted = await decryptData(key, data, privateKeyPem)
    } else {
      throw Error('Missing key and/or data in userinfo')
    }
    return { sub, data: decrypted }
  } else {
    handleResponseError(body)
  }
}

/**
 * Verifies the id token from the jwks endpoint
 *
 * @param {string} baseUrl
 * @param {string} token
 * @returns {string}
 */
async function verifyAndDecodeIdToken(baseUrl, token) {
  const response = await fetch(`${baseUrl}/.well-known/jwks.json`, {
    // make a POST request
    method: 'GET',
    cache: 'no-cache',
    // Set the content type header, so that we get the response in JSON
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
  })

  const body = await response.json()
  if (response.ok) {
    const keystore = await JWK.asKeyStore(body)
    const verified = await JWS.createVerify(keystore).verify(token)
    const decoded = Buffer.from(verified.payload).toJSON()

    return decoded.sub
  } else {
    handleResponseError(body)
  }
}

/**
 * Decrypts the block into an object of
 * plaintext key-value pairs
 *
 * @param {string} encKey
 * @param {array} block
 * @param {string} privateKeyPem in pem format
 * @returns {object}
 */
async function decryptData(encKey, block, privateKeyPem) {
  const result = {}

  // Parse private key
  let privateKey
  try {
    privateKey = await JWK.asKey(privateKeyPem, 'pem')
  } catch (e) {
    throw new Error("Unable to parse client's private key")
  }

  // Decrypted encKey to get symmetric key
  let key
  try {
    key = await JWE.createDecrypt(privateKey).decrypt(encKey)
  } catch (e) {
    throw new Error('Unable to decrypt block key')
  }

  // Parse symmetric key
  let decryptedKey
  try {
    decryptedKey = await JWK.asKey(key.plaintext, 'json')
  } catch (e) {
    throw new Error('Unable to parse decrypted symmetric key')
  }

  // Decrypt myinfo data
  for (const [key, value] of Object.entries(block)) {
    try {
      const { plaintext } = await JWE.createDecrypt(decryptedKey).decrypt(value)
      result[key] = plaintext.toString('ascii')
    } catch (e) {
      throw new Error('Unable to decrypt field')
    }
  }
  return result
}

function handleResponseError(body) {
  throw new Error(
    body.error_description || body.error || body.description || body.code
  )
}

module.exports = {
  decryptData,
  verifyAndDecodeIdToken,
  fetchToken,
  fetchUserInfo,
}
