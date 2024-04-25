const conf = require('../conf/conf')

const log = require('../log/winston').logger('singpasscallback Service');

const clientId = conf.SgidClient.CLIENT_ID
const clientSecret = conf.SgidClient.CLIENT_SECRET
const hostname = conf.SgidClient.HOSTNAME

const privateKey = conf.SgidClient.PRIVATE_KEY
const publicKey = conf.SgidClient.PUBLIC_KEY

const { User } = require('../model/user')
const { Driver } = require('../model/driver')
const CONTENT = require('../util/content');
const utils = require('../util/utils.js');

const sgid = require('../lib/sgid')
const config = require('../lib/config')

/**
 * Main controller function to generate the callback page
 *
 * @param {*} req
 * @param {*} res
 */
async function index(req, res) {
  try {
    const { code, state } = req.query
    const baseurl = config.baseUrls[state]

    const { accessToken } = await fetchToken(baseurl, code)
    const { sub, data } = await fetchUserInfo(
      baseurl,
      accessToken,
      privateKey
    )

    let nric = ""
    let name = ""
    for (const [key, value] of data) {
      if (key == "NRIC NUMBER") {
        nric = value
      } else if(key == "NAME") {
        name = value
      }
    }

    // let name = "cr100-1"
    // let nric = "S4444578U"
    // let sub = 'adadsdf'
    console.log("mobileTo singpass callback nric: " + nric + ", sgid: " + sub)

    let loginName = nric.substring(0,1) + nric.substring(nric.length - 4) + name.replace(/\s*/g, '').substring(0,3);
    loginName = loginName.toUpperCase();

    let mobileDriver = await Driver.findOne({ where: { loginName: loginName } })
    if (!mobileDriver) {
      log.warn(`Mobile user login by singpass fail, fullName:${name}, nric:${nric}, loginName:${loginName} driver doesn't exist!`);
      
      res.render('singpassCallback', {
        code: '0',
        nric: loginName + '@' + name,
        singpassError: `Mobile user ${loginName} does not exist!`
      })
    } else {
      let mobileUser = await User.findOne({ where: { username: mobileDriver.loginName, userType: CONTENT.USER_TYPE.MOBILE } })
      if (mobileUser) {
        let aesNric = utils.generateAESCode(nric).toUpperCase();
        mobileUser.sgid = sub
        mobileUser.fullName = name;
        mobileUser.nric = aesNric;
        await mobileUser.save()

        mobileDriver.driverName = name;
        mobileDriver.nric = aesNric;
        await mobileDriver.save();

        res.render('singpassCallback', {
          code: 1,
          nric: loginName + '@' + name,
          singpassError: 'success!'
        })
      } else {
        log.warn(`Mobile user login by singpass fail, fullName:${name}, nric:${nric}, loginName:${loginName} user doesn't exist!`);

        res.render('singpassCallback', {
          code: '0',
          nric: loginName + '@' + name,
          singpassError: `Mobile user ${loginName} does not exist!`
        })
      }
    }
  } catch (error) {
    log.error(error);
    res.render('singpassCallback', {
      code: '0',
      nric: 'error', 
      singpassError: error && error.message ? error.message : "Singpass callback error!"
    })
  }
}

/**
 * Fetches the token from the oauth endpoint
 *
 * @param {string} baseUrl
 * @param {string} code
 */
async function fetchToken(baseUrl, code) {
  try {
    return await sgid.fetchToken(
      baseUrl,
      clientId,
      clientSecret,
      `${hostname}/callback`,
      code
    )
  } catch (error) {
    console.error(`Error in fetchToken: ${error.message}`)
    throw error
  }
}

/**
 * Fetches user info
 *
 * @param {string} baseUrl
 * @param {string} accessToken
 * @param {string} privateKeyPem
 * @return {object} { sub: string, data: array }
 */
async function fetchUserInfo(baseUrl, accessToken, privateKeyPem) {
  try {
    const { sub, data } = await sgid.fetchUserInfo(
      baseUrl,
      accessToken,
      privateKeyPem
    )
    return {
      sub,
      data: formatData(data),
    }
  } catch (error) {
    console.error(`Error in fetchUserInfo: ${error.message}`)
    throw error
  }
}

/**
 * Formats the data into an array of arrays,
 * specifically for the display on the frontend
 *
 * @param {object} result
 * @returns {array}
 */
function formatData(result) {
  const formattedResult = []

  for (const [key, value] of Object.entries(result)) {
    formattedResult.push([prettifyKey(key), value])
  }

  return formattedResult
}

/**
 * Converts a key string from dot-delimited into uppercase
 * for frontend display
 *
 * @param {string} key
 * @returns {string}
 */
function prettifyKey(key) {
  let prettified = key.split('.')[1]
  prettified = prettified.replace(/_/g, ' ')
  return prettified.toUpperCase()
}

module.exports = index
