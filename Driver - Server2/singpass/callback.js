const log = require('../log/winston').logger('singpass callback Service');
const conf = require('../conf/conf')
const CONTENT = require('../util/content');

const clientId = conf.SgidClient.CLIENT_ID
const clientSecret = conf.SgidClient.CLIENT_SECRET
const hostname = conf.SgidClient.HOSTNAME

const privateKey = conf.SgidClient.PRIVATE_KEY
const publicKey = conf.SgidClient.PUBLIC_KEY

const { User } = require('../model/user')

const sgid = require('../lib/sgid')
const config = require('../lib/config')

const { Sequelize, Op, QueryTypes } = require('sequelize');
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
    let temp = ""

    for (const [key, value] of data) {
      if (key == "NRIC NUMBER") {
        temp = value
      }
      else if(key == "NAME") {
        name = value
      }
    }

    // let name = "lf-test"
    // let temp = "S8654565T"
    // let sub = 'adadsdf'

    nric = temp.substring(0,1) + temp.substring(temp.length - 4) + name.replace(/\s*/g, '').substring(0,3);

    let user = await User.findOne({
        where: {
          username: nric,
          fullName: name,
          userType: {
              [Op.ne]: CONTENT.USER_TYPE.MOBILE
          }
        }
    });
    if (user) {
      if (!user.enable) {
        log.warn(`Server user login by singpass fail, fullName:${name}, nric:${temp} user is disabled!`);
        res.render('login/index', {
          title: 'Welcome Mobius',
          singpassError: 'Access denied:User is deactivated,please contact administrator.',
          loginError: 'Access denied:User is deactivated,please contact administrator.'
        })
      } else {
        user.sgid = sub
        await user.save()
        res.render('callback', {
          code: 1,
          nric: nric + '@' + name,
          error: 'Login successful.'
        })
      }
    } else {
      log.warn(`Server user login by singpass fail, fullName:${name}, nric:${temp} doesn't exist!`);
      const error = "Access denied."
      console.log(error)
      res.render('login/index', {
        title: 'Welcome Mobius',
        singpassError: error,
        loginError: error
      })
    }
  } catch (error) {
    log.error(error);
    res.render('login/index', {
      title: 'Welcome Mobius',
      singpassError: error && error.message ? error.message : "Singpass callback error!",
      loginError: error && error.message ? error.message : "Singpass callback error!"
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
