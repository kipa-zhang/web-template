const conf = require('../conf/conf')

const log = require('../log/winston').logger('singpasshome Service');

const scopes = conf.SgidClient.SCOPES
const clientId = conf.SgidClient.CLIENT_ID
const hostname = conf.SgidClient.HOSTNAME
const redirectURL = conf.SgidClient.REDIRECT_URL

const config = require('../lib/config')

/**
 * Main controller function to generate the home page
 *
 * @param {*} _req
 * @param {*} res
 */
function index(_req, res) {
  var temp = ""
  try {
    for (const [env, baseurl] of Object.entries(config.baseUrls)) {
      temp = `${baseurl}/v1/oauth/authorize?response_type=code&client_id=${clientId}&scope=${scopes}&redirect_uri=${redirectURL}/callback&nonce=randomnonce&state=${env}`
    }
    //temp = "https://api.id.gov.sg/v1/oauth/authorize?response_type=code&client_id=MOBIUS-TEST&scope=openid%20myinfo.nric_number%20myinfo.name&redirect_uri=https://cv-staging.mobius.sg/callback&nonce=randomnonce&state=prod"
    //temp = "http://192.168.1.12:5100/callback";
    console.log("mobileSingpass url:" + temp);
  
    return res.json({code: 1,  singpassUrl: temp})
  } catch (err) {
    log.error(err);
  
    return res.json({code: 0,  respMessage: 'get singpassurl fail'})
  }
}

module.exports = index
