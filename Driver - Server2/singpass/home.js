const conf = require('../conf/conf')

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
  let temp = ""
  for (const [env, baseurl] of Object.entries(config.baseUrls)) {

    temp = `${baseurl}/v1/oauth/authorize?response_type=code&client_id=${clientId}&scope=${scopes}&redirect_uri=${redirectURL}/callback&nonce=randomnonce&state=${env}`

    console.log("********************* " + temp);
  }
  //temp = "https://api.id.gov.sg/v1/oauth/authorize?response_type=code&client_id=MOBIUS-TEST&scope=openid%20myinfo.nric_number%20myinfo.name&redirect_uri=https://cv-staging.mobius.sg/mobile-callback&nonce=randomnonce&state=prod"
  //temp = "http://192.168.1.5:5000/callback?tttt=dddd";

  res.redirect(temp)
}

module.exports = index