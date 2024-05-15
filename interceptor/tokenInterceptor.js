const express = require('express');
const router = express.Router();
const utils = require('../util/utils');
const jwt = require('jsonwebtoken');
const jwtConf = require('../conf/jwt');

const log = require('../log/winston').logger('Token Interceptor');

router.use(async (req, res, next) => {
    if (req.url === '/mobileLogin' || req.url === '/mobileLogout' || req.url === '/getAppointList' || req.method.toLowerCase() === 'get') {
        next();
    } else {
        let auth = req.header('Authorization');
        if (!auth) {
            log.warn('There is no token !');
            return res.json(utils.response(0, 'There is no token !'));
        } else {
            log.info('Token: ' + auth)
        }

        // https://www.npmjs.com/package/jsonwebtoken
        jwt.verify(auth, jwtConf.Secret, { algorithms: jwtConf.Header.algorithm.toUpperCase() }, async function (err) {
            if (err) {
                if (err.expiredAt) {
                    log.warn('(Token Interceptor): Token is expired at ', err.expiredAt);
                    return res.json(utils.response(-100, 'Token is over time !'));
                } else {
                    log.warn('(Token Interceptor): Token is invalid !');
                    return res.json(utils.response(0, 'Token is not correct !'));
                }
            } else {
                log.warn('(Token Interceptor): Token is correct !');
                next();
            }
        });
    }
});
module.exports = router;