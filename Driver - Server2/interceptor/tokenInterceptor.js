const express = require('express');
const router = express.Router();
const jwt = require('jsonwebtoken');
const jwtConf = require('../conf/jwt');

const utils = require('../util/utils');
const log = require('../log/winston').logger('Token Interceptor');

router.use(async (req, res, next) => {
    if(req.url.indexOf('?') >= 0) {
        req.url = req.url.substring(0, req.url.indexOf('?'))
    }
    if (req.url == '/home' || req.url.startsWith('/callback') || req.url == '/loginUseSingpass' || req.url == '/guide' 
    || req.url== '/user/registerUser' || req.url== '/user/getSystemRole' || req.url == '/user/registerAccountUser' || req.url == '/assign/initSystemTaskByTripId'
    || req.url == '/user/getAccountUserData' || req.url == '/user/getSystemUrl' || req.url == '/user/getServiceTypeBySelectedGroup'
    || req.url == '/user/getHqTypeList' || req.url == '/user/urlParameterDecode' || req.url == '/user/getUserBaseByUserId' || req.url == '/user/editAccountUser') {
        next();
    } else if (req.url !== '/login' && (!req.session.token && !req.cookies.token) && !req.query.token) {
        if (req.method.toLowerCase() == 'get') {
            return res.redirect('../../login')
        } else {
            res.json(utils.response(-100, 'Token is invalid !'));
        }
    } else if (req.url === '/login') {
        next();
    } else if (req.url.startsWith('/') && req.query.token) {
        next();
    } else {
        let token = req.url.includes('/mobile') ? req.header('Authorization') : req.session.token ?? req.cookies.token ;
        if (!token) {
            log.warn('There is no token !');
            res.json(utils.response(-100, 'There is no token !'));
        } else {
            // https://www.npmjs.com/package/jsonwebtoken
            jwt.verify(token, jwtConf.Secret, { algorithms: jwtConf.Header.algorithm.toUpperCase() }, function (err) {
                if (err) {
                    if (err.expiredAt) {
                        // while token is out of time, update it directly
                        log.warn('(Token Interceptor): Token is expired at ', err.expiredAt);

                        let result = jwt.decode(token, jwtConf.Secret);
                        let newJwtToken = utils.generateTokenKey({ userId: result.data.userId })

                        res.cookie('token', newJwtToken, { expires: utils.expiresCookieDate() });
                        req.session.token = newJwtToken

                        if (result.data.userId) {
                            req.cookies.userId = result.data.userId;
                            req.body.userId = result.data.userId
                        }

                        log.warn('(Token Interceptor): Token is updated now ! ');
                        next();
                        // If coding here, still will run the code!!!
                    } else {
                        log.warn('(Token Interceptor): Token is invalid !');
                        res.json(utils.response(-100, 'Token is invalid !'));
                    }
                } else {
                    log.info('(Token Interceptor): Token is correct !');

                    // get userId
                    let result = jwt.decode(token, jwtConf.Secret);
                    if (result.data.userId) {
                        req.cookies.userId = result.data.userId;
                        req.body.userId = result.data.userId
                    }

                    next();
                    // If coding here, still will run the code!!!
                }
            });

        }
    }
});
module.exports = router;