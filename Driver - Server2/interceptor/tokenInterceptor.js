const express = require('express');
const router = express.Router();
const jwt = require('jsonwebtoken');
const jwtConf = require('../conf/jwt');

const utils = require('../util/utils');
const log = require('../log/winston').logger('Token Interceptor');

const whitePageList = ['/home', '/callback', '/loginUseSingpass', '/guide', '/user/registerUser', '/user/getSystemRole', '/user/registerAccountUser',
     '/assign/initSystemTaskByTripId', '/user/getAccountUserData', '/user/getSystemUrl', '/user/getServiceTypeBySelectedGroup',
     '/user/getHqTypeList', '/user/urlParameterDecode', '/user/getUserBaseByUserId', '/user/editAccountUser'];

router.use(async (req, res, next) => {
    if(req.url.indexOf('?') >= 0) {
        req.url = req.url.substring(0, req.url.indexOf('?'))
    }
    let token = req.session.token || req.cookies.token ;
    if (whitePageList.indexOf(req.url) != -1) {
        next();
    } else if (req.url !== '/login' && !token && !req.query.token) {
        if (req.method.toLowerCase() == 'get') {
            return res.redirect('../../login')
        } else {
            res.json(utils.response(-100, 'Token is invalid !'));
        }
    } else if (req.url === '/login') {
        next();
    } else if (req.url.startsWith('/') && req.query.token) {
        next();
    } else if (token) {
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
    } else {
        log.warn('There is no token !');
        res.json(utils.response(-100, 'There is no token !'));
    }
});
module.exports = router;