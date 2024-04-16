const express = require('express');
const router = express.Router();

const log = require('../log/winston').logger('URL Interceptor');
const utils = require('../util/utils');

const { User } = require('../model/user');

router.use(async (req, res, next) => {
    if(req.url.indexOf('?') >= 0) {
        req.url = req.url.substring(0, req.url.indexOf('?'))
    }
    log.info('HTTP Request URL : ', req.url);
    log.info('HTTP Request UserId : ', req.cookies.userId);
    log.info('HTTP Request Body: ', JSON.stringify(req.body));

    if (req.url == '/home' || req.url.startsWith('/callback') || req.url == '/loginUseSingpass' || req.url == '/guide'     
    || req.url== '/user/registerUser' || req.url== '/user/getSystemRole' || req.url == '/user/registerAccountUser' || req.url == '/assign/initSystemTaskByTripId'
    || req.url == '/user/getAccountUserData' || req.url == '/user/getSystemUrl' || req.url == '/user/getServiceTypeBySelectedGroup'
    || req.url == '/user/getHqTypeList' || req.url == '/user/urlParameterDecode' || req.url == '/user/getUserBaseByUserId' || req.url == '/user/editAccountUser') {
        next();
        return;
    }

    if (req.url.startsWith('/') && req.query.token) {
        next();
        return;
    }

    if (req.url != '/login' && req.method.toLowerCase() == 'get') {
        if (!req.cookies.userId) {
            return res.redirect('/login')
        } else {
            let userId = req.cookies.userId;
            let user = await User.findByPk(userId);
            if (!user || !user.jwtToken) {
                log.warn(`UserId ${ userId } does not exist!`);
                return res.redirect('/login')
            }
        }
    }

    if (req.url != '/login' && req.method.toLowerCase() == 'post') {
        if (!req.cookies.userId) {
            // return res.redirect('/login')
            return res.json(utils.response(-100, `UserId does not exist!`));
        } else {
            let userId = req.cookies.userId;
            let user = await User.findByPk(userId);
            if (!user) {
                log.warn(`UserId ${ userId } does not exist!`);
                // return res.redirect('/login')
                return res.json(utils.response(-100, `UserId ${ userId } does not exist!`));
            }
        }
    }    
    next();
})
module.exports = router;