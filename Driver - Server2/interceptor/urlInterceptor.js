const express = require('express');
const router = express.Router();

const log = require('../log/winston').logger('URL Interceptor');
const utils = require('../util/utils');

const { User } = require('../model/user');

const whitePageList = ['/home', '/callback', '/loginUseSingpass', '/guide', '/user/registerUser', '/user/getSystemRole', '/user/registerAccountUser',
'/assign/initSystemTaskByTripId', '/user/getAccountUserData', '/user/getSystemUrl', '/user/getServiceTypeBySelectedGroup',
'/user/getHqTypeList', '/user/urlParameterDecode', '/user/getUserBaseByUserId', '/user/editAccountUser'];

router.use(async (req, res, next) => {
    if(req.url.indexOf('?') >= 0) {
        req.url = req.url.substring(0, req.url.indexOf('?'))
    }
    log.info('HTTP Request URL : ', req.url);
    log.info('HTTP Request UserId : ', req.cookies.userId);
    log.info('HTTP Request Body: ', JSON.stringify(req.body));

    if (whitePageList.indexOf(req.url) != -1) {
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