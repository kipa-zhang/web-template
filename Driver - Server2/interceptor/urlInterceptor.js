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

    if ((whitePageList.indexOf(req.url) != -1 || req.url == '/login') 
        || (req.url.startsWith('/') && req.query.token)) {
        next();
        return;
    }

    let userId = req.cookies.userId;
    if (req.method.toLowerCase() == 'get') {
        if (!userId) {
            return res.redirect('/login')
        }
        let user = await User.findByPk(userId);
        if (user?.jwtToken) {
            next();
            return;
        }
        log.warn(`UserId ${ userId } does not exist!`);
        return res.redirect('/login')
    } else if (req.method.toLowerCase() == 'post') {
        if (!userId) {
            return res.json(utils.response(-100, `UserId does not exist!`));
        }
        
        let user = await User.findByPk(userId);
        if (!user) {
            log.warn(`UserId ${ userId } does not exist!`);
            return res.json(utils.response(-100, `UserId ${ userId } does not exist!`));
        }
    }    
    next();
})
module.exports = router;