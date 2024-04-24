const express = require('express');
const router = express.Router();

const log = require('../log/winston').logger('Role Interceptor');
const conf = require('../conf/conf');
const utils = require('../util/utils');
const CONTENT = require('../util/content');
const moment = require('moment');

const userService = require('../services/userService');
const { User } = require('../model/user');
const { ModulePageWhite } = require('../model/permission/modulePageWhite');

const checkLink = function (url, pageList) {
    let result = pageList.some(item => {
        if (item.link) {
            let linkList = item.link.split(',')
            return linkList.some(link => {
                let __link = link.replaceAll('\r\n', '').trim()
                if (__link) {
                    return url == __link
                }
            })
        } 
    })
    return result
}

router.use(async (req, res, next) => {
    let userId = req.cookies.userId
    if (!userId) {
        // Need login
        next()
    } else {
        let url = req.url.split('?')[0]
        let method = req.method.toLowerCase()
        let pageList = await userService.getUserPageList(userId)
        let pageWhiteList = await ModulePageWhite.findAll({ where: { link: url } });

        if (pageWhiteList.length) {
            next()
        } else if (url == '/' || url.startsWith('/login') || url.startsWith('/logout')) {
            // Home page
            next()
        } else if (checkLink(url, pageList)) {
            next()
        } else if (method == 'get') {
            res.render('404');
        } else if (method == 'post') {
            log.warn('No permission: ', JSON.stringify({
                userId,
                url,
                method,
            }))
            res.json(utils.response(0, 'No permission.'));
        }
    }
})

module.exports = router;