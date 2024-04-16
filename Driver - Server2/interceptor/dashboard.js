const express = require('express');
const router = express.Router();

const log = require('../log/winston').logger('Dashboard Interceptor');

const moment = require('moment');
const cacheTool = require('../cache_tool/dashboard.js');

const utils = require('../util/utils');

const URL_List = [
    '/dashboard/getTodayOffenceList',
    '/dashboard/getTodayOffenceDashboard',
    '/traffic/getTrafficList',
    '/traffic/getTrafficImages',
    '/dashboard/getTrafficList',
    '/dashboard/getDriverStateSos',
]

router.use(async (req, res, next) => {
    if (URL_List.includes(req.url)) {
        if (req.body.timeSelected && req.body.timeSelected == moment().format('YYYY-MM-DD')) {
            // if do not exist timeSelected(default 1 min)
            let cacheData = await cacheTool.returnCacheData(req);
            if (cacheData) {
                log.info(`Response data from cache(${ req.url })`)
                return res.json(utils.response(1, cacheData));
            } else {
                next()
            }
        } else {
            // if do not exist timeSelected(default 10 min)
            let cacheData = await cacheTool.returnCommonCacheData(req);
            if (cacheData) {
                log.info(`Response data from cache(${ req.url })`)
                return res.json(utils.response(1, cacheData));
            } else {
                next()
            }
        }
    } else {
        next();
    }
})
module.exports = router;