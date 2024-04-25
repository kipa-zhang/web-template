const express = require('express');
const router = express.Router();

const log = require('../log/winston').logger('URL Interceptor');
const gpsLog = require('../log/winston').GPSLogger('Position Service');

router.use(async (req, res, next) => {
    if (req.url.indexOf('updatePositionByFile') > -1 || req.url.indexOf('getStateRecord') > -1) {
        gpsLog.info('HTTP Request URL: ' + req.url);
        gpsLog.info('HTTP Request Body: ' + JSON.stringify(req.body));
    } else {
        log.info('HTTP Request URL: ' + req.url);
        log.info('HTTP Request Body: ' + JSON.stringify(req.body));
    }
    next();

});

module.exports = router;