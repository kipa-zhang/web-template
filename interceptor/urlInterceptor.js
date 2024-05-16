const express = require('express');
const router = express.Router();
const utils = require('../util/utils');

const log = require('../winston/logger').logger('URL Interceptor');

router.use((req, res, next) => {
    log.info('HTTP Request URL : ', req.url);
    log.info('HTTP Request Body: ', JSON.stringify(req.body, null, 4));

    if (![ '/publicFirebaseNotification', '/publicFirebaseNotificationBySystem' ].includes(req.url)) {
        log.warn('This request url do not exist here!');
        return res.json(utils.response(0, 'This request do not exist here!'));
    } else {
        next();
        // If coding here, still will run the code!!!
    }
})
module.exports = router;