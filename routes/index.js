const express = require('express');
const router = express.Router();
require('express-async-errors');

const indexService = require('../services/indexService');

router.post('/publicFirebaseNotification', indexService.publicFirebaseNotification);
router.post('/publicFirebaseNotificationBySystem', indexService.publicFirebaseNotificationBySystem);


module.exports = router;
