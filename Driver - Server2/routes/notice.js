const express = require('express');
const router = express.Router();
require('express-async-errors');

const userService = require('../services/userService');
const noticeService = require('../services/noticeService');

router.get('/', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'Notification')
    res.render('notification/notification-view', { title: 'Notices', userType: req.cookies.userType, pageList });
});

router.post('/createOrUpdateNotice', noticeService.createOrUpdateNotice);
router.post('/deleteNotice', noticeService.deleteNotice);
// For manage
router.post('/getNoticeList', noticeService.getNoticeList);
// For laptop
router.post('/getLaptopNoticeList', noticeService.getLaptopNoticeList);
router.post('/readNotice', noticeService.readNotice);

router.post('/getNoticeCreateInfo', noticeService.getNoticeCreateInfo);

module.exports = router;
