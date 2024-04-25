const express = require('express');
const router = express.Router();
require('express-async-errors');

const mobileTOService = require('../service/mobileTOService')

router.get('/singpass', function(req, res, next) {
    res.render('singpassStart', { title: 'Singpass Login' });
});

router.post('/startTask', mobileTOService.startTask);
router.post('/endTask', mobileTOService.endTask);

router.post('/updateDriver', mobileTOService.updateDriver);
router.post('/updateVehicle', mobileTOService.updateVehicle);
router.post('/getCheckList', mobileTOService.getCheckList);
router.post('/updateCheckList', mobileTOService.updateCheckList);
router.post('/completePretaskCheckItem', mobileTOService.completePretaskCheckItem);
router.post('/getDriverNextTask', mobileTOService.getDriverNextTask);
router.post('/submitComment', mobileTOService.submitComment);
router.post('/getComment', mobileTOService.getComment);

router.post('/loadGuidePageUrl', mobileTOService.loadGuidePageUrl);

module.exports = router;