const express = require('express');
const router = express.Router();
require('express-async-errors');

const keyService = require('../service/vehicleKeyService.js');

router.post('/getSupportSiteList', keyService.getSupportSiteList);
router.post('/generateWithdrawQRCode', keyService.generateWithdrawQRCode);
router.post('/generateReturnQRCode', keyService.generateReturnQRCode);
router.post('/generateKeypressTransactionQRCode', keyService.generateKeypressTransactionQRCode);

router.post('/scanQrcodeForWithdraw', keyService.scanQrcodeForWithdraw);
router.post('/scanQrcodeForReturn', keyService.scanQrcodeForReturn);

router.post('/getTaskVehicleKeyStatus', keyService.getTaskVehicleKeyStatus);

module.exports = router;
