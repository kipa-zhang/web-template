const express = require('express');
const router = express.Router();
require('express-async-errors');

const creditService = require('../services/creditService');

router.get('/getPurposeTypeList', creditService.getPurposeTypeList);
router.post('/getCreditInfo', creditService.getCreditInfo);
router.post('/getCreditInfoByYear', creditService.getCreditInfoByYear);

module.exports = router;
