const express = require('express');
const router = express.Router();

const loanOutService = require('../services/loanOutService.js');

router.post('/returnDriverResources', loanOutService.returnResources)
router.post('/returnVehicleResources', loanOutService.returnResources)
router.post('/returnLoanByLoanId', loanOutService.returnLoanByLoanId)

module.exports = router;
