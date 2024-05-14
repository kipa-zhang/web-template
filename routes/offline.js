const express = require('express');
const router = express.Router();
require('express-async-errors');

const offlineService = require('../service/offlineService');

router.post('/offlineRequest', offlineService.offlineRequest);

module.exports = router;