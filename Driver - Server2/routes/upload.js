const express = require('express');
const router = express.Router();
require('express-async-errors');

const uploadService = require('../services/uploadService');

router.get('', (req, res) => { res.render('upload/upload') });

router.post('/uploadVehicle', uploadService.uploadVehicle)
router.post('/uploadWaypoint', uploadService.uploadWaypoint)


router.post('/uploadImage', uploadService.uploadImage)

module.exports = router;
