const express = require('express');
const router = express.Router();
require('express-async-errors');

const incidentService = require('../services/incidentService');

router.post('/getIncidentList', incidentService.getIncidentList);
router.post('/getIncidentTypeList', incidentService.getIncidentTypeList);
router.post('/createIncident', incidentService.createIncident);
router.post('/deleteIncident', incidentService.deleteIncident);
router.post('/updateIncident', incidentService.updateIncident);

module.exports = router;
