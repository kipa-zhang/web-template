const express = require('express');
const router = express.Router();
require('express-async-errors');

const incidentDetailService = require('../services/incidentDetailService');

router.post('/getWeather', incidentDetailService.getWeather);
router.post('/getTrafficCondition', incidentDetailService.getTrafficCondition);
router.post('/getTypeOfDelail', incidentDetailService.getTypeOfDelail);
router.post('/getSecondOrderOfDetail', incidentDetailService.getSecondOrderOfDetail);
router.post('/getDirectionOfMovement', incidentDetailService.getDirectionOfMovement);
router.post('/getTypeOfManoeuvre', incidentDetailService.getTypeOfManoeuvre);
router.post('/getLocalionOfImpact', incidentDetailService.getLocalionOfImpact);
router.post('/getLocationType', incidentDetailService.getLocationType);
router.post('/getWeeklyDateByDate', incidentDetailService.getWeeklyDateByDate);
router.post('/getIncidentDetailBySosId', incidentDetailService.getIncidentDetailBySosId);
router.post('/createIncidentDetail', incidentDetailService.createIncidentDetail);
router.post('/updateIncidentDetailById', incidentDetailService.updateIncidentDetailById);
router.post('/updateIncidentIssueById', incidentDetailService.updateIncidentIssueById);


module.exports = router;
