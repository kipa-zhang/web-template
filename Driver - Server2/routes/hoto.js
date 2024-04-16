const express = require('express');
const router = express.Router();
require('express-async-errors');

const hotoService = require('../services/hotoService');

router.post('/getHubNode', hotoService.getHubNode);
router.post('/getVehicleTypeList', hotoService.getVehicleTypeList);
router.post('/getVehicleList', hotoService.getVehicleList);
router.post('/getVehicleListByReplace', hotoService.getVehicleListByReplace);
router.post('/getDriverList', hotoService.getDriverList);
router.post('/getDriverListByReplace', hotoService.getDriverListByReplace);
router.post('/getRequestListByHistory', hotoService.getRequestListByHistory);
router.post('/createHoto', hotoService.createHoto);
router.post('/replaceHotoByResource', hotoService.replaceHotoByResource);
router.post('/createHotoRecord', hotoService.createHotoRecord);
router.get('/getHubNodeByUser', hotoService.getHubNodeByUser);
router.post('/createHotoRequest', hotoService.createHotoRequest);
router.post('/editHotoRequest', hotoService.editHotoRequest);
router.post('/getHotoRequestById', hotoService.getHotoRequestById);
router.post('/getHotoRequest', hotoService.getHotoRequest);
router.post('/rejectRequestById', hotoService.operateRequestById);
router.post('/endorseRequestById', hotoService.operateRequestById);
router.post('/cancelRequestById', hotoService.operateRequestById);
router.post('/approveRequestById', hotoService.operateRequestById);

router.post('/cancelAssignHotoById', hotoService.cancelAssignHotoById);

router.get('/viewManagement', (req, res) => {
    res.render('hotoManagement/viewManagement');
});

module.exports = router;
