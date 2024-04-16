const express = require('express');
const router = express.Router();
require('express-async-errors');

const routeService = require('../services/routeService');

router.post('/askRouteLine', routeService.askRouteLine);
router.post('/reRouteLine', routeService.reRouteLine);

router.post('/getRouteList', routeService.getRouteList);
router.post('/getRoute', routeService.getRoute);
router.post('/createRoute', routeService.createRoute);
router.post('/deleteRoute', routeService.deleteRoute);
router.post('/updateRoute', routeService.updateRoute);
router.post('/copyRoute', routeService.copyRoute);

router.post('/getWaypointList', routeService.getWaypointList);
router.post('/createWaypoint', routeService.createWaypoint);
router.post('/deleteWaypoint', routeService.deleteWaypoint);
router.post('/updateWaypoint', routeService.updateWaypoint);

router.post('/getPointByPositionName', routeService.getPointByPositionName);
router.post('/getNameByPositionName', routeService.getNameByPositionName);

module.exports = router;
