const express = require('express');
const router = express.Router();
require('express-async-errors');

const utils = require('../util/utils');
const assignService = require('../services/assignService');

router.get('/', (req, res) => {
    res.render('assign/index', { title: 'Task Assign', userType: req.cookies.userType });
});

router.post('/getHubNode', assignService.getHubNode);
router.post('/getVehicleType', assignService.getVehicleType);
router.post('/getTaskIdDriverAndVehicle', assignService.getTaskIdDriverAndVehicle);
router.post('/getDriverListByTaskId', assignService.getDriverListByTaskId);
router.post('/getVehicleList', assignService.getVehicleList);

// old atms (mb task)
// router.post('/assignTask', assignService.assignTask);
// router.post('/re-assignTask', assignService.assignTask);
// router.post('/checkTaskRoute', assignService.checkTaskRoute);

// lod atms task (By uploading the task)
// router.post('/getMBTaskById', assignService.getMBTaskById);
// router.post('/preMbAssign', assignService.preMbAssign);
// router.post('/assignMbTask', assignService.assignMbTask);
// router.post('/cancalTaskByMb', assignService.cancalTaskByMb);

router.post('/reassignTaskVehicle', assignService.reassignTaskVehicle);// Task reassignment of emptied vehicles at the top of the home page

router.post('/initSystemTaskByTripId', assignService.initSystemTaskByTripId); // Automatic matching vehicle/driver
router.post('/loanOutTaskByTaskId', assignService.loanOutTaskByTaskId);// Check whether the task is loan in
router.post('/preAssign', assignService.preAssign); // Set the hub/node of the task
router.post('/reassignMvTask', assignService.reassignMvTask); // assign、re-assign vehicle/driver
router.post('/reassignMvTaskApprove', assignService.reassignMvTaskApprove); // Tasks assigned by default need to be approved before being reassigned

const assignService2 = require('../services/assignService2');
router.post('/getAssignableTaskList2', assignService2.getAssignableTaskList);
router.post('/CheckListByTaskId', assignService2.CheckListByTaskId); // Check whether the task has started

// lod atms task (By uploading the task)
// router.post('/getAssignableTaskListByMtAdmin', assignService2.getAssignableTaskListByMtAdmin)

module.exports = router;
