const express = require('express');
const router = express.Router();

const utils = require('../util/utils');
const { Task } = require('../model/task.js');
const log = require('../log/winston').logger('TaskStatusCheck Interceptor');

const taskOptUrls = ['/mobileTO/completePretaskCheckItem', '/startTask', '/updateTaskOptTime', '/createMT_RAC', '/verifyMT_RAC'];

router.use(async (req, res, next) => {
    let reqUrlStr = req.url;
    if (taskOptUrls.includes(reqUrlStr)) {
        let taskId = req.body.taskId;
        if (taskId) {
            //check task cancelled status
            let currentTask = await Task.findByPk(taskId);
            if (currentTask && currentTask.driverStatus == 'Cancelled') {
                log.warn(reqUrlStr + ` Task[${taskId}] is cancelled, option has been Interceptor!`);
                return res.json(utils.response(-200, `Current task [${taskId}] is cancelled!`));
            } else {
                next();
            }
        } else {
            log.warn(reqUrlStr + " doesn't has taskId params;");
            next();
        } 
    } else {
        next();
    }
});

module.exports = router;