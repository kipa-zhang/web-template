const log = require('../log/winston').logger('VehicleMaintenanceInfoCalcUtils');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const { Vehicle } = require('../model/vehicle');
const moment = require('moment');

const { Task } = require('../model/task.js');
const { User } = require('../model/user.js');
const { Mileage } = require('../model/mileage.js');
const { OperationRecord } = require('../model/operationRecord.js');

module.exports.reCalcVehicleMaintenanceInfo = async function(taskId, indentId) {
    try {
        if (!taskId) return null
        let task0 = null;
        if (taskId.startsWith('DUTY')) {
            let idArray = taskId.split('-');
            if (idArray.length < 2) {
                log.warn(`getCurrentTaskById error: TaskId ${taskId} format error.`)
                return null;
            }
            taskId = `DUTY-${idArray[1]}`;
            let taskList = await sequelizeObj.query(` 
                SELECT
                    ui.mobileStartTime,
                    ui.vehicleNo as vehicleNumber,
                    uc.purpose,
                    ui.driverId
                FROM urgent_indent ui
                LEFT JOIN urgent_duty ud on ui.dutyId = ud.id
                LEFT JOIN urgent_config uc on ud.configId = ud.id
                where ui.id=${indentId}
            `, { 
                type: QueryTypes.SELECT, replacements: []
            });
            if (taskList && taskList.length > 0) {
                task0 = taskList[0];
            }
        } else {
            task0 = await Task.findOne({ where: { taskId } })
        }
        if (task0) {
            task0 = task0.dataValues ? task0.dataValues : task0;
        }
        if (task0 && task0.vehicleNumber) {
            let taskPurpose = task0.purpose;
            let vehicleNo = task0.vehicleNumber;
            let vehicle = await Vehicle.findByPk(vehicleNo);
            if (vehicle) {
                let updateFields = {};
                let needUpdate = false;
                if (taskPurpose && taskPurpose.toLowerCase() == 'wpt') {
                    let wptTaskTime = task0.mobileStartTime;
                    if (wptTaskTime) {
                        wptTaskTime = moment(wptTaskTime);
                        let wptTaskTimeStr = wptTaskTime.format('YYYY-MM-DD HH:mm:ss');
                        let wptTimeStr = wptTaskTime.endOf('isoWeek').format('YYYY-MM-DD');
                        if (wptTimeStr == vehicle.nextWpt1Time) {
                            updateFields.nextWpt1Time = null;
                            updateFields.wpt1CompleteTime = wptTaskTimeStr;
                            needUpdate = true;
                        } else if (wptTimeStr == vehicle.nextWpt2Time) {
                            updateFields.nextWpt1Time = null;
                            updateFields.nextWpt2Time = null;
                            updateFields.wpt2CompleteTime = wptTaskTimeStr;
                            needUpdate = true;
                        } else if (wptTimeStr == vehicle.nextWpt3Time) {
                            updateFields.nextWpt1Time = null;
                            updateFields.nextWpt2Time = null;
                            updateFields.nextWpt3Time = null;
                            updateFields.wpt3CompleteTime = wptTaskTimeStr;
                            needUpdate = true;
                        } else if (vehicle.nextWpt3Time && wptTimeStr >= vehicle.nextWpt3Time) {
                            updateFields.nextWpt1Time = null;
                            updateFields.nextWpt2Time = null;
                            updateFields.nextWpt3Time = null;
                            updateFields.wpt3CompleteTime = wptTaskTimeStr;
                            needUpdate = true;
                        }
                    }
                } else {
                    // task mileage >= 1 km
                    let mileage = await Mileage.findByPk(taskId);
                    if (mileage && mileage.mileageTraveled >= 1) {
                        let weekStartDate = moment().startOf('isoWeek');
                        let weekEndDate = moment().endOf('isoWeek');

                        let weekStartDateStr = moment().startOf('isoWeek').format('YYYY-MM-DD HH:mm:ss');
                        let weekEndDateStr = moment().endOf('isoWeek').format('YYYY-MM-DD HH:mm:ss');
                        //this week has avi task completed
                        let thisWeekAviTasks = await sequelizeObj.query(` SELECT
                                t.taskId
                            FROM task t
                            LEFT JOIN mileage m ON t.taskId = m.taskId
                            where t.vehicleNumber='${vehicleNo}' and t.driverStatus = 'completed' and LOWER(t.purpose)='avi' 
                            and t.mobileEndTime BETWEEN  STR_TO_DATE('${weekStartDateStr}', '%Y-%m-%d %H:%i:%s')  AND STR_TO_DATE('${weekEndDateStr}', '%Y-%m-%d %H:%i:%s') `, 
                        { type: QueryTypes.SELECT, replacements: []})

                        let newWpt1Date = moment().endOf('isoWeek').add(7, 'day');
                        let newWpt2Date = moment().endOf('isoWeek').add(14, 'day');
                        let newWpt3Date = moment().endOf('isoWeek').add(21, 'day');
                        let newMptDate = moment().endOf('isoWeek').add(28, 'day');
                        if (thisWeekAviTasks && thisWeekAviTasks.length > 0) {
                            newWpt1Date = moment().endOf('isoWeek').add(14, 'day');
                            newWpt2Date = moment().endOf('isoWeek').add(21, 'day');
                            newWpt3Date = moment().endOf('isoWeek').add(28, 'day');
                            newMptDate = moment().endOf('isoWeek').add(35, 'day');
                        }
                        
                        updateFields.nextWpt1Time = newWpt1Date;
                        updateFields.wpt1CompleteTime = null;
                        updateFields.nextWpt2Time = newWpt2Date;
                        updateFields.wpt2CompleteTime = null;
                        updateFields.nextWpt3Time = newWpt3Date;
                        updateFields.wpt3CompleteTime = null;
                        updateFields.nextMptTime = newMptDate;
                        needUpdate = true;
                    }
                    if (taskPurpose && taskPurpose.toLowerCase() == 'avi') {
                        let nextAviTime = moment().add(1, 'year').add(-1, 'day');
                        updateFields.nextAviTime = nextAviTime;
                        needUpdate = true;
                    }
                }

                if (needUpdate) {
                    await Vehicle.update(updateFields, {where: {vehicleNo: vehicleNo}})
                    let user = await User.findOne({ where: { driverId: task0.driverId } })

                    let operationRecord = {
                        operatorId: user.userId,
                        businessType: 'vehicle',
                        businessId: vehicleNo,
                        optType: 'update wpt',
                        beforeData: JSON.stringify(vehicle),
                        afterData: JSON.stringify(updateFields),
                        optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                        remarks: 'Mobile end task, will update wpt here.'
                    }
                    await OperationRecord.create(operationRecord);

                    log.info(`vehicle[${vehicleNo}] complete ${taskPurpose} task, update pm/avi time:${JSON.stringify(updateFields)}`);
                }
            }
        }
    } catch (error) {
        log.error(`(reCalcVehicleMaintenanceInfo ${moment().format('YYYY-MM-DD HH:mm:ss')} ): working failed, taskId: ${taskId},  ${error}`);
        log.error(error);
    }
}


