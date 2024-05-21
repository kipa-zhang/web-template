const log = require('../log/winston').logger('Mobile Service');

const Response = require('../util/response.js');
const { DRIVER_STATUS } = require('../util/content')
const moment = require('moment');
const _ = require('lodash')
const utils = require('../util/utils');
const conf = require('../conf/conf');

const { QueryTypes, Model, Op } = require('sequelize');
const { sequelizeSystemObj } = require('../db/dbConf_system');
const { sequelizeObj } = require('../db/dbConf');
const _SystemDriver = require('../model/system/driver');
const _SystemVehicle = require('../model/system/vehicle');
const _SystemJob = require('../model/system/job.js');
const _SystemTask = require('../model/system/task');
const { CheckList } = require('../model/checkList')
const { Task } = require('../model/task')
const { Mileage } = require('../model/mileage.js');
const { MT_RAC } = require('../model/mtRAC');
const { MtAdmin } = require('../model/mtAdmin');
const { User } = require('../model/user.js');
const { Vehicle } = require('../model/vehicle.js');
const { Comment } = require('../model/comment');
const { Comment: System_Comment } = require('../model/system/comment');
const taskService = require('./taskService.js');

const { UrgentDuty } = require('../model/urgent/urgentDuty.js');
const { UrgentIndent } = require('../model/urgent/urgentIndent.js');
const urgentService = require('../service/urgentService');

const CHECKLIST = {
    "1": "Route Familiarisation",
    "2": "Force Preparation",
    "3": "Vehicle Check",
    "4": "Just-In-Time Training",
    "5": "MT-RAC",
}

const buildDriverTask = async function(dataItem, systemTask) {
    dataItem.startTime = moment(dataItem.indentStartTime).format('HH:mm');
    // Change 2023-04-19
    dataItem.endTime = dataItem.indentEndTime ? moment(dataItem.indentEndTime).format('DD MMM,HH:mm') : '';

    const generateDataTag = function () {
        if (dataItem.indentStartTime) {
            dataItem.executionDate = moment(dataItem.indentStartTime).format("DD MMM")
            dataItem.executionDateTime = moment(dataItem.indentStartTime).format("YYYY-MM-DD HH:mm")
            
            let today = moment().format("YYYY-MM-DD")
            if (today == moment(dataItem.indentStartTime).format("YYYY-MM-DD")) {
                dataItem.tag = "TODAY"
            } else if (moment(dataItem.indentStartTime).diff(today, 'd') == 1) {
                dataItem.tag = "TOMORROW"
            } else {
               dataItem.tag = moment(dataItem.indentStartTime).format("MM/DD")
            }
        }
    }
    generateDataTag()

    if (dataItem.dataFrom == 'SYSTEM' && systemTask) {
        dataItem.pickupDestinationLat = systemTask.pickupDestinationLat
        dataItem.pickupDestinationLng = systemTask.pickupDestinationLng
        dataItem.dropoffDestinationLat = systemTask.dropoffDestinationLat
        dataItem.dropoffDestinationLng = systemTask.dropoffDestinationLng
        dataItem.poc = systemTask.poc
        dataItem.pocNumber = systemTask.pocNumber
        dataItem.serviceModeName = systemTask.serviceModeName
        dataItem.serviceModeValue = systemTask.serviceModeValue
        dataItem.taskStatus = systemTask.taskStatus
        dataItem.additionalRemarks = systemTask.additionalRemarks
        dataItem.completedTime = systemTask.completedTime
        dataItem.arrivalTime = systemTask.arrivalTime
        dataItem.departTime = systemTask.departTime
        dataItem.additionalRemarks = systemTask.additionalRemarks
    } else if (dataItem.dataFrom !== 'SYSTEM') {
        if (dataItem?.serviceModeName.toLowerCase() == '1-way') {
            dataItem.serviceModeValue = 'delivery'
        } else if (dataItem?.serviceModeName.toLowerCase() == 'disposal') {
            dataItem.serviceModeValue = 'pickup'
        }

        log.info(`*********************`)
        log.info(dataItem.serviceModeName)
        log.info(dataItem.serviceModeValue)
        log.info(`*********************`)

        dataItem.pickupDestinationLat = dataItem.pickupGPS.split(',')[0].trim();
        dataItem.pickupDestinationLng = dataItem.pickupGPS.split(',')[1].trim();
        dataItem.dropoffDestinationLat = dataItem.dropoffGPS.split(',')[0].trim();
        dataItem.dropoffDestinationLng = dataItem.dropoffGPS.split(',')[1].trim();

        dataItem.pickupDestinationLat = dataItem.pickupDestinationLat ? dataItem.pickupDestinationLat : 0;
        dataItem.pickupDestinationLng = dataItem.pickupDestinationLng ? dataItem.pickupDestinationLng : 0;
        dataItem.dropoffDestinationLat = dataItem.dropoffDestinationLat ? dataItem.dropoffDestinationLat : 0;
        dataItem.dropoffDestinationLng = dataItem.dropoffDestinationLng ? dataItem.dropoffDestinationLng : 0;

        delete dataItem.pickupGPS;
        delete dataItem.dropoffGPS;

        const checkTaskStatus = function () {
            if (!dataItem.mobileEndTime) {
                dataItem.taskStatus = 'Assigned'
            } else {
                dataItem.taskStatus = 'Completed'
            }
            if (dataItem.driverStatus == 'waitcheck') {
                dataItem.taskStatus = 'Pending PRE-TASK'
            }
            if (dataItem.dataFrom == 'MOBILE') {
                if (dataItem.driverStatus == 'Pending Approval') {
                    dataItem.taskStatus = 'Pending Approval'
                }
            }
        }
        checkTaskStatus()
    }

    dataItem.mobileStartTime = dataItem.mobileStartTime ? moment(dataItem.mobileStartTime).format('YYYY-MM-DD HH:mm:ss') : '';
    dataItem.mobileEndTime = dataItem.mobileEndTime ? moment(dataItem.mobileEndTime).format('YYYY-MM-DD HH:mm:ss') : '';
    dataItem.arrivalTime = dataItem.arrivalTime ? moment(dataItem.arrivalTime).format('YYYY-MM-DD HH:mm:ss') : '';
    dataItem.departTime = dataItem.departTime ? moment(dataItem.departTime).format('YYYY-MM-DD HH:mm:ss') : '';
    dataItem.completedTime = dataItem.completedTime ? moment(dataItem.completedTime).format('YYYY-MM-DD HH:mm:ss') : '';
    dataItem.indentStartTime = dataItem.indentStartTime ? moment(dataItem.indentStartTime).format('YYYY-MM-DD HH:mm:ss') : '';
    dataItem.indentEndTime = dataItem.indentEndTime ? moment(dataItem.indentEndTime).format('YYYY-MM-DD HH:mm:ss') : '';
}

module.exports = {
    GETDriverTaskList: async function(driverId) {
        //all driver task
        log.info(`(GETDriverTaskList) start query1 time: ${ moment().format('YYYY-MM-DD HH:mm:ss SSS') }`)
        let dataList1 = await sequelizeObj.query(`
            SELECT t.taskId, t.dataFrom, t.driverId, t.vehicleNumber, t.hub, t.node, t.driverStatus, t.pickupGPS, t.dropoffGPS,
            t.indentStartTime, t.indentEndTime, t.purpose AS purposeType, t.pickupDestination, t.dropoffDestination,
            t.mobileStartTime, t.mobileEndTime, t.startLateReason, t.indentId, 
            v.vehicleType, v.limitSpeed, 
            d.driverName, d.state, 
            m.activityName AS additionalRemarks,
            m.reportingLocationLat AS pickupDestinationLat, m.reportingLocationLng AS pickupDestinationLng,
            m.destinationLat AS dropoffDestinationLat, m.destinationLng AS dropoffDestinationLng, 
            m.serviceMode AS serviceModeName, m.serviceMode AS serviceModeValue, m.poc, m.mobileNumber AS pocNumber,
            m.arrivalTime, m.departTime
            FROM task t 
            LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
            LEFT JOIN driver d ON d.driverId = t.driverId
            LEFT JOIN mt_admin m ON m.id=t.indentId
            WHERE t.driverId = ? and t.vehicleNumber is not null and t.vehicleNumber != ''
            AND (
                (t.driverStatus = 'waitcheck' AND t.indentEndTime > NOW())
                OR t.driverStatus = 'ready'
                OR t.driverStatus = 'started'
            )
            ORDER BY t.indentStartTime ASC 
            limit 25
        `, { type: QueryTypes.SELECT, replacements: [driverId] })
        let dataList2 = await sequelizeObj.query(`
            SELECT t.taskId, t.dataFrom, t.driverId, t.vehicleNumber, t.hub, t.node, t.driverStatus, t.pickupGPS, t.dropoffGPS,
            t.indentStartTime, t.indentEndTime, t.purpose AS purposeType, t.pickupDestination, t.dropoffDestination,
            t.mobileStartTime, t.mobileEndTime, t.startLateReason, t.indentId, 
            v.vehicleType, v.limitSpeed, 
            d.driverName, d.state, 
            m.activityName AS additionalRemarks,
            m.reportingLocationLat AS pickupDestinationLat, m.reportingLocationLng AS pickupDestinationLng,
            m.destinationLat AS dropoffDestinationLat, m.destinationLng AS dropoffDestinationLng, 
            m.serviceMode AS serviceModeName, m.serviceMode AS serviceModeValue, m.poc, m.mobileNumber AS pocNumber,
            m.arrivalTime, m.departTime
            FROM task t 
            LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
            LEFT JOIN driver d ON d.driverId = t.driverId
            LEFT JOIN mt_admin m ON m.id=t.indentId
            WHERE t.driverId = ?
            AND t.driverStatus = 'completed'
            ORDER BY t.mobileEndTime DESC 
            limit 25
        `, { type: QueryTypes.SELECT, replacements: [driverId] })

        //DV Pending Approval mobile trip
        let dataList3 = await sequelizeObj.query(`
            SELECT CONCAT('CU-M-', mt.id) as taskId, 'MOBILE' as dataFrom, MT.driverId, MT.vehicleNumber, t.hub, t.node, 'Pending Approval' AS driverStatus, mt.pickupGPS, mt.dropoffGPS,
                mt.indentStartTime, mt.indentEndTime, mt.purpose AS purposeType, mt.pickupDestination, mt.dropoffDestination,
                t.mobileStartTime, t.mobileEndTime, t.startLateReason, t.indentId, 
                v.vehicleType, v.limitSpeed, d.driverName, d.state
            FROM mobile_trip mt
            LEFT JOIN task t ON t.taskId = CONCAT('CU-M-', mt.id) 
            LEFT JOIN vehicle v ON v.vehicleNo = mt.vehicleNumber
            LEFT JOIN driver d ON d.driverId = mt.driverId
            WHERE mt.driverId = ? and mt.status = 'Pending Approval' and mt.indentEndTime > NOW()
            ORDER BY mt.indentStartTime DESC 
            limit 25
        `, { type: QueryTypes.SELECT, replacements: [driverId] })

        let dataList = [].concat(dataList1, dataList2, dataList3)

        log.info(`(GETDriverTaskList) end query1 time: ${ moment().format('YYYY-MM-DD HH:mm:ss SSS') }`)

        log.info(`(GETDriverTaskList) start query2 time: ${ moment().format('YYYY-MM-DD HH:mm:ss SSS') }`)
        let systemTaskList1 = await sequelizeSystemObj.query(`
            SELECT
                b.taskId,
                f.lat AS pickupDestinationLat,
                f.lng AS pickupDestinationLng,
                g.lat AS dropoffDestinationLat,
                g.lng AS dropoffDestinationLng,
                a.poc,
                a.pocNumber,
                e.\`name\` as serviceModeName,
                e.\`value\` as serviceModeValue,
                a.taskStatus,
                r.additionalRemarks,
                a.endTime AS completedTime,
                a.arrivalTime,
                a.departTime
            FROM
                job_task a
            LEFT JOIN driver b ON a.id = b.taskId
            LEFT JOIN vehicle c ON a.id = c.taskId
            LEFT JOIN job d ON a.tripId = d.id
            LEFT JOIN request r ON r.id = a.requestId
            LEFT JOIN service_mode e ON d.serviceModeId = e.id
            LEFT JOIN location f ON a.pickupDestination = f.locationName
            LEFT JOIN location g ON a.dropoffDestination = g.locationName
            WHERE
                a.driverId IS NOT NULL 
                AND b.driverId = ? 
                AND b.driverFrom = 'transport' 
                AND a.taskStatus != 'completed'
            ORDER BY
                a.startDate asc
            limit 25
        `, {
            type: QueryTypes.SELECT,
            replacements: [driverId]
        });
        let systemTaskList2 = await sequelizeSystemObj.query(`
            SELECT
                b.taskId,
                f.lat AS pickupDestinationLat,
                f.lng AS pickupDestinationLng,
                g.lat AS dropoffDestinationLat,
                g.lng AS dropoffDestinationLng,
                a.poc,
                a.pocNumber,
                e.\`name\` as serviceModeName,
                e.\`value\` as serviceModeValue,
                a.taskStatus,
                r.additionalRemarks,
                a.endTime AS completedTime,
                a.arrivalTime,
                a.departTime
            FROM
                job_task a
            LEFT JOIN driver b ON a.id = b.taskId
            LEFT JOIN vehicle c ON a.id = c.taskId
            LEFT JOIN job d ON a.tripId = d.id
            LEFT JOIN request r ON r.id = a.requestId
            LEFT JOIN service_mode e ON d.serviceModeId = e.id
            LEFT JOIN location f ON a.pickupDestination = f.locationName
            LEFT JOIN location g ON a.dropoffDestination = g.locationName
            WHERE
                a.driverId IS NOT NULL 
                AND b.driverId = ? 
                AND b.driverFrom = 'transport' 
                AND a.taskStatus = 'completed'
            ORDER BY
                a.endDate desc
            limit 25
        `, {
            type: QueryTypes.SELECT,
            replacements: [driverId]
        });
        let systemTaskList = [].concat(systemTaskList1, systemTaskList2)
        log.info(`(GETDriverTaskList) end query2 time: ${ moment().format('YYYY-MM-DD HH:mm:ss SSS') }`)

        let upcomingList = [], completeList = []
        let reportLateReasonMinutes = conf.reportLateReasonMinutes ? conf.reportLateReasonMinutes : 60;
        for (let dataItem of dataList) {
            let latestStartTime = moment(dataItem.indentStartTime).add(reportLateReasonMinutes, 'minute');

            dataItem.latestStartTime = latestStartTime.format('YYYY-MM-DD HH:mm:ss');
            dataItem.mobileStartTime = dataItem.mobileStartTime ? moment(dataItem.mobileStartTime).format('YYYY-MM-DD HH:mm:ss') : null;

            dataItem.state = dataItem.state ? dataItem.state : '';
            let tempTaskId = dataItem.taskId;
            if (dataItem?.dataFrom == 'SYSTEM' && tempTaskId?.startsWith('AT-')) {
                tempTaskId = tempTaskId.replace('AT-', '');
            }
            await buildDriverTask(dataItem, systemTaskList.find(item => item.taskId == tempTaskId));

            if (dataItem.driverStatus.toLowerCase() == 'completed') {
                dataItem.tag = '';
                completeList.push(dataItem)
            } else {
                upcomingList.push(dataItem)
            }
        }

        // Concat urgent complete indent
        let completeUrgentList = await urgentService.getCompletedDutyList(driverId)
        completeList = completeList.concat(completeUrgentList)

        let result = {
            totalTrip: dataList.length,
            indents: [],
        }
        
        let urgentList = await urgentService.getUrgentList(driverId);
        // urgent task don't need input start late reason
        if (urgentList && urgentList.length > 0) {
            urgentList.forEach(function(ele, index) {
                ele.startLateReason = 'urgent;'
            });
        }

        // urgent + upcoming
        upcomingList = upcomingList.concat(urgentList)
        result.indents.push({
            name: _.capitalize('Upcoming'),
            dataList: _.sortBy(upcomingList, function(o) { 
                // Mobile can not change, so update here
                return o.mobileStartTime;
            }),
            length: upcomingList.length
        })

        result.indents.push({
            name: _.capitalize('Completed'),
            dataList: _.sortBy(completeList, function(o) { 
                // Mobile can not change, so update here
                return o.mobileEndTime;
            }),
            length: completeList.length
        })

        // Abandon
        result.indents.push({
            name: _.capitalize('Urgent'),
            dataList: [],
            length: [].length,
            // dataList: urgentList,
            // length: urgentList.length,
        })

        return {
            "code": 1,
            "message": "success",
            "data": result,
        }
    },
    GETDriverTaskByTaskId: async function(taskId) {
        //all driver task
        let dataList = await sequelizeObj.query(`
            SELECT t.*, t.purpose as purposeType, v.vehicleType, v.limitSpeed, d.driverName, d.contactNumber, d.permitType, d.state, 
                m.activityName as additionalRemarks,
                m.reportingLocationLat AS pickupDestinationLat,m.reportingLocationLng AS pickupDestinationLng,
                m.destinationLat AS dropoffDestinationLat,m.destinationLng AS dropoffDestinationLng, 
                m.serviceMode as serviceModeName, m.serviceMode as serviceModeValue, m.poc, m.mobileNumber as pocNumber,
                m.endTime AS completedTime, m.arrivalTime, m.departTime
            FROM task t 
            LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
            LEFT JOIN driver d ON d.driverId = t.driverId
            LEFT JOIN mt_admin m ON m.id=t.indentId
            WHERE t.taskId = ? order by t.indentStartTime asc

        `, { type: QueryTypes.SELECT, replacements: [taskId] })

        let dataItem = dataList.length ? dataList[0] : null;
        if (dataItem) {
            let systemTask = null;
            if (dataItem.dataFrom == 'SYSTEM') {
                let systemTaskId = taskId;
				if (systemTaskId?.startsWith('AT-')) {
					systemTaskId = systemTaskId.replace('AT-', '');
				}

                let systemTaskList = await sequelizeSystemObj.query(`
                    SELECT
                        a.id AS taskId,
                        f.lat AS pickupDestinationLat,
                        f.lng AS pickupDestinationLng,
                        g.lat AS dropoffDestinationLat,
                        g.lng AS dropoffDestinationLng,
                        a.poc,
                        a.pocNumber,
                        e.\`name\` as serviceModeName,
                        e.\`value\` as serviceModeValue,
                        a.taskStatus,
                        r.additionalRemarks,
                        a.endTime AS completedTime,
                        a.arrivalTime,
                        a.departTime
                    FROM
                        job_task a
                    LEFT JOIN driver b ON a.id = b.taskId
                    LEFT JOIN vehicle c ON a.id = c.taskId
                    LEFT JOIN job d ON a.tripId = d.id
                    LEFT JOIN request r ON r.id = a.requestId
                    LEFT JOIN service_mode e ON d.serviceModeId = e.id
                    LEFT JOIN location f ON a.pickupDestination = f.locationName
                    LEFT JOIN location g ON a.dropoffDestination = g.locationName
                    WHERE
                        a.driverId IS NOT NULL and a.id = ? AND b.driverFrom = 'transport'
                    ORDER BY
                        a.startDate
                `, {
                    type: QueryTypes.SELECT,
                    replacements: [systemTaskId]
                });
                systemTask = systemTaskList.length ? systemTaskList[0] : null;
            }

            let reportLateReasonMinutes = conf.reportLateReasonMinutes ? conf.reportLateReasonMinutes : 60;
            let latestStartTime = moment(dataItem.indentStartTime).add(reportLateReasonMinutes, 'minute');
            dataItem.latestStartTime = latestStartTime.format('YYYY-MM-DD HH:mm:ss');
            
            dataItem.state = dataItem.state ? dataItem.state : '';
            await buildDriverTask(dataItem, systemTask);

            return {
                "code": 1,
                "message": "success",
                "data": dataItem,
            }
        }
        return {
            "code": 0,
            "message": "Task doesn't exist.",
            "data": {},
        }
    },
    updateDriver: async function (req, res) {
        let { taskId, driverId, permitType, status, name, nric, contactNumber } = req.body;
        if (!taskId) {
            log.warn(`TaskId ${taskId} can not be null.`)
            return Response.error(res, `TaskId ${taskId} can not be null.`)
        }
        let driver = await _SystemDriver.Driver.findByPk(taskId);
        if (!driver) {
            log.warn(`TaskId ${taskId} do not exist.`)
            return Response.error(res, `TaskId ${taskId} do not exist.`)
        }
        if (driverId) driver.driverId = driverId;
        if (permitType) driver.permitType = permitType;
        if (status) driver.status = status;
        if (name) driver.name = name;
        if (nric) driver.nric = nric;
        if (contactNumber) driver.contactNumber = contactNumber;
        await driver.save();

        return Response.success(res, 'success')
    },
    updateVehicle: async function (req, res) {
        let { taskId, vehicleStatus, vehicleNo, permitType, vehicleType } = req.body;
        if (!taskId) {
            log.warn(`TaskId ${taskId} can not be null.`)
            return Response.error(res, `TaskId ${taskId} can not be null.`)
        }
        let vehicle = await _SystemVehicle.Vehicle.findByPk(taskId);
        if (!vehicle) {
            log.warn(`TaskId ${taskId} do not exist.`)
            return Response.error(res, `TaskId ${taskId} do not exist.`)
        }
        if (vehicleStatus) vehicle.vehicleStatus = vehicleStatus;
        if (vehicleNo) vehicle.vehicleNumber = vehicleNo;
        if (permitType) vehicle.permitType = permitType;
        if (vehicleType) vehicle.vehicleType = vehicleType;
        await vehicle.save();
        return Response.success(res, 'success')
    },
    startTask: async function (body) {
        let { taskId, taskStatus, driverStatus, mobileStartTime } = body;
        if (!taskId) {
            log.warn(`TaskId ${taskId} can not be null.`)
            return {
                "code": 0,
                "msg": `TaskId ${taskId} can not be null.`,
                "data": ""
            }
        }
        
        let task = await Task.findByPk(taskId);
        if (!task) {
            log.warn(`TaskId ${taskId} do not exist.`)
            return {
                "code": 0,
                "msg": `TaskId ${taskId} do not exist.`,
                "data": ""
            }
        }

        // Task already started
        if (task.mobileStartTime) {
            log.warn(`TaskID => ${ taskId }: has already started, ignore this request.`)
            return {
                "code": 2,
                "msg": `TaskId ${ taskId } has already started.`,
                "data": ""
            }
        }

        // 1 update task status
        if (task.dataFrom == 'SYSTEM') {
            await sequelizeSystemObj.transaction(async (t1) => {
                let sysTask = await _SystemTask.Task.findByPk(taskId);
                if (!sysTask) {
                    log.warn(`SystemTask TaskId ${taskId} do not exist.`)
                    return {
                        "code": 0,
                        "msg": `SystemTask TaskId ${taskId} do not exist.`,
                        "data": ""
                    }
                }
                sysTask.taskStatus = taskStatus;
                sysTask.mobileStartTime = mobileStartTime;
                await sysTask.save();
    
                // 2 update driver status
                let driver = await _SystemDriver.Driver.findByPk(taskId);
                driver.status = driverStatus;
                await driver.save();
            }).catch(error => {
                log.error(error)
                throw error
            })
        }

        task.mobileStartTime = mobileStartTime;
        task.driverStatus = "started"
        task.vehicleStatus = "started"
        await task.save();

        return {
            "code": 1,
            "message": "success"
        }
    },
    endTask: async function (body) {
        let { taskId, taskStatus, driverStatus, endTime } = body;
        if (!taskId) {
            log.warn(`TaskId ${taskId} can not be null.`)
            return {
                "code": 0,
                "msg": `TaskId ${taskId} can not be null.`,
                "data": ""
            }
        }
        let task = await Task.findByPk(taskId);
        if (!task) {
            log.warn(`TaskId ${taskId} do not exist.`)
            return {
                "code": 0,
                "msg": `TaskId ${taskId} do not exist.`,
                "data": ""
            }
        }

        if (task.dataFrom == 'SYSTEM') {
            await sequelizeSystemObj.transaction(async (t1) => {
                // 1 update task status
                let systask = await _SystemTask.Task.findByPk(taskId);
                if (!systask) {
                    log.warn(`SystemTask TaskId ${taskId} do not exist.`)
                    return {
                        "code": 0,
                        "msg": `SystemTask TaskId ${taskId} do not exist.`,
                        "data": ""
                    }
                }
            
                systask.taskStatus = taskStatus;
                systask.endTime = endTime;
                await systask.save();

                // 2 update driver status
                let driver = await _SystemDriver.Driver.findByPk(taskId);
                driver.status = driverStatus;
                await driver.save();
                
            }).catch(error => {
                log.error(error)
                throw error
            })
            
        }

        task.mobileEndTime = endTime;
        await task.save();
        
        return {
            "code": 1,
            "message": "success"
        }
    },
    getCheckList: async function (req, res) {
        let today = moment().format("YYYY-MM-DD")

        const checkMTRacSigned = async function (checkListName, task) {
            if (checkListName === CHECKLIST[5]) {
                let mtRAC = await MT_RAC.findOne({ where: { taskId: task.taskId }, order: [ [ 'id', 'DESC' ] ], limit: 1 })
                // wpt task don`t need mt_rac
                if (task?.purpose.toLowerCase() == 'wpt') {
                    return {
                        "name": checkListName,
                        "due": "Complete",
                        "disabled": 1,
                    }
                }
                if (mtRAC) {
                    if (!mtRAC.officerSignature) {
                        return {
                            "name": checkListName,
                            "due": "un-signed",
                            "signature": 'officer',
                            "disabled": 1,
                        }
                    }
                    if (mtRAC.needCommander && !mtRAC.commanderSignature) {
                        return {
                            "name": checkListName,
                            "due": "un-signed",
                            "signature": 'commander',
                            "disabled": 1,
                        }
                    }
                    return {
                        "name": checkListName,
                        "due": "Complete",
                        "disabled": 1,
                    }
                }
            }

            return null
        }
        const GetData = async function (task, type, checkListName, checkListItem) {
            let executionDate = task.indentStartTime
            // already complete
            if (checkListItem) {
                return {
                    "name": checkListName,
                    "due": "Complete",
                    "disabled": 1,
                }
            }

            let result = await checkMTRacSigned(checkListName, task)
            if (result) {
                return result
            }

            // overdue
            let duetime = ""
            if (type == 1) {
                duetime = moment(executionDate).format("YYYY-MM-DD 00:00")
            } else {
                duetime = moment(executionDate).add(1, 'd').format("YYYY-MM-DD 00:00")
            }
            let dayDiff = moment(duetime).diff(moment(), 's')
            if (dayDiff > 0 ? 0 : 1) {
                return {
                    "name": checkListName,
                    "due": "OVERDUE",
                    "disabled": 1,
                }
            }

            let disabled = 0
            let isTomorrow = 0
            if (type == 2) {
                disabled = executionDate == today ? 0 : 1
                isTomorrow = today == moment(executionDate).subtract(1, 'd').format("YYYY-MM-DD")
            } else {
                isTomorrow = today == moment(executionDate).subtract(2, 'd').format("YYYY-MM-DD")
            }
            let mDiff = moment(duetime).diff(moment(moment().format("YYYY-MM-DD HH:mm")), 'm')
            let dueHour = parseInt(mDiff / 60)
            let dueMin = mDiff % 60
            let due = []
            if (dueHour != 0) due.push(dueHour + 'h')
            if (dueMin != 0) due.push(dueMin + 'm')

            return {
                "name": checkListName,
                "due": isTomorrow ? "Due TOMORROW" : `Due in ${due.join(' ')}`,
                "disabled": disabled,
            }

        }
        let { taskId } = req.body
        let task = null;
        let indentId = null;
        if (taskId.startsWith('DUTY')) {
            let idArray = taskId.split('-');
            if (idArray.length < 2) {
                log.warn(`TaskId ${taskId} format error.`)
                return res.json(utils.response(0, `TaskId ${taskId} format error.`));
            }
            taskId = `DUTY-${idArray[1]}`;
            if (idArray.length == 3) {
                indentId = idArray[2];
                let taskList = await sequelizeObj.query(`
                    SELECT
                        ud.dutyId as taskId,
                        ui.startTime as indentStartTime,
                        ui.endTime as indentEndTime,
                        uc.purpose,
                        ui.vehicleNo as vehicleNumber,
                        ui.driverId,
                        ui.indentId,
                        ui.status as driverStatus
                    FROM urgent_indent ui
                    LEFT JOIN urgent_duty ud on ui.dutyId = ud.id
                    LEFT JOIN urgent_config uc ON ud.configId = uc.id
                    WHERE ui.id = '${indentId}'
                `, { 
                    type: QueryTypes.SELECT, replacements: []
                });
                if (taskList && taskList.length > 0) {
                    task = taskList[0];
                }
            } else {
                let taskList = await sequelizeObj.query(` 
                    SELECT
                        ud.dutyId as taskId,
                        ud.indentStartDate as indentStartTime,
                        ud.indentEndDate as indentEndTime,
                        uc.purpose,
                        uc.vehicleNo as vehicleNumber,
                        uc.driverId,
                        '' as indentId,
                        ud.status as driverStatus
                    FROM urgent_duty ud
                    LEFT JOIN urgent_config uc ON ud.configId = uc.id
                    WHERE ud.dutyId = '${taskId}'
                `, { 
                    type: QueryTypes.SELECT, replacements: []
                });
                if (taskList.length) {
                    task = taskList[0];
                }
            }
        } else {
            task = await Task.findOne({ where: { taskId } })
        }
        if (!task) {
            log.warn(`TaskId ${taskId} do not exist.`)
            return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
        }
        task = task.dataValues ? task.dataValues : task;
        let result = {
            "beforeExecutionDay": {
                "time": "TAKES 2 HRS",
                "data": []
            },
            "executionDay": {
                "time": "TAKES 45 MINS",
                "data": []
            }
        }
        await sequelizeObj.transaction(async (t1) => {
            // Update Training info in checkList
            let trainingCheckList = await CheckList.findOne({ where: { taskId: taskId, checkListName: CHECKLIST[4] } })
            if (!trainingCheckList) {
                let vehicle = await Vehicle.findByPk(task.vehicleNumber)
                let taskHistory = await sequelizeObj.query(`
                    SELECT * FROM task t
                    LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
                    WHERE t.driverId = ? AND v.vehicleType = ?
                    AND t.mobileEndTime IS NOT NULL
                    ORDER BY t.mobileStartTime DESC LIMIT 1
                `, { type: QueryTypes.SELECT, replacements: [ task.driverId, vehicle.vehicleType ] })
                if (taskHistory && taskHistory.length > 0) {
                    // Exist vehicleType record
                    if (moment(task.indentStartTime).diff(moment(taskHistory[0].mobileStartTime), 'days') < conf.Training_LimitDays) {
                        // Check days
                        await CheckList.create({
                            taskId: taskId,
                            indentId: task.indentId,
                            driverId: task.driverId,
                            vehicleNo: vehicle.vehicleNo,
                            checkListName: CHECKLIST[4],
                        })
                    }
                }
            }

            let datas = await CheckList.findAll({ where: { taskId: taskId } })
            let checkList1 = datas.find(item => item.checkListName == CHECKLIST[1])
            let checkList2 = datas.find(item => item.checkListName == CHECKLIST[2])
            let checkList3 = datas.find(item => item.checkListName == CHECKLIST[3])
            let checkList4 = datas.find(item => item.checkListName == CHECKLIST[4])
            let checkList5 = datas.find(item => item.checkListName == CHECKLIST[5])
            
            result.beforeExecutionDay.data.push(await GetData(task, 1, CHECKLIST[1], checkList1))
            result.beforeExecutionDay.data.push(await GetData(task, 1, CHECKLIST[2], checkList2))

            //result.executionDay.data.push(await GetData(sysTask, 2, CHECKLIST[3], checkList3))
            result.executionDay.data.push(await GetData(task, 2, CHECKLIST[4], checkList4))
            result.executionDay.data.push(await GetData(task, 2, CHECKLIST[5], checkList5))
        }).catch(error => {
            throw error
        })
        
        return res.json(utils.response(1, result));
    },
    updateCheckList: async function (req, res) {
        try {
            let { taskId, indentId, driverId, vehicleNo, checkListName } = req.body
            let task = await Task.findOne({ where: { taskId } })
            if (!task) {
                log.warn(`TaskId ${taskId} do not exist.`)
                return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
            }
    
            let checkListObj = await CheckList.findOne({
                where: {
                    taskId: taskId,
                    checkListName: checkListName,
                }
            })
            if (checkListObj) {
                return res.json(utils.response(0, `Already complete.`));
            }
    
            await sequelizeObj.transaction(async (t1) => {
                if (checkListName == CHECKLIST[2]) {
                    task.vehicleStatus = "ready"
                    await task.save();
                } else if (checkListName == CHECKLIST[5]) {
                    // task.driverStatus = "ready"
                    // await task.save();

                    return;
                }
    
                await CheckList.create({
                    taskId: taskId,
                    indentId: indentId,
                    driverId: driverId,
                    vehicleNo: vehicleNo,
                    checkListName: checkListName,
                })
            }).catch(error => {
                throw error
            })
            return res.json(utils.response(1, "Success"));
        } catch (ex) {
            log.error(ex)
            return res.json(utils.response(0, "Failed"));
        }
    },
    completePretaskCheckItem: async function(req, res) {
        let { taskId, checkListName } = req.body
        let task = null;
        let indentId = null;
        if (taskId.startsWith('DUTY')) {
            let idArray = taskId.split('-');
            if (idArray.length < 2) {
                log.warn(`TaskId ${taskId} format error.`)
                return res.json(utils.response(0, `TaskId ${taskId} format error.`));
            }
            taskId = `DUTY-${idArray[1]}`;
            if (idArray.length == 3) {
                indentId = idArray[2];
                let taskList = await sequelizeObj.query(`
                    SELECT
                        ud.dutyId as taskId,
                        ui.startTime as indentStartTime,
                        ui.endTime as indentEndTime,
                        uc.purpose,
                        ui.vehicleNo as vehicleNumber,
                        ui.driverId,
                        ui.indentId,
                        ui.status as driverStatus
                    FROM urgent_indent ui
                    LEFT JOIN urgent_duty ud on ui.dutyId = ud.id
                    LEFT JOIN urgent_config uc ON ud.configId = uc.id
                    WHERE ui.id = '${indentId}'
                `, { 
                    type: QueryTypes.SELECT, replacements: []
                });
                if (taskList && taskList.length > 0) {
                    task = taskList[0];
                }
            } else {
                let taskList = await sequelizeObj.query(` 
                    SELECT
                        ud.dutyId as taskId,
                        ud.indentStartDate as indentStartTime,
                        ud.indentEndDate as indentEndTime,
                        uc.purpose,
                        uc.vehicleNo as vehicleNumber,
                        uc.driverId,
                        '' as indentId,
                        ud.status as driverStatus
                    FROM urgent_duty ud
                    LEFT JOIN urgent_config uc ON ud.configId = uc.id
                    WHERE ud.dutyId = '${taskId}'
                `, { 
                    type: QueryTypes.SELECT, replacements: []
                });
                if (taskList && taskList.length > 0) {
                    task = taskList[0];
                }
            }
        } else {
            task = await Task.findOne({ where: { taskId } })
        }
        if (!task) {
            log.warn(`TaskId ${taskId} do not exist.`)
            return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
        }
        task = task.dataValues ? task.dataValues : task;
        await CheckList.create({
            taskId: taskId,
            indentId: task.indentId,
            driverId: task.driverId,
            vehicleNo: task.vehicleNumber,
            checkListName: checkListName,
        })

        let checkList = await CheckList.findAll({ where: { taskId: task.taskId } })
        let checkList1 = checkList.find(item => item.checkListName == CHECKLIST[1])
        let checkList2 = checkList.find(item => item.checkListName == CHECKLIST[2])
        let checkList4 = checkList.find(item => item.checkListName == CHECKLIST[4])
        let checkList5 = checkList.find(item => item.checkListName == CHECKLIST[5])

        const updateTaskStatus = async function () {
            let newStatus = '';
            if (task.driverStatus == 'waitcheck' && checkList1 && checkList2 && checkList4) {
                if (task.purpose?.toLowerCase() == 'wpt' || checkList5) {
                    newStatus = "ready";
                } 
                if (newStatus == 'ready') {
                    if (taskId.startsWith('DUTY')) {
                        await UrgentDuty.update({status: "ready" }, {where: {dutyId: taskId, status: 'waitcheck' }});
                        await UrgentIndent.update({status: "ready" }, {where: {indentId: task.indentId, status: 'waitcheck'}});
                    } else {
                        await Task.update({driverStatus: "ready", vehicleStatus: "ready"}, {where: {taskId: taskId}});
                    }
                }
            } 
        }
        await updateTaskStatus()

        return res.json(utils.response(1, "Success."));
    },
    updateTaskOptTime: async function (body) {
        const DriverComplete = function (driver, trip, task, operationTime, executeTime) {
            let status = DRIVER_STATUS.COMPLETED
            // NO SHOW arrive time - execute time >=30 mins
            // LATE TRIP arrive time - execute time >=15 mins <30 mins
            let arrivalTime = task.arrivalTime;
            if (arrivalTime) {
                let noshowTime = moment(arrivalTime).subtract(30, 'minute');
                let lateTripTime = moment(arrivalTime).subtract(15, 'minute');
                if (noshowTime.isSameOrAfter(moment(executeTime))) {
                    status = DRIVER_STATUS.NOSHOW
                } else if (lateTripTime.isSameOrAfter(moment(executeTime))) {
                    status = DRIVER_STATUS.LATE
                }
            }
        
            if (driver) {
                driver.set({ status: status })
            }
            task.set({ taskStatus: status })
            let completeCount = trip.completeCount + 1
            let noOfVehicle = Number(trip.noOfVehicle)
            trip.set({ completeCount: completeCount })
            if (noOfVehicle == 1) {
                trip.set({ status: status })
            } else if (noOfVehicle == completeCount) {
                trip.set({ status: DRIVER_STATUS.COMPLETED })
            }
            
            return status
        }
        try {
            let { taskId, serviceModeValue, operationTime, operationType } = body;
            await sequelizeSystemObj.transaction(async (t1) => {
                const checkTask = async function (taskId) {
                    let task = await _SystemTask.Task.findByPk(taskId);
                    if (!task) {
                        log.warn(`TaskId ${ taskId } do not exist.`)
                        throw new Error(`TaskId ${ taskId } do not exist.`)
                    }
                    return task;
                }
                
                log.info(`updateTaskOptTime => taskId: ${ taskId }`);
                log.info(`updateTaskOptTime => serviceModeValue: ${ serviceModeValue }`);
                log.info(`updateTaskOptTime => operationTime: ${ operationTime }`);
                log.info(`updateTaskOptTime => operationType: ${ operationType }`);
                let task = await checkTask(taskId);
                let trip = await _SystemJob.Job.findByPk(task.tripId)                
                let driver = await _SystemDriver.Driver.findByPk(taskId);
                serviceModeValue = serviceModeValue.toLowerCase();
                operationType = operationType.toLowerCase();
                if (serviceModeValue === 'ferry service' && operationType === 'arrive') {
                    // driver.status = 'Completed';
                    // task.taskStatus = 'Completed'
                    DriverComplete(driver, trip, task, operationTime, task.startDate)
                    task.arrivalTime = operationTime;
                } else if (serviceModeValue === 'delivery') {
                    if (operationType === 'arrive') {
                        driver.status = DRIVER_STATUS.ARRIVED;
                        task.taskStatus = DRIVER_STATUS.ARRIVED
                        task.arrivalTime = operationTime;
                    } else if (operationType === 'depart') {
                        // driver.status = 'Completed';
                        // task.taskStatus = 'Completed'
                        DriverComplete(driver, trip, task, operationTime, task.startDate)
                        task.departTime = operationTime;
                    }
                } else if (serviceModeValue === 'pickup') {
                    if (operationType === 'arrive') {
                        driver.status = DRIVER_STATUS.ARRIVED;
                        task.taskStatus = DRIVER_STATUS.ARRIVED
                        task.arrivalTime = operationTime;
                    } else if (operationType === 'end') {
                        // driver.status = 'Completed';
                        // task.taskStatus = 'Completed'
                        DriverComplete(driver, trip, task, operationTime, task.startDate)
                        task.endTime = operationTime;
                    }
                } else {
                    if (operationType === 'arrive') {
                        driver.status = DRIVER_STATUS.ARRIVED;
                        task.taskStatus = DRIVER_STATUS.ARRIVED
                        task.arrivalTime = operationTime;
                    } else if (operationType === 'depart') {
                        driver.status = DRIVER_STATUS.DEPARTED
                        task.taskStatus = DRIVER_STATUS.DEPARTED
                        task.departTime = operationTime;
                    } else if (operationType === 'end') {
                        // driver.status = 'Completed';
                        // task.taskStatus = 'Completed'
                        DriverComplete(driver, trip, task, operationTime, task.startDate)
                        task.endTime = operationTime;
                    }
                }
                await task.save();
                await driver.save();

                await _SystemJob.OperationHistory.create({
					requestId: task.requestId,
					tripId: task.tripId,
					taskId: taskId,
					status: driver.status,
					action: driver.status,
					remark: driver.name
				});
            }).catch(error => {
                throw error
            })
            return {
                "code": 1,
                "message": "success"
            }
        } catch (error) {
            log.error(error)
            return {
                "code": 0,
                "message": error
            }
        }
    },
    updateTaskStartTime: async function (taskId, mobileStartTime) {
        try {
            await _SystemTask.Task.update({ mobileStartTime }, { where: { id: taskId } })
            return {
                "code": 1,
                "message": "success"
            }
        } catch (error) {
            log.error(error)
            return {
                "code": 0,
                "message": error
            }
        }
    },
    // get the same TO is assigned to the next task in the same indent with the same vehicle
    getDriverNextTask: async function (req, res) {
        try {
            let userId = req.body.userId;
            let user = await User.findByPk(userId);
            if (!user) {
                throw new Error(`GetDriverNextTask UserId ${ userId } do not exist.`)
            }
            let currentTaskId = req.body.currentTaskId;
            let currentTask = null;
            if (currentTaskId.startsWith('DUTY')) {
				return res.json(utils.response(1, null));
			} else {
				currentTask = await Task.findOne({ where: { taskId: currentTaskId } });
			}

            if (!currentTask) {
                throw new Error(`GetDriverNextTask TaskId ${ currentTaskId } do not exist.`)
            }
            log.info(`Task complete GetDriverNextTask start, currentTaskId: ${currentTaskId}.`);
            let vehicleNo = currentTask.vehicleNumber
            let indentId = currentTask.indentId
            let driverId = currentTask.driverId

            let nextTask = null;
            if (vehicleNo && indentId && driverId) {
                let nextTaskList = await sequelizeObj.query(`
                    SELECT tt.* FROM task tt
                    WHERE tt.indentId =?
                    AND tt.vehicleNumber =?
                    AND tt.driverId =?
                    AND tt.driverStatus != 'completed'
                    AND tt.indentEndTime > NOW()
                    ORDER BY tt.indentStartTime ASC LIMIT 1
                `, { type: QueryTypes.SELECT, replacements: [ indentId, vehicleNo, driverId ] });
                if (nextTaskList && nextTaskList.length > 0) {
                    nextTask = nextTaskList[0];
                }
            }
            log.info(`Task complete GetDriverNextTask success, userId: ${userId}.`);
            return res.json(utils.response(1, nextTask));
        } catch (error) {
            log.info(`Task complete GetDriverNextTask fail, errorMsg: ` + (error && error.message ? error.message : "GetDriverNextTask info fail!"));
            log.error(error)
            return res.json(utils.response(0, null));
        }
    },
    submitComment: async function (req, res) {
        try {
            let { taskId, createdBy, starVal, question, options, driverId, remark } = req.body

            let tempTask = null;
			if (taskId.startsWith('DUTY')) {
				let temp = taskId.split('-')
				let urgentId = temp[2];
                tempTask = await UrgentIndent.findByPk(urgentId);
                if (tempTask) {
                    taskId = tempTask.indentId;
                    tempTask.dataFrom = 'SYSTEM';
                }
			} else {
				tempTask = await Task.findByPk(taskId);
			}
			if (!tempTask) {
				log.warn(`TaskId ${taskId} do not exist.`)
				return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
			}

            if (tempTask.dataFrom == 'SYSTEM') {
                await System_Comment.create({
                    taskId: taskId,
                    starVal: starVal,
                    question: question,
                    options: JSON.stringify(JSON.parse(options)),
                    remark: remark,
                    createdBy: createdBy,
                    driverId: driverId,
                    dataFrom: "TO",
                });
            }
            await Comment.create({
                taskId: taskId,
                starVal: starVal,
                question: question,
                options: JSON.stringify(JSON.parse(options)),
                remark: remark,
                createdBy: createdBy,
                driverId: driverId,
                dataFrom: "TO",
            });
            return res.json(utils.response(1, "Success."));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getComment: async function (req, res) {
        try {
            let { taskId } = req.body
            let comment = await Comment.findOne({
                attributes: ['taskId', 'starVal', 'options', 'remark'],
                where: {
                    taskId: taskId,
                    dataFrom: "TO"
                },
                order: [
                    ['id', 'desc']
                ]
            })
            return res.json(utils.response(1, comment));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    loadGuidePageUrl: async function (req, res) {
        return res.json(utils.response(1, {guidePageUrl: conf.mobileGuidePageUrl}));
    }
}
