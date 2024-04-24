const log = require('../log/winston').logger('mobile Task Service');
const CONTENT = require('../util/content');
const moment = require('moment');

const utils = require('../util/utils');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const { sequelizeSystemObj } = require('../db/dbConf_system');

const { User } = require('../model/user.js');
const { Unit } = require('../model/unit.js');
const { Vehicle } = require('../model/vehicle.js');
const { Driver } = require('../model/driver.js');
const { PurposeMode } = require('../model/purposeMode.js');
const { Task } = require('../model/task.js');
const { MobileTrip } = require('../model/mobileTrip.js');
const { OperationRecord } = require('../model/operationRecord.js');
const { DriverPlatformConf } = require('../model/driverPlatformConf.js');

const _SystemLocation = require('../model/system/location');
const { UserUtils } = require('../services/userService');
const { TaskUtils } = require('../services/mtAdminService.js');

module.exports = {
    cancelMobileTaskById: async function (req, res) {
        try {
            let { tripId } = req.body;
            let userId = req.cookies.userId;
            let taskId = 'CU-M-' + tripId;

            let mobileTrip = await MobileTrip.findByPk(tripId);
            if (!mobileTrip) {
                throw new Error(`Trip:${ tripId } does not exist!`)
            }

            let task = await Task.findByPk(taskId);
            if (!task || [ 'waitcheck', 'ready'].indexOf(task.driverStatus) > -1) {
                await MobileTrip.update({ status: 'Cancelled', cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), cancelledBy: userId}, { where: { id: tripId } } );
                if (task) {
                    await Task.update({ driverStatus: 'Cancelled', vehicleStatus: 'Cancelled' }, { where: { taskId, dataFrom: 'MOBILE' } } )
                }
                //opt log
                let operationRecord = {
                    operatorId: req.cookies.userId,
                    businessType: 'task',
                    businessId: taskId,
                    optType: 'cancel',
                    beforeData: '',
                    afterData: '',
                    optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                    remarks: 'Server user cancel task.'
                }
                await OperationRecord.create(operationRecord);
            } else {
                throw new Error(`Task:${ taskId } current status is ${ task.driverStatus }, can not cancel.`)
            }
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    approveMobileTaskById: async function (req, res) {
        try {
            let { tripId } = req.body;
            let userId = req.cookies.userId;
            let taskId = 'CU-M-' + tripId;
            let user = await User.findByPk(userId)
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }
            let trip = await MobileTrip.findByPk(tripId);
            if (!trip) {
                throw new Error(`Trip:${ tripId } does not exist!`)
            }

            if (trip.status != 'Pending Approval') {
                throw new Error(`Trip:${ tripId } status is ${trip.status}, can't approve!`)
            }

            let tripUnitId = trip.unitId;
            let hub = '-';
            let node = '-';
            if (tripUnitId) {
                let unit = await Unit.findByPk(tripUnitId);
                if (unit) {
                    hub = unit.unit;
                    node = unit.subUnit;
                }
            }

            // dv trip need approve
            await MobileTrip.update({ status: 'Approved', approveDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), approveBy: userId}, { where: { id: tripId } } );
            let task = await Task.findByPk(taskId);
            if (!task) {
                await Task.create({
                    taskId: taskId,
                    dataFrom: "MOBILE",
                    driverId: trip.driverId,
                    indentId: trip.id,
                    vehicleNumber: trip.vehicleNumber,
                    driverStatus: 'waitcheck',
                    vehicleStatus: 'waitcheck',
                    indentStartTime: trip.indentStartTime,
                    indentEndTime: trip.indentEndTime,
                    purpose: trip.purpose,
                    pickupDestination: trip.pickupDestination,
                    dropoffDestination: trip.dropoffDestination,
                    pickupGPS: trip.pickupGPS,
                    dropoffGPS: trip.dropoffGPS,
                    groupId: trip.groupId ? trip.groupId : null,
                    creator: userId,
                    hub: hub,
                    node: node
                })
            } else {
                log.warn(`mobileTaskService.approveMobileTaskById trip: ${tripId} has aleady create task!`);
            }

            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getSystemLocation: async function (req, res) {
        try {
            let locationList = await _SystemLocation.Location.findAll()
            return res.json(utils.response(1, locationList));
        } catch (ex) {
            log.error(ex)
            return res.json(utils.response(0, "Get Location Failed"));
        }
    },
    getPurpose: async function (req, res) {
        try {
            let list = await PurposeMode.findAll()
            return res.json(utils.response(1, list));
        } catch (ex) {
            log.error(ex)
            return res.json(utils.response(0, "Get Purpose Failed"));
        }
    },
    getDVLOATaskVehicle: async function (req, res) {
        try {
            let userId = req.cookies.userId;

            let { tripId, startDate, endDate } = req.body;
            let groupId = null;
            let unitId = null;
            let driverId = null;

            let mobileTrip = await MobileTrip.findByPk(tripId);
            if (!mobileTrip) {
                let msg = `Mobile task ${userId} not exist!`
                log.warn(msg)
                return res.json(utils.response(0, msg));
            }
            log.info(`getDVLOATaskVehicle: mobileTripInfo:${JSON.stringify(mobileTrip.dataValues)}`);

            driverId = mobileTrip.driverId;
            groupId = mobileTrip.groupId;
            unitId = mobileTrip.unitId;

            if (!startDate || !endDate) {
                log.warn(`startDate or endDate is empty!`)
                return res.json(utils.response(1, []));
            }

            // driver support vehicle type
            let driverSupportVehicleTypeStr = '';
            if (driverId) {
                let driverSupportVehicleTypes = await sequelizeObj.query(`
                    SELECT GROUP_CONCAT(vehicleType) as vehicleTypes FROM driver_platform_conf where driverId=${driverId} and approveStatus='Approved'
                `, { type: QueryTypes.SELECT });
                if (driverSupportVehicleTypes && driverSupportVehicleTypes.length > 0) {
                    driverSupportVehicleTypeStr =  driverSupportVehicleTypes[0].vehicleTypes;
                }
            }

            log.info(`getDVLOATaskVehicle: groupId:${groupId}, unitId: ${unitId}, purpose: ${mobileTrip.purpose}, driverSupportVehicleType: ${driverSupportVehicleTypeStr}`);

            let vehicleList = [];
            if (groupId) {
                //mobile task Or customer task has groupId, mobile task no vehicleType
                vehicleList = await TaskUtils.getVehicleByGroup(mobileTrip.purpose, true, groupId, driverSupportVehicleTypeStr, mobileTrip.indentStartTime, mobileTrip.indentEndTime);
            } else {
                vehicleList = await TaskUtils.getVehicleList(mobileTrip.purpose, driverSupportVehicleTypeStr, mobileTrip.indentStartTime, mobileTrip.indentEndTime, unitId, null, null,  null );
            }
            log.info(`getDVLOATaskVehicle: resultLength:${vehicleList ? vehicleList.length : null}`);
            let result = []
            if (vehicleList && vehicleList.length > 0) {
                let vehicleNumnerList = [...new Set(vehicleList.map(a => a.vehicleNo))]
                for (let vehicleNumber of vehicleNumnerList) {
                    result.push({
                        vehicleNumber: vehicleNumber,
                        startDate: null,
                        endDate: null,
                        vehicleType: '',
                    })
                }
            }
            
            return res.json(utils.response(1, result));
        } catch (ex) {
            log.error(ex)
            return res.json(utils.response(0, "Get Vehicle Failed"));
        }
    },

    editMobileTask: async function (req, res) {
        try {
            let {
                tripId,
                purpose,
                vehicle,
                reportingLocation,
                destination,
                startDatetime,
                endDatetime
            } = req.body
            
            let vehicleData = await Vehicle.findByPk(vehicle)
            if (!vehicleData) {
                return res.json(utils.response(0, `Vehicle ${vehicle} not exist`));
            }

            let mobileTrip = await MobileTrip.findByPk(tripId);
            if (!mobileTrip) {
                return res.json(utils.response(0, `Mobile Trip[${tripId}] not exist!`));
            }
            if (mobileTrip.status == 'Cancelled') {
                return res.json(utils.response(0, `Mobile Trip[${tripId}] is cancelled, can't edit!`));
            }
            
            let driverId = mobileTrip.driverId;
            let driverUser = await User.findOne({where: {driverId: driverId}});
            if (driverUser?.role == 'DV') {
                let reportingLocationObj = await _SystemLocation.Location.findOne({where: {locationName: reportingLocation}});
                let destinationObj = await _SystemLocation.Location.findOne({where: {locationName: destination}});
                let pickupGPS = "0.0,0.0";
                if (reportingLocationObj) {
                    pickupGPS = reportingLocationObj.lat + "," + reportingLocationObj.lng;
                }
                let dropoffGPS = "0.0,0.0";
                if (destinationObj) {
                    dropoffGPS = destinationObj.lat + "," + destinationObj.lng;
                }
                let newTaskInfo = {
                    vehicleNumber: vehicle,
                    indentStartTime: startDatetime,
                    indentEndTime: endDatetime,
                    purpose: purpose,
                    pickupDestination: reportingLocation,
                    dropoffDestination: destination,
                    pickupGPS: pickupGPS,
                    dropoffGPS: dropoffGPS,
                };

                await MobileTrip.update(newTaskInfo, {where: {id: tripId}})

                let task = await Task.findByPk('CU-M-' + tripId);
                if (task) {
                    if (task.mobileStartTime || task.driverStatus == 'Cancelled' || task.driverStatus == 'completed') {
                        return res.json(utils.response(0, `Task[${'CU-M-' + tripId}] can't be edit!`));
                    }
                    await Task.update({
                        vehicleNumber: vehicle,
                        indentStartTime: startDatetime,
                        indentEndTime: endDatetime,
                        purpose: purpose,
                        pickupDestination: reportingLocation,
                        dropoffDestination: destination,
                        pickupGPS: pickupGPS,
                        dropoffGPS: dropoffGPS,
                    }, {where: {taskId: 'CU-M-' + tripId}})    
                }

                //opt log
                let operationRecord = {
                    operatorId: req.cookies.userId,
                    businessType: 'task',
                    businessId: tripId,
                    optType: 'editMobileTrip',
                    beforeData: JSON.stringify(mobileTrip),
                    afterData: JSON.stringify(newTaskInfo),
                    optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                    remarks: 'Server user edit mobileTrip.'
                }
                await OperationRecord.create(operationRecord);

                return res.json(utils.response(1, true));
            }
        } catch (ex) {
            log.error(ex)
            return res.json(utils.response(0, "Edit Trip Failed"));
        }
    }
}