const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const incidentConf = require('../conf/incidentConf');
const CONTENT = require('../util/content');
const SOCKET = require('../socket/socket');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const path = require('path');
const fs = require('graceful-fs');
const xlsx = require('node-xlsx');

const { Route } = require('../model/route');
const { RouteWaypoint } = require('../model/routeWaypoint');
const { Driver, Emergency } = require('../model/driver');
const { DriverHistory } = require('../model/driverHistory.js');
const { DriverPlatform } = require('../model/driverPlatform');
const { DriverMileage } = require('../model/driverMileage');
const { PermitType } = require('../model/permitType');
const { DriverTask } = require('../model/driverTask');
const { DriverPosition } = require('../model/driverPosition');
const { VehicleRelation } = require('../model/vehicleRelation.js');
const { User } = require('../model/user.js');
const { UserGroup } = require('../model/userGroup.js');
const { Unit } = require('../model/unit.js');
const { Friend } = require('../model/friend.js');
const { DriverPermit } = require('../model/driverPermit.js');
const { Task } = require('../model/task.js');
const { Mileage } = require('../model/mileage.js');
const { Vehicle } = require('../model/vehicle.js');
const { DriverLeaveRecord } = require('../model/driverLeaveRecord.js');
const { DriverAssessmentRecord } = require('../model/driverAssessmentRecord.js');
const { DriverPlatformConf } = require('../model/driverPlatformConf.js');
const { DriverPermitTypeDetail, DriverPermitTypeDetailHistory } = require('../model/driverPermitTypeDetail.js');

const { Track } = require('../model/event/track.js');
const { TrackHistory } = require('../model/event/trackHistory.js');

const groupService = require('../services/groupService');
const userService = require('../services/userService');
const unitService = require('../services/unitService');
const vehicleService = require('../services/vehicleService');
const { UserZone } = require('../model/userZone.js');
const { MtAdmin } = require('../model/mtAdmin.js');
const { OperationRecord } = require('../model/operationRecord.js');

const _SystemDriver = require('../model/system/driver');
const _SystemVehicle = require('../model/system/vehicle');
const _SystemTask = require('../model/system/task');

const FirebaseService = require('../firebase/firebase');

const { DriverLicenseExchangeApply } = require('../model/DriverLicenseExchangeApply.js');
const { Notification } = require('../model/notification.js');

const { DriverMonthAchievement } = require('../model/driverMonthAchievement.js');
const { HOTO } = require('../model/hoto');
const { HOTORecord } = require('../model/hotoRecord');
const { loan } = require('../model/loan.js');
const { loanRecord } = require('../model/loanRecord.js');
const { MobileTrip } = require('../model/mobileTrip.js');
const { DriverPermitLog } = require('../model/driverPermitLog.js');
const { UrgentConfig } = require('../model/urgent/urgentConfig.js');
const { UrgentDuty } = require('../model/urgent/urgentDuty.js');
const { UrgentIndent } = require('../model/urgent/urgentIndent.js');
const { DriverCivilianLicence } = require('../model/driverCivilianLicence');

let TaskUtils = {
    getGroupById: async function (id) {
        let group = await sequelizeSystemObj.query(
            `select id, groupName from \`group\` where id = ${ id }`,
            {
                type: QueryTypes.SELECT
            }
        );
        return group[0]
    },
    getTaskByDriverId: async function(driverId, newHub, newNode, newUnit) {
        newNode = newNode && newNode != 'null' ? newNode : null;
        newUnit = newUnit && newUnit != 'null' ? newUnit : null;
        let validTask = await sequelizeObj.query(
            `
            SELECT * FROM task WHERE driverId = ${ Number(driverId) } 
            AND vehicleStatus NOT IN ('completed', 'cancelled') 
            AND (NOW() BETWEEN indentStartTime AND indentEndTime
            OR vehicleStatus = 'started' 
            )
            GROUP BY taskId
            `,
            {
                type: QueryTypes.SELECT
            }
        );
        let oldDriver = await Driver.findOne({ where: { driverId: driverId } });
        if(oldDriver.unitId){   
            let oldUint = await Unit.findOne({ where: { id: oldDriver.unitId } })
            if(oldUint){
                if(oldUint.unit == newHub){
                    if(oldUint.subUnit){
                        if(oldUint.subUnit == newNode){
                            validTask = []
                        }
                    } else if (!newNode) {
                        validTask = []
                    }
                }
            }
        } else {
            if(oldDriver.unit){
               if(oldDriver.unit == newUnit) {
                 validTask = []
               }
            }
        }
        return validTask;
    }
}

module.exports.TaskUtils = TaskUtils

const getDriverCategory = async function (driverId) {
    try {
        let category = '-'
        let driverCategory = await sequelizeObj.query(`
            SELECT
                assessmentType
            FROM driver_assessment_record
            WHERE driverId = ? AND \`status\` = 'Pass' and approveStatus='Approved'
            ORDER BY assessmentType ASC LIMIT 1
        `, { type: QueryTypes.SELECT, replacements: [ driverId ] });

        if (driverCategory && driverCategory.length > 0) {
            category = driverCategory[0].assessmentType
        }
        category.category = category.category == 'Category A Assessment' ? 'A' : category.category == 'Category B Assessment' ? 'B' : category.category == 'Category C Assessment' 
        ? 'C' : category.category == 'Category D Assessment' ? 'D' : '-';

        return category
    } catch (error) {
        log.error(error)
        return '-'
    }
}
module.exports.getDriverCategory = getDriverCategory

module.exports.getDriverTaskList = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
                throw Error(`User ${ userId } does not exist.`);
			}
			return user;
		}

        let driverName = req.body.driverName
        let user = await checkUser(req.cookies.userId);
        let baseSQL = `
            SELECT dt.*, d.driverName, vr.vehicleNo, r.fromAddress, r.toAddress, dt.id AS driverTaskId, d.unitId FROM driver_task dt
            LEFT JOIN vehicle_relation vr ON vr.id = dt.vehicleRelationId
            LEFT JOIN driver d ON d.driverId = vr.driverId
            LEFT JOIN route r ON r.routeNo = dt.routeNo 
        `
        let driverList = [];
        if (user.userType === CONTENT.USER_TYPE.HQ) {
            if (driverName) {
                baseSQL += ` WHERE d.driverName like ` + sequelizeObj.escape("%" + driverName + "%")
            }
            driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [] })
        } else {
            let driverUnitIdList = await unitService.getUnitPermissionIdList(user);
            let paramList = []
            if (driverUnitIdList.length) {
                paramList.push(` d.unitId IN (${ driverUnitIdList }) `)
            }
            if (paramList.length) {
                baseSQL += ' WHERE ' + paramList.join(' AND ')
                if (driverName) {
                    baseSQL += ` d.driverName like ` + sequelizeObj.escape("%" + driverName + "%")
                }
                driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
            }
        }
        return res.json(utils.response(1, driverList));    
    } catch (error) {
        log.error('(getDriverTaskList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getDriverTaskById = async function (req, res) {
    try {
        let driverTaskId = req.body.driverTaskId;
        let baseSQL = `
            SELECT dt.*, u.unit, d.driverName, vr.vehicleNo, r.fromPosition, r.toPosition, dt.id AS driverTaskId, d.unitId FROM driver_task dt
            LEFT JOIN vehicle_relation vr ON vr.id = dt.vehicleRelationId
            LEFT JOIN driver d ON d.driverId = vr.driverId
            LEFT JOIN route r ON r.routeNo = dt.routeNo 
            LEFT JOIN unit u ON u.id = d.unitId 
            WHERE dt.id = ? 
        `
        let driverTask = await sequelizeObj.query(baseSQL, { replacements: [driverTaskId], type: QueryTypes.SELECT })
        return res.json(utils.response(1, driverTask));    
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
};

module.exports.createDriverTask = async function (req, res) {
    try {
        let driverTask = req.body.driverTask;
        await sequelizeObj.transaction(async transaction => {
            const checkDriver = async function (driver) {
                let driverResult = await Driver.findByPk(driver.driverId)
                if (!driverResult) {
                    throw Error(`DriverId ${ driver.DriverId } does not exist!`)
                }
                return driverResult;
            }
            const createVehicleRelation = async function (vehicleRelation) {
                let vehicleRelationResult = await VehicleRelation.findOne({ where: { driverId: vehicleRelation.driverId, vehicleNo: vehicleRelation.vehicleNo } })
                if (vehicleRelationResult) {
                    return vehicleRelationResult
                } else {
                    // check if exist driverId or vehicleNo id null
                    let option = [{ driverId: { [Op.is]: null }, vehicleNo: vehicleRelation.vehicleNo }, { driverId: vehicleRelation.driverId, vehicleNo: { [Op.is]: null } }]
                    vehicleRelationResult = await VehicleRelation.findOne({ where: { [Op.or]: option } })
                    if (vehicleRelationResult) {
                        console.log(vehicleRelationResult)
                        if (!vehicleRelationResult.driverId) vehicleRelationResult.driverId = vehicleRelation.driverId;
                        else if (!vehicleRelationResult.vehicleNo) vehicleRelationResult.vehicleNo = vehicleRelation.vehicleNo;
                        await vehicleRelationResult.save();
                        return vehicleRelationResult;
                    } else {
                        // create new 
                        return await VehicleRelation.create(vehicleRelation, { returning: true })
                    }
                }
            }
            const createDriverTask = async function (driverTask) {
                // init arrive waypoint info
                let routeWaypointList = await RouteWaypoint.findAll({ where: { routeNo: driverTask.routeNo } });
                driverTask.arrivedInfo = [];
                for (let routeWaypoint of routeWaypointList) {
                    driverTask.arrivedInfo.push({ waypointId: routeWaypoint.waypointId, arrivedTime: '' })
                }
                driverTask.arrivedInfo = JSON.stringify(driverTask.arrivedInfo)

                // check if exist same vehicle_relation data
                let __driverTask = await DriverTask.findOne({ where: { vehicleRelationId: driverTask.vehicleRelationId } })
                if (__driverTask) {
                    await DriverTask.update(driverTask, { where: { vehicleRelationId: driverTask.vehicleRelationId } });
                } else {
                    await DriverTask.create(driverTask);
                }
            }
            
            let driver = await checkDriver({ driverId: driverTask.driverId });
            driver.unitId = driverTask.unitId;
            driver.save();
            let vehicleRelation = await createVehicleRelation({ driverId: driverTask.driverId, vehicleNo: driverTask.vehicleNo });
            driverTask.vehicleRelationId = vehicleRelation.id;
            driverTask.status = CONTENT.DRIVER_STATUS.ASSIGNED;
            await createDriverTask(driverTask);
            await Route.update({ state: CONTENT.ROUTE_STATUS.ASSIGNED }, { where: { routeNo: driverTask.routeNo } })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'success'));    
    } catch (error) {
        log.error('(createDriverTask) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.deleteDriverTask = async function (req, res) {
    try {
        let driverTaskId = req.body.driverTaskId;
        await sequelizeObj.transaction(async transaction => {
            const checkDriverTask = async function (driverTaskId) {
                let driverTask = await DriverTask.findByPk(driverTaskId);
                if (!driverTask) {
                    throw Error(`DriverTask ID ${ driverTaskId } does not exist.`)
                }
                return driverTask;
            }
            
            let driverTask = await checkDriverTask(driverTaskId)
            if (driverTask.startTime) {
                throw Error(`Can not delete driverTask, because it has started.`)
            }
            await driverTask.destroy();
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
};

module.exports.updateDriverTask = async function (req, res) {
    try {
        let driverTask = req.body.driverTask;
        await sequelizeObj.transaction(async transaction => {
            const checkDriverTask = async function (driverTaskId) {
                let driverTask = await DriverTask.findByPk(driverTaskId);
                if (!driverTask) {
                    throw Error(`DriverTask ID ${ driverTaskId } does not exist.`)
                }
                return driverTask
            }
            
            const getNewRouteInfo = async function (routeNo) {
                // init arrive waypoint info
                let routeWaypointList = await RouteWaypoint.findAll({ where: { routeNo: routeNo } });
                let arrivedInfo = [];
                for (let routeWaypoint of routeWaypointList) {
                    arrivedInfo.push({ waypointId: routeWaypoint.waypointId, arrivedTime: '' })
                }
                arrivedInfo = JSON.stringify(arrivedInfo)
                return arrivedInfo
            }

            let checkedDriverTask = await checkDriverTask(driverTask.driverTaskId)
            if (checkedDriverTask.startTime) {
                throw Error(`Can not update driverTask, because it has started.`)
            }
            // Update driver unitId
            let vehicleRelation = await VehicleRelation.findByPk(checkedDriverTask.vehicleRelationId)
            let driver = await Driver.findByPk(vehicleRelation.driverId);
            if (driverTask.unitId) driver.unitId = driverTask.unitId;
            driver.save();
            // Update driverTask routeInfo
            checkedDriverTask.startTime = driverTask.startTime;
            checkedDriverTask.endTime = driverTask.ebdTime;
            if (checkedDriverTask.routeNo !== driverTask.routeNo) {
                checkedDriverTask.routeNo = driverTask.routeNo;
                checkedDriverTask.arrivedInfo = await getNewRouteInfo(driverTask.routeNo)
            }
            // Update driverTask vehicleNo
            if (vehicleRelation.vehicleNo !== driverTask.vehicleNo) {
                let newVehicleRelation = await createOrUpdateVehicleRelation(driver.driverId, driverTask.vehicleNo)
                checkedDriverTask.vehicleRelationId = newVehicleRelation.id;
            }
            await checkedDriverTask.save();
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getDriverList = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
				throw Error(`User ${ userId } does not exist.`);
			}
			return user;
		}

        let user = await checkUser(req.cookies.userId);
        let baseSQL = `
            select * from (
                select 
                    d.driverId,
                    d.driverName,
                    IF(hh.toHub is NULL, u.unit, hh.toHub) as unit, 
                    IF(hh.toHub is NULL, u.subUnit, hh.toNode) as subUnit,
                    IF(hh.toHub is NULL, d.unitId, hh.unitId) as unitId
                from driver d
                LEFT JOIN unit u ON u.id = d.unitId
                left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and NOW() BETWEEN ho.startDateTime AND ho.endDateTime) hh ON hh.driverId = d.driverId
            ) dd
        `
        let driverList = [];
        if (user.userType === CONTENT.USER_TYPE.HQ || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            baseSQL += ` GROUP BY dd.driverId  `
            driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
        } else {
            let driverUnitIdList = await unitService.getUnitPermissionIdList(user);
            let paramList = [ ]
            if (driverUnitIdList.length) {
                paramList.push(` dd.unitId IN (${ driverUnitIdList }) `)
            }
            if (paramList.length) {
                baseSQL += ' WHERE ' + paramList.join(' AND ')
                baseSQL += ` GROUP BY dd.driverId  `;
                driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
            }
        }
        return res.json(utils.response(1, driverList));    
    } catch (error) {
        log.error('(getDriverList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.createDriver = async function (req, res) {
    try {
        let driver = req.body.driver;
        await sequelizeObj.transaction(async transaction => {
            const checkDriver = async function (driver) {
                if (!driver.driverName) {
                    throw Error(`There is no driverName`)
                }
                let driverList = await Driver.findAll({ where: { driverName: driver.driverName } })
                if (driverList.length) {
                    throw Error(`DriverName ${ driver.driverName } already exist!`)
                }
            }
            const createDriver = async function (driver) {
                let driverList = []
                driverList.push({ driverName: driver.driverName, unitId: driver.unitId, status: CONTENT.DRIVER_STATUS.NEW, 
                    updatedAt: moment().format('YYYY-MM-DD HH:mm:ss'), creator: req.cookie.id })
                let newDriverList = await Driver.bulkCreate(driverList, { returning: true })
                return newDriverList;
            }
            const createDriverAccount = async function (newDriverList) {
                if (!newDriverList.length) return;

                let newUserList = []
                for (let newDriver of newDriverList) {
                    newUserList.push({ driverId: newDriver.driverId, username: newDriver.driverName, userType: CONTENT.USER_TYPE.MOBILE })
                }
                let createNewUserResultList = await User.bulkCreate(newUserList, { returning: true });
                // create friendship
                let laptopUserList = await User.findAll({
                    where: {
                        userType: [CONTENT.USER_TYPE.ACE, CONTENT.USER_TYPE.CA]
                    }
                });
                let friendList = [];
                // add laptop friends
                for (let createNewUserResult of createNewUserResultList) {
                    for (let laptopUser of laptopUserList) {
                        friendList.push({ driverId: createNewUserResult.userId, friendId: laptopUser.userId })
                    }
                }
                // add same driverName as friends
                for (let createNewUserResult1 of createNewUserResultList) {
                    for (let createNewUserResult2 of createNewUserResultList) {
                        if (createNewUserResult1.driverName === createNewUserResult2.driverName
                            && createNewUserResult1.userId !== createNewUserResult2.userId) {
                                friendList.push({ driverId: createNewUserResult1.userId, friendId: createNewUserResult2.userId })
                            }
                    }
                }
                
                if (friendList.length) {
                    // driverId and driendId is primary key, just update
                    await Friend.bulkCreate(friendList);
                }
            }
            const createVehicleRelation = async function (driver, newDriverList) {
                let vehicleRelationList = []
                for (let newDriver of newDriverList) {
                    vehicleRelationList.push({ driverId: newDriver.driverId, vehicleNo: driver.vehicleNo })
                }
                if (vehicleRelationList.length) {
                    await VehicleRelation.bulkCreate(vehicleRelationList);
                }
            }
            const createDriverTask = async function (driver, newDriverList) {
                for (let newDriver of newDriverList) {
                    // find out vehicleRelation
                    let vehicleRelation = await VehicleRelation.findOne({ where: { driverId: newDriver.driverId, vehicleNo: driver.vehicleNo } })
                    if (!vehicleRelation) {
                        log.warn(`There is no \${ driverId: ${ newDriver.driverId }, vehicleNo: ${ driver.vehicleNo } }  in vehicleRelation table`);
                        continue;
                    }
                    let driverTask = { vehicleRelationId: vehicleRelation.id, routeNo: driver.routeNo, 
                        estimateStartTime: driver.estimateStartTime, estimateEndTime: driver.estimateEndTime, highPriority: driver.highPriority }
                    await DriverTask.create(driverTask);
                }

            }
            
            await checkDriver(driver);
            await createDriver(driver);
            await createDriverAccount(newDriverList);
            await createVehicleRelation(driver, newDriverList);
            await createDriverTask(driver, newDriverList);
        }).catch(error => {
            log.error(error)
            return res.json(utils.response(0, 'Server error!'));
        })
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(createDriver) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.deleteDriver = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }
        let driver = await Driver.findByPk(driverId);
        if (!driver) {
            log.warn(`Driver ${ driverId } does not exist.`);
            return res.json(utils.response(0, `Driver ${ driverId } does not exist.`));
        }
        driverId = driver.driverId;
        let driverTaskList = await sequelizeObj.query(`
            SELECT t.taskId, t.indentId, t.driverStatus, t.indentStartTime, t.indentEndTime, t.purpose, t.dataFrom from task t
            where driverId='${driverId}' and t.driverStatus != 'Cancelled' and t.driverStatus != 'completed' and (t.indentEndTime > NOW() or t.driverStatus = 'started') 
        `, { type: QueryTypes.SELECT });
        let driverUrgentTaskList = await sequelizeObj.query(`
            SELECT
                ud.dutyId as taskId, ui.indentId, ud.status as driverStatus, ui.indentId as urgentIndentId, 'DUTY' as dataFrom, uc.id as configId
            FROM urgent_duty ud
            LEFT JOIN urgent_indent ui ON ud.id = ui.dutyId
            LEFT JOIN urgent_config uc on ud.configId = uc.id
            where ud.driverId='${driverId}' and ud.status != 'Cancelled' and ud.status != 'completed' and (ud.indentEndDate > NOW() or ud.status = 'started')
        `, { type: QueryTypes.SELECT });

        driverTaskList = driverTaskList.concat(driverUrgentTaskList);
        await sequelizeObj.transaction(async transaction => {
            const deleteDriverAccount = async function (driverId) {
                await User.update({enable: 0}, { where: { driverId: driverId } })
            }
            const deleteDriver = async function (driverId) {
                let oldDriver = await Driver.findByPk(driverId);
                if (oldDriver) {
                    await DriverHistory.create(oldDriver.dataValues);
                    await Driver.destroy({ where: { driverId: driverId } })

                    //opt log
                    let operationRecord = {
                        operatorId: req.cookies.userId,
                        businessType: 'driver',
                        businessId: driverId,
                        optType: 'deactivate',
                        beforeData: JSON.stringify(oldDriver),
                        afterData: '',
                        optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                        remarks: 'Server user deactivate driver.'
                    }
                    await OperationRecord.create(operationRecord);
                }
            }

            let startedTask = driverTaskList ? driverTaskList.find(item => item.driverStatus.toLowerCase() == 'started') : null;
            if (startedTask) {
                return res.json(utils.response(0, `The following task:${startedTask.taskId} is started, can't deactivate Driver: ${driver.driverName}.`))
            }

            if (driverTaskList && driverTaskList.length > 0) {
                for (let task of driverTaskList) {
                    if (task.dataFrom == 'MOBILE') {
                        //cancel task
                        await Task.update({driverStatus: 'Cancelled', vehicleStatus: 'Cancelled'}, {where: {taskId: task.taskId}});
                        await MobileTrip.update({ status: 'Cancelled', cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), cancelledBy: userId}, { where: { id: task.indentId } } );
                    } else if (task.dataFrom == 'MT-ADMIN') {
                        //clear task driverId
                        await Task.update({driverId: null}, {where: {taskId: task.taskId}});
                        //clear mt_admin.driverId, drivername
                        await MtAdmin.update({driverId: null, driverName: null, mobileNumber: ''}, {where: {id: task.indentId}});
                    } else if (task.dataFrom == 'SYSTEM') {
                        //clear task driverId
                        await Task.update({driverId: null}, {where: {taskId: task.taskId}});
                    } else if (task.dataFrom == 'DUTY') {
                        await UrgentConfig.update({driverId: null}, {where: {id: task.configId}});
                        await UrgentDuty.update({driverId: null}, {where: {dutyId: task.taskId}});
                        await UrgentIndent.update({driverId: null}, {where: {dutyId: task.taskId.replace('DUTY-', '')}});
                    }
                }
            }

            //return driver hoto
            let driverHotoList = await sequelizeObj.query(`
                select ho.id from hoto ho where ho.driverId='${driverId}' and ho.status='Approved'
            `, { type: QueryTypes.SELECT });
            await returnDriverHoto(driverId, driverHotoList, userId);

            await deleteDriverAccount(driverId);
            await deleteDriver(driverId);
        }).catch(error => {
            throw error
        }); 

        //return driver loan
        let driverLoanOutList = await sequelizeObj.query(`
            select l.id from loan l where l.driverId ='${driverId}'
        `, { type: QueryTypes.SELECT });
        await returnDriverLoan(driverId, driverLoanOutList, userId);

        //clear system data
        await sequelizeSystemObj.transaction(async transaction => {
            if (driverTaskList && driverTaskList.length > 0) {
                for (let task of driverTaskList) {
                    if (task.dataFrom == 'SYSTEM') {
                        //clear tms driver table, and job_task.driverID 
                        let systemTaskId = task.taskId;
                        if(systemTaskId.includes('AT-')) systemTaskId = task.taskId.slice(3)
                        await sequelizeSystemObj.query(`
                            update job_task set driverId = null where id = '${ systemTaskId }'
                        `, { type: QueryTypes.UPDATE, replacements: [] })

                        await sequelizeSystemObj.query(`
                            DELETE FROM driver where taskId='${ systemTaskId }'
                        `, { type: QueryTypes.DELETE, replacements: [] })
                    } else if (task.dataFrom == 'DUTY') {
                        if (task.urgentIndentId) {
                            //clear tms driver table, and job_task.driverID 
                            await sequelizeSystemObj.query(`
                                update job_task set driverId = null where id = '${ task.urgentIndentId }'
                            `, { type: QueryTypes.UPDATE, replacements: [] })

                            await sequelizeSystemObj.query(`
                                DELETE FROM driver where taskId='${ task.urgentIndentId }'
                            `, { type: QueryTypes.DELETE, replacements: [] })
                        }
                    }
                }
            }
        });
        return res.json(utils.response(1, 'Deactivate driver success!'));  
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, 'Deactivate driver failed!'));
    }
};

const createOrUpdateVehicleRelation = async function (driverId, vehicleNo) {
    try {
        let vehicleRelationResult = await VehicleRelation.findOne({ where: { driverId, vehicleNo } })
        if (vehicleRelationResult) {
            return vehicleRelationResult
        } else {
            // check if exist driverId or vehicleNo id null
            let option = [{ driverId: { [Op.is]: null }, vehicleNo }, { driverId, vehicleNo: { [Op.is]: null } }]
            vehicleRelationResult = await VehicleRelation.findOne({ where: { [Op.or]: option } })
            if (vehicleRelationResult) {
                console.log(vehicleRelationResult)
                if (!vehicleRelationResult.driverId) vehicleRelationResult.driverId = driverId;
                else if (!vehicleRelationResult.vehicleNo) vehicleRelationResult.vehicleNo = vehicleNo;
                await vehicleRelationResult.save();
                return vehicleRelationResult;
            } else {
                // create new 
                return await VehicleRelation.create({ driverId, vehicleNo }, { returning: true })
            }
        }
    } catch (error) {
        throw error
    }
}
module.exports.createOrUpdateVehicleRelation = createOrUpdateVehicleRelation


module.exports.getPermitTypeList = async function (req, res) {
    let permitTypeList = await PermitType.findAll()
    return res.json(utils.response(1, permitTypeList));
}

module.exports.getTODriverStatusList = async function (req, res) {
    return res.json(utils.response(1, CONTENT.TO_DRIVER_STATUS))
}

module.exports.getTODriverList = async function (req, res) {
    const checkUser = async function (userId) {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            throw Error(`User ${ userId } does not exist.`);
        }
        return user;
    }

    const getUpcomingLeave = function(list){
        let data = []
        for(let row of list){
            data.push(row)
            if(!row.nextTime){
                break
            }
        }
        if(data.length > 0){
            let startTime = moment(data[0].startTime).format("YYYY-MM-DD HH:mm")
            let endTime = moment(data[data.length-1].endTime).add(-1, 'second').format("YYYY-MM-DD HH:mm");
            return {
                startTime: startTime,
                endTime: endTime,
            }
        }
        return null
    }

    let userId = req.cookies.userId;
    let pageNum = Number(req.body.start);
    let pageLength = Number(req.body.length);
    let { permitType, unit, subUnit, selectGroup, groupId, driverStatus, searchCondition, driverORDStatus, driverDataType } = req.body;

    try {
        let user = await checkUser(userId)

        let pageList = await userService.getUserPageList(userId, 'Resources', 'TO List')
        let pageList2 = await userService.getUserPageList(userId, 'View Full NRIC')
        let newPageList = pageList
        if(pageList2.length > 0) {
            pageList2[0].action = 'View Full NRIC'
            newPageList = pageList.concat(pageList2);
        }
        let operationList = newPageList.map(item => `${ item.action }`).join(',')

        let permissionPageList = await userService.getUserPageList(userId, 'TO', 'Indent')
        if (permissionPageList && permissionPageList.length > 0) {
            operationList += ',ViewIndent';
        }

        if (driverDataType && driverDataType.toLowerCase() == 'deactivate') {
            let result = await getDeactivateDriver(req);
            for (let rr of result.respMessage) {
                rr.operation = operationList
            }
            return res.json(result);
        }
        
        //driver current task
        let currentTaskList = await sequelizeObj.query(`
            SELECT DISTINCT tt.driverId as driverId
            FROM task tt
            WHERE tt.driverId is not null and tt.driverId != '' and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed'
            and (now() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
        `, { type: QueryTypes.SELECT , replacements: []})
        let currentDriverIdStr = '';
        if (currentTaskList && currentTaskList.length > 0) {
            for (let temp of currentTaskList) {
                currentDriverIdStr += temp.driverId + ','
            }
        }
        //loan out driver
        let currentLoanOutDriverList = await sequelizeObj.query(`
            select l.driverId from loan l where l.driverId IS NOT NULL and now() BETWEEN l.startDate and l.endDate
        `, { type: QueryTypes.SELECT , replacements: []})
        let currentLoanOutDriverIds = '';
        if (currentLoanOutDriverList && currentLoanOutDriverList.length > 0) {
            for (let temp of currentLoanOutDriverList) {
                currentLoanOutDriverIds += temp.driverId + ','
            }
        }

        let baseSQL = `
            select * from (
                SELECT 
                    d.driverId, d.driverName, d.nric, d.totalMileage, d.permitType, d.operationallyReadyDate,
                    lo.indentId as loanIndentId, lo.taskId as loanTaskId, lo.startDate as loanStartDate, lo.endDate as loanEndDate,
                    d.status, d.overrideStatus, lr.updatedAt as lastLoginTime, d.updatedAt, 
                    IF(d.groupId is not null, d.groupId, lo.groupId) as groupId,
                    uu.role, u.unit, u.subUnit, hh.toHub as hotoUnit,hh.toNode as hotoSubUnit,
                    IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit, IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit,
                    IF(hh.toHub is NULL, d.unitId, hh.unitId) as unitId,
        `
                    if (user.userType != CONTENT.USER_TYPE.CUSTOMER) {
                        baseSQL += `
                            IF(d.permitStatus = 'invalid', 'Permit Invalid',
                                IF(FIND_IN_SET(d.driverId, '${currentLoanOutDriverIds}'), 'Loan Out', 
                                    IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                        IF(FIND_IN_SET(d.driverId, '${currentDriverIdStr}'), 'Deployed', 'Deployable')
                                    )
                                ) 
                            ) as currentStatus
                        `;
                    } else {
                        baseSQL += `
                            IF(d.permitStatus = 'invalid', 'Permit Invalid',
                                IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                    IF(FIND_IN_SET(d.driverId, '${currentDriverIdStr}'), 'Deployed', 'Deployable')
                                ) 
                            ) as currentStatus
                        `;
                    }
                
        baseSQL += `   
                FROM driver d 
                LEFT JOIN unit u ON u.id = d.unitId
                LEFT JOIN user uu on d.driverId =uu.driverId and uu.userType='MOBILE'
                LEFT JOIN login_record lr on lr.userId = uu.userId
                left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and NOW() BETWEEN ho.startDateTime AND ho.endDateTime) hh ON hh.driverId = d.driverId
                left join (select dl.driverId, dl.reason from driver_leave_record dl where dl.status=1 and NOW() BETWEEN dl.startTime AND dl.endTime) ll ON ll.driverId = d.driverId
                left join (select l.driverId, l.indentId, l.taskId, l.groupId, l.startDate, l.endDate from loan l where now() BETWEEN l.startDate and l.endDate) lo ON lo.driverId = d.driverId
                GROUP BY d.driverId
            ) dd where 1=1
        `;
        let limitSQL = []
        let replacements = [];
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            unit = null;
            subUnit = null;
            baseSQL += ` and dd.groupId = ${user.unitId} `
        } else if (user.userType == CONTENT.USER_TYPE.HQ) {
            if (selectGroup == '1') {
                unit = null;
                subUnit = null;
                if (groupId) {
                    baseSQL += ` and dd.groupId = ? `;
                    replacements.push(groupId);
                } else {
                    let groupList = await unitService.UnitUtils.getGroupListByHQUnit(user.hq);
                    let hqUserGroupIds = groupList.map(item => item.id);
                    if (hqUserGroupIds && hqUserGroupIds.length > 0) {
                        baseSQL += ` and dd.groupId in(${hqUserGroupIds}) `;
                    } else {
                        return res.json({ respMessage: [], recordsFiltered: 0, recordsTotal: 0 });
                    }
                }
            } else if (selectGroup == '0') {
                baseSQL += ` and dd.groupId is null `;
            }
        } else if (user.userType == CONTENT.USER_TYPE.ADMINISTRATOR) {
            if (selectGroup == '1') {
                unit = null;
                subUnit = null;
                if (groupId) {
                    baseSQL += ` and dd.groupId = ? `;
                    replacements.push(groupId);
                } else {
                    baseSQL += ` and dd.groupId is not null `
                }
            } else if (selectGroup == '0') {
                baseSQL += ` and dd.groupId is null `
            }
        }
        if (unit) {
            baseSQL += ` and (dd.currentUnit =? or dd.unit = ?) `;
            replacements.push(unit);
            replacements.push(unit);
        } else {
            if (selectGroup == '0' && user.userType == CONTENT.USER_TYPE.HQ) {
                let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
                let hqUserUnitNameList = userUnitList.map(item => item.unit);
                if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                    hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
                }
                if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                    baseSQL += ` and dd.currentUnit in('${hqUserUnitNameList.join("','")}') `;
                } else {
                    return res.json({ respMessage: [], recordsFiltered: 0, recordsTotal: 0 });
                }
            }
        }
        if (subUnit) {
            if (subUnit == 'null') {
                baseSQL += ` and (dd.currentSubUnit is null or dd.subUnit is null) `
            } else {
                baseSQL += ` and (dd.currentSubUnit =? or dd.subUnit =?) `
                replacements.push(subUnit);
                replacements.push(subUnit);
            }
        }
       
        if (permitType) {
            let list = permitType.split(',')
            let permitTypeSql = []
            for (let item of list) {
                permitTypeSql.push(` FIND_IN_SET('${ item }', dd.permitType) `)
            }
            limitSQL.push(` ( ${ permitTypeSql.join(' OR ') } ) `)
        }
        if (driverStatus) {
            limitSQL.push(` FIND_IN_SET(dd.currentStatus, ?) `);
            replacements.push(driverStatus);
        }
        if (driverORDStatus) {
            if (driverORDStatus.toLowerCase() == 'effective') {
                limitSQL.push(` (dd.operationallyReadyDate is null OR dd.operationallyReadyDate > DATE_FORMAT(NOW(), '%Y-%m-%d'))`);
            } else {
                limitSQL.push(` (dd.operationallyReadyDate is not null and dd.operationallyReadyDate <= DATE_FORMAT(NOW(), '%Y-%m-%d'))`);
            }
        }
        if (searchCondition) {
            let likeCondition = sequelizeObj.escape("%" +searchCondition+ "%");
            limitSQL.push(` ( 
                dd.driverName LIKE `+likeCondition+` OR dd.currentUnit LIKE `+likeCondition+` OR dd.currentSubUnit LIKE `+likeCondition
                +` OR dd.permitType LIKE `+likeCondition+` OR dd.status LIKE `+likeCondition
                +` OR dd.nric = '${ utils.generateAESCode(searchCondition) }' 
            ) `);
        }
        if (limitSQL.length) {
            baseSQL += ' and ' + limitSQL.join(' AND ') ;
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements })
        let totalRecord = countResult.length

        let driverNameOrder = req.body.driverNameOrder;
        let orderList = [];
        if (driverNameOrder) {
            orderList.push(` dd.driverName ` + driverNameOrder);
        }
        let hubOrder = req.body.hubOrder;
        if (hubOrder) {
            orderList.push(` dd.unit ` + hubOrder);
        }
        if (orderList.length) {
            baseSQL += ' ORDER BY ' + orderList.join(' , ')
        } else {
            baseSQL += ' ORDER BY dd.updatedAt desc'
        }

        baseSQL += ` limit ?, ?`
        replacements.push(pageNum);
        replacements.push(pageLength);
        let driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements });

        // upcoming leave
        let driverLeaveRecords = []
        let driverIdList = driverList.map(a=>a.driverId)
        if(driverList.length > 0){
            driverLeaveRecords = await sequelizeObj.query(`
                select a.*, b.startTime as nextTime from 
                (
                    select driverId, startTime, date_add(endTime, interval 1 second ) as endTime from driver_leave_record where status = 1 and DATE_FORMAT(startTime, '%Y-%m-%d') >= DATE_FORMAT(NOW(), '%Y-%m-%d') and driverId in (?)
                ) a 
                LEFT JOIN 
                (
                    select driverId, startTime, date_add(endTime, interval 1 second ) as endTime from driver_leave_record where status = 1 and DATE_FORMAT(startTime, '%Y-%m-%d') >= DATE_FORMAT(NOW(), '%Y-%m-%d') and driverId in (?) 
                ) b on a.endTime = b.startTime and a.driverId = b.driverId
                ORDER BY a.driverId, a.startTime
            `, { type: QueryTypes.SELECT, replacements: [driverIdList, driverIdList] })
        }

        //stat driver totalMileage
        let driverTaskMileageList = [];
        let driverBaseMileageList = [];
        if(driverIdList && driverIdList.length > 0){
            driverTaskMileageList = await sequelizeObj.query(`
                SELECT m.driverId, sum(m.mileageTraveled) as taskMileage
                FROM mileage m
                LEFT JOIN vehicle veh ON m.vehicleNo = veh.vehicleNo
                WHERE m.driverId in(?) AND mileageTraveled is NOT NULL and mileageTraveled > 0 and veh.permitType in(select permitType from driver_permittype_detail where driverId=m.driverId)
                GROUP BY m.driverId
            `, { type: QueryTypes.SELECT, replacements: [driverIdList] })

            driverBaseMileageList = await sequelizeObj.query(`
                select driverId, SUM(baseMileage) as baseMileage from driver_permittype_detail where driverId in(?) GROUP BY driverId
            `, { type: QueryTypes.SELECT, replacements: [driverIdList] })
        }

        //driver has mileage warning task
        let driverMileageWaringTaskNums = [];
        if(driverIdList && driverIdList.length > 0){
            driverMileageWaringTaskNums = await sequelizeObj.query(`
                select driverId, COUNT(*) as taskNum from mileage m where m.mileageTraveled>100 and m.driverId in(?) GROUP BY driverId
            `, { type: QueryTypes.SELECT, replacements: [driverIdList] })
        }

        // set customer driver group name
        let systemGroupList = await sequelizeSystemObj.query(` select * from \`group\` `, { type: QueryTypes.SELECT });

        // Update vehicleTypeList, update position
        for (let driver of driverList) {
            let taskTotalMileage = 0;
            if (driverTaskMileageList) {
                let driverTaskMileage = driverTaskMileageList.find(item => item.driverId == driver.driverId);
                if (driverTaskMileage && driverTaskMileage.taskMileage && driverTaskMileage.taskMileage > 0) {
                    taskTotalMileage += driverTaskMileage.taskMileage;
                }
            }
            if (driverBaseMileageList) {
                let driverBaseMileage = driverBaseMileageList.find(item => item.driverId == driver.driverId);
                if (driverBaseMileage && driverBaseMileage.baseMileage && driverBaseMileage.baseMileage > 0) {
                    taskTotalMileage += driverBaseMileage.baseMileage;
                }
            }

            driver.totalMileage = taskTotalMileage;

            if (driverMileageWaringTaskNums) {
                let driverMileageWaringTaskNum = driverMileageWaringTaskNums.find(item => item.driverId == driver.driverId);
                if (driverMileageWaringTaskNum) {
                    driver.driverMileageWaringTaskNum = driverMileageWaringTaskNum.taskNum
                }
            }

            let driverGroup = systemGroupList.find(item => item.id == driver.groupId);
            if (driverGroup && !driver.hub) {
                driver.unit = driverGroup.groupName;
            }

            // 2023-08-29 Get decrypted is nric.
            if(driver.nric){
                if(driver.nric.length > 9) driver.nric = utils.decodeAESCode(driver.nric);
            }
            if(pageList2.length <= 0) {
                if(driver.nric) driver.nric = ((driver.nric).toString()).substr(0, 1) + '****' + ((driver.nric).toString()).substr(((driver.nric).toString()).length-4, 4)
            }
            if (driver.permitType) {
                let permitTypeList = await PermitType.findAll({ where: { permitType: driver.permitType.split(',') } })
                driver.vehicleTypeList = permitTypeList.map(permitType => permitType.vehicleType)
            } else {
                driver.vehicleTypeList = ''
            }

            //find out upcoming task
            let latestTask = await sequelizeObj.query(`
                SELECT * FROM task t
                WHERE t.driverId = ? AND t.driverStatus != 'Cancelled' and t.driverStatus != 'completed'
                and DATE_FORMAT(NOW(), '%Y-%m-%d') BETWEEN DATE_FORMAT(t.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(t.indentEndTime, '%Y-%m-%d')
                order by t.indentEndTime desc
                LIMIT 1
            `, { type: QueryTypes.SELECT , replacements: [ driver.driverId ]})
            if (latestTask.length > 0) {
                driver.taskId = latestTask[0].taskId
                driver.indentId = latestTask[0].indentId
                driver.indentStartTime = latestTask[0].indentEndTime
                driver.purpose = latestTask[0].purpose
            } else {
                driver.taskId = ''
                driver.indentId = ''
                driver.indentStartTime = ''
                driver.purpose = ''
            }

            // upcoming leave
            let records = driverLeaveRecords.filter(a=>a.driverId == driver.driverId)
            driver.upcomingLeave = getUpcomingLeave(records)

            driver.operation = operationList
        }

        return res.json({ respMessage: driverList, recordsFiltered: totalRecord, recordsTotal: totalRecord });
    } catch(error) {
        log.error(error)
		return res.json(utils.response(0, error));
    }
}

const getDeactivateDriver = async function(req) {
    try {
        let pageNum = Number(req.body.start);
        let pageLength = Number(req.body.length);
        let { searchCondition, unit, subUnit } = req.body;

        let baseSQL = `
            SELECT 
                d.driverId, d.driverName, d.nric, d.totalMileage, d.permitType, d.operationallyReadyDate,
                d.status, d.overrideStatus, lr.updatedAt as lastLoginTime, d.updatedAt, uu.unitId as groupId,
                uu.role, u.unit, u.subUnit, u.unit as currentUnit, u.subUnit as currentSubUnit, d.unitId as unitId,
                'Deactivate' as currentStatus
            FROM driver_history d 
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN user uu on d.driverId =uu.driverId and uu.userType='MOBILE'
            LEFT JOIN login_record lr on lr.userId = uu.userId
            where 1=1
        `;

        let params = [];
        if (searchCondition) {
            let likeCondition = sequelizeObj.escape("%" +searchCondition+ "%");
            baseSQL += ` and d.driverName LIKE `+likeCondition+` OR d.nric LIKE `+likeCondition
        }
        if (unit) {
            baseSQL += ` and u.unit = ? `;    
            params.push(unit);
            if (subUnit) {
                baseSQL += ` and u.subUnit = ? `;   
                params.push(subUnit);
            }
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })
        let totalRecord = countResult.length

        let driverNameOrder = req.body.driverNameOrder;
        let orderList = [];
        if (driverNameOrder) {
            orderList.push(` d.driverName ` + driverNameOrder);
        }
        let hubOrder = req.body.hubOrder;
        if (hubOrder) {
            orderList.push(` u.unit ` + hubOrder);
        }
        if (orderList.length) {
            baseSQL += ' ORDER BY ' + orderList.join(' , ')
        } else {
            baseSQL += ' ORDER BY d.updatedAt desc'
        }

        baseSQL += ` limit ?, ?`
        params.push(pageNum);
        params.push(pageLength);
        let driverInfoList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })
        for(let item of driverInfoList){
            if(item.nric) {
                if(item.nric.length > 9) item.nric = utils.decodeAESCode(item.nric);
            } 
        }
        return {respMessage: driverInfoList, recordsFiltered: totalRecord, recordsTotal: totalRecord};
    } catch(error) {
        throw error;
    }
}

module.exports.reactivateDriver = async function(req, res) {
    try {
        // check is HQ user
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let driverId = req.body.driverId;
        if (driverId) {
            let driver = await Driver.findByPk(driverId);
            if (driver) {
                return res.json(utils.response(0, `Driver[${driverId}] is exist!`))
            } else {
                let driverHistory = await DriverHistory.findByPk(driverId);
                if (driverHistory) {
                    //check login name
                    let loginName = driverHistory.loginName;
                    driver = await Driver.findOne({where: {loginName: loginName}});
                    if (driver) {
                        return res.json(utils.response(0, `Driver loginName[${loginName}] is exist!`))
                    }

                    await User.update({enable: 1}, { where: { driverId: driverId } })
                    await Driver.create(driverHistory.dataValues);
                    await DriverHistory.destroy({where: {driverId: driverId}});

                    //opt log
                    let operationRecord = {
                        operatorId: req.cookies.userId,
                        businessType: 'driver',
                        businessId: driverId,
                        optType: 'reactivate',
                        beforeData: '',
                        afterData: JSON.stringify(driverHistory),
                        optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                        remarks: 'Server user reactivate driver.'
                    }
                    await OperationRecord.create(operationRecord);
                }

                return res.json(utils.response(1, 'Reactivate driver success!'))
            }
        } else {
            return res.json(utils.response(0, 'Reactivate driver failed!'))
        }
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, 'Reactivate driver failed!'));
    }
}

module.exports.getTODriverAssignedTasks = async function (req, res) {
    let pageNum = Number(req.body.start);
    let pageLength = Number(req.body.length);

    let driverId = req.body.driverId;
    let searchCondition = req.body.searchCondition;
    let past = Number(req.body.past);
    let warningStatus = Number(req.body.warningStatus);

    let paramList = []
    paramList.push(` tt.driverId = '${ Number(driverId) }' `)
    if (searchCondition) {
        let likeCondition = sequelizeObj.escape("%" + searchCondition + "%");
        paramList.push(` (tt.purpose like `+likeCondition
            +` or tt.vehicleNumber like `+likeCondition
            +` or  tt.pickupDestination like `+likeCondition
            +` or tt.dropoffDestination like `+likeCondition +`)` )
    }
    if (past == 0) {
        paramList.push(` tt.indentStartTime > now() `)
    } else {
        paramList.push(` tt.indentStartTime <= now() `)
    }
    if (warningStatus == 1) {
        paramList.push(` mm.mileageTraveled >= 100 `)
    }

    let baseSQL = `
        SELECT
            tt.indentId, tt.taskId, tt.hub, tt.node, tt.indentStartTime,tt.vehicleNumber,tt.vehicleStatus,veh.vehicleNo,tt.purpose,tt.pickupDestination,tt.dropoffDestination,tt.dataFrom, 
            veh.totalMileage as vehicleTotalMileage,drv.driverName,drv.contactNumber,drv.totalMileage as driverTotalMileage,
            mm.startMileage, mm.endMileage, mm.mileageTraveled, mm.status,mm.oldStartMileage, mm.oldEndMileage
        FROM
            task tt
        LEFT JOIN mileage mm on tt.taskId=mm.taskId
        LEFT JOIN vehicle veh ON veh.vehicleNo = tt.vehicleNumber
        LEFT JOIN driver drv on tt.driverId = drv.driverId
    `;

    if (paramList.length) {
        baseSQL += " WHERE tt.driverStatus != 'Cancelled' and " + paramList.join(' AND ')
    }
    let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
    let totalRecord = countResult.length

    baseSQL += ` ORDER BY tt.indentStartTime DESC limit ?, ? `;
    let driverAssignedTaskList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [pageNum, pageLength] })

    let userId = req.cookies.userId
    let pageList = await userService.getUserPageList(userId, 'TO', 'Indent')
    let operationList = pageList.map(item => `${ item.action }`).join(',')
    for (let data of driverAssignedTaskList) {
        data.operation = operationList
    }
    
    return res.json({respMessage: driverAssignedTaskList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
}

module.exports.getTOCalenderDriverList = async function (req, res) {
    const checkUser = async function (userId) {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            throw Error(`User ${ userId } does not exist.`);
        }
        return user;
    }
    let userId = req.cookies.userId;
    let { startDate, endDate, driverName, unit, subUnit } = req.body;
    let user = await checkUser(userId);

    //query select week all effective tasks(task and leave record)
    let taskSql = `
        SELECT 
            driverId, taskId, indentStartTime, indentEndTime, purpose, dataFrom
        FROM task
        WHERE driverStatus IN ('waitcheck', 'ready', 'started', 'completed') and ( 
            DATE_FORMAT(indentStartTime,'%Y-%m-%d') BETWEEN ? AND ?
            or DATE_FORMAT(indentEndTime,'%Y-%m-%d') BETWEEN ? AND ?
            or (DATE_FORMAT(indentStartTime,'%Y-%m-%d') < ? and DATE_FORMAT(indentEndTime,'%Y-%m-%d') > ?)
        ) 
    `;
    if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
        taskSql += ` and taskId like 'CU-%' `
    } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
        taskSql += ` and taskId not like 'CU-%' `
    }
    let allTaskList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements: [startDate, endDate, startDate, endDate, startDate, endDate] })
        
    let tempLeaveList = await sequelizeObj.query(`
        SELECT
            driverId, startTime as indentStartTime, date_add(endTime, interval 1 second) as indentEndTime, reason
        FROM
            driver_leave_record dl
        where status=1 and DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN ? and ? order by dl.startTime asc
    `, { type: QueryTypes.SELECT, replacements: [startDate, endDate] })    

    //Merge consecutive times.
    let allDriverLeaveList = [];
    let leaveDriverIds = tempLeaveList.map(item => item.driverId);
    leaveDriverIds = Array.from(new Set(leaveDriverIds));
    for (let tempDriverId of leaveDriverIds) {
        let driverLeaveRecords = tempLeaveList.filter(item => item.driverId == tempDriverId);
        let newDriverLeaveRecords = [];
        if (driverLeaveRecords && driverLeaveRecords.length > 0) {
            for (let temp of driverLeaveRecords) {
                temp.indentStartTime = moment(temp.indentStartTime).format('YYYY-MM-DD HH:mm:ss')
                temp.indentEndTime = moment(temp.indentEndTime).format('YYYY-MM-DD HH:mm:ss')
                if (newDriverLeaveRecords.length > 0) {
                    let preLeave = newDriverLeaveRecords[newDriverLeaveRecords.length - 1];
                    if (preLeave.indentEndTime == temp.indentStartTime) {
                        preLeave.indentEndTime = temp.indentEndTime;
                    } else {
                        newDriverLeaveRecords.push(temp);
                    }
                } else {
                    newDriverLeaveRecords.push(temp);
                }
            }
        }
        let onLeaveIndex = 1;
        for (let temp of newDriverLeaveRecords) {
            temp.taskId='onLeave-' +  onLeaveIndex;
            let entTimeStr = moment(temp.indentEndTime).format('HH:mm:ss');
            if (entTimeStr == '00:00:00') {
                let endDateTime = moment(temp.indentEndTime).add(-1, 'minute');
                temp.indentEndTime = endDateTime.format('YYYY-MM-DD HH:mm:ss');
            }
            onLeaveIndex++;
        }

        allDriverLeaveList = allDriverLeaveList.concat(newDriverLeaveRecords);
    }
    
    let resultList = allTaskList.concat(allDriverLeaveList);
    let driverIds = resultList.map(item => item.driverId);
    driverIds = Array.from(new Set(driverIds));
    log.info(`driverService.getTOCalenderDriverList ${startDate} - ${endDate} ${driverIds ? driverIds.length : 0} driver has task!`)
    if (!driverIds || driverIds.length == 0) {
        return res.json(utils.response(1, []));
    }
    let baseSQL = `
        select * from (
            SELECT d.driverId, d.driverName, us.unitId, us.role,
            IF(d.groupId is not null, d.groupId, lo.groupId) as groupId,
            IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit, IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit
            FROM driver d 
            LEFT JOIN user us ON d.driverId = us.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and NOW() BETWEEN ho.startDateTime AND ho.endDateTime) hh ON hh.driverId = d.driverId
            left join (select l.driverId, l.indentId, l.groupId from loan l where now() BETWEEN l.startDate and l.endDate) lo ON lo.driverId = d.driverId
            where d.driverId in (?)
        ) dd where 1=1 
    `;
    let limitSQL = []
    let replacements = [driverIds];
    if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
        baseSQL += ` and dd.groupId = ${user.unitId} `
    }

    if (unit) {
        limitSQL.push(` dd.unit =? `);
        replacements.push(unit);
    } else {
        if (user.userType == CONTENT.USER_TYPE.HQ) {
            let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
            let hqUserUnitNameList = userUnitList.map(item => item.unit);
            if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
            }
            if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                limitSQL.push(` dd.currentUnit in('${hqUserUnitNameList.join("','")}') `);
            } else {
                return res.json(utils.response(1, []));
            }
        }
    }
    if (subUnit) {
        if (subUnit == 'null') {
            limitSQL.push(` dd.currentSubUnit is null `);
        } else {
            limitSQL.push(` dd.currentSubUnit =? `);
            replacements.push(subUnit);
        }
        
    }
    if (driverName) {
        limitSQL.push(` dd.driverName LIKE `+ sequelizeObj.escape("%" + driverName + "%"));
    }
    if (limitSQL.length) {
        baseSQL += ' and ' + limitSQL.join(' AND ') ;
    }
    baseSQL += ` GROUP BY dd.driverId `

    let driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements });
    for (let driver of driverList) {
        let driverTaskOrLeaves = resultList.filter(item => item.driverId == driver.driverId);
        if (driverTaskOrLeaves && driverTaskOrLeaves.length > 0) {
            driverTaskOrLeaves = driverTaskOrLeaves.sort(function(item1, item2) {
                if (moment(item1.indentStartTime).isBefore(moment(item2.indentStartTime))) {
                    return -1;
                } 
                return 1;
            })
        }

        driver.taskList = driverTaskOrLeaves
    }
    return res.json(utils.response(1, driverList));
}

module.exports.getTODriverDetailInfo = async function (req, res) {
    let user = await userService.getUserDetailInfo(req.cookies.userId)
    if (!user) {
        log.warn(`User ${ user.userId } does not exist.`);
        return res.json(utils.response(0, `User ${ user.userId } does not exist.`));
    }
    
    let driverId = req.body.driverId;
    if (!driverId) {
        log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
        return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
    }
    try {
        //driver current task
        let currentTaskList = await sequelizeObj.query(`
            SELECT DISTINCT tt.driverId as driverId
            FROM task tt
            WHERE tt.driverId =? and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed'
            and (now() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
        `, { type: QueryTypes.SELECT , replacements: [driverId]})
        let currentTaskLength = currentTaskList.length;

        let baseSql = `
            SELECT d.*, u.unit AS hub, u.subUnit AS node, h.id as hotoId, l.id as loanId,
        `
        if (user.userType != CONTENT.USER_TYPE.CUSTOMER) {
            baseSql += `
                IF(d.permitStatus = 'invalid', 'Permit Invalid',
                    IF(l.id is not null, 'Loan Out', 
                        IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                            IF(${currentTaskLength} > 0, 'Deployed', 'Deployable')
                        )
                    ) 
                ) as currentStatus
            `;
        } else {
            baseSql += `
                IF(d.permitStatus = 'invalid', 'Permit Invalid',
                    IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                        IF(${currentTaskLength} > 0, 'Deployed', 'Deployable')
                    ) 
                ) as currentStatus
            `;
        }
        baseSql += ` FROM driver d 
            LEFT JOIN unit u ON u.id = d.unitId
            left join hoto h on h.driverId = d.driverId AND NOW() BETWEEN h.startDateTime AND h.endDateTime
            LEFT JOIN loan l ON l.driverId = d.driverId AND NOW() BETWEEN l.startDate and l.endDate
            left join (select dl.driverId, dl.reason from driver_leave_record dl where dl.status = 1 and NOW() BETWEEN dl.startTime AND dl.endTime) ll ON ll.driverId = d.driverId
            WHERE d.driverId = ? limit 1
        `


        let driverList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements: [ driverId ] });
        let currentDriver = driverList && driverList.length > 0 ? driverList[0] : null;
        if (!currentDriver) {
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }

        let currentAssignedDriverIdBaseSql = `
            SELECT taskId
            FROM task tt
            WHERE tt.driverId =? and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed' 
            and (now() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
        `;
        let currentAssignedTasks = await sequelizeObj.query(currentAssignedDriverIdBaseSql, { type: QueryTypes.SELECT, replacements: [driverId] })
        if (currentAssignedTasks && currentAssignedTasks.length > 0) {
            if (currentDriver && currentDriver.currentStatus == 'Deployable') {
                currentDriver.currentStatus = 'Deployed'
            }
        }

        // Init emergency
        let emergencyList = await Emergency.findByPk(driverId);
        currentDriver.emergencyList = emergencyList;
        let driverUser = await User.findOne({ where: { driverId } })
        currentDriver.role = driverUser ? driverUser.role : '';

        let driverCategory = await sequelizeObj.query(`
            SELECT
                assessmentType
            FROM driver_assessment_record
            WHERE driverId = ? AND \`status\` = 'Pass' and approveStatus='Approved'
            ORDER BY assessmentType ASC LIMIT 1
        `, { type: QueryTypes.SELECT, replacements: [ driverId ] });

        if (driverCategory && driverCategory.length > 0) {
            currentDriver.category = driverCategory[0].assessmentType
        }
        currentDriver.category = currentDriver.category == 'Category A Assessment' ? 'A' : currentDriver.category == 'Category B Assessment' ? 'B' : currentDriver.category == 'Category C Assessment' 
        ? 'C' : currentDriver.category == 'Category D Assessment' ? 'D' : '-';

        if (currentDriver.unit && currentDriver.role && [ 'dv', 'loa' ].indexOf(currentDriver.role.toLowerCase()) > -1) {
            let group = await TaskUtils.getGroupById(currentDriver.unit);
            currentDriver.group = group.groupName;
        }

        let oneYearsAgoDateStr = moment().subtract(1, 'year').format('YYYY-MM-DD');
        let driverDemeritPointsObj = await sequelizeObj.query(`
            SELECT sum(demeritPoint) as driverDemeritPoints
            FROM sos
            WHERE driverId = ? and demeritPoint > 0 and optAt IS NOT NULL and DATE_FORMAT(optAt, '%Y-%m-%d') >= '${oneYearsAgoDateStr}'
        `, { replacements: [driverId], type: QueryTypes.SELECT });
        let driverDemeritPoints = 0;
        if (driverDemeritPointsObj && driverDemeritPointsObj.length > 0) {
            driverDemeritPoints = driverDemeritPointsObj[0].driverDemeritPoints;
        }
        currentDriver.demeritPoints = driverDemeritPoints;
        // 2023-08-29 Get decrypted is nric.
        if(currentDriver.nric){
            if(currentDriver.nric.length > 9) currentDriver.nric = utils.decodeAESCode(currentDriver.nric);
        }
        let pageList2 = await userService.getUserPageList(req.cookies.userId, 'View Full NRIC')
        if(pageList2.length <= 0) {
            if(currentDriver.nric) currentDriver.nric = ((currentDriver.nric).toString()).substr(0, 1) + '****' + ((currentDriver.nric).toString()).substr(((currentDriver.nric).toString()).length-4, 4)
            currentDriver.nricShow = false
        } else {
            currentDriver.nricShow = true
        }
        return res.json(utils.response(1, currentDriver)); 
    } catch(error) {
        log.error(error);
        return res.json(utils.response(0, error && error.message ? error.message : "Query driver info fail!")); 
    } 
}

module.exports.getDriverTaskList2 = async function (req, res) {
    let driverId = req.body.driverId;
    let taskList = await sequelizeObj.query(`
        SELECT
            taskId,
            dataFrom,
            vehicleNumber,
            driverStatus AS STATUS,
            indentStartTime,
            mobileStartTime,
            vv.permitType,
            vv.totalMileage,
            vv.vehicleType
        FROM task tt
        LEFT JOIN vehicle vv ON vv.vehicleNo = tt.vehicleNumber
        WHERE tt.driverId = ?
    `, { type: QueryTypes.SELECT, replacements: [ driverId ] });

    return res.json(utils.response(1, taskList));  
}

module.exports.reassignDriverTask = async function (req, res) {
    let taskId = req.body.taskId;
    let vehicleNo = req.body.vehicleNo;
    let driverId = req.body.driverId;
    let reason = req.body.reason;
    let remarks = req.body.remarks;
    
    let task = await Task.findOne({where: {taskId: taskId}});
    let currentDriver = await Driver.findOne({ where: { driverId: driverId } })
    let vehicle = await Vehicle.findOne({where: {vehicleNo: vehicleNo}});
    if (task && currentDriver && moment().isBefore(moment(task.indentStartTime)) && task.driverStatus == 'waitcheck') {

        let oldDriverId = task.driverId;
        let oldVehicleNo = task.vehicleNumber;

        await sequelizeObj.transaction(async transaction => {
            //update task
            await Task.update({ vehicleNumber: vehicleNo, driverId: driverId, reassignReasons: reason, reassignRemarks: remarks, reassignAt: new Date()}, { where: { taskId: taskId } });
            if (task.dataFrom == 'MT-ADMIN') {
                let driverName = currentDriver.driverName
                if (currentDriver.contactNumber) {
                    driverName += '(' + currentDriver.contactNumber + ')'
                }
                await MtAdmin.update({driverId: driverId, driverName: driverName, vehicleNumber: vehicleNo}, {where: {id: task.indentId}})
            }
            //driver change
            if (oldDriverId != driverId) {
                log.info('Task reassign firebase notification!');
                let oldDriver = await User.findOne({ where: { driverId: task.driverId } })
                if (oldDriver) {
                    FirebaseService.createFirebaseNotification2([{
                        taskId,
                        token: "",
                        driverId: oldDriverId,
                        vehicleNo: oldVehicleNo
                    }], 'INFO', 'Task cancelled!')
                }
                if (currentDriver) {
                    FirebaseService.createFirebaseNotification2([{
                        taskId,
                        token: "",
                        driverId: driverId,
                        vehicleNo: vehicleNo
                    }], 'INFO', 'New task assigned!')
                }
            }
            //opt log
            let operationRecord = {
                operatorId: req.cookies.userId,
                businessType: 'task',
                businessId: taskId,
                optType: 're-assign',
                beforeData: `driverId: ${ oldDriverId }, vehicleNo: ${ oldVehicleNo }`,
                afterData: `driverId: ${ driverId }, vehicleNo: ${ vehicleNo }`,
                optTime: moment().format('yyyy-MM-DD HH:mm'),
                remarks: 're-assigned'
            }
            await OperationRecord.create(operationRecord)

            // Update vehicle-relation db
            await vehicleService.createVehicleRelation(driverId, vehicleNo)
        }).catch(error => {
            throw error
        })

        if (task.dataFrom == 'SYSTEM') {
            await sequelizeSystemObj.transaction(async transaction => {
                let systemTaskId = taskId;
                if(systemTaskId.includes('AT-')) systemTaskId = taskId.slice(3)
                if (task.driverId != driverId) {
                    await _SystemTask.Task.update({ taskStatus: 'Assigned', driverId: driverId }, { where: { id: systemTaskId } })
                    await _SystemDriver.Driver.upsert({
                        taskId: systemTaskId,
                        driverId: driverId,
                        status: 'Assigned',
                        name: currentDriver.driverName,
                        nric: currentDriver.nric,
                        contactNumber: currentDriver.contactNumber,
                        permitType: currentDriver.permitType,
                        driverFrom: 'transport'
                    })
                }

                if (task.vehicleNumber != vehicleNo) {
                    await _SystemVehicle.Vehicle.upsert({
                        taskId: systemTaskId,
                        vehicleNumber: vehicleNo,
                        vehicleType: vehicle.vehicleType,
                        permitType: vehicle.permitType,
                        vehicleStatus: 'available'
                    })
                }
            }).catch(error => {
                throw error
            })
        }
        return res.json(utils.response(1, 'Success'));  
    } else {
        return res.json(utils.response(0, 'Can not reassign task.'));  
    }
}

module.exports.getDriverLeaveDays = async function (req, res) {
    let driverId = req.body.driverId;
    
    let driverLeaveRecords = await sequelizeObj.query(`
        SELECT
            startTime,
            endTime
        FROM
            driver_leave_record dl
        where driverId=? and status=1
    `, { type: QueryTypes.SELECT, replacements: [driverId] });
    let leaveDayArray = [];
    if (driverLeaveRecords && driverLeaveRecords.length > 0) {
        for (let leaveRecord of driverLeaveRecords) {
            let days = moment(leaveRecord.endTime).add(-1, 'minute').diff(moment(leaveRecord.startTime), 'days');
            leaveDayArray.push(moment(leaveRecord.startTime).format('YYYY-M-D'));
            if (days > 0) {
                for (let index = 1; index <= days; index++) {
                    leaveDayArray.push(moment(leaveRecord.startTime).add(index, 'day').format('YYYY-M-D'));
                }
            }
        }
    }

    return res.json(utils.response(1, leaveDayArray));  
}

module.exports.getDriverAndVehicleForTask = async function (req, res) {
    let taskId = req.body.taskId;
    let vehicleType = req.body.vehicleType;

    let currentTask = await Task.findOne({where: {taskId: taskId}});
    // customer user or common user
    let taskCreatorId = currentTask.creator;
    let taskCreator = await User.findByPk(taskCreatorId);
    if (taskCreator && taskCreator.userType == "CUSTOMER") {
        let driverVchicleArray = { driverList: [], vehicleList: [] }
        let sql = `
            select v.vehicleNumber as vehicleNo, v.vehicleType, "" as permitType, 0 as totalMileage, 0 as taskNum
            from vehicle v
            LEFT JOIN job_task jt ON jt.id = v.taskId
            LEFT JOIN job j ON j.requestId = jt.requestId
            LEFT JOIN service_type st ON j.serviceTypeId = st.id
            LEFT JOIN request r ON r.id = jt.requestId
            where r.groupId = ${ taskCreator.unitId } and j.driver = 0 
            and jt.taskStatus = 'Assigned'
            and st.category = 'MV'
            and ('${ moment(currentTask.indentStartTime).format('YYYY-MM-DD HH:mm') }' >= DATE_FORMAT(jt.startDate, '%Y-%m-%d %H:%i:%s') and '${ moment(currentTask.indentEndTime).format('YYYY-MM-DD HH:mm') }' <= DATE_FORMAT(jt.endDate, '%Y-%m-%d %H:%i:%s'))
        `
        if(vehicleType) {
            sql += ` and v.vehicleType = `+ sequelizeSystemObj.escape(vehicleType)
        }
        let vehicleList = await sequelizeSystemObj.query(sql + ` GROUP BY v.vehicleNumber`, {
            type: QueryTypes.SELECT,
            replacements: []
        });
        driverVchicleArray.vehicleList = vehicleList;

        sql = `
            select
                d.driverId, d.driverName, d.permitType, d.totalMileage, 0 as taskNum
            from driver d
            LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            LEFT JOIN user u ON u.driverId = d.driverId
            where d.driverId is not null and u.role in ('DV', 'LOA')
        `
        if(taskCreator.unitId){
            sql += ` and d.unit = '${ taskCreator.unitId }'`
        }
        if(vehicleType) {
            sql += ` and FIND_IN_SET(`+sequelizeSystemObj.escape(vehicleType)+`, dc.vehicleType)`
        }
        if(currentTask.indentEndTime) {
            sql += ` and (d.operationallyReadyDate > '${ moment(currentTask.indentEndTime).format('YYYY-MM-DD') }' OR d.operationallyReadyDate is null)`
        }
        let driverList = await sequelizeObj.query(
            sql + ` GROUP BY d.driverId`,
            {
                type: QueryTypes.SELECT
            }
        );
        driverVchicleArray.driverList = driverList;

        return res.json(utils.response(1, driverVchicleArray));
    }

    let currentHub = currentTask.hub;
    let currentNode = currentTask.node;
    let unitIdArray = [];
    if (currentNode) {
        let currentUnit =  await Unit.findOne({where: { unit: currentHub, subUnit: currentNode}})
        if (currentUnit) {
            unitIdArray.push(currentUnit.id);
        }
    } else {
        let unitList = await Unit.findAll({where: { unit: currentHub }})
        if (unitList && unitList.length > 0) {
            for (let temp of unitList) {
                unitIdArray.push(temp.id);
            }
        }
    }
    // before 2022-12-18 system preParkTask's indentEndTime maybe null
    if (!currentTask.indentEndTime) {
        currentTask.indentEndTime = moment(currentTask.indentStartTime).add(5, 'year');
    }    
    try {
        let indentStartTimeStr = moment(currentTask.indentStartTime).format('YYYY-MM-DD HH:mm:ss');
        let indentEndTimeStr = moment(currentTask.indentEndTime).format('YYYY-MM-DD HH:mm:ss');

        //vehicles: user unit vehicle + hoto user unit vehicle - hoto others unit vehicle - markAsUnavailable vehicle
        // query hoto others unit vehicle
        let hotoOthersUnitVehicleNos = await sequelizeObj.query(`
            SELECT 
                DISTINCT hh.vehicleNo 
            from hoto hh
            LEFT JOIN vehicle vv on hh.vehicleNo = vv.vehicleNo
            where hh.vehicleNo is not null ${(unitIdArray && unitIdArray.length > 0) ? ' and vv.unitId in(?) ' : ''} 
            and (
                hh.startDateTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}'
                or hh.endDateTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}' 
                or ( '${indentStartTimeStr}' < hh.startDateTime and '${indentEndTimeStr}'  > hh.endDateTime)
            )
        `, { type: QueryTypes.SELECT, replacements: (unitIdArray && unitIdArray.length > 0) ? [unitIdArray] : [] });
        let hotoOthersVehicleNoArray = Array.from(new Set(hotoOthersUnitVehicleNos.map(hotoOtherVehicle => hotoOtherVehicle.vehicleNo)))

        // query markAsUnavailable vehicle
        let leaveVehicleNos = await sequelizeObj.query(`
            SELECT
                DISTINCT vl.vehicleNo
            FROM vehicle_leave_record vl
            LEFT JOIN vehicle vv on vl.vehicleNo = vv.vehicleNo
            WHERE vl.status = 1 and vl.endTime > now()
            AND (
                vl.startTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}' 
                or vl.endTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}' 
                or ( '${indentStartTimeStr}' < vl.startTime and '${indentEndTimeStr}'  > vl.endTime)
            )
        `, { type: QueryTypes.SELECT, replacements: [] });
        let leaveVehicleNoArray = Array.from(new Set(leaveVehicleNos.map(leaveVehicle => leaveVehicle.vehicleNo)))

        //query user unit vehicle + hoto user unit vehicle
        let vehicleList = await sequelizeObj.query(`
            select * from (
                SELECT
                    vv.vehicleNo, vv.vehicleType, vv.permitType, vv.totalMileage, count(taskId) as taskNum,
                    IF(hh.toHub is NULL, vv.unitId, hh.unitId) as currentUnitId
                FROM vehicle vv
                LEFT JOIN (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and ho.startDateTime < '${indentStartTimeStr}'  AND ho.endDateTime > '${indentEndTimeStr}') hh ON hh.vehicleNo = vv.vehicleNo
                LEFT JOIN task tt ON vv.vehicleNo=tt.vehicleNumber
                where vv.vehicleType=`+sequelizeSystemObj.escape(vehicleType)+` GROUP BY vv.vehicleNo 
            ) rr where rr.currentUnitId is not null ${(unitIdArray && unitIdArray.length > 0) ? ' and rr.currentUnitId in(?) ' : ''} 
        `, {
            replacements: (unitIdArray && unitIdArray.length > 0) ? [unitIdArray] : [],
            type: QueryTypes.SELECT,
        });
        vehicleList = vehicleList.filter(vehicle => hotoOthersVehicleNoArray.indexOf(vehicle.vehicleNo) == -1);
        let canUseVehicles = vehicleList.filter(vehicle => leaveVehicleNoArray.indexOf(vehicle.vehicleNo) == -1);

        //drivers: user unit driver + hoto user unit driver - hoto others unit driver - markAsUnavailable driver
        // query hoto others unit driver
        let hotoOthersUnitDriverIds = await sequelizeObj.query(`
            SELECT 
                DISTINCT hh.driverId 
            from hoto hh
            LEFT JOIN driver dd on hh.driverId = dd.driverId
            where hh.driverId is not null ${(unitIdArray && unitIdArray.length > 0) ? ' and dd.unitId in(?) ' : ''} 
            and (
                hh.startDateTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}'
                or hh.endDateTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}' 
                or ('${indentStartTimeStr}' < hh.startDateTime and '${indentEndTimeStr}' > hh.endDateTime)
            )
        `, { type: QueryTypes.SELECT, replacements: (unitIdArray && unitIdArray.length > 0) ? [unitIdArray] : [] });
        let hotoOthersDriverIdArray = Array.from(new Set(hotoOthersUnitDriverIds.map(hotoOtherDrvier => hotoOtherDrvier.driverId)))
        // query markAsUnavailable driver
        let leaveDriverIds = await sequelizeObj.query(`
            SELECT
                DISTINCT dl.driverId
            FROM driver_leave_record dl
            LEFT JOIN driver dd on dd.driverId = dl.driverId
            WHERE dl.status = 1 and dl.endTime > now()
            AND (
                dl.startTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}' 
                or dl.endTime BETWEEN '${indentStartTimeStr}' and '${indentEndTimeStr}' 
                or ('${indentStartTimeStr}' < dl.startTime and '${indentEndTimeStr}'  > dl.endTime)
            )
        `, { type: QueryTypes.SELECT, replacements: [] });
        let leaveDriverIdArray = Array.from(new Set(leaveDriverIds.map(leaveDriver => leaveDriver.driverId)))

        let currentTaskEndDateStr = moment(currentTask.indentEndTime).format("YYYY-MM-DD");
        let driverList = []
        let needVehicleTypeFilter = currentTask.purpose && currentTask.purpose.toLowerCase() != 'driving training' && currentTask.purpose.toLowerCase() != 'familiarisation';
        let needUnitFilter = currentTask.purpose && currentTask.purpose.toLowerCase() != 'familiarisation'
        let driverReplacements = []
        if (needVehicleTypeFilter) {
            driverReplacements.push(vehicleType);
        }
        if (needUnitFilter && unitIdArray && unitIdArray.length > 0) {
            driverReplacements.push(unitIdArray);
        }
        driverList = await sequelizeObj.query(`
            select * from (SELECT
                dd.driverId, dd.driverName, dd.permitType, dd.totalMileage, count(taskId) as taskNum,
                IF(hh.toHub is NULL, dd.unitId, hh.unitId) as currentUnitId,
                GROUP_CONCAT(dp.vehicleType) as driverSupportVehicleTypes
                FROM driver dd
                left join (SELECT driverId, vehicleType FROM driver_platform_conf where approveStatus='Approved') dp on dp.driverId = dd.driverId
                left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and ho.startDateTime < '${indentStartTimeStr}'  AND ho.endDateTime > '${indentEndTimeStr}') hh ON hh.driverId = dd.driverId
                LEFT JOIN task tt ON tt.driverId = dd.driverId
                where (dd.operationallyReadyDate is null OR dd.operationallyReadyDate > '${currentTaskEndDateStr}')
                GROUP BY dd.driverId
            ) dr where 1=1 
            ${needVehicleTypeFilter ? " and FIND_IN_SET(?, dr.driverSupportVehicleTypes) " : ' '}
            ${needUnitFilter && unitIdArray && unitIdArray.length > 0 ? ' and dr.currentUnitId in (?) ' : ' '} 
        `, {
            replacements: driverReplacements,
            type: QueryTypes.SELECT,
        });
        driverList = driverList.filter(driver => hotoOthersDriverIdArray.indexOf(driver.driverId) == -1);
        let canUseDrivers = driverList.filter(driver => leaveDriverIdArray.indexOf(driver.driverId) == -1);

        return res.json(utils.response(1, { driverList: canUseDrivers, vehicleList: canUseVehicles }));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

module.exports.getUserDriverSummaryList = async function (req, res) {
    let userId = req.body.userId;

    let currentUser = await User.findByPk(userId);
    if (!currentUser) {
        log.warn(`User ${ userId } does not exist.`);
        return res.json(utils.response(0, `User ${ userId } does not exist.`));
    }

    try {
        let sql = ``;
        if (currentUser.userType == "CUSTOMER") {
            sql = `
                select
                    d.driverId, d.driverName, d.nric
                from driver d
                left join user u on u.driverId = d.driverId
                where u.role in ('DV', 'LOA') and (d.operationallyReadyDate is null OR d.operationallyReadyDate > now())
            `
            if(currentUser.unitId){
                sql += ` and d.unit = '${ currentUser.unitId }'`
            }
        } else {
            sql = `
                SELECT
                    d.driverId, d.driverName, d.nric
                FROM driver d 
                left join user u on u.driverId = d.driverId
                where u.role = 'TO' and (d.operationallyReadyDate is null OR d.operationallyReadyDate > now())
            `
            let { hubNodeIdList } = await unitService.UnitUtils.getPermitUnitList2(userId);

            if (hubNodeIdList && hubNodeIdList.length > 0) {
                sql += ` and d.unitId in (${ hubNodeIdList })`
            } else {
                return res.json(utils.response(1, { driverList: [] }));
            }
        }

        let driverList = await sequelizeObj.query(sql, {
            replacements: [],
            type: QueryTypes.SELECT,
        });
        let pageList2 = await userService.getUserPageList(req.cookies.userId, 'View Full NRIC')
        if (driverList && driverList.length > 0) {
            for (let temp of driverList) {
                if(temp.nric && temp.nric.length > 9) {
                    temp.nric = utils.decodeAESCode(temp.nric);
                } 
                if(pageList2.length <= 0) {
                    if(temp.nric) temp.nric = ((temp.nric).toString()).substr(0, 1) + '****' + ((temp.nric).toString()).substr(((temp.nric).toString()).length-4, 4)
                }
            }
        }

        return res.json(utils.response(1, { driverList: driverList }));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

module.exports.cancelMarkAsUnavailable = async function (req, res) {
    let driverId = req.body.driverId;
    let startDate = req.body.startDate;
    let endDate = req.body.endDate;
    let additionalNotes = req.body.additionalNotes;
    if (startDate > endDate) {
        return res.json(utils.response(0, 'Date to error'));  
    }
    try {
        //cancel driver leave
        await sequelizeObj.query(`
            UPDATE driver_leave_record
                SET status=0, cancelRemarks=?
            WHERE driverId=? and DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN ? and ?
        `, { type: QueryTypes.UPDATE, replacements: [additionalNotes, driverId, startDate, endDate] })

        return res.json(utils.response(1, 'Cancel MarkAsUnavailable Success!'));  
    } catch(error) {
        log.error(error);
        return res.json(utils.response(0, error && error.message ? error.message : 'Cancel MarkAsUnavailable exec fail!'));  
    }
}

module.exports.markAsUnavailable = async function (req, res) {
    let driverId = req.body.driverId;
    let startDate = req.body.startDate;
    let endDate = req.body.endDate;
    let dayType = req.body.dayType;
    let reason = req.body.reason;
    let additionalNotes = req.body.additionalNotes;

    try {
        let momentStartDate = moment(startDate);
        let momentEndDate = moment(endDate);
        if (momentEndDate.isBefore(momentStartDate)) {
            return res.json(utils.response(0, 'Date to error'));  
        }
        let startTimeStr = startDate + ' 00:00:00';
        let endTimeStr = endDate + ' 23:59:59';
        if (startDate == endDate) {
            if (dayType == 'am') {
                endTimeStr = endDate + ' 12:00:00';
            } else if (dayType == 'pm') {
                startTimeStr = startDate + ' 12:00:00';
            }
        }
        
        //check has assigned task
        let assignedTaskList = await sequelizeObj.query(`
            SELECT
                tt.taskId,
                tt.dataFrom,
                tt.driverId,
                tt.vehicleNumber,
                tt.indentStartTime
            FROM
                task tt
            WHERE
                driverId = ?
            AND tt.driverId is not null and tt.driverStatus != 'completed' and tt.driverStatus != 'Cancelled' and (indentEndTime > now() or tt.driverStatus != 'started')
            AND (
                (indentStartTime >= '${startTimeStr}' AND indentStartTime <= '${endTimeStr}')
                OR (indentEndTime >= '${startTimeStr}' AND indentEndTime <= '${endTimeStr}')
                OR (indentStartTime < '${startTimeStr}' AND indentEndTime > '${endTimeStr}')
            )
        `, { type: QueryTypes.SELECT, replacements: [driverId] });

        if (assignedTaskList && assignedTaskList.length > 0) {
            return res.json(utils.response(0, 'Please reassign the task executionTime during ' + startTimeStr + ' - ' + endTimeStr));  
        } else {
            let driverLeaveRecords = [];
            if (startDate == endDate) {
                //query exist leave record
                let exitLeaveReocrds = await sequelizeObj.query(`
                    SELECT
                        id
                    FROM driver_leave_record dl
                    WHERE dl.driverId = ? AND status=1 AND DATE_FORMAT(startTime, '%Y-%m-%d') = ?
                `, { type: QueryTypes.SELECT, replacements: [driverId, startDate] })
                let recordId = null;
                if (exitLeaveReocrds && exitLeaveReocrds.length > 0) {
                    recordId = exitLeaveReocrds[0].id
                }

                driverLeaveRecords.push({id: recordId,
                    driverId: driverId, startTime: startTimeStr, endTime: endTimeStr,
                    dayType: dayType, reason: reason, remarks: additionalNotes, creator: req.cookies.userId
                });
            } else {
                let leaveDays = momentEndDate.diff(momentStartDate, 'day');
                let index = 0;
                let exitLeaveReocrds = await sequelizeObj.query(`
                    SELECT
                        id, DATE_FORMAT(startTime, '%Y-%m-%d') as startTime
                    FROM driver_leave_record dl
                    WHERE dl.driverId = ? AND status=1 AND DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN ? and ?
                `, { type: QueryTypes.SELECT, replacements: [driverId, startDate, endDate] });

                while(index <= leaveDays) {
                    momentStartDate = index == 0 ? momentStartDate : momentStartDate.add(1, 'day');
                    let currentDateStr = momentStartDate.format('YYYY-MM-DD');
                    let tempStartDateStr = currentDateStr + ' 00:00:00';
                    let tempEndDateStr = currentDateStr + ' 23:59:59';
                    
                    let recordId = null;
                    if (exitLeaveReocrds && exitLeaveReocrds.length > 0) {
                        let currentDateRecord = exitLeaveReocrds.find(item => item.startTime == currentDateStr);
                        recordId = currentDateRecord ? currentDateRecord.id : null;
                    }
                    driverLeaveRecords.push({id: recordId,
                        driverId: driverId, startTime: tempStartDateStr, endTime: tempEndDateStr,
                        dayType: 'all', reason: reason, remarks: additionalNotes, creator: req.cookies.userId
                    });
                    index++;
                }
            }

            await DriverLeaveRecord.bulkCreate(driverLeaveRecords, { updateOnDuplicate: [ 'startTime','endTime', 'dayType', 'optTime', 'reason', 'remarks','creator','updatedAt' ] });

            return res.json(utils.response(1, 'Success!'));  
        }
    } catch(error) {
        log.error(error);
        return res.json(utils.response(0, error && error.message ? error.message : 'MarkAsUnavailable exec fail!'));  
    }
}

module.exports.getLeaveRecordByDate = async function (req, res) {
    let driverId = req.body.driverId;
    let currentDate = req.body.currentDate;
    let userId = req.cookies.userId

    try {
        
        //query exist leave record
        let exitLeaveReocrds = await sequelizeObj.query(`
            SELECT reason, remarks, dayType
            FROM driver_leave_record dl
            WHERE dl.driverId = ? AND status=1 AND DATE_FORMAT(startTime, '%Y-%m-%d') = ? limit 1
        `, { type: QueryTypes.SELECT, replacements: [driverId, currentDate] })

        let pageList = await userService.getUserPageList(userId, 'Resources', 'TO List')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let leaveRecord = exitLeaveReocrds && exitLeaveReocrds.length > 0 ? exitLeaveReocrds[0] : null;
        
        return res.json(utils.response(1, {leaveRecord, operation: operationList}));  
    } catch(error) {
        log.error(error);
        return res.json(utils.response(0, error && error.message ? error.message : 'getLeaveRecordByDate exec fail!'));  
    }
}

module.exports.getDriverTaskByTaskId = async function (req, res) {
    try {
        let taskId = req.body.taskId;
        let taskList = await sequelizeObj.query(`
            SELECT
                taskId,
                tt.indentId,
                tt.purpose,
                dataFrom,
                tt.vehicleNumber,
                driverStatus AS STATUS,
                indentStartTime,
                dd.driverId,
                dd.driverName,
                tt.pickupDestination,
                tt.dropoffDestination,
                ma.serviceMode,
                ma.vehicleType
            FROM task tt
            LEFT JOIN driver dd ON tt.driverId = dd.driverId
            LEFT JOIN mt_admin ma on ma.id = tt.indentId and tt.dataFrom='MT-ADMIN'
            WHERE tt.taskId = ? limit 1
        `, { type: QueryTypes.SELECT, replacements: [ taskId ] });

        let task = taskList ? taskList[0] : null;
        if (task && task.dataFrom == 'SYSTEM') {
            let systemTaskId = taskId;
            if(systemTaskId.includes('AT-')) systemTaskId = taskId.slice(3)
            let sysTaskList = await sequelizeSystemObj.query(`
                SELECT
                    jj.vehicleType,
                    jj.serviceModeId,
                    sm.\`name\`
                FROM
                    job_task jt
                LEFT JOIN job jj on jt.tripId = jj.id
                LEFT JOIN service_mode sm on sm.id = jj.serviceModeId
                where jt.id=? limit 1
            `, {
                replacements: [systemTaskId],
                type: QueryTypes.SELECT,
            });
            if (sysTaskList && sysTaskList.length > 0) {
                let sysTask = sysTaskList[0]
                task.serviceMode = sysTask.name;
                task.vehicleType = sysTask.vehicleType;
            }
        }

        return res.json(utils.response(1, taskList ? taskList[0] : null));  
    } catch(error) {
        log.error(error);
        return res.json(utils.response(1, null));  
    }
}

module.exports.getDriverMileageStatInfo = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let taskId = req.body.taskId;
        let currentTask = null;
        if (taskId) {
            currentTask = await Task.findOne({where: { taskId: taskId}});
            driverId = currentTask.driverId
        }

        let permitTypes = new Set();
        let statResult = [];
        let driverTotalMileage = 0;
        // Real-time mileage statistics
		let driverPermitTaskMileageList = await sequelizeObj.query(` 
            SELECT veh.permitType, sum(m.mileageTraveled) as permitMileage
            FROM mileage m
            LEFT JOIN vehicle veh ON m.vehicleNo = veh.vehicleNo
            WHERE m.driverId = ? and m.endMileage IS NOT NULL 
            GROUP BY veh.permitType
        `, { 
            type: QueryTypes.SELECT, replacements: [driverId]
        });
            
        let driverMileageStatList = await sequelizeObj.query(`
            SELECT
                dm.permitType,
                dm.passDate,
                dm.baseMileage
            FROM driver_permittype_detail dm
            where dm.driverId=? AND dm.approveStatus='Approved' ORDER BY dm.permitType asc
        `, { type: QueryTypes.SELECT, replacements: [ driverId ] });
        for (let permitTypeMileage of driverMileageStatList) {
            permitTypes.add(permitTypeMileage.permitType);
        }
        for (let permitType of permitTypes) {
            let driverPermitTypeTaskMileage = driverPermitTaskMileageList.find(item => item.permitType == permitType);
            let driverPermitTypeBaseMileage = driverMileageStatList.find(item => item.permitType == permitType);

            let totalMileage = 0;
            if (driverPermitTypeTaskMileage) {
                totalMileage += driverPermitTypeTaskMileage.permitMileage ? driverPermitTypeTaskMileage.permitMileage : 0;
            }
            if (driverPermitTypeBaseMileage) {
                totalMileage += driverPermitTypeBaseMileage.baseMileage ? driverPermitTypeBaseMileage.baseMileage : 0;
            }

            let permitTypeConf = await PermitType.findOne({ where: { permitType : permitType} });
            if (permitTypeConf && permitTypeConf.parent) {
                let parentPermitType = permitTypeConf.parent;
                let parentMileageObj = statResult.find(item => item.permitType == parentPermitType);
                if (parentMileageObj) {
                    parentMileageObj.totalMileage += totalMileage;
                    continue;
                } else {
                    permitType = parentPermitType;
                    permitTypeConf = await PermitType.findOne({ where: { permitType : permitType} });
                }
            } else {
                continue;
            }

            let eligibilityMileage = permitTypeConf && permitTypeConf.eligibilityMileage ? permitTypeConf.eligibilityMileage : 4000;
            statResult.push({permitType: permitType, totalMileage: totalMileage, eligibilityMileage});
        }

        if (statResult && statResult.length > 0) {
            for (let temp of statResult) {
                driverTotalMileage += temp.totalMileage;
            }
        }
        
        let result = {
            statResult: statResult, 
            driverTotalMileage: driverTotalMileage ? driverTotalMileage.toFixed(2) : 0,
            currentTaskMileage: 0
        }
        if (taskId) {
            let currentTaskMileageData = await Mileage.findOne({where: { taskId: taskId}});
            if (currentTaskMileageData) {
                result.currentTaskMileage = currentTaskMileageData.mileageTraveled
            }

            //query vehicle total mileage
            if (currentTask) {
                let currentVehicle = await Vehicle.findOne({where: { vehicleNo: currentTask.vehicleNumber}});
                result.vehicleTotalMileage = currentVehicle.totalMileage
                result.actiulStartTime = currentTask.mobileStartTime
                result.actiulEndTime = currentTask.mobileEndTime
            }
        }

        return res.json(utils.response(1, result));
    } catch (err) {
        return res.json(utils.response(0, err));
    }
}

const checkDriver = async function (driver) {
    if(driver.nric) {
        let driver1 = await Driver.findOne({ where: { nric: driver.nric, driverId: { [Op.ne]: driver.driverId } } })
        if (driver1) {
            log.error(`NRIC already exist!`)
            throw Error(`NRIC already exist!`)
        }
    }

    if(driver.loginName){
        let driver2 = await Driver.findOne({ where: { loginName: driver.loginName, driverId: { [Op.ne]: driver.driverId } } })
        if (driver2) {
            log.error(`The user already exist, please change your NRIC or Name.`)
            throw Error(`The user already exist, please change your NRIC or Name.`)
        }
    }
    
    if(driver.driverName){
        let driver3 = await Driver.findOne({ where: { driverName: driver.driverName, driverId: { [Op.ne]: driver.driverId } } })
        if (driver3) {
            log.error(`DriverName already exist!`)
            throw Error(`DriverName already exist!`)
        }
    }
}

const createDriver = async function (driver, creator) {
    let driver1 = await Driver.findOne({ where: { nric: driver.nric } })
    if (driver1) {
        log.error(`NRIC already exist!`)
        throw Error(`NRIC already exist!`)
    }
    
    let driver2 = await Driver.findOne({ where: { driverName: driver.driverName } })
    if (driver2) {
        log.error(`DriverName already exist!`)
        throw Error(`DriverName already exist!`)
    }
    let newDriver = await Driver.create({ ...driver, creator })
    return newDriver.driverId;
}

const createDriver2 = async function (driver, creator) {    
    let driver1 = await Driver.findOne({ where: { loginName: driver.loginName } })
    if (driver1) {
        log.error(`The user already exist, please change your NRIC or Name.`)
        throw Error(`The user already exist, please change your NRIC or Name.`)
    }
    let newDriver = await Driver.create({ ...driver, creator })
    return newDriver.driverId;
}

module.exports.driverUtils = {
    createDriver,
    checkDriver,
}

module.exports.getDriverByDriverId = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let sql = `
            SELECT
                d.*, u.role, un.unit as hub, un.subUnit as node
            FROM
                driver d
            LEFT JOIN unit un on un.id = d.unitId
            LEFT JOIN user u on u.driverId = d.driverId
            where d.driverId = ? limit 1
        `;
 
        let currentDriver = null;
        let driverList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: [driverId] })
        // 2023-08-29 Get decrypted is nric.
        for(let item of driverList){
            if(item.nric){
                if(item.nric.length > 9) item.nric = utils.decodeAESCode(item.nric);
            }
        }
        if (driverList && driverList.length > 0) {
            currentDriver = driverList[0];
        }

        let driverMileage = await DriverMileage.findAll({ where: { driverId }, attributes: [ 'permitType', 'mileage' ] })
        currentDriver.driverMileage = driverMileage

        return res.json(utils.response(1, currentDriver));
    } catch (err) {
        console.log(err)
        return res.json(utils.response(0, err));
    }
}

module.exports.getUnitList = async function (req, res) {
    try{
        let unitData = await unitService.UnitUtils.getPermitUnitList2(req.body.userId);
        return res.json(utils.response(1, unitData));;
    } catch(err){
        return res.json(utils.response(0, err));
    }
}

module.exports.updateDriverStatus = async function (req, res) {
    let statusDataList = req.body.newStatus;
    try {
        let userId = req.cookies.userId
        if (statusDataList && statusDataList.length > 0) {
            for (let temp of statusDataList) {

                let oldDriver = await Driver.findOne({where: {driverId: temp.driverId}});
                if (oldDriver) {
                    temp.overrideStatusTime = moment();
                    await Driver.update(temp, {where: {driverId: temp.driverId}})
                    let newOptRecord = {
                        operatorId: userId,
                        businessType: 'paradestate_driver',
                        businessId: temp.driverId,
                        optType: 'changeStatus', 
                        beforeData: oldDriver.status,
                        afterData: temp.status
                    }
                    await OperationRecord.create(newOptRecord);
                }
            }
        }
        return res.json(utils.response(1, 'success'));
    } catch (err) {
        log.error(err);
        return res.json(utils.response(0, err));
    }
}

module.exports.getVehicleTypeByPermitType = async function (req, res) {
    let permitType = req.body.permitType;
    let permitTypeList = await PermitType.findAll({where: { permitType: permitType }})
    return res.json(utils.response(1, permitTypeList));
}

// Driver platforms
module.exports.getPlatformList = async function (req, res) {
    try {
        let driverId = req.body.driverId;

        if (!driverId) {
            log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }

        let platformList = await DriverPlatform.findAll({ where: { driverId },  order: [['tripDate', 'DESC']]})
        return res.json(utils.response(1, platformList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getPlatformListGroupByVehicleType = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let platformList = await DriverPlatform.findAll({ where: { driverId }, order: [['tripDate', 'DESC']]})
        let result = [];
        if (platformList && platformList.length > 0) {
            for (let temp of platformList) {
                let existTemp = result.find(item => item.vehicleType == temp.vehicleType)
                if (existTemp) {
                    existTemp.totalMileage = existTemp.totalMileage + temp.totalMileage;
                } else {
                    result.push(temp);
                }
            }
        }
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.createPlatform = async function (taskId) {
    try {
        let platform = { taskId };
        let task = await Task.findByPk(taskId);
        if (task) {
            platform.driverId = task.driverId
            platform.vehicleNo = task.vehicleNumber
            let vehicle = await Vehicle.findOne({ where: { vehicleNo: task.vehicleNo } });
            platform.permitType = vehicle.permitType
            platform.vehicleType = vehicle.vehicleType
            platform.tripDate = task.mobileEndTime;
            let mileage = await Mileage.findByPk(taskId);
            platform.totalMileage = mileage.mileageTraveled;
            await DriverPlatform.create(platform);
        } else {
            log.warn(`TaskId does not exist.`)
        }
    } catch (error) {
        log.error(error)
        throw error
    }
}

module.exports.getDriverIncidentList = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let vehicleNo = req.body.vehicleNo;

        if (!driverId) {
            log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }

        let driver = await Driver.findByPk(driverId);
        let incidentList = [];
        if (vehicleNo) {
            incidentList = await TrackHistory.findAll({ where: { deviceId: driverId, vehicleNo, violationType: { [Op.ne]: CONTENT.ViolationType.Missing } } })
        } else {
            incidentList = await TrackHistory.findAll({ where: { deviceId: driverId, violationType: { [Op.ne]: CONTENT.ViolationType.Missing } } })
        }
        for (let incident of incidentList) {
            incident.driverName = driver.driverName;
            if (incident.violationType === CONTENT.ViolationType.HardBraking) {
                incident.point = incidentConf.HARD_BRAKING
            } else if (incident.violationType === CONTENT.ViolationType.RapidAcc) {
                incident.point = incidentConf.RAPID_ACCELERATION
            } else if (incident.violationType === CONTENT.ViolationType.Speeding) {
                incident.point = incidentConf.SPEEDING
            }
        }
        return res.json(utils.response(1, incidentList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.createDriverIncident = async function (req, res) {
    try {
        let incident = req.body;
        let trackCurrent = await TrackHistory.findOne({ where: {
            deviceId: incident.driverId,
            violationType: incident.violationType,
            occTime: incident.startTime,
        } });
        if (trackCurrent) {
            log.error(`This driver can not create incident at same time`);
            throw Error(`This driver can not create incident at same time`)
        }
        
        await sequelizeObj.transaction(async transaction => {
            await TrackHistory.create({
                deviceId: incident.driverId,
                violationType: incident.violationType,
                vehicleNo: incident.vehicleNo,
                occTime: incident.startTime,
                startTime: incident.startTime,
                endTime: incident.endTime,
                dataFrom: 'mobile',
            })
            let track = await Track.findOne({ where: {
                deviceId: incident.driverId,
                violationType: incident.violationType,
                vehicleNo: incident.vehicleNo,
                dataFrom: 'mobile',
            } })
            if (track) {
                track.count = track.count + 1;
                track.startTime = incident.startTime;
                track.endTime = incident.endTime;
                track.occTime = incident.startTime;
                track.lastOccTime = incident.endTime;
                await track.save();
            } else {
                await Track.create({ 
                    deviceId: incident.driverId,
                    violationType: incident.violationType,
                    vehicleNo: incident.vehicleNo,
                    occTime: incident.startTime,
                    lastOccTime: incident.endTime,
                    startTime: incident.startTime,
                    endTime: incident.endTime,
                    dataFrom: 'mobile',
                    count: 1,
                })
            }
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getLicensingDriverList = async function (req, res) {
    const checkUser = async function (userId) {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            throw Error(`User ${ userId } does not exist.`);
        }
        return user;
    }

    let userId = req.cookies.userId;
    let userType = req.cookies.userType;
    let pageNum = Number(req.body.start);
    let pageLength = Number(req.body.length);
    let { permitType, applyStatus, unit, subUnit, searchCondition } = req.body;

    try {
        let user = await checkUser(userId)
        let cusromerGroupId = null;
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            cusromerGroupId = user.unitId;
            unit = null;
            subUnit = null;
        }

        let pageList = await userService.getUserPageList(userId, 'Resources', 'Licensing')
        let pageList2 = await userService.getUserPageList(req.cookies.userId, 'View Full NRIC')
        let newPageList = pageList
        if(pageList2.length > 0) {
            pageList2[0].action = 'View Full NRIC'
            newPageList = pageList.concat(pageList2);
        }
        let operationList = newPageList.map(item => `${ item.action }`).join(',')
        
        let replacements = [];
        let baseSQL = `
            SELECT
               '${ operationList }' AS operation, 
                d.driverId,
                d.driverName,
                da.permitTypeMileage as driverMileageInfo,
                d.nric,
                da.permitType as exchangePermitType,
                d.permitType,
                da.status,
                d.enlistmentDate,
                d.operationallyReadyDate,
                d.birthday,
                u.unit AS hub,
                u.subUnit AS node, 
                da.rejectDate,
                da.rejectReason,
                ru.username as rejectUser,
                da.failDate,
                da.failReason,
                fu.username as failUser,
                da.pendingDate, 
                da.pendingReason,
                pu.userName as pendingUser
            FROM
                driver_license_exchange_apply da
            LEFT JOIN driver d on da.driverId = d.driverId
            LEFT JOIN user us ON us.driverId = d.driverId 
            LEFT JOIN user ru ON da.rejectBy = ru.userId
            LEFT JOIN user fu ON da.failBy = fu.userId
            LEFT JOIN user pu ON da.pendingBy = pu.userId
            LEFT JOIN unit u ON u.id = d.unitId
            where d.driverId is not null
        `;
        let limitSQL = []
        if (userType && userType.toUpperCase() == CONTENT.USER_TYPE.LICENSING_OFFICER) {
            let date21YearsAgo = moment().add(-21, 'year').format("YYYY-MM-DD");
            limitSQL.push(` (LOWER(da.permitType) != 'cl 4' OR (LOWER(da.permitType) = 'cl 4' and d.birthday <= '${date21YearsAgo}') ) `);
        }
        
        if (unit) {
            limitSQL.push( ` u.unit = ? `);
            replacements.push(unit);
        } else if (user.userType == CONTENT.USER_TYPE.HQ) {
            let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
            let hqUserUnitNameList = userUnitList.map(item => item.unit);
            if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
            }
            if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                limitSQL.push( ` u.unit in('${hqUserUnitNameList.join("','")}') `);
            } else {
                return res.json({ respMessage: [], recordsFiltered: 0, recordsTotal: 0 });
            }
        }
        if (subUnit) {
            if(subUnit.toLowerCase() == 'null') {
                limitSQL.push( ` u.subUnit is null `);
            } else {
                limitSQL.push( ` u.subUnit = ? `);
                replacements.push(subUnit);  
            }
        }
        if (cusromerGroupId) {
            limitSQL.push( ` us.unitId = ${cusromerGroupId} `);
            limitSQL.push( ` us.role in('DV', 'LOA') `);
        }

        if (permitType) {
            limitSQL.push(` da.permitType = ? `);
            replacements.push(permitType);
        }
        if (applyStatus) {
            limitSQL.push(` da.status = ? `);
            replacements.push(applyStatus);
        }
        if (searchCondition) {
            limitSQL.push(` d.driverName LIKE `+ sequelizeObj.escape("%" +searchCondition+ "%"));
        }

        if (limitSQL.length) {
            baseSQL += ' and ' + limitSQL.join(' AND ');
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements })
        let totalRecord = countResult.length

        baseSQL += ` ORDER BY da.applyDate desc, d.driverName asc, da.permitType asc  limit ?, ?`
        replacements.push(pageNum);
        replacements.push(pageLength);
        let driverExchangePermitApplyList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements });
        if (driverExchangePermitApplyList) {
            for (let apply of driverExchangePermitApplyList) {
                let driverMileageInfo = apply.driverMileageInfo;
                if (driverMileageInfo && driverMileageInfo.toLowerCase().indexOf('km') == -1) {
                    let classType = apply.exchangePermitType;
                    apply.driverMileage = driverMileageInfo.replaceAll(';', ' km;');
                    let totalMileage = 0;
                    let mileageArray = driverMileageInfo.split(';');
                    if (mileageArray && mileageArray.length > 0) {
                        if (classType.toLowerCase() == 'cl 3' || classType.toLowerCase() == 'cl 4') {
                            if (mileageArray.length > 1) {
                                let mileage1 = mileageArray[0].split(":")[1];
                                let mileage2 = mileageArray[1].split(":")[1];
                                let maxMileage = mileage1 > mileage2 ? mileage1 : mileage2;

                                if (apply.driverMileage.indexOf(':0 km') != -1) {
                                    apply.driverMileage = apply.driverMileage.replaceAll(':0 km', `:${maxMileage} km`);
                                    driverMileageInfo = driverMileageInfo.replaceAll(':0', `:${maxMileage}`);

                                    mileageArray = driverMileageInfo.split(';');
                                }
                            }
                        }
                        for (let mileageInfo of mileageArray) {
                            if (mileageInfo) {
                                let tempArray = mileageInfo.split(":");
                                if (tempArray && tempArray.length >= 2) {
                                    let mileage = tempArray[1];
                                    totalMileage += Number(mileage);
                                }
                            }
                        }
                    }
                    apply.totalMileage = totalMileage;
                } else {
                    apply.driverMileage = driverMileageInfo;
                }
            }
        }

        // 2023-08-29 Get decrypted is nric.
        for(let driver of driverExchangePermitApplyList){
            if(driver.nric){
                if(driver.nric.length > 9) driver.nric = utils.decodeAESCode(driver.nric);
                if(pageList2.length <= 0) {
                    if(driver.nric) driver.nric = ((driver.nric).toString()).substr(0, 1) + '****' + ((driver.nric).toString()).substr(((driver.nric).toString()).length-4, 4)
                }
            } 
        }
        return res.json({ respMessage: driverExchangePermitApplyList, recordsFiltered: totalRecord, recordsTotal: totalRecord });
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

module.exports.updateAssessmentRecord = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let recordId = req.body.recordId;
        let driverId = req.body.driverId;
        let assessmentType = req.body.assessmentType;
        let issueDate = req.body.issueDate;
        let status = req.body.status;
        if (!assessmentType || !issueDate || !status) {
            return res.json(utils.response(0, `Params assessmentType, issueDate, status is required.`));
        }

        if (!driverId) {
            log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }

        let driver = await Driver.findOne({where: {driverId: driverId}});
        if (!driver) {
            return res.json(utils.response(0, `Driver ${ driverId } does not exist.`));
        }
        //if user has approve permisson 
        let pageList = await userService.getUserPageList(userId, 'License', 'Category')
        let operationList = pageList.map(item => `${ item.action }`).join(',')
        let approveStatus = 'Edited'
        if (operationList && operationList.includes('Approve')) {
            approveStatus = 'Approved';
        }

        if (recordId) {
            await DriverAssessmentRecord.update({driverId,assessmentType,issueDate,status,updatedAt: moment(), approveStatus: approveStatus}, { where: {id: recordId} })
        } else {
            await DriverAssessmentRecord.create({driverId,assessmentType,issueDate,status,creator: userId, approveStatus: 'Edited'})
        }

        return res.json(utils.response(1, 'success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.deleteAssessmentRecord = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let recordId = req.body.recordId;
        if (recordId) {
            await DriverAssessmentRecord.destroy({ where: { id: recordId } });
        }

        return res.json(utils.response(1, 'Delete success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getAssessmentRecord = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let userId = req.cookies.userId;

        let pageList = await userService.getUserPageList(userId, 'License', 'Category')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        if (!driverId) {
            log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }

        let driver = await Driver.findOne({where: {driverId: driverId}});
        if (!driver) {
            return res.json(utils.response(0, `Driver ${ driverId } does not exist.`));
        }

        let assessmentRecordList = await sequelizeObj.query(`
            select *, '${ operationList }' as operation from driver_assessment_record where driverId=? ORDER BY issueDate desc  
        `, { type: QueryTypes.SELECT, replacements: [driverId]});

        return res.json(utils.response(1, assessmentRecordList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getPlatformConfList = async function (req, res) {
    try {
        let driverId = req.body.driverId;

        if (!driverId) {
            log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }
        let userId = req.cookies.userId;

        let pageList = await userService.getUserPageList(userId, 'License', 'Platforms')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let driverPlatformMileages = await sequelizeObj.query(`
            select veh.vehicleType, sum(tm.mileageTraveled) as platformTaskMileage from mileage tm 
            LEFT JOIN vehicle veh on tm.vehicleNo = veh.vehicleNo
            where tm.driverId=? and tm.mileageTraveled is not NULL and veh.vehicleType is not NULL
            GROUP BY veh.vehicleType  
        `, { type: QueryTypes.SELECT, replacements: [driverId]});

        let platformConfList = await DriverPlatformConf.findAll({ where: { driverId }});
        for (let platform of platformConfList) {
            platform.operation = operationList
            let platformMileage = driverPlatformMileages.find(item => item.vehicleType == platform.vehicleType);
            if (platformMileage) {
                platform.dataValues.taskMileage = platformMileage.platformTaskMileage;
            } else {
                platform.dataValues.taskMileage = 0;
            }
        }

        return res.json(utils.response(1, platformConfList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getVehicleTypeByDriverId = async function (req, res) {
    try {
        let driverId = req.body.driverId;

        if (!driverId) {
            log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }

        let driverSupportVehicleTypes = await sequelizeObj.query(`
            select DISTINCT vc.vehicleName as typeOfVehicle
            from vehicle_category vc 
            where FIND_IN_SET(vc.vehicleClass, (select dd.permitType from driver dd where dd.driverId=?))
        `, { type: QueryTypes.SELECT, replacements: [driverId]});

        return res.json(utils.response(1, driverSupportVehicleTypes));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error && error.message ? error.message : 'Get driver support vehicleType fail!'));
    }
}

module.exports.updatePlatformConf = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let confId = req.body.confId;
        let driverId = req.body.driverId;
        let permitType = req.body.permitType;
        let vehicleType = req.body.vehicleType;
        let assessmentDate = req.body.assessmentDate;
        let lastDrivenDate = req.body.lastDrivenDate;

        if (!vehicleType || !assessmentDate) {
            return res.json(utils.response(0, `Vehicle Type and Assessment Date are required.`));
        }
        let driver = await Driver.findOne({where: {driverId: driverId}});
        if (!driver) {
            return res.json(utils.response(0, `Driver ${ driverId } does not exist.`));
        }
        if (confId) {
            let oldConf = await DriverPlatformConf.findByPk(confId);
            if (oldConf) {
                //if user has approve permisson 
                let pageList = await userService.getUserPageList(userId, 'License', 'Platforms')
                let operationList = pageList.map(item => `${ item.action }`).join(',')
                let approveStatus = 'Edited'
                if (operationList && operationList.includes('Approve')) {
                    approveStatus = 'Approved';
                }

                let updateObj = {
                    permitType,
                    assessmentDate,
                    updatedAt: moment(),
                    approveStatus: approveStatus
                }
                if (lastDrivenDate) {
                    updateObj.lastDrivenDate = lastDrivenDate
                }
                await DriverPlatformConf.update(updateObj, { where: {id: confId} })
            }
        } else {
            //check exist
            let existConf = await DriverPlatformConf.findOne({where: {driverId, vehicleType}});
            if (existConf) {
                return res.json(utils.response(0, `Platform is exist, Vehicle Type: ${vehicleType}.`));
            }
            let createObj = {
                driverId,
                permitType,
                vehicleType,
                assessmentDate,
                creator: userId,
                approveStatus: 'Edited'
            }
            if (lastDrivenDate) {
                createObj.lastDrivenDate = lastDrivenDate
            }

            await DriverPlatformConf.create(createObj)
        }

        return res.json(utils.response(1, 'success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.deleteDriverPlatformConf = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let confId = req.body.confId;
        if (confId) {
            await DriverPlatformConf.destroy({ where: { id: confId } });
        }

        return res.json(utils.response(1, 'Delete success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getPermitTypeDetailList = async function (req, res) {
    try {
        let driverId = req.body.driverId;

        if (!driverId) {
            log.error(`DriverId ${ driverId } is not correct! current login user is ${ req.cookies.userId }`)
            return res.json(utils.response(0, `DriverId ${ driverId } is not correct!`));
        }

        let userId = req.cookies.userId;

        let pageList = await userService.getUserPageList(userId, 'License', 'Test Results')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let permitTypeDetailList = await DriverPermitTypeDetail.findAll({ where: { driverId }})
        for (let temp of permitTypeDetailList) {
            temp.operation = operationList
        }

        let permitTypeDetailHistoryList = await sequelizeObj.query(`
            SELECT 
                h.driverId, h.permitType, h.passDate, h.baseMileage, h.attemptNums, h.testerCode, h.score, h.demeritPoint, h.deleteAt, h.deleteReason, h.deleteBy, uu.fullName as deleteUser
            FROM driver_permittype_detail_history h
            LEFT JOIN user uu on uu.userId = h.deleteBy
            where h.driverId = ? order by deleteAt desc
        `, { type: QueryTypes.SELECT, replacements: [driverId] });

        if (permitTypeDetailHistoryList && permitTypeDetailHistoryList.length > 0) {
            for (let temp of permitTypeDetailHistoryList) {
                temp.approveStatus = 'Deleted';
                temp.operation = operationList;
            }
        }

        let result = [];
        result = result.concat(permitTypeDetailList);
        result = result.concat(permitTypeDetailHistoryList);

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updatePermitTypeDetail = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let detailId = req.body.detailId;
        let driverId = req.body.driverId;
        let permitType = req.body.permitType;
        let passDate = req.body.passDate;
        let baseMileage = req.body.baseMileage;
        let attemptNums = req.body.attemptNums;
        let testerCode = req.body.testerCode;
        let score = req.body.score;
        let demeritPoint = req.body.demeritPoint;

        let emptyField = '';
        if (!permitType) {
            emptyField = 'Permit Type';
        }
        if (!passDate) {
            emptyField += emptyField ? ', Assessment Date' : 'Assessment Date';
        }

        if (!attemptNums) {
            emptyField += emptyField ? ', No of Attempts' : 'No of Attempts';
        }
        if (!testerCode) {
            emptyField += emptyField ? ', Tester Code' : 'Tester Code';
        }

        let errorMsg = ``;
        if (emptyField) {
            errorMsg = emptyField + ' is required;'
        }
        if (attemptNums <= 0) {
            errorMsg += `No of Attempts must more than 0;`;
        }

        let driver = await Driver.findOne({where: {driverId: driverId}});
        if (!driver) {
            errorMsg += `Driver ${ driverId } does not exist;`;
        }

        let permitTypeConf = await PermitType.findOne({where: {permitType}})
        if (!permitTypeConf) {
            errorMsg += `PermitType ${ permitType } does not exist;`;
        }
        if (errorMsg) {
            return res.json(utils.response(0, errorMsg));
        }
        if (baseMileage == null || baseMileage == '' || baseMileage == 'null') {
            baseMileage = 0;
        }
        await sequelizeObj.transaction(async transaction => {
            if (detailId) {
                let oldDetailInfo = await DriverPermitTypeDetail.findByPk(detailId);
                if (oldDetailInfo) {
                    let updateObj = {
                        permitType,
                        passDate,
                        baseMileage,
                        attemptNums,
                        testerCode,
                        score,
                        demeritPoint,
                        approveStatus: 'Approved'
                    }
                    await DriverPermitTypeDetail.update(updateObj, { where: {id: detailId} })
                }
            } else {
                //check exist
                let existDetail = await DriverPermitTypeDetail.findOne({where: {driverId, permitType}});
                if (existDetail) {
                    return res.json(utils.response(0, `PermitType is exist, Permit Type: ${permitType}.`));
                }
                let createObj = {
                    driverId,
                    permitType,
                    passDate,
                    baseMileage,
                    attemptNums,
                    testerCode,
                    score,
                    demeritPoint,
                    creator: userId,
                    approveStatus: 'Approved'
                }
                await DriverPermitTypeDetail.create(createObj)
            }

            let driverPermitTypes = [];
            let driverPermitTypeDetailList = await DriverPermitTypeDetail.findAll({where: {driverId}});
            if (driverPermitTypeDetailList && driverPermitTypeDetailList.length > 0) {
                for (let permitTypeDetail of driverPermitTypeDetailList) {
                    if (permitTypeDetail.permitType && permitTypeDetail.permitType.toUpperCase().indexOf('CL') != -1) {
                        driverPermitTypes.push(permitTypeDetail.permitType)
                    }
                }
            }
            await Driver.update({permitType: driverPermitTypes.join(',')}, {where: {driverId}});
        }).catch(error => {
            throw error
        });

        return res.json(utils.response(1, 'success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.deletePermitTypeDetail = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let detailId = req.body.detailId;
        let deleteReason = req.body.deleteReason;
        if (detailId) {
            await sequelizeObj.transaction(async transaction => {
                let detail = await DriverPermitTypeDetail.findByPk(detailId);
                if (detail) {
                    let detailMileage = detail.baseMileage;

                    await DriverPermitTypeDetail.destroy({ where: { id: detailId } });
                    let history = detail.dataValues;
                    history.deleteBy = userId;
                    history.deleteAt = moment().format('YYYY-MM-DD HH:mm:ss');
                    history.deleteReason = deleteReason;
                    await DriverPermitTypeDetailHistory.create(history);

                    let permitTypeConf = await PermitType.findOne({where: {permitType: detail.permitType }})
                    if (!permitTypeConf) {
                        return res.json(utils.response(0, `PermitType ${ detail.permitType } does not exist.`));
                    }
                    let parentPermitType = permitTypeConf.parent && permitTypeConf.parent.trim() ? permitTypeConf.parent : permitTypeConf.permitType;
                    let childPermitTypes = await sequelizeObj.query(`
                        select permitType from permittype pt where pt.parent = '${parentPermitType}' or pt.permitType = '${parentPermitType}'
                    `, { type: QueryTypes.SELECT, replacements: [] });
                    let childPermitTypeArray = [];
                    if (childPermitTypes && childPermitTypes.length > 0) {
                        for (let temp of childPermitTypes) {
                            childPermitTypeArray.push(temp.permitType);
                        }
                        let childPermitTypeDetails = await sequelizeObj.query(`
                            select permitType from driver_permittype_detail dd where dd.driverId=${detail.driverId} and dd.permitType in('${childPermitTypeArray.join("','")}')
                        `, { type: QueryTypes.SELECT, replacements: [] });
                        if (childPermitTypeDetails && childPermitTypeDetails.length > 0) {
                            let driverPermitMileage = await DriverMileage.findOne({where: {driverId: detail.driverId, permitType: parentPermitType }});
                            if (driverPermitMileage) {
                                let newMileage = driverPermitMileage.mileage - detailMileage;
                                if (newMileage < 0) {
                                    newMileage = 0;
                                }
                                await DriverMileage.update({mileage: newMileage}, {where: {driverId: detail.driverId, permitType: parentPermitType}})
                            }
                        } else {
                            await DriverMileage.destroy({where: {driverId: detail.driverId, permitType: parentPermitType }});
                        }
                    }

                    let driverPermitTypes = [];
                    let driverPermitTypeDetailList = await DriverPermitTypeDetail.findAll({where: {driverId: detail.driverId }});
                    if (driverPermitTypeDetailList && driverPermitTypeDetailList.length > 0) {
                        for (let permitTypeDetail of driverPermitTypeDetailList) {
                            if (permitTypeDetail.permitType && permitTypeDetail.permitType.toUpperCase().indexOf('CL') != -1) {
                                driverPermitTypes.push(permitTypeDetail.permitType)
                            }
                        }
                    }
                    await Driver.update({permitType: driverPermitTypes.join(',')}, {where: {driverId: detail.driverId }});
                }
            }).catch(error => {
                throw error
            });
        }

        return res.json(utils.response(1, 'Delete success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updateDriverBaseinfo = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let driverId = req.body.driverId;
        let permitNo = req.body.permitNo;
        let permitIssueDate = req.body.permitIssueDate;
        let permitStatus = req.body.permitStatus;
        let permitInvalidReason = req.body.permitInvalidReason;

        if (!permitNo || !permitIssueDate) {
            return res.json(utils.response(0, `Permit No and Date of Issue are required.`));
        }

        let existDrivers = await sequelizeObj.query(`
            select driverId from driver where driverId != ? and permitNo=?
        `, { type: QueryTypes.SELECT, replacements: [driverId, permitNo]});
        if (existDrivers && existDrivers.length > 0) {
            return res.json(utils.response(0, `Permit No: [${permitNo}] is repeated!`));
        }

        let driver = await Driver.findOne({where: {driverId: driverId}});
        if (!driver) {
            return res.json(utils.response(0, `Driver ${ driverId } does not exist.`));
        }
        driver.permitNo=permitNo;
        driver.permitIssueDate = moment(permitIssueDate);
        driver.permitStatus=permitStatus;
        driver.permitInvalidReason=permitInvalidReason;
        await driver.save();

        return res.json(utils.response(1, 'Update Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.addDriverPermitLog = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let driverId = req.body.driverId;
        let remarks = req.body.remarks;
        let reason = req.body.reason;

        let driver = await Driver.findOne({where: {driverId: driverId}});
        if (!driver) {
            return res.json(utils.response(0, `Driver ${ driverId } does not exist.`));
        }
        let permitStatus = driver.permitStatus;
        let permitLog = {
            driverId,
            optType: permitStatus,
            remarks,
            reason,
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            creator: userId
        }

        await DriverPermitLog.create(permitLog);

        return res.json(utils.response(1, 'Add remarks Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverPermitLogs = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let driverId = req.body.driverId;
        let logs = await sequelizeObj.query(`
            SELECT
                dd.permitStatus,
                dl.reason,
                uu.fullName as createName,
                dl.optTime,
                dl.remarks
            FROM driver_permit_log dl
            LEFT JOIN driver dd on dl.driverId = dd.driverId
            LEFT JOIN user uu on dl.creator = uu.userId
            WHERE dl.driverId = ? ORDER BY dl.optTime DESC
        `, { type: QueryTypes.SELECT, replacements: [driverId]});

        return res.json(utils.response(1, logs));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updateTaskMileageInfo = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let taskId = req.body.taskId;
        let resource = req.body.resourceType
        let startMileage = req.body.startMileage;
        let endMileage = req.body.endMileage;
        if (!startMileage || !endMileage) {
            return res.json(utils.response(0, `Start Mileage, End Mileage is required.`));
        }
        if (Number(startMileage) < 0) {
            return res.json(utils.response(0, `Start Mileage less than 0.`));
        }
        if (Number(endMileage) < 0) {
            return res.json(utils.response(0, `End Mileage less than 0.`));
        }
        if (Number(endMileage) < Number(startMileage)) {
            return res.json(utils.response(0, `End Mileage less than Start Mileage.`));
        }
        //update mileage
        let mileageTraveled = Number(endMileage) - Number(startMileage);
        // Update status
        let pageList = await userService.getUserPageList(userId, resource, 'Indent')
        let status = 'Edited'
        let actionList = pageList.map(item => {
            return item.action
        })

        let preMileage = await Mileage.findByPk(taskId)
        if (actionList.includes('Endorse') && actionList.includes('Approve')) {
            status = 'Approved'
            await updateTaskVehicleMileage(taskId, preMileage.vehicleNo, endMileage);
        }

        await Mileage.update({
            startMileage: startMileage, 
            endMileage: endMileage, 
            oldStartMileage: preMileage.startMileage,
            oldEndMileage: preMileage.endMileage, 
            mileageTraveled: mileageTraveled, 
            status, 
            updatedAt: moment() 
        }, {where: {taskId: taskId}});

        return res.json(utils.response(1, 'Update Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updateTaskMileageStatus = async function (req, res) {
    try {
        let { taskId, status } = req.body;
        if (!taskId) {
            log.warn('Task ID is needed.')
            return res.json(utils.response(0, 'Update fail.'));
        }
        if (['Endorsed', 'Approved', 'Cancelled', 'Rejected'].includes(status)) {
            let mileage = await Mileage.findByPk(taskId)
            if (status == 'Cancelled' || status == 'Rejected') {
                // status = null
                await Mileage.update({ 
                    status, 
                    startMileage: mileage.oldStartMileage, 
                    endMileage: mileage.oldEndMileage,
                    mileageTraveled: mileage.oldEndMileage - mileage.oldStartMileage,
                    oldEndMileage: null, 
                    oldStartMileage: null, 
                    updatedAt: moment() 
                }, { where: { taskId: taskId } })
            } else {
                await Mileage.update({ status, updatedAt: moment() }, { where: { taskId: taskId } });
            }
            if (status == 'Approved') {
                await updateTaskVehicleMileage(taskId, mileage.vehicleNo, mileage.endMileage);
            }
            return res.json(utils.response(1, 'Update success.'));
        } else {
            log.warn('Status is not correct.')
            return res.json(utils.response(0, 'Update fail.'));
        }
        
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const updateTaskVehicleMileage = async function(taskId, vehicleNo, newEndMileage) {
    //vehicle last complete task.
    let vehicleLastCompleteTask = await sequelizeObj.query(`
        select taskId from task where vehicleNumber = ? and driverStatus = 'completed' ORDER BY mobileEndTime desc LIMIT 1;
    `, { type: QueryTypes.SELECT, replacements: [vehicleNo]});
    let vehicleLastCompleteTaskId = '';
    if (vehicleLastCompleteTask && vehicleLastCompleteTask.length > 0) {
        vehicleLastCompleteTaskId = vehicleLastCompleteTask[0].taskId;
    }
    if (taskId == vehicleLastCompleteTaskId) {
        //udpate vehicle totalMieage
        await Vehicle.update({ totalMileage: newEndMileage}, { where: { vehicleNo } })
    }
}

module.exports.getAllBusyDrivers = async function (currentTaskId, startTime, endTime) {
    try {
        let baseSql = `SELECT DISTINCT t.driverId
            FROM task t
            WHERE t.driverStatus != 'completed' `;
        if (currentTaskId) {
            baseSql += ` and t.taskId != ${currentTaskId} `;
        }
        let startTimeStr = moment(startTime).format('YYYY-MM-DD HH:mm:ss');
        let endTimeStr = moment(endTime).format('YYYY-MM-DD HH:mm:ss');
        baseSql += ` and (DATE_FORMAT(t.indentStartTime,'%Y-%m-%d %H:%i:%s') BETWEEN '${startTimeStr}' AND '${endTimeStr}'
            OR DATE_FORMAT(t.indentEndTime, '%Y-%m-%d %H:%i:%s') BETWEEN '${startTimeStr}' AND '${endTimeStr}')
        `;

        let allBusyDrivers = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements: []});
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.approvePermitExchangeApply = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let driverId = req.body.driverId;
        let permitType = req.body.permitType;
        let optType = req.body.optType;
        let reason = req.body.reason;

        let updateJson = {}

        if (driverId) {
            let driver = await Driver.findByPk(driverId);
            let driverEmail = driver && driver.email ? driver.email : null;
            let permitTypeApply = await DriverLicenseExchangeApply.findOne({where: {driverId, permitType}});

            updateJson.emailConfirm = driverEmail;
            if (permitTypeApply) {
                if (permitTypeApply.status == 'fail' || permitTypeApply.status == 'rejected' || permitTypeApply.status == 'success') {
                    return res.json(utils.response(0, 'ApprovePermitExchangeApply data status error, please refresh page!'));
                }
                let pageList = await userService.getUserPageList(userId, 'Resources', 'Licensing')
                let actionList = [];
                if (pageList) {
                    actionList = pageList.map(item => {return item.action});
                }
                if (optType == 'reject') {
                    if (!actionList.includes('Reject')) {
                        return res.json(utils.response(0, 'No permission!'));
                    }
                    updateJson.rejectDate = moment();
                    updateJson.rejectBy = userId;
                    updateJson.status = 'Rejected'
                    updateJson.rejectReason = reason
                } else if (optType == 'pending') {
                    if (!actionList.includes('Approval Status')) {
                        return res.json(utils.response(0, 'No permission!'));
                    }
                    updateJson.status = 'Pending'
                    updateJson.pendingReason = reason;
                    updateJson.pendingDate = moment();
                    updateJson.pendingBy = userId;
                } else if (optType == 'success' || optType == 'fail') {
                    if (!actionList.includes('Approval Status')) {
                        return res.json(utils.response(0, 'No permission!'));
                    }
                    updateJson.approveDate = moment();
                    updateJson.approveBy = userId;
                    updateJson.status = optType == 'success' ? 'Success' : 'Failed';
                    if (optType == 'fail') {
                        updateJson.failReason = reason;
                        updateJson.failDate = moment();
                        updateJson.failBy = userId;
                    } else {
                        updateJson.successDate = moment();
                        updateJson.successBy = userId;
                    }
                }else if (optType == 'submit') {
                    if (!actionList.includes('Submit')) {
                        return res.json(utils.response(0, 'No permission!'));
                    }
                    if (permitTypeApply.status != 'Pending Approval') {
                        return res.json(utils.response(0, 'ApprovePermitExchangeApply data status error, please refresh page!'));
                    }
                    updateJson.submitDate = moment();
                    updateJson.submitBy = userId;
                    updateJson.status = 'Submitted'
                    if (actionList.includes('Endorse')) {
                        updateJson.status = 'Endorsed'
                        updateJson.endorseDate = moment();
                        updateJson.endorseBy = userId;
                        if (actionList.includes('Verify')) {
                            updateJson.status = 'Verified'
                            updateJson.verifyDate = moment();
                            updateJson.verifyBy = userId;
                            if (actionList.includes('Recommend')) {
                                updateJson.status = 'Recommended'
                                updateJson.recommendDate = moment();
                                updateJson.recommendBy = userId;
                            }
                        }
                    }
                } else if (optType == 'endorse') {
                    if (!actionList.includes('Endorse')) {
                        return res.json(utils.response(0, 'No permission!'));
                    }
                    if (permitTypeApply.status != 'Submitted') {
                        return res.json(utils.response(0, 'ApprovePermitExchangeApply data status error, please refresh page!'));
                    }
                    updateJson.status = 'Endorsed'
                    updateJson.endorseDate = moment();
                    updateJson.endorseBy = userId;
                    if (actionList.includes('Verify')) {
                        updateJson.status = 'Verified'
                        updateJson.verifyDate = moment();
                        updateJson.verifyBy = userId;
                        if (actionList.includes('Recommend')) {
                            updateJson.status = 'Recommended'
                            updateJson.recommendDate = moment();
                            updateJson.recommendBy = userId;
                        }
                    }
                } else if (optType == 'verify') {
                    if (!actionList.includes('Verify')) {
                        return res.json(utils.response(0, 'No permission!'));
                    }
                    if (permitTypeApply.status != 'Endorsed') {
                        return res.json(utils.response(0, 'ApprovePermitExchangeApply data status error, please refresh page!'));
                    }
                    updateJson.status = 'Verified'
                    updateJson.verifyDate = moment();
                    updateJson.verifyBy = userId;
                    if (actionList.includes('Recommend')) {
                        updateJson.status = 'Recommended'
                        updateJson.recommendDate = moment();
                        updateJson.recommendBy = userId;
                    }
                } else if (optType == 'recommend') {
                    if (!actionList.includes('Recommend')) {
                        return res.json(utils.response(0, 'No permission!'));
                    }
                    if (permitTypeApply.status != 'Verified') {
                        return res.json(utils.response(0, 'ApprovePermitExchangeApply data status error, please refresh page!'));
                    }
                    updateJson.status = 'Recommended'
                    updateJson.recommendDate = moment();
                    updateJson.recommendBy = userId;
                } else {
                    return res.json(utils.response(0, 'Unsupport option: ' + optType));
                }

                await sequelizeObj.transaction(async transaction => {
                    await DriverLicenseExchangeApply.update(updateJson, {where: {applyId: permitTypeApply.applyId}});
                }).catch(error => {
                    throw error
                })
            }
        } else {
            return res.json(utils.response(0, 'ApprovePermitExchangeApply params driverId is empty!'));
        }
        
        return res.json(utils.response(1, 'Operation success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error && error.message ? error.message : 'ApprovePermitExchangeApply fail!'));
    }
}

module.exports.getSystemGroup = async function (req, res) {
    try {
        let { checkRole } = req.body;
        let sql = ` select id, groupName from \`group\` `
        let user = await User.findOne({ where: { userId: req.cookies.userId } });
        if (!user) throw Error(`User does not exist.`)

        let replacements = []
        if(user.userType.toUpperCase() == 'CUSTOMER') {
          if(user.unitId){
            sql += ` where id = ${ user.unitId } `
          }
        } else if (checkRole) {
            let user = await userService.UserUtils.getUserDetailInfo(req.cookies.userId)
            if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                sql += ` where id = ${ user.unitId } `
            } else if (user.userType == CONTENT.USER_TYPE.HQ) {
                let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId)
                if (groupIdList.length) {
                    sql += ` where id in (?) `
                    replacements.push(groupIdList)
                }
            }
        }
        let result = await sequelizeSystemObj.query(sql, { type: QueryTypes.SELECT, replacements });
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.downloadDriverPermitExchangeApply = async function (req, res) {
    const checkUser = async function (userId) {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            throw Error(`User ${ userId } does not exist.`);
        }
        return user;
    }

    let userId = req.cookies.userId;
    let { permitType, unit, subUnit, searchCondition } = req.body;

    let user = await checkUser(userId)
    
    let replacements = [];
    let baseSQL = `
        SELECT
            d.driverId,
            d.driverName,
            d.contactNumber,
            da.permitTypeMileage as driverMileage,
            d.nric,
            da.permitType as exchangePermitType,
            d.permitType,
            d.enlistmentDate,
            d.birthday,
            u.unit AS hub,
            u.subUnit AS node,
            da.driverDemeritPoints,
            da.emailConfirm,
            pd.passDate,
            pd.score,
            d.operationallyReadyDate,
            da.status
        FROM
            driver_license_exchange_apply da
        LEFT JOIN driver_permittype_detail pd on da.driverId = pd.driverId and da.permitType = pd.permitType
        LEFT JOIN driver d on da.driverId = d.driverId
        LEFT JOIN user us ON us.driverId = d.driverId 
        LEFT JOIN unit u ON u.id = d.unitId
    `;
    let limitSQL = []

    let date21YearsAgo = moment().add(-21, 'year').format("YYYY-MM-DD");
    limitSQL.push(` da.status in('Recommended', 'Pending') and ( LOWER(da.permitType) != 'cl 4' OR (LOWER(da.permitType) = 'cl 4' and d.birthday <= '${date21YearsAgo}') ) `);
    
    if (unit) {
        limitSQL.push( ` u.unit = ? `);
        replacements.push(unit);
    }
    if (subUnit) {
        if(subUnit.toLowerCase() == 'null') {
            limitSQL.push( ` u.subUnit is null `);
        } else {
          limitSQL.push( ` u.subUnit = ? `);
          replacements.push(subUnit);  
        }
    }
    if (permitType) {
        limitSQL.push(` da.permitType = ? `);
        replacements.push(permitType);
    }
    if (searchCondition) {
        limitSQL.push(`d.driverName LIKE ` + sequelizeObj.escape("%" + searchCondition + "%"));
    }
    if (limitSQL.length) {
        baseSQL = baseSQL + ' WHERE d.driverId is not null and ' + limitSQL.join(' AND ');
    }
    baseSQL += ` ORDER BY da.applyDate desc `
    let driverExchangePermitApplyList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements });
    if (driverExchangePermitApplyList && driverExchangePermitApplyList.length > 0) {
        let fileName = moment().format('YYYYMMDDHHmm') + '-DriverLicensing.xlsx';
        let baseFilePath = './public/resources/download/'
        if(!fs.existsSync(baseFilePath)) fs.mkdirSync(baseFilePath);
        let filePath = path.join(baseFilePath, fileName)
        
        let rows = []
        for (let row of driverExchangePermitApplyList) {
            let { driverName, nric, hub, node, contactNumber, emailConfirm, birthday, exchangePermitType, driverMileage, driverDemeritPoints, passDate, score, operationallyReadyDate, status } = row
            // 2023-08-29 Get decrypted is nric.
            if(nric) {
                if(nric.length > 9) nric = utils.decodeAESCode(nric);
            }
            rows.push([
                nric,
                driverName,
                hub,
                node,
                contactNumber,
                emailConfirm,
                birthday ? moment(birthday).format('YYYY-MM-DD') : '', 
                exchangePermitType,
                driverMileage,
                driverDemeritPoints ? driverDemeritPoints : "0",
                passDate ? moment(passDate).format('YYYY-MM-DD') : '',
                score ? score : '0',
                operationallyReadyDate ? moment(operationallyReadyDate).format('YYYY-MM-DD') : '',
                status ? status : ''
            ])
        }

        let title = [
            ['Full NRIC', 'Full Name', 'Hub', 'Node', 'Contact Number', 'Email', 'Date Of Birth', 'Test Type', 'Mileage', 'Demerit Points', 'Pass Date', 'Score', 'Operationally Ready Date (ORD)', 'Status']
        ]
        const sheetOptions = {'!cols': 
            [
                { wch: 10 }, { wch: 20 }, { wch: 10 }, { wch: 10 }, { wch: 20 }, { wch: 20 }, { wch: 20 }, { wch: 20 }, { wch: 40 }, { wch: 15 }, { wch: 20 }, { wch: 10 }, { wch: 30 }, { wch: 10 }
            ], 
            '!rows': [{ hpt: 20 }],
        };
        let buffer = xlsx.build([
            {
                name: 'sheet1',
                data: [].concat(title, rows)
            }
        ],
        {
            sheetOptions
        });
        fs.writeFileSync(filePath, buffer, { 'flag': 'w' });

        return res.json(utils.response(1, {fileName: fileName}));
    } else {
        return res.json(utils.response(0, 'No data to download!'));
    }
}

//2023-07-18 Get the driver's unit/subUnit based on different times
module.exports.getDriverHoto = async function (req, res) {
    try {
       let { driverIdList, timeNeeded } = req.body;
       timeNeeded = timeNeeded ? moment(timeNeeded).format('YYYY-MM-DD') : moment().format('YYYY-MM-DD')
       let sql = `
       SELECT d.driverId,
        IF(h.toHub IS NULL AND  hr.toHub IS NULL, u.unit, IF(h.toHub IS NULL, hr.toHub, h.toHub)) AS currentUnit, 
        IF(h.toHub IS NULL AND  hr.toHub IS NULL, u.subUnit, IF(h.toHub IS NULL, hr.toNode, h.toNode)) AS currentSubUnit
        FROM driver d 
        LEFT JOIN (
            SELECT ho.id, ho.driverId, ho.toHub, ho.toNode FROM hoto ho WHERE '${ timeNeeded }' BETWEEN DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(ho.endDateTime, '%Y-%m-%d')
        ) h ON h.driverId = d.driverId 
        LEFT JOIN (
            SELECT hr.id, hr.driverId, hr.toHub, hr.toNode FROM hoto_record hr 
            WHERE '${ timeNeeded }' BETWEEN DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(hr.endDateTime, '%Y-%m-%d')
            AND '${ timeNeeded }' < DATE_FORMAT(hr.returnDateTime , '%Y-%m-%d')
        ) hr ON hr.driverId = d.driverId
        LEFT JOIN unit u ON u.id = d.unitId
        where 1=1
       `
       let params = [];
       if(driverIdList) {
        if(driverIdList instanceof Array) {
            if(driverIdList.length > 0) {
                sql += ` and d.driverId in(?)`
                params.push(driverIdList.join(","));
            }
        } else {
            sql += ` and d.driverId = ?`
            params.push(driverIdList);
        }
       }
       let driverHoto = await sequelizeObj.query(sql, {
            type: QueryTypes.SELECT, replacements: params
        });
        return res.json(utils.response(1, driverHoto));
    } catch(error) {
        log.error(error)
		return res.json(utils.response(0, error));
    }
}

module.exports.getDriverInfo = async function (option = {}) {
    try {
        // Need check hoto_record/loan_record table
        let baseSql = `
            SELECT d.driverId, us.fullName, u.id AS rootUnitId, u.unit AS rootHub, u.subUnit AS rootNode,
            hh.id AS hotoId, hh2.id AS hoto2Id,
            IF(hh.id IS NOT NULL, hh.unitId, IF(hh2.id IS NOT NULL, hh2.unitId, u.id)) AS unitId,
            IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, u.unit)) AS hub,
            IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, u.subUnit)) AS node,
            ll.id AS loanId,
            IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.groupId, NULL))) AS groupId
            FROM (
                select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver 
                UNION ALL 
                select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
            ) d
            LEFT JOIN user us ON us.driverId = d.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN (
                SELECT ho.id, ho.driverId, ho.unitId, ho.toHub, ho.toNode 
                FROM hoto ho 
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' } BETWEEN ho.startDateTime AND ho.endDateTime
                and ho.status = 'Approved'
            ) hh ON hh.driverId = d.driverId
            LEFT JOIN (
                SELECT ho2.id, ho2.driverId, u.id AS unitId, ho2.toHub, ho2.toNode 
                FROM hoto_record ho2 
                LEFT JOIN unit u ON u.unit = ho2.toHub AND u.subUnit = ho2.toNode
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' } BETWEEN ho2.startDateTime AND ho2.returnDateTime
                and ho2.status = 'Approved'
            ) hh2 ON hh2.driverId = d.driverId
            LEFT JOIN (
                SELECT lo.id, lo.driverId, lo.groupId 
                FROM loan lo 
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' } >= lo.startDate
            ) ll ON ll.driverId = d.driverId
            LEFT JOIN (
                SELECT lo2.id, lo2.driverId, lo2.groupId 
                FROM loan_record lo2 
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' } BETWEEN lo2.startDate AND lo2.returnDate
            ) ll2 ON ll2.driverId = d.driverId
        `

        let sql = ` SELECT * FROM ( ${ baseSql } ) dd WHERE 1=1 `
        let replacements = []
        if (option.hub) {
            sql += ` AND dd.hub = ? `
            replacements.push(option.hub);
        }
        if (option.node) {
            sql += ` AND dd.node = ? `
            replacements.push(option.node);
        }

        // Default search ignore group
        if (option.groupId == null || option.groupId == undefined) option.groupId = -1;
        if (option.groupId > 0) {
            // Limit group
            sql += ` AND dd.groupId = ? `
            replacements.push(option.groupId);
        } else if (option.groupId == 0) {
            // No limit

        } else if (option.groupId < 0) {
            // ignore group driver
            sql += ` AND dd.groupId IS NULL `
        }

        if (option.unitId) {
            sql += ` AND dd.unitId = ? `
            replacements.push(option.unitId);
        }
        if (option.driverId) {
            sql += ` AND dd.driverId = ? `
            replacements.push(option.driverId);
        }
        console.log(sql)
        let result = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements });
        return result;
    } catch (error) {
        throw error
    }
}

module.exports.getDriverInfoSQL = async function (option = {}) {
    try {
        // Need check hoto_record/loan_record table
        let baseSql = `
            SELECT d.driverId, us.fullName, u.id AS rootUnitId, u.unit AS rootHub, u.subUnit AS rootNode,
            hh.id AS hotoId, hh2.id AS hoto2Id,
            IF(hh.id IS NOT NULL, hh.unitId, IF(hh2.id IS NOT NULL, hh2.unitId, u.id)) AS unitId,
            IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, u.unit)) AS hub,
            IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, u.subUnit)) AS node,
            ll.id AS loanId,
            IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), us.unitId, NULL))) AS groupId
            FROM driver d
            LEFT JOIN USER us ON us.driverId = d.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN (
                SELECT ho.id, ho.driverId, ho.unitId, ho.toHub, ho.toNode 
                FROM hoto ho 
                WHERE ${ option.selectedDate } BETWEEN ho.startDateTime AND ho.endDateTime
                and ho.status = 'Approved'
            ) hh ON hh.driverId = d.driverId
            LEFT JOIN (
                SELECT ho2.id, ho2.driverId, u.id AS unitId, ho2.toHub, ho2.toNode 
                FROM hoto_record ho2 
                LEFT JOIN unit u ON u.unit = ho2.toHub AND u.subUnit = ho2.toNode
                WHERE ${ option.selectedDate } BETWEEN ho2.startDateTime AND ho2.returnDateTime
                and ho2.status = 'Approved'
            ) hh2 ON hh2.driverId = d.driverId
            LEFT JOIN (
                SELECT lo.id, lo.driverId, lo.groupId 
                FROM loan lo 
                WHERE ${ option.selectedDate } >= lo.startDate
            ) ll ON ll.driverId = d.driverId
            LEFT JOIN (
                SELECT lo2.id, lo2.driverId, lo2.groupId 
                FROM loan_record lo2 
                WHERE ${ option.selectedDate } BETWEEN lo2.startDate AND lo2.returnDate
            ) ll2 ON ll2.driverId = d.driverId
        `

        let sql = ` SELECT * FROM ( ${ baseSql } ) dd WHERE 1=1 `
        let replacements = []
        if (option.hub) {
            sql += ` AND dd.hub = ? `
            replacements.push(option.hub);
        }
        if (option.node) {
            sql += ` AND dd.node = ? `
            replacements.push(option.node);
        }

        // Default search ignore group
        if (option.groupId == null || option.groupId == undefined) option.groupId = -1;
        if (option.groupId > 0) {
            // Limit group
            sql += ` AND dd.groupId = ? `
            replacements.push(option.groupId);
        } else if (option.groupId == 0) {
            // No limit

        } else if (option.groupId < 0) {
            // ignore group driver
            sql += ` AND dd.groupId IS NULL `
        }

        if (option.unitId) {
            sql += ` AND dd.unitId = ? `
            replacements.push(option.unitId);
        }
        if (option.driverId) {
            sql += ` AND dd.driverId = ? `
            replacements.push(option.driverId);
        }
        // sql += ` GROUP BY dd.driverId `
        console.log(sql)
        return sql;
    } catch (error) {
        throw error
    }
}


module.exports.getVehicleInfo = async function (option) {
    try {
        let baseSql = `
            SELECT v.vehicleNo, u.id AS rootUnitId, u.unit AS rootHub, u.subUnit AS rootNode,
            hh.id AS hotoId,
            IF(hh.id IS NOT NULL, hh.unitId, IF(hh2.id IS NOT NULL, hh2.unitId, u.id)) AS unitId,
            IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, u.unit)) AS hub,
            IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, u.subUnit)) AS node,
            ll.id AS loanId,
            IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, v.groupId)) AS groupId
            FROM (
                select vehicleNo, unitId, groupId, vehicleType, limitSpeed, deviceId FROM vehicle 
                UNION ALL 
                select vehicleNo, unitId, groupId, vehicleType, limitSpeed, deviceId FROM vehicle_history
            ) v
            LEFT JOIN unit u ON u.id = v.unitId
            LEFT JOIN (
                SELECT ho.id, ho.vehicleNo, ho.unitId, ho.toHub, ho.toNode 
                FROM hoto ho 
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' }  BETWEEN ho.startDateTime AND ho.endDateTime
                and ho.status = 'Approved'
            ) hh ON hh.vehicleNo = v.vehicleNo
            LEFT JOIN (
                SELECT ho2.id, ho2.vehicleNo, u.id AS unitId, ho2.toHub, ho2.toNode 
                FROM hoto_record ho2 
                LEFT JOIN unit u ON u.unit = ho2.toHub AND u.subUnit = ho2.toNode
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' }  BETWEEN ho2.startDateTime AND ho2.returnDateTime
                and ho2.status = 'Approved'
            ) hh2 ON hh2.vehicleNo = v.vehicleNo
            LEFT JOIN (
                SELECT lo.id, lo.vehicleNo, lo.groupId 
                FROM loan lo 
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' }  >= lo.startDate
            ) ll ON ll.vehicleNo = v.vehicleNo
            LEFT JOIN (
                SELECT lo2.id, lo2.vehicleNo, lo2.groupId 
                FROM loan_record lo2 
                WHERE ${ option.selectedDate ? `'${ option.selectedDate }'` : 'NOW()' }  BETWEEN lo2.startDate AND lo2.returnDate
            ) ll2 ON ll2.vehicleNo = v.vehicleNo
        `
        let sql = ` SELECT * FROM ( ${ baseSql } ) vv WHERE 1=1 `
        let replacements = []
        if (option.hub) {
            sql += ` AND vv.hub = ? `
            replacements.push(option.hub);
        }
        if (option.node) {
            sql += ` AND vv.node = ? `
            replacements.push(option.node);
        }

        // Default search ignore group
        if (option.groupId == null || option.groupId == undefined) option.groupId = -1;
        if (option.groupId > 0) {
            // Limit group
            sql += ` AND vv.groupId = ? `
            replacements.push(option.groupId);
        } else if (option.groupId == 0) {
            // No limit

        } else if (option.groupId < 0) {
            // ignore group driver
            sql += ` AND vv.groupId IS NULL `
        }

        if (option.unitId) {
            sql += ` AND vv.unitId = ? `
            replacements.push(option.unitId);
        }
        if (option.vehicleNo) {
            sql += ` AND vv.vehicleNo = ? `
            replacements.push(option.vehicleNo);
        }

        console.log(sql)
        let vehicleList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements });
        return vehicleList
    } catch (error) {
        throw error
    }
}

module.exports.getDriverAchievementData = async function (req, res) {
	try {
		let { userId } = req.body
		let user = await User.findByPk(userId)

		if (!user) {
			let msg = `User id ${userId} not exist!`
			log.warn(msg)
			return res.json(utils.response(0, msg));
		}

		let result = {
			driverAchievementInfo: {},
			driverNodeTaskHoursLeaderBoardTop20: {},
			allNodeTaskHoursLeaderBoardTop10: {}
		}

		let driverId = req.body.driverId;
		let selectDate = req.body.selectDate;
		if (driverId) {
            let driver = await Driver.findByPk(driverId);
			let groupId = null;
			let hub = null;
			let node = null;
			if (driver.groupId) {
				groupId = driver.groupId;
			} else {
				let unit = await Unit.findByPk(driver.unitId);
				if (unit) {
					hub = unit.unit;
					node = unit.subUnit;
				}
			}
			let driverAchievementInfo = await DriverMonthAchievement.findOne({where : {driverId, month: selectDate}});

			//all node top 10
			let allNodeTop10 = await sequelizeObj.query(`
				SELECT
					d.driverName, dm.month, dm.taskPerfectHours
				FROM driver_month_achievement dm
				LEFT JOIN driver d on dm.driverId = d.driverId
				WHERE taskPerfectHours > 0 and dm.month=?
				ORDER BY taskPerfectHours DESC LIMIT 10
			`, { replacements: [selectDate], type: QueryTypes.SELECT });

			result.driverAchievementInfo = driverAchievementInfo;
			result.allNodeTaskHoursLeaderBoardTop10 = allNodeTop10;

			//my node top 20   DV/LOA my group
			let baseSql = `
				SELECT
					d.driverName, dm.month, dm.taskPerfectHours
				FROM driver_month_achievement dm
				LEFT JOIN driver d on dm.driverId = d.driverId
				LEFT JOIN unit u on d.unitId = u.id
				WHERE taskPerfectHours > 0 and dm.month=?
			`;
			if (groupId) {
				baseSql += ` and d.groupId = ${groupId} `;
			} else {
				if (hub) {
					baseSql += ` and u.unit = '${hub}' `;
				}
				if (node) {
					baseSql += ` and u.subUnit = '${node}' `;
				}
			}
			baseSql += ` ORDER BY taskPerfectHours DESC LIMIT 20 `;
			let myNodeTop20 = await sequelizeObj.query(baseSql, { replacements: [selectDate], type: QueryTypes.SELECT });

			result.driverNodeTaskHoursLeaderBoardTop20 = myNodeTop20;


			return res.json(utils.response(1, result));
		} else {
			log.warn('getDriverAchievementData parmas driverId is empty!');
			return res.json(utils.response(0, `Parmas driverId is empty!`));
		}

	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.approveAssessmentRecord = async function (req, res) {
    try {
        let recordId = req.body.recordId;
        let newStatus = req.body.newStatus;

        if (newStatus == 'Approved' || newStatus == 'Rejected') {
            await DriverAssessmentRecord.update({approveStatus: newStatus}, {where: {id: recordId}});
        } else {
            return res.json(utils.response(0, 'Unknown data status!'));
        }
        
        return res.json(utils.response(1, 'Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error && error.message ? error.message : 'ApproveAssessmentRecord fail!'));
    }
}

module.exports.approvePermitTypeDetail = async function (req, res) {
    try {
        let recordId = req.body.recordId;
        let newStatus = req.body.newStatus;

        if (newStatus == 'Approved' || newStatus == 'Rejected') {
            await DriverPermitTypeDetail.update({approveStatus: newStatus}, {where: {id: recordId}});
        } else {
            return res.json(utils.response(0, 'Unknown data status!'));
        }
        
        return res.json(utils.response(1, 'Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error && error.message ? error.message : 'ApprovePermitTypeDetail fail!'));
    }
}

module.exports.approvePlatformConf = async function (req, res) {
    try {
        let recordId = req.body.recordId;
        let newStatus = req.body.newStatus;

        if (newStatus == 'Approved' || newStatus == 'Rejected') {
            await DriverPlatformConf.update({approveStatus: newStatus}, {where: {id: recordId}});
        } else {
            return res.json(utils.response(0, 'Unknown data status!'));
        }
        
        return res.json(utils.response(1, 'Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error && error.message ? error.message : 'ApprovePlatformConf fail!'));
    }
}

module.exports.getDriverEffectiveData = async function (req, res) {
    try {
        let driverId = req.body.driverId;

        let baseSql = ` 
            SELECT
                t.taskId, t.driverStatus, t.indentStartTime, t.indentEndTime, t.purpose, t.dataFrom
            FROM task t
            WHERE t.driverId = ?
            and t.driverStatus != 'Cancelled' and t.driverStatus != 'completed' and (t.indentEndTime > NOW() or t.driverStatus = 'started');
         `;
		let driverEffectiveTaskList = await sequelizeObj.query(baseSql, { replacements: [driverId], type: QueryTypes.SELECT });

        let driverUrgentTaskList = await sequelizeObj.query(`
            SELECT
                ud.dutyId as taskId, ud.status as driverStatus,
                ud.indentStartDate as indentStartTime, ud.indentEndDate as indentEndTime, uc.purpose, 'DUTY' as dataFrom
            FROM urgent_duty ud
            LEFT JOIN urgent_config uc on ud.configId = uc.id
            where ud.driverId=? and ud.status != 'Cancelled' and ud.status != 'completed' and (ud.indentEndDate > NOW() or ud.status = 'started')
        `, { type: QueryTypes.SELECT, replacements: [driverId]});

        driverEffectiveTaskList = driverEffectiveTaskList.concat(driverUrgentTaskList);

        baseSql = ` 
            select 
                ho.id, ho.requestId, ho.startDateTime, ho.endDateTime, ht.purpose
            from hoto ho 
            LEFT JOIN hoto_request ht on ho.requestId = ht.id
            where ho.driverId=? and ho.status='Approved'
         `;
		let driverEffectiveHotoList = await sequelizeObj.query(baseSql, { replacements: [driverId], type: QueryTypes.SELECT });

        baseSql = ` 
            select l.id, l.indentId, l.startDate, l.endDate, l.purpose from loan l where l.driverId =?
         `;
		let driverEffectiveLoanList = await sequelizeObj.query(baseSql, { replacements: [driverId], type: QueryTypes.SELECT });
        
        return res.json(utils.response(1, {taskList: driverEffectiveTaskList, hotoList: driverEffectiveHotoList, loanList: driverEffectiveLoanList}));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error && error.message ? error.message : 'getDriverEffectiveData fail!'));
    }
}

const returnDriverHoto = async function(driverId, hotoList, userId) {
    let errorMsg = '';
    try {
        for (let item of hotoList) {
            let hotoItem = await HOTO.findOne({ where: { id: item.id } });
            if (hotoItem) {
                await HOTO.destroy({ where: { id: item.id } });
                let obj = {
                    vehicleNo: hotoItem.vehicleNo, 
                    driverId: hotoItem.driverId,
                    fromHub: hotoItem.fromHub,
                    fromNode: hotoItem.fromNode,
                    toHub: hotoItem.toHub,
                    toNode: hotoItem.toNode,
                    returnBy: userId,
                    returnDateTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    startDateTime:  moment(hotoItem.startDateTime).format("YYYY-MM-DD HH:mm"),
                    endDateTime: moment(hotoItem.endDateTime).format("YYYY-MM-DD HH:mm"),
                    creator: hotoItem.creator,
                    status: hotoItem.status,
                    requestId: hotoItem.requestId,
                    createdAt: hotoItem.createdAt
                }
                await HOTORecord.create(obj)

                await OperationRecord.create({
                    id: null,
                    operatorId: userId,
                    businessType: 'hoto',
                    businessId: driverId,
                    optType: 'return',
                    beforeData: `driverId:${ driverId }`,
                    afterData: '',
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: `System auto return when driver deactivate.` 
                })
            }
        }
    } catch (error) {
        log.error(error)
        errorMsg = `returnDriverHoto fail: driverId[${driverId}]`;
    }
    return errorMsg;
}

const returnDriverLoan = async function(driverId, loanList, userId) {
    let errorMsg = '';
    try {
        for (let loanOut of loanList) {
            loanOut = await loan.findByPk(loanOut.id);
            if (loanOut) {
                await sequelizeObj.transaction(async transaction => {
                    let newLoanRecord = {
                        driverId: loanOut.driverId,
                        vehicleNo: loanOut.vehicleNo,
                        indentId: loanOut.indentId, 
                        taskId: loanOut.taskId,
                        startDate: loanOut.startDate,
                        endDate: loanOut.endDate, 
                        groupId: loanOut.groupId,
                        returnDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                        returnBy: userId,
                        creator: loanOut.creator,
                        returnRemark: 'System auto return when driver deactivate.',
                        actualStartTime: loanOut.actualStartTime,
                        actualEndTime: loanOut.actualEndTime,
                        unitId: loanOut.unitId,
                        activity: loanOut.activity,
                        purpose: loanOut.purpose,
                        createdAt: loanOut.createdAt
                    };
                    await loanRecord.create(newLoanRecord);
                    await loan.destroy({ where: { id: loanOut.id } });
                    await OperationRecord.create({
                        id: null,
                        operatorId: userId,
                        businessType: 'loan',
                        businessId: driverId,
                        optType: 'return loan',
                        beforeData: 'driverId:' + driverId,
                        afterData: '',
                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remarks: `System auto return when driver deactivate.` 
                    })
                }).catch(error => {
                    return `returnDriverLoan fail: driverId[${driverId}]`;
                })

                await sequelizeSystemObj.transaction(async transaction => {
                    if(loanOut.groupId > 0){
                        await sequelizeSystemObj.query(`
                            update job_task set taskStatus = 'Completed' where id = ${ loanOut.taskId }
                        `, { type: QueryTypes.UPDATE, replacements: [] })
                        let sysTask = await sequelizeSystemObj.query(`
                            SELECT tripId FROM job_task
                            WHERE id = ${ loanOut.taskId } 
                        `, { type: QueryTypes.SELECT })
                        let tripStatus = await sequelizeSystemObj.query(`
                            SELECT jt.taskStatus FROM job_task jt
                            LEFT JOIN job j ON j.id = jt.tripId
                            WHERE j.id = ${ sysTask[0].tripId } 
                            GROUP BY jt.taskStatus
                        `, { type: QueryTypes.SELECT })
                        let tripStatus2 = await sequelizeSystemObj.query(`
                            SELECT jt.taskStatus FROM job_task jt
                            LEFT JOIN job j ON j.id = jt.tripId
                            WHERE j.id = ${ sysTask[0].tripId } 
                            and jt.taskStatus = 'completed'
                            GROUP BY jt.taskStatus
                        `, { type: QueryTypes.SELECT })
                        let jobStatus = null;
                        if(tripStatus2.length == tripStatus.length) {
                            jobStatus = 'Completed'
                        }
                        if(jobStatus) {
                            await sequelizeSystemObj.query(`
                                UPDATE job SET status = '${ jobStatus }' WHERE id = ${ sysTask[0].tripId }
                            `, { type: QueryTypes.UPDATE })
                        }
                    }
                })
            }
        }
    } catch (error) {
        log.error(error)
        errorMsg = `returnDriverLoan fail: driverId[${driverId}]`;
    }

    return errorMsg;
}

module.exports.addCivilianLicence = async function (req, res) {
    try {
        await sequelizeObj.transaction(async transaction => {
            let { cardSerialNumber, civilianLicence, dateOfIssue, driverId } = req.body;
            let oldCivilianLicence = await DriverCivilianLicence.findAll({ where: { driverId: driverId, civilianLicence: civilianLicence, status: 'Approved' } })
            if(oldCivilianLicence.length > 0) return res.json(utils.response(0, `The Civilian Licence cannot be the same.`));
            const newCivilianLicence = await DriverCivilianLicence.create({ cardSerialNumber: cardSerialNumber, civilianLicence: civilianLicence, dateOfIssue: dateOfIssue, driverId: driverId, creator: req.cookies.userId, status: 'Approved' });
            let CivilianLicence2 = await DriverCivilianLicence.findOne({ where: { driverId: newCivilianLicence.driverId, cardSerialNumber: { [Op.not]: null }, status: 'Approved' } })
            if(CivilianLicence2) {
                await DriverCivilianLicence.update({ cardSerialNumber: CivilianLicence2.cardSerialNumber }, { where: { driverId: newCivilianLicence.driverId } });
            }
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Civilian Licence',
                businessId: newCivilianLicence.driverId,
                optType: 'Add',
                afterData: `${ JSON.stringify(newCivilianLicence) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'create the civilian licence.'
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.editCivilianLicence = async function (req, res) {
    try {
        await sequelizeObj.transaction(async transaction => {
            let civilianLicenceObj = req.body.civilianLicenceObj;
            let id = req.body.id;
            let oldCivilianLicence2 = await DriverCivilianLicence.findAll({ where: { driverId: civilianLicenceObj.driverId, civilianLicence: civilianLicenceObj.civilianLicence, id: { [Op.ne]: id }, status: 'Approved'  } })
            if(oldCivilianLicence2.length > 0) return res.json(utils.response(0, `The Civilian Licence cannot be the same.`));
            let oldCivilianLicence = await DriverCivilianLicence.findOne({ where: { id: id } })
            await DriverCivilianLicence.update(civilianLicenceObj, { where: { id: id } });
            let CivilianLicence2 = await DriverCivilianLicence.findOne({ where: { id: id, cardSerialNumber: { [Op.not]: null }, status: 'Approved' } })
            if(CivilianLicence2) {
                await DriverCivilianLicence.update({ cardSerialNumber: CivilianLicence2.cardSerialNumber }, { where: { driverId: civilianLicenceObj.driverId } });
            }
            let newCivilianLicence = await DriverCivilianLicence.findOne({ where: { id: id } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Civilian Licence',
                businessId: newCivilianLicence.driverId,
                optType: 'Edit',
                beforeData: `${ JSON.stringify(oldCivilianLicence) }`,
                afterData: `${ JSON.stringify(newCivilianLicence) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'update the civilian licence.'
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getCivilianLicence = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let civilianLicenceList = await DriverCivilianLicence.findAll({ where: { driverId: driverId, status: 'Approved' } })
        let pageList = await userService.getUserPageList(req.cookies.userId, 'License', 'Civilian Licence')
        let operationList = pageList .map(item => `${ item.action }`).join(',')
        return res.json(utils.response(1, { civilianLicenceList, operationList }));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getCivilianLicenceById = async function (req, res) {
    try {
        let id = req.body.id;
        let civilianLicence = await DriverCivilianLicence.findOne({ where: { id: id } })
        return res.json(utils.response(1, civilianLicence));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.deleteCivilianLicenceById = async function (req, res) {
    try {
        await sequelizeObj.transaction(async transaction => {
            let id = req.body.id;
            let oldCivilianLicence = await DriverCivilianLicence.findOne({ where: { id: id } })
            await DriverCivilianLicence.update({ status: 'Deleted' }, { where: { id: id } });
            let newCivilianLicence = await DriverCivilianLicence.findOne({ where: { id: id } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Civilian Licence',
                businessId: newCivilianLicence.driverId,
                optType: 'Delete',
                beforeData: `${ JSON.stringify(oldCivilianLicence) }`,
                afterData: `${ JSON.stringify(newCivilianLicence) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'delete the civilian licence.'
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}