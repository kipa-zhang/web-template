const log = require('../log/winston').logger('Task Service');

const moment = require('moment');

const utils = require('../util/utils');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const { sequelizeSystemObj } = require('../db/dbConf_system');

const { User } = require('../model/user.js');
const { Vehicle } = require('../model/vehicle.js');
const { Driver } = require('../model/driver.js');
const { Unit } = require('../model/unit.js');
const { PurposeMode } = require('../model/purposeMode.js');
const { MobileTrip } = require('../model/mobileTrip.js');
const { Task } = require('../model/task.js');
const { CheckList } = require('../model/checkList');
const { ODD } = require('../model/odd.js');
const { MT_RAC } = require('../model/mtRAC');
const { Mileage } = require('../model/mileage.js');
const { DriverPlatformConf } = require('../model/driverPlatformConf.js');

const _SystemLocation = require('../model/system/location');

const mobileTOService = require('../service/mobileTOService');
const { UserUtils } = require('../service/userService');

const CHECKLIST = {
    "1": "Route Familiarisation",
    "2": "Force Preparation",
    "3": "Vehicle Check",
    "4": "Just-In-Time Training",
    "5": "MT-RAC",
}

module.exports = {
    getTripIndents: async function (req, res) {
        try {
            let { userId } = req.body;
            let user = await UserUtils.getUserDetailInfo2(userId)
            if (!user) {
                throw new Error(`UserID => ${ userId } do not exist!`)
            }
            if (!['dv', 'loa'].includes(user.role.toLowerCase())) {
                throw new Error(`UserID => ${ userId } role is not in ['DV', 'LOA'] !`)
            }

            let result = await mobileTOService.GETDriverTaskList(user.driverId)
			log.info(`getTripIndents API Data => ${ JSON.stringify(result) }`)

			if (result.code === 1) {
				for (let indent of result.data.indents) {
					for (let task of indent.dataList) {
						let odd = await ODD.findOne({ where: { taskId: task.taskId }, order: [ [ 'updatedAt', 'DESC' ] ]})
						task.odd = odd ? odd.content : '';

						// let assignedTask = await Task.findOne({ where: { taskId: task.taskId }});
						let mtRAC = await MT_RAC.findOne({ where: { taskId: task.taskId }, order: [ [ 'id', 'DESC' ] ] })
						task.commanderContact = (mtRAC && mtRAC.needCommander) ? { username: mtRAC.commander, contact: mtRAC.commanderContactNumber, hub: task.hub, node: task.node } : null;

						let mileage = await Mileage.findByPk(task.taskId)
						if (mileage) {
							task.startMileage = mileage.startMileage;
							task.endMileage = mileage.endMileage;
						} else {
							let vehicle = await Vehicle.findByPk(task.vehicleNumber)
							task.startMileage = vehicle ? vehicle.totalMileage : 0;
							task.endMileage = 0;
						}

						let checkList = await CheckList.findAll({ where: { taskId: task.taskId } })
						let checkList1 = checkList.find(item => item.checkListName == CHECKLIST[1])
						let checkList2 = checkList.find(item => item.checkListName == CHECKLIST[2])
						let checkList4 = checkList.find(item => item.checkListName == CHECKLIST[4])
						let checkList5 = checkList.find(item => item.checkListName == CHECKLIST[5])

                        task.taskReady = false;
                        if (checkList1 && checkList2 && checkList4) {
                            if (task.purpose && task.purpose.toLowerCase() == 'wpt') {
                                task.taskReady = true;
                            } else if (checkList5) {
                                task.taskReady = true;
                            }
                        } 
					}
				}
				log.info(`result.data => `, JSON.stringify(result.data))
				return res.json(utils.response(1, result.data));
			} else {
				return res.json(utils.response(0, result.msg));
			}
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    cancelTripById: async function (req, res) {
        try {
            let { taskId } = req.body;
            let userId = req.cookies.userId;
            let tripId = taskId ? taskId.replace('CU-M-', '') : '';

            let mobileTrip = await MobileTrip.findByPk(tripId);
            if (!mobileTrip) {
                throw `Trip:${ tripId } do not exist!`
            }

            let task = await Task.findByPk(taskId);
            if (!task || [ 'waitcheck', 'ready'].indexOf(task.driverStatus) > -1) {
                await MobileTrip.update({ status: 'Cancelled', cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), cancelledBy: userId}, { where: { id: tripId } } );
                if (task) {
                    await Task.update({ driverStatus: 'Cancelled', vehicleStatus: 'Cancelled' }, { where: { taskId, dataFrom: 'MOBILE' } } )
                }
            } else {
                throw `Task:${ taskId } is ${ task.driverStatus }, can not cancel.`
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
    getVehicle: async function (req, res) {
        try {
            let { userId, startDate, endDate } = req.body;
            if (!startDate || !endDate) {
                return res.json(utils.response(1, []));
            }

            let user = await User.findByPk(userId);

            if (!user) {
                let msg = `User id ${userId} not exist!`
                log.warn(msg)
                return res.json(utils.response(0, msg));
            }
            let driver = await Driver.findByPk(user.driverId);

            let loanOutDriver = await sequelizeObj.query(`
                select l.driverId, l.groupId from loan l where l.driverId = ${user.driverId} and now() >= l.startDate
            `, { type: QueryTypes.SELECT });

            if (user.role != 'DV' && user.role != 'LOA' && loanOutDriver.length == 0) {
                let msg = `User ${user.username} role is not DV,LOA or loan out driver!`
                log.warn(msg)
                return res.json(utils.response(0, msg));
            }

            // driver support vehicle type
            let driverSupportVehicleTypes = await sequelizeObj.query(`
                SELECT GROUP_CONCAT(vehicleType) as vehicleTypes FROM driver_platform_conf where driverId=${user.driverId} and approveStatus='Approved'
            `, { type: QueryTypes.SELECT });
            let driverSupportVehicleTypeStr = '';
            if (driverSupportVehicleTypes && driverSupportVehicleTypes.length > 0) {
                driverSupportVehicleTypeStr =  driverSupportVehicleTypes[0].vehicleTypes;
            }

            if (!driverSupportVehicleTypeStr) {
                log.warn('Current driver unconfig support platform.');
                return res.json(utils.response(1, []));
            }

            let vehicleList = [];
            if (driver.groupId || loanOutDriver.length > 0) {
                let groupId = driver.groupId ? driver.groupId : loanOutDriver[0].groupId;
                vehicleList = await getUsableVehicleByGroup(groupId, startDate, endDate, driverSupportVehicleTypeStr);
            } else {
                let unitId = driver.unitId;
                vehicleList = await getUsableVehicleByUnit(unitId, startDate, endDate, driverSupportVehicleTypeStr);
            }
            let result = []
            if (vehicleList && vehicleList.length > 0) {
                let vehicleNumberList = [...new Set(vehicleList.map(a => a.vehicleNo))]
                for (let vehicleNumber of vehicleNumberList) {
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
    addTrip: async function (req, res) {
        try {
            let {
                purpose,
                vehicle,
                reportingLocation,
                reportingLocationLng,
                reportingLocationLat,
                destination,
                destinationLng,
                destinationLat,
                startDatetime,
                endDatetime,
                userId
            } = req.body
            let user = await User.findByPk(userId)
            if (startDatetime) {
                startDatetime = moment(startDatetime).format('YYYY-MM-DD HH:mm:ss')
            }
            if (endDatetime) {
                endDatetime = moment(endDatetime).format('YYYY-MM-DD HH:mm:ss')
            }

            if (!user) {
                let msg = `User id ${userId} not exist!`
                log.warn(msg)
                return res.json(utils.response(0, msg));
            }

            let vehicleData = await Vehicle.findByPk(vehicle)
            if (!vehicleData) {
                return res.json(utils.response(0, `Vehicle ${vehicle} not exist`));
            }

            let loanOutDriver = await sequelizeObj.query(`
                select l.driverId, l.groupId from loan l where l.driverId = ${user.driverId} and now() >= l.startDate
            `, { type: QueryTypes.SELECT });
            if (user.role != 'DV' && user.role != 'LOA' && loanOutDriver.length == 0) {
                let msg = `User ${user.username} role is not DV,LOA or loan out driver!`
                log.warn(msg)
                return res.json(utils.response(0, msg));
            }

            let driverId = user.driverId

            let unitId = null;
            let groupId = null;
            let hub = null;
            let node = null;
            let driver = await Driver.findByPk(driverId);
            if (driver.groupId || loanOutDriver.length > 0) {
                groupId = driver.groupId ? driver.groupId : loanOutDriver[0].groupId;
                hub = '-';
                node = '-';
            } else if (driver.unitId) {
                unitId = driver.unitId;
                let unit = await Unit.findByPk(unitId);
                if (unit) {
                    hub = unit.unit;
                    node = unit.subUnit
                }
            }

            let pickupGPS = reportingLocationLat + "," + reportingLocationLng
            let dropoffGPS = destinationLat + "," + destinationLng

            await sequelizeObj.transaction(async (t1) => {
                let trip = await MobileTrip.create({
                    driverId: driverId,
                    indentStartTime: startDatetime,
                    indentEndTime: endDatetime,
                    purpose: purpose,
                    dataFrom: "MOBILE",
                    vehicleNumber: vehicle,
                    pickupDestination: reportingLocation,
                    dropoffDestination: destination,
                    pickupGPS: pickupGPS,
                    dropoffGPS: dropoffGPS,
                    creator: userId,
                    status: user.role == 'DV' ? 'Pending Approval' : 'Approved',
                    groupId: groupId,
                    unitId: unitId
                })

                // dv trip need approve
                if (user.role != 'DV') {
                    let taskId = "CU-M-" + trip.id
                    await Task.create({
                        taskId: taskId,
                        dataFrom: "MOBILE",
                        driverId: driverId,
                        indentId: trip.id,
                        vehicleNumber: vehicle,
                        driverStatus: 'waitcheck',
                        vehicleStatus: 'waitcheck',
                        indentStartTime: startDatetime,
                        indentEndTime: endDatetime,
                        purpose: purpose,
                        pickupDestination: reportingLocation,
                        dropoffDestination: destination,
                        pickupGPS: pickupGPS,
                        dropoffGPS: dropoffGPS,
                        groupId: groupId,
                        creator: userId,
                        hub: hub,
                        node: node
                    })
                }
            })
            return res.json(utils.response(1, true));
        } catch (ex) {
            log.error(ex)
            return res.json(utils.response(0, "Add Trip Failed"));
        }
    }
}

const getUsableVehicleByGroup = async function(groupId, startDate, endDate, driverSupportVehicleTypeStr) {
    try {
        startDate = startDate ? moment(startDate).format('YYYY-MM-DD HH:mm:ss') : null;
        endDate = endDate ? moment(endDate).format('YYYY-MM-DD HH:mm:ss') : null;

        // event vehicle on task cycle
        let eventVehicles = await sequelizeObj.query(`
            select vl.vehicleNo from vehicle_leave_record vl 
            where vl.status = 1 
            AND ( ('${ startDate }' >= vl.startTime AND '${ startDate }' <= vl.endTime) 
            OR ('${ endDate }' >= vl.startTime AND '${ endDate }' <= vl.endTime) 
            OR ('${ startDate }' < vl.startTime AND '${ endDate }' > vl.endTime))
            GROUP BY vl.vehicleNo
        `, { type: QueryTypes.SELECT });
        let eventVehicleNumbers = eventVehicles.map(a => a.vehicleNo);

        let sql = `
            SELECT vv.vehicleNo, vv.groupId, vv.vehicleType, vv.nextAviTime FROM (
                SELECT v.vehicleType, v.nextAviTime,
                    v.vehicleNo,
                    IF(l.groupId IS NULL, v.groupId, l.groupId) AS groupId
                FROM vehicle v 
                LEFT JOIN (SELECT lo.vehicleNo, lo.groupId FROM loan lo WHERE '${ startDate }' BETWEEN lo.startDate AND lo.endDate) l ON l.vehicleNo = v.vehicleNo
            ) vv 
            WHERE vv.vehicleNo IS NOT NULL and vv.groupId = ${ groupId } and FIND_IN_SET(vv.vehicleType, '${driverSupportVehicleTypeStr}')
            AND (vv.nextAviTime IS NULL OR vv.nextAviTime > '${ moment(endDate).format('YYYY-MM-DD') }')
            ${ eventVehicleNumbers.length > 0 ? ` and vv.vehicleNo not in (?) ` : '' }   
        `;
        let replacements = [];
        if (eventVehicleNumbers.length > 0) {
            replacements.push(eventVehicleNumbers);
        }
        console.log(sql);
        return await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
    } catch (error) {
        log.error(error);
        return [];
    }
    
}

const getUsableVehicleByUnit = async function(unitId, startDate, endDate, driverSupportVehicleTypeStr) {
    let unitIdList = []
    if (unitId) {
        let unit = await Unit.findByPk(unitId);
        if (unit && !unit.subUnit) {
            let newUnit = await Unit.findAll({ where: { unit: unit.unit } })
            unitIdList = newUnit.map(item => item.id)
        } else if (unit && unit.subUnit) {
            unitIdList.push(unitId)
        }
    }
    log.info(`getUsableVehicleByUnit unitIDList ${ JSON.stringify(unitIdList) }`);

    try {
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss');
        endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss');

        // event vehicle on task cycle
        let eventVehicles = await sequelizeObj.query(`
            select vl.vehicleNo from vehicle_leave_record vl 
            where vl.status = 1 
            AND ( ('${ startDate }' >= vl.startTime AND '${ startDate }' <= vl.endTime) 
            OR ('${ endDate }' >= vl.startTime AND '${ endDate }' <= vl.endTime) 
            OR ('${ startDate }' < vl.startTime AND '${ endDate }' > vl.endTime))
            GROUP BY vl.vehicleNo
        `, { type: QueryTypes.SELECT });
        let eventVehicleNumbers = eventVehicles.map(a => a.vehicleNo);

        let loanOutVehicle = await sequelizeObj.query(`
            SELECT vehicleNo FROM loan 
            WHERE (('${ startDate }' >= startDate AND '${ startDate }' <= endDate) 
            OR ('${ endDate }' >= startDate AND '${ endDate }' <= endDate) 
            OR ('${ startDate }' < startDate AND '${ endDate }' > endDate))
            and vehicleNo is not null
            group by vehicleNo
        `, { type: QueryTypes.SELECT })

        let loanOutVehicleNos = loanOutVehicle.map(item => item.vehicleNo)
        log.warn(`getUsableVehicleByUnit loan out vehicleList ${ JSON.stringify(loanOutVehicleNos) }`)

        // Not within the specified range hoto vehicle
        let hotoVehicleListByNotScope = await sequelizeObj.query(`
            select vehicleNo, startDateTime, endDateTime
            from hoto 
            where status = 'Approved' and vehicleNo is not null and (
                ('${ startDate }' >= startDateTime AND '${ startDate }' <= endDateTime) 
                OR ('${ endDate }' >= startDateTime AND '${ endDate }' <= endDateTime) 
                OR ('${ startDate }' < startDateTime AND '${ endDate }' > endDateTime)
            )
            and vehicleNo not in (select vehicleNo from hoto 
                where '${ startDate }' >= startDateTime AND '${ endDate }' <= endDateTime 
                and vehicleNo is not null  and status = 'Approved'
            ) 
            group by vehicleNo
        `, { type: QueryTypes.SELECT });
        hotoVehicleListByNotScope = hotoVehicleListByNotScope.map(item => item.vehicleNo)
        log.warn(`getUsableVehicleByUnit hoto Not within the specified range vehicleList ${ JSON.stringify(hotoVehicleListByNotScope) }`)

        let hotoVehicle = []
        let excludeVehicle = loanOutVehicle.concat(hotoVehicle).concat(hotoVehicleListByNotScope).concat(eventVehicleNumbers);
        excludeVehicle = excludeVehicle.map(item => item);
        excludeVehicle = Array.from(new Set(excludeVehicle))  
        log.warn(`getUsableVehicleByUnit Need to exclude the vehicle ${ JSON.stringify(excludeVehicle) }`)

        let sql = `
            select vv.vehicleNo, vv.unitId  from (
                SELECT a.vehicleNo, IF(h.unitId is NULL, a.unitId, h.unitId) as unitId, a.groupId 
                FROM vehicle a
                left join (
                    select ho.vehicleNo, ho.unitId from hoto ho where (('${ startDate }' >= ho.startDateTime AND '${ endDate }' <= ho.endDateTime)) and ho.status = 'Approved'
                ) h ON h.vehicleNo = a.vehicleNo 
                where a.groupId is null and FIND_IN_SET(a.vehicleType, '${driverSupportVehicleTypeStr}')
                AND (a.nextAviTime IS NULL OR a.nextAviTime > '${ moment(endDate).format('YYYY-MM-DD') }')
            ) vv 
            where vv.unitId is not null and vv.unitId in (${ unitIdList }) 
            ${ excludeVehicle.length > 0 ? ` and vv.vehicleNo not in (?) ` : '' }
            GROUP BY vv.vehicleNo
        `;
        let replacements = [];
        if (excludeVehicle.length > 0) {
            replacements.push(excludeVehicle);
        }
        console.log(sql)
        return await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
    } catch(error) {
        log.error(error);
        return [];
    }
}