const log = require('../log/winston').logger('Track Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const hubNodeConf = require('../conf/hubNodeConf');
const CONTENT = require('../util/content');
const SOCKET = require('../socket/socket');
const jsonfile = require('jsonfile')

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { Route } = require('../model/route');
const { RouteWaypoint } = require('../model/routeWaypoint');
const { Driver } = require('../model/driver');
const { Vehicle } = require('../model/vehicle');
const { DriverTask } = require('../model/driverTask');
const { VehicleRelation } = require('../model/vehicleRelation.js');
const { User } = require('../model/user.js');
const { UserGroup } = require('../model/userGroup.js');
const { Unit } = require('../model/unit.js');
const { Friend } = require('../model/friend.js');

const { TrackHistory } = require('../model/event/trackHistory.js');
const { DevicePositionHistory } = require('../model/event/devicePositionHistory.js');
const { DriverPositionHistory } = require('../model/event/driverPositionHistory.js');
const { DriverOffenceHistory } = require('../model/event/driverOffenceHistory.js');
const { DeviceOffenceHistory } = require('../model/event/deviceOffenceHistory.js');

const groupService = require('./groupService');
const userService = require('./userService');
const unitService = require('./unitService');
const { Device } = require('../model/device.js');
const { DriverPosition } = require('../model/driverPosition.js');

const outputService = require('./outputService');
const mtAdminService = require('./mtAdminService');
const fs = require('graceful-fs');

module.exports.getDriverAndDeviceList2 = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
				throw Error(`User ${ userId } does not exist.`);
			}
			return user;
		}

        const getAvailableDriverAndDeviceList = async function (selectDate) {
            try {
                let availableDeviceIdList = [], availableDriverIdList = []
                let pattern = new RegExp(/^\d*$/)
                let deviceList = fs.readdirSync(conf.dataPath);
                for (let device of deviceList) {
                    let result = fs.existsSync(`${ conf.dataPath }/${ device }/${ moment(selectDate, 'YYYY-MM-DD').format('YYYYMMDD') }.txt`)
                    // device is driverId OR obd deviceId
                    if (result) {
                        if (pattern.test(device)) {
                            availableDriverIdList.push(Number.parseInt(device))
                        } else {
                            availableDeviceIdList.push(device)
                        }
                    }
                }
                return { availableDeviceIdList, availableDriverIdList }
            } catch (error) {
                log.error(error)
                return []
            }
        }

        const getDriverList = async function (user, selectedDate, groupDriver, availableDriverIdList) {
            let driverList = []

            let replacements = []
            let dateSelectSql = `NOW()`;
            if(selectedDate){
                dateSelectSql = '?'
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
            }
            let baseSqL = `
                SELECT dp.driverId, dp.vehicleNo, dp.lat, dp.lng, dp.speed, dp.state, dp.updatedAt, d.driverName, vr.limitSpeed, 
                
                hh.id AS hotoId, hh2.id AS hoto2Id,
                IF(hh.id IS NOT NULL, hh.unitId, IF(hh2.id IS NOT NULL, hh2.unitId, u.id)) AS unitId,
                IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, u.unit)) AS hub,
                IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, u.subUnit)) AS node,
                ll.id AS loanId,
                IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.groupId, NULL))) AS groupId

                FROM driver_position dp
                LEFT JOIN (
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver 
                    UNION ALL 
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) d ON d.driverId = dp.driverId 
                LEFT JOIN user us ON us.driverId = dp.driverId 
                LEFT JOIN vehicle_relation vr ON vr.driverId = dp.driverId AND vr.vehicleNo = dp.vehicleNo
                
                LEFT JOIN unit u on u.id = dp.unitId
                LEFT JOIN (
                    SELECT ho.id, ho.driverId, ho.unitId, ho.toHub, ho.toNode 
                    FROM hoto ho 
                    WHERE ${ dateSelectSql } BETWEEN ho.startDateTime AND ho.endDateTime
                    and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                LEFT JOIN (
                    SELECT ho2.id, ho2.driverId, u.id AS unitId, ho2.toHub, ho2.toNode 
                    FROM hoto_record ho2 
                    LEFT JOIN unit u ON u.unit = ho2.toHub AND u.subUnit = ho2.toNode
                    WHERE ${ dateSelectSql } BETWEEN ho2.startDateTime AND ho2.returnDateTime
                    and ho2.status = 'Approved'
                ) hh2 ON hh2.driverId = d.driverId
                LEFT JOIN (
                    SELECT lo.id, lo.driverId, lo.groupId 
                    FROM loan lo 
                    WHERE ${ dateSelectSql } BETWEEN lo.startDate AND lo.endDate
                ) ll ON ll.driverId = d.driverId
                LEFT JOIN (
                    SELECT lo2.id, lo2.driverId, lo2.groupId 
                    FROM loan_record lo2 
                    WHERE ${ dateSelectSql } BETWEEN lo2.startDate AND lo2.returnDate
                ) ll2 ON ll2.driverId = d.driverId
                
                WHERE dp.lat != 0 and dp.lng != 0
            `
            if(availableDriverIdList.length){
                baseSqL += ` and dp.driverId in (?)`
                replacements.push(availableDriverIdList.join(','))
            } else {
                baseSqL += ` and 1=2`
            }
            let sql = ` SELECT * FROM ( ${ baseSqL } ) tt WHERE 1=1 `

            if (user.userType === CONTENT.USER_TYPE.HQ) {
                let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId);
                
                let tempSqlList = []
                if (unitIdList.length) {
                    tempSqlList.push(` ( tt.unitId IN ( ? ) AND tt.groupId IS NULL ) `) 
                    replacements.push(unitIdList.map(item => `'${ item }'`).join(','))
                }
                if (groupIdList.length) {
                    tempSqlList.push(` tt.groupId in (?) `) 
                    replacements.push(groupIdList.map(item => `'${ item }'`).join(','))
                }
                
                if (tempSqlList.length) {
                    sql += ` and (${ tempSqlList.join(' OR ') }) `
                } else {
                    sql += ` AND 1 = 2 `;
                }
            } else if (user.userType === CONTENT.USER_TYPE.UNIT) {
                let { unitIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId);
                if (unitIdList.length) {
                    sql += ` AND ( tt.unitId IN ( ? ) AND tt.groupId IS NULL ) `
                    replacements.push(unitIdList.map(item => `'${ item }'`).join(','))
                } else {
                    sql += ` AND 1 = 2 `;
                }
            } else if (user.userType === CONTENT.USER_TYPE.CUSTOMER) {
                sql += ` AND tt.groupId = ? `
                replacements.push(user.unitId)
            }

            if (groupDriver) {
                sql += ` GROUP BY tt.driverId `
            } else {
                sql += ` GROUP BY tt.driverId, tt.vehicleNo `
            }
            log.info(sql)
            driverList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })

            let hubConf = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
            for (let driver of driverList) {
                for (let hubNode of hubConf) {
                    if (driver.hub?.toLowerCase() == hubNode.hub?.toLowerCase()) {
                        driver.circleColor = hubNode.color
                    }
                }
                if (!driver.circleColor) {
                    driver.circleColor = hubNodeConf.defaultColor
                }
            }

            return driverList;
        }
        const getDeviceList = async function (user, selectedDate, availableDeviceIdList) {
            let deviceList = [];
            
            let replacements = []
            let selectedDateSql = `NOW()`
            if(selectedDate){
                selectedDateSql = ` ? `
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
            }
            let baseSqL = ` 
                SELECT d.deviceId, v.vehicleNo, d.speed, d.lat, d.lng, v.limitSpeed, d.updatedAt, 
                
                hh.id AS hotoId, 
                IF(hh.id IS NOT NULL, hh.unitId, IF(hh2.id IS NOT NULL, hh2.unitId, u.id)) AS unitId,
                IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, u.unit)) AS hub,
                IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, u.subUnit)) AS node,
                ll.id AS loanId,
                IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, v.groupId)) AS groupId

                FROM device d
                LEFT JOIN (
                    select vehicleNo, unitId, groupId, vehicleType, limitSpeed, deviceId FROM vehicle 
                    UNION ALL 
                    select vehicleNo, unitId, groupId, vehicleType, limitSpeed, deviceId FROM vehicle_history
                ) v ON v.deviceId = d.deviceId
                LEFT JOIN unit u ON u.id = v.unitId

                LEFT JOIN (
                    SELECT ho.id, ho.vehicleNo, ho.unitId, ho.toHub, ho.toNode 
                    FROM hoto ho 
                    WHERE ${ selectedDateSql }  BETWEEN ho.startDateTime AND ho.endDateTime
                    and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = v.vehicleNo
                LEFT JOIN (
                    SELECT ho2.id, ho2.vehicleNo, u.id AS unitId, ho2.toHub, ho2.toNode 
                    FROM hoto_record ho2 
                    LEFT JOIN unit u ON u.unit = ho2.toHub AND u.subUnit = ho2.toNode
                    WHERE ${ selectedDateSql }  BETWEEN ho2.startDateTime AND ho2.returnDateTime
                    and ho2.status = 'Approved'
                ) hh2 ON hh2.vehicleNo = v.vehicleNo
                LEFT JOIN (
                    SELECT lo.id, lo.vehicleNo, lo.groupId 
                    FROM loan lo 
                    WHERE ${ selectedDateSql }  BETWEEN lo.startDate AND lo.endDate 
                ) ll ON ll.vehicleNo = v.vehicleNo
                LEFT JOIN (
                    SELECT lo2.id, lo2.vehicleNo, lo2.groupId 
                    FROM loan_record lo2 
                    WHERE ${ selectedDateSql }  BETWEEN lo2.startDate AND lo2.returnDate
                ) ll2 ON ll2.vehicleNo = v.vehicleNo

                WHERE 1=1 AND d.lat != 0 and d.lng != 0
            `
            if(availableDeviceIdList.length){
                baseSqL += ` and d.deviceId in (?)`
                replacements.push(availableDeviceIdList.map(item => `'${ item }'`).join(','))
            } else {
                baseSqL += ` and 1=2`
            }
            let sql = ` SELECT * FROM ( ${ baseSqL } ) vv WHERE 1=1 `

            if (user.userType === CONTENT.USER_TYPE.HQ) {
                let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId);
                
                let tempSqlList = []
                if (unitIdList.length) {
                    tempSqlList.push(` ( vv.unitId IN ( ? ) AND vv.groupId IS NULL ) `) 
                    replacements.push(unitIdList.map(item => `'${ item }'`).join(','))
                }
                if (groupIdList.length) {
                    tempSqlList.push(` vv.groupId in (?) `) 
                    replacements.push(groupIdList.map(item => `'${ item }'`).join(','))
                }
                
                if (tempSqlList.length) {
                    sql += ` and (${ tempSqlList.join(' OR ') }) `
                } else {
                    sql += ` AND 1 = 2 `;
                }
            } else if (user.userType === CONTENT.USER_TYPE.UNIT) {
                let unitIdList = await unitService.getUnitPermissionIdList(user);
                if (unitIdList.length) {
                    sql += ` AND ( vv.unitId IN ( ? ) AND vv.groupId IS NULL ) `
                    replacements.push(unitIdList.map(item => `'${ item }'`).join(','))
                } else {
                    sql += ` AND 1 = 2 `
                }
            } else if (user.userType === CONTENT.USER_TYPE.CUSTOMER) {
                sql += ` AND vv.groupId = ? `
                replacements.push(user.unitId)
            }
            sql += ` group by d.deviceId `
            log.info(sql)
			deviceList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })	
            
            let hubConf = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
			for (let device of deviceList) {
                for (let hubNode of hubConf) {
                    if (device.hub?.toLowerCase() == hubNode.hub?.toLowerCase()) {
                        device.circleColor = hubNode.color
                    }
                }
                if (!device.circleColor) {
                    device.circleColor = hubNodeConf.defaultColor
                }
            }

            return deviceList;
        }

        const checkTimeIfMissing = function (time) {
            let flag = conf.VehicleMissingFrequency;
            flag = flag ? flag : 0;
            flag = Number.parseInt(flag);
            return moment().diff(moment(time), 'm') > flag;
        }

        let user = await checkUser(req.cookies.userId);
        let selectedDate = req.body.selectedDate;
        let groupDriver = req.body.groupDriver;
        let { availableDeviceIdList, availableDriverIdList } = await getAvailableDriverAndDeviceList(selectedDate);
        let driverList = await getDriverList(user, selectedDate, groupDriver, availableDriverIdList);
        let deviceList = await getDeviceList(user, selectedDate, availableDeviceIdList);
        let result = [];
        for (let driver of driverList) {
            result.push({ missing: checkTimeIfMissing(driver.updatedAt), type: 'mobile', ...driver  })
        }
        for (let device of deviceList) {
            result.push({ missing: checkTimeIfMissing(device.updatedAt), type: 'obd', ...device  })
        }
        return res.json(utils.response(1, result));    
    } catch (error) {
        log.error('(getVehicleRelationList) : ', error);
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverAndDeviceList = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
				throw Error(`User ${ userId } does not exist.`);
			}
			return user;
		}

        const getDriverList = async function (user, selectedDate, groupDriver) {
            let driverList = []
            
            let replacements = []
            let selectedDateDriverSql = 'NOW()'
            if(selectedDate){
                selectedDateDriverSql = ` ? `
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
            }
            let baseSqL = `
                SELECT dp.driverId, dp.vehicleNo, dp.lat, dp.lng, dp.speed, dp.state, dp.updatedAt, d.driverName, vr.limitSpeed, 
                
                hh.id AS hotoId, hh2.id AS hoto2Id,
                IF(hh.id IS NOT NULL, hh.unitId, IF(hh2.id IS NOT NULL, hh2.unitId, u.id)) AS unitId,
                IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, u.unit)) AS hub,
                IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, u.subUnit)) AS node,
                ll.id AS loanId,
                IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.groupId, NULL))) AS groupId

                FROM driver_position dp
                LEFT JOIN (
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver 
                    UNION ALL 
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) d ON d.driverId = dp.driverId 
                LEFT JOIN user us ON us.driverId = dp.driverId 
                LEFT JOIN vehicle_relation vr ON vr.driverId = dp.driverId AND vr.vehicleNo = dp.vehicleNo
                
                LEFT JOIN unit u on u.id = dp.unitId
                LEFT JOIN (
                    SELECT ho.id, ho.driverId, ho.unitId, ho.toHub, ho.toNode 
                    FROM hoto ho 
                    WHERE ${ selectedDateDriverSql } BETWEEN ho.startDateTime AND ho.endDateTime
                    and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                LEFT JOIN (
                    SELECT ho2.id, ho2.driverId, u.id AS unitId, ho2.toHub, ho2.toNode 
                    FROM hoto_record ho2 
                    LEFT JOIN unit u ON u.unit = ho2.toHub AND u.subUnit = ho2.toNode
                    WHERE ${ selectedDateDriverSql } BETWEEN ho2.startDateTime AND ho2.returnDateTime
                    and ho2.status = 'Approved'
                ) hh2 ON hh2.driverId = d.driverId
                LEFT JOIN (
                    SELECT lo.id, lo.driverId, lo.groupId 
                    FROM loan lo 
                    WHERE ${ selectedDateDriverSql } BETWEEN lo.startDate AND lo.endDate
                ) ll ON ll.driverId = d.driverId
                LEFT JOIN (
                    SELECT lo2.id, lo2.driverId, lo2.groupId 
                    FROM loan_record lo2 
                    WHERE ${ selectedDateDriverSql } BETWEEN lo2.startDate AND lo2.returnDate
                ) ll2 ON ll2.driverId = d.driverId
                
                WHERE dp.lat != 0 and dp.lng != 0
            `
            let sql = ` SELECT * FROM ( ${ baseSqL } ) tt WHERE 1=1 `
            if (selectedDate) {
                sql += ` AND tt.updatedAt LIKE ? `
                replacements.push(selectedDate+'%')
            }
            if (user.userType === CONTENT.USER_TYPE.UNIT) {
                let { unitIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId);
                if (unitIdList.length) {
                    sql += ` AND ( tt.unitId IN ( ? ) AND tt.groupId IS NULL ) `
                    replacements.push(unitIdList.map(item => `'${ item }'`).join(','))
                } else {
                    sql += ` AND 1 = 2 `;
                }
            } else if (user.userType === CONTENT.USER_TYPE.CUSTOMER) {
                sql += ` AND tt.groupId = ? `
                replacements.push(user.unitId)
            }
            log.info(sql)

            if (groupDriver) {
                sql += ` GROUP BY tt.driverId `
            } else {
                sql += ` GROUP BY tt.driverId, tt.vehicleNo `
            }
            driverList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })

            let hubConf = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
            for (let driver of driverList) {
                for (let hubNode of hubConf) {
                    if (driver.hub?.toLowerCase() == hubNode.hub?.toLowerCase()) {
                        driver.circleColor = hubNode.color
                    }
                }
                if (!driver.circleColor) {
                    driver.circleColor = hubNodeConf.defaultColor
                }
            }

            return driverList;
        }
        const getDeviceList = async function (user, selectedDate) {
            let deviceList = [];
            let replacements = [];
            let selectedDateDeviceSql = 'NOW()'
            if(selectedDate){
                selectedDateDeviceSql = ' ? '
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
                replacements.push(selectedDate)
            }
            let baseSqL = ` 
                SELECT d.deviceId, v.vehicleNo, d.speed, d.lat, d.lng, v.limitSpeed, d.updatedAt, 
                
                hh.id AS hotoId, 
                IF(hh.id IS NOT NULL, hh.unitId, IF(hh2.id IS NOT NULL, hh2.unitId, u.id)) AS unitId,
                IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, u.unit)) AS hub,
                IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, u.subUnit)) AS node,
                ll.id AS loanId,
                IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, v.groupId)) AS groupId

                FROM device d
                LEFT JOIN (
                    select vehicleNo, unitId, groupId, vehicleType, limitSpeed, deviceId FROM vehicle 
                    UNION ALL 
                    select vehicleNo, unitId, groupId, vehicleType, limitSpeed, deviceId FROM vehicle_history
                ) v ON v.deviceId = d.deviceId
                LEFT JOIN unit u ON u.id = v.unitId

                LEFT JOIN (
                    SELECT ho.id, ho.vehicleNo, ho.unitId, ho.toHub, ho.toNode 
                    FROM hoto ho 
                    WHERE ${ selectedDateDeviceSql }  BETWEEN ho.startDateTime AND ho.endDateTime
                    and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = v.vehicleNo
                LEFT JOIN (
                    SELECT ho2.id, ho2.vehicleNo, u.id AS unitId, ho2.toHub, ho2.toNode 
                    FROM hoto_record ho2 
                    LEFT JOIN unit u ON u.unit = ho2.toHub AND u.subUnit = ho2.toNode
                    WHERE ${ selectedDateDeviceSql }  BETWEEN ho2.startDateTime AND ho2.returnDateTime
                    and ho2.status = 'Approved'
                ) hh2 ON hh2.vehicleNo = v.vehicleNo
                LEFT JOIN (
                    SELECT lo.id, lo.vehicleNo, lo.groupId 
                    FROM loan lo 
                    WHERE ${ selectedDateDeviceSql }  BETWEEN lo.startDate AND lo.endDate 
                ) ll ON ll.vehicleNo = v.vehicleNo
                LEFT JOIN (
                    SELECT lo2.id, lo2.vehicleNo, lo2.groupId 
                    FROM loan_record lo2 
                    WHERE ${ selectedDateDeviceSql }  BETWEEN lo2.startDate AND lo2.returnDate
                ) ll2 ON ll2.vehicleNo = v.vehicleNo

                WHERE 1=1 AND d.lat != 0 and d.lng != 0
            `

            let sql = ` SELECT * FROM ( ${ baseSqL } ) vv WHERE 1=1 `

            if (selectedDate) {
                sql += ` AND vv.updatedAt LIKE ? `
                replacements.push(selectedDate+'%')
            }
            if (user.userType === CONTENT.USER_TYPE.HQ || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
                
            } else if (user.userType === CONTENT.USER_TYPE.UNIT) {
                let unitIdList = await unitService.getUnitPermissionIdList(user);
                if (unitIdList.length) {
                    sql += ` AND ( vv.unitId IN ( ? ) AND vv.groupId IS NULL ) `
                    replacements.push(unitIdList.map(item => `'${ item }'`).join(','))
                } else {
                    sql += ` AND 1 = 2 `
                }
            } else if (user.userType === CONTENT.USER_TYPE.CUSTOMER) {
                sql += ` AND vv.groupId = ? `
                replacements.push(user.unitId)
            }
            sql += ` group by d.deviceId `
            log.info(sql)
			deviceList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })	
            
            let hubConf = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
			for (let device of deviceList) {
                for (let hubNode of hubConf) {
                    if (device.hub?.toLowerCase() == hubNode.hub?.toLowerCase()) {
                        device.circleColor = hubNode.color
                    }
                }
                if (!device.circleColor) {
                    device.circleColor = hubNodeConf.defaultColor
                }
            }

            return deviceList;
        }

        const checkTimeIfMissing = function (time) {
            let flag = conf.VehicleMissingFrequency;
            flag = flag ? flag : 0;
            flag = Number.parseInt(flag);
            return moment().diff(moment(time), 'm') > flag;
        }

        let user = await checkUser(req.cookies.userId);
        let selectedDate = req.body.selectedDate;
        let groupDriver = req.body.groupDriver
        let driverList = await getDriverList(user, selectedDate, groupDriver);
        let deviceList = await getDeviceList(user, selectedDate);
        let result = [];
        for (let driver of driverList) {
            result.push({ missing: checkTimeIfMissing(driver.updatedAt), type: 'mobile', ...driver  })
        }
        for (let device of deviceList) {
            result.push({ missing: checkTimeIfMissing(device.updatedAt), type: 'obd', ...device  })
        }
        return res.json(utils.response(1, result));    
    } catch (error) {
        log.error('(getVehicleRelationList) : ', error);
        return res.json(utils.response(0, error));
    }
};
module.exports.getDriverAndDevicePositionList = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
				throw Error(`User ${ userId } does not exist.`);
			}
			return user;
		}

        const getDriverPositionList = async function (driverList, startDateTime, endDateTime) {
            let driverPositionList = [];
            for (let driver of driverList) {
                let list = [];
                try {
                    list = await outputService.readFromFile(driver.driverId, null, [startDateTime, endDateTime])
                } catch (error) {
                    log.error(error);
                }
                
                let limitSpeed = 0;
                if (list.length) {
                    let vehicle = await Vehicle.findByPk(list[0].vehicleNo)
                    limitSpeed = vehicle.limitSpeed
                }

                driverPositionList.push({ username: driver.driverName, vehicleNo: null, type: 'mobile', limitSpeed, list })
            }
            
            return driverPositionList;
        }
        const getDevicePositionList = async function (deviceList, startDateTime, endDateTime) {
            let devicePositionList = [];
            for (let device of deviceList) {
                let list = [];
                try {
                    list = await outputService.readFromFile(device.deviceId, null, [startDateTime, endDateTime])
                } catch (error) {
                    log.error(error);
                }
                
                let deviceName = device.vehicleNo ? device.vehicleNo : device.deviceId
                
                let limitSpeed = 0;
                if (list.length) {
                    let vehicle = await Vehicle.findByPk(device.vehicleNo)
                    limitSpeed = vehicle.limitSpeed
                }

                devicePositionList.push({ username: deviceName, vehicleNo: device.vehicleNo, type: 'obd', limitSpeed, list })
            }
            return devicePositionList;
        }

        const generateDirection = function (list) {
            for (let data of list) {
                let preDirection = null;
                data.list.forEach(function(item, index) {
                    if (index !== data.list.length - 1) {
                        preDirection = Number.parseFloat(utils.getDirection([ item, data.list[index + 1] ]).toFixed(2))
                    }
                    item.direction = preDirection
                })
            }
            return list
        }

        await checkUser(req.cookies.userId);
        let driverList = req.body.driverList;
        let deviceList = req.body.deviceList;
        let selectedDate = req.body.selectedDate;
        let selectedTime = req.body.selectedTime;

        let startDateTime = `${ selectedDate } ${ selectedTime.split('-')[0].trim() }`
        let endDateTime = `${ selectedDate } ${ selectedTime.split('-')[1].trim() }`

        let driverPositionList = await getDriverPositionList(driverList, startDateTime, endDateTime);
        let devicePositionList = await getDevicePositionList(deviceList, startDateTime, endDateTime);

        let result = [].concat(devicePositionList, driverPositionList)
        result = generateDirection(result)

        return res.json(utils.response(1, result));    
    } catch (error) {
        log.error('(getVehicleRelationList) : ', error);
        return res.json(utils.response(0, error));
    }
};


module.exports.getTrackDashboardInfo = async function (req, res) {
    try {
        let userId = req.cookies.userId;

        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
				throw Error(`User ${ userId } does not exist.`);
			}
			return user;
		}

        let user = await checkUser(userId)
        let unitIdList = null;
        
        unitIdList = await unitService.getUnitPermissionIdList(user)
        
        let result = {}
        if (!unitIdList.length) {
            unitIdList = await Unit.findAll();
            unitIdList = unitIdList.map(item => item.id);
        } 
        let latestTrackList = await sequelizeObj.query(`
            SELECT t.deviceId, t.violationType, t.count, t.dataFrom, vr.vehicleNo, d.driverName, t.occTime 
            FROM track t 
            LEFT JOIN vehicle_relation vr ON vr.deviceId = t.deviceId
            LEFT JOIN driver d ON d.driverId = vr.driverId
            LEFT JOIN device de ON de.deviceId = t.deviceId
            WHERE t.dataFrom = 'obd' 
            AND t.violationType NOT IN (?)
            AND d.unitId IN (?)
            UNION
            SELECT t.deviceId, t.violationType, t.count, t.dataFrom, t.vehicleNo, d.driverName, t.occTime 
            FROM track t 
            LEFT JOIN vehicle_relation vr ON vr.driverId = t.deviceId
            LEFT JOIN driver d ON d.driverId = vr.driverId
            WHERE t.dataFrom = 'mobile' 
            AND t.violationType NOT IN (?)
            AND d.unitId IN (?)
        `, { replacements: [ CONTENT.ViolationType.IDLETime, unitIdList, CONTENT.ViolationType.IDLETime, unitIdList ], type: QueryTypes.SELECT })

        let differentDeviceIdAndVehicleNoList = [];
        for (let latestTrack of latestTrackList) {
            let ifExist = differentDeviceIdAndVehicleNoList.some(obj => {
                if (obj.deviceId == latestTrack.deviceId && obj.vehicleNo == latestTrack.vehicleNo) {
                    return true;
                }
            })
            if (!ifExist) {
                differentDeviceIdAndVehicleNoList.push({ deviceId: latestTrack.deviceId, vehicleNo: latestTrack.vehicleNo });
            }
        }
        let resultLatestTrackList = [];
        for (let deviceIdAndVehicleNo of differentDeviceIdAndVehicleNoList) {
            let device = { 
                deviceId: deviceIdAndVehicleNo.deviceId, 
                vehicleNo: deviceIdAndVehicleNo.vehicleNo, 
                hardBraking: {
                    eventCount: 0,
                    occTime: null
                },
                rapidAcc: {
                    eventCount: 0,
                    occTime: null
                },
                speeding: {
                    eventCount: 0,
                    occTime: null
                },
                missing: {
                    eventCount: 0,
                    occTime: null
                },
                idle: {
                    eventCount: 0,
                    occTime: null
                }
            };
            for (let latestTrack of latestTrackList) {
                if (latestTrack.deviceId == deviceIdAndVehicleNo.deviceId 
                    && deviceIdAndVehicleNo.vehicleNo == latestTrack.vehicleNo) {
                    device.vehicleNo = latestTrack.vehicleNo;
                    device.driver = latestTrack.driverName;
                    device.dataFrom = latestTrack.dataFrom;
                    
                    if (latestTrack.violationType === CONTENT.ViolationType.HardBraking) {
                        device.hardBraking = latestTrack
                    } else if (latestTrack.violationType === CONTENT.ViolationType.RapidAcc) {
                        device.rapidAcc = latestTrack
                    } else if (latestTrack.violationType === CONTENT.ViolationType.Speeding) {
                        device.speeding = latestTrack
                    } else if (latestTrack.violationType === CONTENT.ViolationType.IDLETime) {
                        device.idle = latestTrack
                    } else if (latestTrack.violationType === CONTENT.ViolationType.Missing) {
                        device.missing = latestTrack
                    }
                }
            }
            resultLatestTrackList.push(device)
        }

        result = { list: resultLatestTrackList, hardBraking: 0, rapidAcc: 0, speeding: 0, missing: 0, idleTime: 0, outOfService: 0, parked: 0, onRoad: 0 };
        
        for (let track of latestTrackList) {
            if (track.violationType === CONTENT.ViolationType.HardBraking) result.hardBraking += track.count;
            else if (track.violationType === CONTENT.ViolationType.RapidAcc) result.rapidAcc += track.count;
            else if (track.violationType === CONTENT.ViolationType.Speeding) result.speeding += track.count;
            else if (track.violationType === CONTENT.ViolationType.IDLETime) result.idleTime += track.count;
            else if (track.violationType === CONTENT.ViolationType.Missing) result.missing += track.count;
        }
        let positionList = await sequelizeObj.query(`
            SELECT * FROM vehicle_relation vr
            LEFT JOIN vehicle v ON v.vehicleNo = vr.vehicleNo 
            WHERE v.unitId IN (?)
        `, { type: QueryTypes.SELECT, replacements: [ unitIdList ] })
        let mobileList = await sequelizeObj.query(`
            SELECT * FROM driver_position dp
            LEFT JOIN driver d ON d.driverId = dp.driverId 
            WHERE d.unitId IN (?)
        `, { type: QueryTypes.SELECT, replacements: [ unitIdList ] });
        for (let position of positionList) {
            if (position.state === CONTENT.DEVICE_STATE.PARKED) result.parked++;
            if (position.state === CONTENT.DEVICE_STATE.ON_ROAD) result.onRoad++;
        }
        for (let position of mobileList) {
            if (position.state === CONTENT.DEVICE_STATE.PARKED) result.parked++;
            if (position.state === CONTENT.DEVICE_STATE.ON_ROAD) result.onRoad++;
        }
        
        // Sort by event time
        if (result.list && result.list.length) {
            let newList = result.list.sort((a, b) => {
                let a_time = [a.hardBraking.occTime, a.rapidAcc.occTime, a.speeding.occTime, a.missing.occTime]
                let b_time = [b.hardBraking.occTime, b.rapidAcc.occTime, b.speeding.occTime, b.missing.occTime]
                a_time.sort((a1, a2) => {
                    return a1 < a2 ? 1 : -1
                })
                b_time.sort((b1, b2) => {
                    return b1 < b2 ? 1 : -1
                })
                return a_time[0] < b_time[0] ? 1 : -1
            })
            result.list = newList;
        }
        return res.json(utils.response(1, result));
    } catch (err) {
        log.error('(getTrackDashboardInfo2) : ', err);
        return res.json(utils.response(0, 'Server Error!'));
    }
}

module.exports.getEventHistory = async function (req, res) {
    try {
        let { deviceId, vehicleNo, violationType, date, dataFrom } = req.body;
        if (!date) date = moment().format('YYYY-MM-DD')
        if (!violationType) violationType = 'all'

        if (violationType && violationType != 'all') {
            if (violationType == 'speeding') {
                violationType = CONTENT.ViolationType.Speeding
            } else if (violationType == 'rapidAcc') {
                violationType = CONTENT.ViolationType.RapidAcc
            } else if (violationType == 'missing') {
                violationType = CONTENT.ViolationType.Missing
            } else if (violationType == 'hardBraking') {
                violationType = CONTENT.ViolationType.HardBraking
            } else if (violationType == 'noGoZone') {
                violationType = CONTENT.ViolationType.NoGoZoneAlert
            }
        }
        let trackHistoryList = []
        let sql = ``
        let replacements = []
        if (dataFrom == 'mobile') {
            sql = `
                SELECT th.*, d.driverName, z.*, nt.selectedTimes   
                FROM track_history th
                LEFT JOIN driver d ON d.driverId = th.deviceId
                LEFT JOIN (
                    SELECT t.driverId, t.taskId, t.mobileStartTime, t.mobileEndTime, t.hub, t.node, t.vehicleNumber, t.groupId 
                    FROM task t WHERE
                    ( (Date(?) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                    OR 
                    t.driverStatus = 'started'

                    union 

                    SELECT ui.driverId, CONCAT('DUTY-', ui.dutyId) as taskId, ui.mobileStartTime, ui.mobileEndTime, 
                    ui.hub, ui.node, ui.vehicleNo as vehicleNumber, ui.groupId
                    FROM urgent_indent ui 
                    WHERE ( (Date(?) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                    OR ui.status IN ('started', 'ready')
                ) tt ON tt.driverId = th.deviceId 
                left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                left join nogo_zone z on z.id = th.zoneId
                LEFT JOIN (
                    SELECT GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes, nt.zoneId
                    FROM nogo_time nt
                    GROUP BY nt.zoneId
                ) nt ON nt.zoneId = z.id
                WHERE (th.occTime >= tt.mobileStartTime AND (tt.mobileEndTime is null or th.occTime <= tt.mobileEndTime ) )
                AND th.occTime LIKE ?
                AND th.deviceId = ?
                AND th.vehicleNo = ?
            `;
            replacements.push(date)
            replacements.push(date)
            replacements.push(date+'%')
            replacements.push(deviceId)
            replacements.push(vehicleNo)
            if (violationType !== 'all') {
                sql += ` AND th.violationType = ? `
                replacements.push(violationType)
            }
            sql += ` ORDER BY th.occTime DESC  `
        } else if (dataFrom == 'obd') {
            sql = `
                SELECT th.*, v.vehicleNo, z.*, nt.selectedTimes
                FROM track_history th
                LEFT JOIN vehicle v ON v.deviceId = th.deviceId
                LEFT JOIN (
                    SELECT t.driverId, t.taskId, t.mobileStartTime, t.mobileEndTime, t.hub, t.node, t.vehicleNumber, t.groupId  
                    FROM task t WHERE
                    ( (Date(?) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                    OR 
                    t.driverStatus = 'started'

                    union 

                    SELECT ui.driverId, CONCAT('DUTY-', ui.dutyId) as taskId, ui.mobileStartTime, ui.mobileEndTime, 
                    ui.hub, ui.node, ui.vehicleNo as vehicleNumber, ui.groupId
                    FROM urgent_indent ui 
                    WHERE ( (Date(?) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                    OR ui.status IN ('started', 'ready')
                    
                ) tt ON tt.vehicleNumber = v.vehicleNo
                left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                left join nogo_zone z on z.id = th.zoneId
                LEFT JOIN (
                    SELECT GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes, nt.zoneId
                    FROM nogo_time nt
                    GROUP BY nt.zoneId
                ) nt ON nt.zoneId = z.id
                WHERE (th.occTime >= tt.mobileStartTime AND (tt.mobileEndTime is null or th.occTime <= tt.mobileEndTime ) )
                AND th.occTime LIKE ?
                AND th.deviceId = ?
            `;
            replacements.push(date)
            replacements.push(date)
            replacements.push(date+'%')
            replacements.push(deviceId)
            if (violationType !== 'all') {
                sql += ` AND th.violationType = ? `
                replacements.push(violationType)
            }
            sql += ` ORDER BY th.occTime DESC  `
        }
        log.info(sql)
        if (sql) {
            trackHistoryList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        }
        
        return res.json(utils.response(1, trackHistoryList));
    } catch (error) {
        log.error('(getEventHistory) : ', error);
        return res.json(utils.response(0, 'Server Error!'));
    }
}

module.exports.getAllEventHistory = async function (req, res) {
    try {
        let { deviceId, vehicleNo, violationType } = req.body;

        if (!violationType) violationType = 'all'

        if (violationType && violationType != 'all') {
            if (violationType == 'speeding') {
                violationType = CONTENT.ViolationType.Speeding
            } else if (violationType == 'rapidAcc') {
                violationType = CONTENT.ViolationType.RapidAcc
            } else if (violationType == 'missing') {
                violationType = CONTENT.ViolationType.Missing
            } else if (violationType == 'hardBraking') {
                violationType = CONTENT.ViolationType.HardBraking
            }
        }
        let sql = `
            SELECT th.*, d.driverName 
            FROM track_history th
            LEFT JOIN driver d ON d.driverId = th.deviceId
            WHERE th.deviceId = ?
            
        `;
        let replacements = [deviceId]
        if (vehicleNo == null || vehicleNo == 'null') {
            sql += ` AND th.vehicleNo is null `
        } else {
            sql += ` AND th.vehicleNo = ? `
            replacements.push(vehicleNo)
        }
        if (violationType !== 'all') {
            sql += ` AND th.violationType = ? `
            replacements.push(violationType)
        }
        sql += ` ORDER BY th.occTime DESC  `

        let trackHistoryList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        
        return res.json(utils.response(1, trackHistoryList));
    } catch (error) {
        log.error('(getEventHistory) : ', error);
        return res.json(utils.response(0, 'Server Error!'));
    }
}

module.exports.getEventLatestSpeedInfo = async function (req, res) {
    try {
        let deviceId = req.body.deviceId;
        let vehicleNo = req.body.vehicleNo;
        let type = req.body.type;
        let occTime = req.body.occTime;
        let startTime = req.body.startTime;
        let endTime = req.body.endTime;

        if (!endTime) endTime = startTime

        let list = [], limitSpeed = 0;
        if (type === 'mobile') {
            list = await DriverOffenceHistory.findAll({
                where: {
                    driverId: deviceId,
                    vehicleNo,
                    createdAt: {
                        [Op.gte]: moment(startTime).subtract(15, 's').format('YYYY-MM-DD HH:mm:ss'),
                        [Op.lte]: moment(endTime).add(15, 's').format('YYYY-MM-DD HH:mm:ss')
                    },
                },
                order: [
                    ['createdAt', 'ASC']
                ]
            })
            let vehicle = await Vehicle.findByPk(vehicleNo)
            limitSpeed = vehicle?.limitSpeed;
            limitSpeed = limitSpeed ?? 60
        } else if (type === 'obd') {
            list = await DeviceOffenceHistory.findAll({
                where: {
                    deviceId,
                    createdAt: {
                        [Op.gte]: moment(startTime).subtract(15, 's').format('YYYY-MM-DD HH:mm:ss'),
                        [Op.lte]: moment(endTime).add(15, 's').format('YYYY-MM-DD HH:mm:ss')
                    }
                },
                order: [
                    ['createdAt', 'ASC']
                ]
            })
            let vehicle = null
            if (vehicleNo) {
                vehicle = await Vehicle.findByPk(vehicleNo)
            } else {
                vehicle = await Vehicle.findOne({ where: { deviceId } })
            }
            limitSpeed = vehicle?.limitSpeed;
            limitSpeed = limitSpeed ?? 60;
        }
        
        return res.json(utils.response(1, { list, limitSpeed }));
    } catch (err) {
        log.error('(getEventLatestSpeedInfo) : ', err);
        return res.json(utils.response(0, 'Server Error!'));
    }
}

module.exports.getEventPositionHistory = async function (req, res) {
    try {
        let { deviceId, dataFrom, startTime, endTime, driverId, vehicleNo } = req.body;
        if (!endTime) endTime = startTime
        
        if (dataFrom === 'obd') {
            let positionList = await DeviceOffenceHistory.findAll({
                where: {
                    deviceId,
                    createdAt: {
                        [Op.between]: [
                            moment(startTime).subtract(10, 'seconds'),
                            moment(endTime).add(10, 'seconds')
                        ]
                    }
                }
            })
            return res.json(utils.response(1, positionList));
        } else if (dataFrom === 'mobile') {
            let positionList = await DriverOffenceHistory.findAll({
                where: {
                    driverId,
                    vehicleNo,
                    createdAt: {
                        [Op.between]: [
                            moment(startTime).subtract(10, 'seconds'),
                            moment(endTime).add(10, 'seconds')
                        ]
                    }
                }
            })
            return res.json(utils.response(1, positionList));
        }
    } catch (error) {
        log.error('(getEventPositionHistory) : ', error);
        return res.json(utils.response(0, 'Server Error!'));
    }
}

module.exports.getDriverLastPosition = async function (req, res) {
    let { driverIdList } = req.body;
    let driverList = await Driver.findAll({ where: { driverId: driverIdList } })
    for (let driver of driverList) {
        let position = await DriverPosition.findOne({ where: { driverId: driver.driverId }, order: [ [ 'updatedAt', 'DESC' ] ] })
        if (position) {
            driver.vehicleNo = position.vehicleNo;
            driver.lat = position.lat;
            driver.lng = position.lng;
        }
    }

    return res.json(utils.response(1, driverList));
}

module.exports.getDriverLastPositionByVehicleNo = async function (req, res) {
    let { vehicleNoList } = req.body;
    let vehiclePosList = [];
    for (let vehicleNo of vehicleNoList) {
        let vehiclePos = {vehicleNo: vehicleNo};
        let position = await DriverPosition.findOne({ where: { vehicleNo: vehicleNo}, order: [ [ 'updatedAt', 'DESC' ] ] })
        if (position) {
            vehiclePos.lat = position.lat;
            vehiclePos.lng = position.lng;
        }
        vehiclePosList.push(vehiclePos);
    }

    return res.json(utils.response(1, vehiclePosList));
}