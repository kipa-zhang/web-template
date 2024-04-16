const log = require('../log/winston').logger('MtAdmin Service');
const FirebaseService = require('../firebase/firebase');
const utils = require('../util/utils');
const moment = require('moment');
const CONTENT = require('../util/content');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { CheckList } = require('../model/checkList');
const { MtAdmin } = require('../model/mtAdmin');
const { Vehicle } = require('../model/vehicle.js');
const { Driver } = require('../model/driver');
const { PermitType } = require('../model/permitType');
const { Unit } = require('../model/unit.js');
const { Task } = require('../model/task.js');
const { lowerCase } = require('lodash');
const { User } = require('../model/user.js');
const { ServiceMode } = require('../model/serviceMode');
const { PurposeMode } = require('../model/purposeMode');

const { VehicleRelation } = require('../model/vehicleRelation.js');
const { OperationRecord } = require('../model/operationRecord');
const vehicleService = require('../services/vehicleService.js');
const { loan } = require('../model/loan');


const userService = require('../services/userService');
const { loanRecord } = require('../model/loanRecord');

const unitService = require('../services/unitService');

const purposeList = ['avi', 'wpt', 'mpt', 'pm']

let TaskUtils = {
    getDriverIdAndVehicleNoByTaskId: async function (taskId) {
        let taskObj = await Task.findOne({where: { indentId: taskId }}) 
        return taskObj;
    },
    GetDestination: async function (locationName) {
        let replacements = []
        let sql = `select id, locationName, secured, lat, lng FROM location where 1=1 `
        if (locationName) {
            sql += ` and locationName in (?) group by locationName`
            replacements.push(locationName)
        }
        let result = await sequelizeSystemObj.query(sql, {
            replacements: replacements,
            type: QueryTypes.SELECT,
        });
        return result;
    },
    getUnitIdByUserId: async function (userId) {
        let user = await User.findAll({ where: { userId: userId } });
        return user[0]
    },
    getUnitByUnitId: async function (unitId) {
        let unit = await Unit.findAll({ where: { id: unitId } })
        return unit[0];
    },
    verifyLocation: async function (reportingLocation, destination) {
        let location = [reportingLocation, destination];
        let newLocation = location.map(loc => { return loc });
        newLocation = Array.from(new Set(newLocation));
        let locationList = await TaskUtils.GetDestination(newLocation);
        if (newLocation.length == 1) {
            if (locationList.length > 0) {
                return true
            } else {
                return false
            }
        } else if (newLocation.length > 1) {
            if (locationList.length > 1) {
                return true
            } else {
                return false
            }
        } else {
            return false
        }
    },
    verifyDriverLeave: async function (driverId, startDate, endDate) {
        let params = [driverId, startDate, startDate, endDate, endDate,startDate, endDate]
        let leaveDriverIds = await sequelizeObj.query(`
            SELECT * FROM driver_leave_record WHERE status = 1 and driverId = ?           
            AND ((? >= startTime AND ? <= endTime) OR (? >= startTime AND ? <= endTime) OR (? < startTime AND ? > endTime))
        `, { type: QueryTypes.SELECT, replacements: params });
        if(leaveDriverIds.length > 0) {
            return false
        } else {
            return true
        }
    },
    verifyVehicleLeave: async function (vehicleNo, startDate, endDate) {
        let params = [vehicleNo, startDate, startDate, endDate, endDate,startDate, endDate]
        let leaveVehicleNos = await sequelizeObj.query(`
            SELECT * FROM vehicle_leave_record WHERE status = 1 and vehicleNo = ?  
            AND ((? >= startTime AND ? <= endTime) OR (? >= startTime AND ? <= endTime) OR (? < startTime AND ? > endTime))
        `, { type: QueryTypes.SELECT, replacements: params });
        if(leaveVehicleNos.length > 0) {
            return false
        } else {
            return true
        }
    },
    findUser: async function (userId) {
        let user = await sequelizeObj.query(`
            SELECT us.userId, us.username, us.userType, us.driverId, us.unitId, un.unit, un.subUnit 
            FROM \`user\` us
            LEFT JOIN unit un ON un.id = us.unitId
            WHERE us.userId = ? LIMIT 1
        `, { type: QueryTypes.SELECT, replacements: [ userId ] });
        if (!user.length) {
            throw `UserID ${ userId } does not exist!`;
        } else {
            return user[0];
        }
    },
    findOutHubNode: async function (hub, node) {
        let unitList = [];
        if (!hub) {
            unitList = await Unit.findAll({ where: { unit: { [Op.not]: null, } }, group: ['unit', 'subUnit'] })
        } else {
            if (node) {
                unitList = await Unit.findAll({ where: { unit: hub, subUnit: node }, group: ['unit', 'subUnit'] })
            } else {
                unitList = await Unit.findAll({ where: { unit: hub }, group: ['unit', 'subUnit'] })
            }
        }
        return unitList;
    },
    findOutHubNodeIDByUserId: async function (userId) {
        let unitList = []
        let user = await TaskUtils.findUser(userId);
        if (user.userType === CONTENT.USER_TYPE.HQ || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            unitList = await TaskUtils.findOutHubNode();
        } else if (user.userType === CONTENT.USER_TYPE.UNIT) {
            if (user.subUnit) {
                unitList = await TaskUtils.findOutHubNode(user.unit, user.subUnit);
            } else {
                unitList = await TaskUtils.findOutHubNode(user.unit);
            }
        }        
        return unitList.map(unit => unit.id);
    },
    initOperationRecord: async function(operatorId, taskId, beforeDriverId, afterDriverId, beforeVehicleNo, afterVehicleNo, businessType) {
        let typeName = null;
        let remarks = null;
        if((!beforeDriverId || beforeDriverId == '') && (!beforeVehicleNo || beforeVehicleNo == '')){
            typeName = 'assign'
        } else {
            typeName = 're-assign'
        }
        if(!afterDriverId || !afterVehicleNo){
            if(afterDriverId) remarks = 'loan out driver'
            if(afterVehicleNo) remarks = 'loan out vehicle'
        } 
        if(!remarks) remarks = typeName + ' task';
        let obj = {
            id: null,
            operatorId: operatorId,
            businessType: businessType,
            businessId: taskId,
            optType: typeName,
            beforeData: `${ beforeDriverId && beforeDriverId != '' ? `driverId:${ beforeDriverId },` : '' }${ beforeVehicleNo && beforeVehicleNo != '' ? `vehicleNo:${ beforeVehicleNo }` : '' }`,
            afterData: `${ afterDriverId && afterDriverId != '' ? `driverId:${ afterDriverId },` : '' }${ afterVehicleNo && afterVehicleNo != '' ? `vehicleNo:${ afterVehicleNo }` : '' }`,
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            remarks: remarks
        }
        await OperationRecord.create(obj)
    },
    getVehicleByGroup: async function(purpose, leaveStatus, groupId, vehicleType, startDate, endDate, dateData, totalStatus) {
        startDate = startDate ? moment(startDate).format('YYYY-MM-DD HH:mm:ss') : null;
        endDate = endDate ? moment(endDate).format('YYYY-MM-DD HH:mm:ss') : null;
        //2023-07-14 loan out group + 2023-08-10 vehicle group
        let sql = `
        SELECT vv.vehicleNo, vv.unitId, vv.vehicleType, vv.nextAviTime, vv.nextMptTime, vv.nextWpt1Time, vv.nextWpt2Time, vv.nextWpt3Time  FROM (
            SELECT v.vehicleType, v.nextAviTime, v.nextMptTime, v.nextWpt1Time, v.nextWpt2Time, v.nextWpt3Time,
        `
        let replacements = []
        if(dateData || startDate){
            sql +=`
            IF(l.vehicleNo IS NULL, IF(lr.vehicleNo IS NULL, v.vehicleNo, lr.vehicleNo), l.vehicleNo) AS vehicleNo,
            IF(l.groupId IS NULL, IF(lr.groupId IS NULL, v.groupId, lr.groupId), l.groupId) AS unitId 
            FROM vehicle v
            `
            if(dateData) {
                sql += `
                    LEFT JOIN (SELECT lo.vehicleNo, lo.groupId FROM loan lo WHERE 
                        ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                    ) l ON l.vehicleNo = v.vehicleNo
                    LEFT JOIN (SELECT lr.vehicleNo, lr.groupId FROM loan_record lr WHERE 
                        ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                    ) lr ON lr.vehicleNo = v.vehicleNo
                `
                replacements.push(moment(dateData).format('YYYY-MM-DD'))
                replacements.push(moment(dateData).format('YYYY-MM-DD'))
            } else {
                sql += `
                    LEFT JOIN (SELECT lo.vehicleNo, lo.groupId FROM loan lo WHERE ? BETWEEN lo.startDate AND lo.endDate) l ON l.vehicleNo = v.vehicleNo
                    LEFT JOIN (SELECT lr.vehicleNo, lr.groupId FROM loan_record lr WHERE 
                        ? >= lr.startDate AND ? <= lr.returnDate
                    ) lr ON lr.vehicleNo = v.vehicleNo
                `
                replacements.push(startDate)
                replacements.push(startDate)
                replacements.push(endDate)
            }
        } else {
            sql += `
                IF(l.vehicleNo IS NULL, v.vehicleNo, l.vehicleNo) AS vehicleNo,
                IF(l.groupId IS NULL, v.groupId, l.groupId) AS unitId
                FROM vehicle v 
                LEFT JOIN (SELECT lo.startDate, lo.groupId, lo.vehicleNo FROM loan lo WHERE NOW() BETWEEN lo.startDate AND lo.endDate) l ON v.vehicleNo = l.vehicleNo 
            `
        }
        sql += ` WHERE 1=1 `
        if(vehicleType){
            sql += `  and FIND_IN_SET(v.vehicleType, ?)`
            replacements.push(vehicleType)
        }
        sql += ` ) vv WHERE vv.vehicleNo IS NOT NULL`
        if(groupId){
            if(groupId.length > 0){
                sql += ` and vv.unitId in(?)`
                replacements.push(groupId.join(","))
            } else {
                sql += ` and vv.unitId = ?`
                replacements.push(groupId)
            }
        } else {
            sql += ` and vv.unitId is not null`
        }
        if(leaveStatus){
            sql += ` 
                and vv.vehicleNo not in (
                    select ifnull(vl.vehicleNo, -1) from vehicle_leave_record vl 
                    where vl.status = 1 
                    AND ( (? >= vl.startTime AND ? <= vl.endTime) 
                    OR (? >= vl.startTime AND ? <= vl.endTime) 
                    OR (? < vl.startTime AND ? > vl.endTime))
                    GROUP BY vl.vehicleNo
                )
            `
            //startDate, startDate, endDate, endDate, startDate, endDate
            replacements.push(startDate)
            replacements.push(startDate)
            replacements.push(endDate)
            replacements.push(endDate)
            replacements.push(startDate)
            replacements.push(endDate)
        }
        if(purpose){
            if((purposeList.indexOf(purpose?.toLowerCase()) == -1 || purpose?.toLowerCase() == 'mpt')){
                sql += `  AND (vv.nextAviTime > ? OR vv.nextAviTime IS NULL)`
                let dateAvi = endDate ?? dateData
                replacements.push(moment(dateAvi).format('YYYY-MM-DD'))
            }
        }
        sql += `
        GROUP BY vv.vehicleNo
        `
        console.log(sql)
        let vehicleList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return vehicleList
    },
    getVehicleList: async function(purpose, vehicleType, startDate, endDate, unitId, unit, subUnit, dataType) {
        let hub = unit && unit != '' && unit != 'null'  ? unit : null;
        let node = subUnit && subUnit != '' && subUnit != 'all' && subUnit != 'null' ? subUnit : null;
        let unitIdList = []
        if(hub){
            if(node){
                let unit2 = await Unit.findOne({ where: { unit: hub, subUnit: node } })
                unitIdList.push(unit2.id)
            } else {
                let unit = await Unit.findAll({ where: { unit: hub } })
                unitIdList = unit.map(item => item.id)
            }
        } else {
            if (unitId) {
                let unit = await TaskUtils.getUnitByUnitId(unitId)
                if (unit && !unit.subUnit) {
                    let newUnit = await Unit.findAll({ where: { unit: unit.unit } })
                    unitIdList = newUnit.map(item => item.id)
                } else if (unit && unit.subUnit) {
                    unitIdList.push(unitId)
                }
            }
        }
        log.info(`vehicleList unitIDList ${ JSON.stringify(unitIdList) }`)
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss');
        endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss');
        // leave vehicle
        let leaveVehicle = await sequelizeObj.query(`
            select ifnull(vl.vehicleNo, -1) as vehicleNo from vehicle_leave_record vl 
            where vl.status = 1 
            AND ((? >= vl.startTime AND ? <= vl.endTime) 
            OR (? >= vl.startTime AND ? <= vl.endTime) 
            OR (? < vl.startTime AND ? > vl.endTime))
            GROUP BY vl.vehicleNo
        `, { type: QueryTypes.SELECT, replacements: [startDate, startDate, endDate, endDate, startDate, endDate] })
        leaveVehicle = leaveVehicle.map(item => item.vehicleNo)
        log.warn(`leave vehicleList ${ JSON.stringify(leaveVehicle) }`)

        //2023-07-13 exclude loan out vehicle
        let loanOutVehicle = await sequelizeObj.query(`
            SELECT ifnull(vehicleNo, -1) as vehicleNo FROM loan 
            WHERE ((? >= startDate AND ? <= endDate) 
            OR (? >= startDate AND ? <= endDate) 
            OR (? < startDate AND ? > endDate))
            and vehicleNo is not null
            group by vehicleNo
        `, { type: QueryTypes.SELECT, replacements: [startDate, startDate, endDate, endDate, startDate, endDate] })
        loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)
        log.warn(`loan out vehicleList ${ JSON.stringify(loanOutVehicle) }`)

        // Not within the specified range hoto vehicle
        let hotoVehicleListByNotScope = await sequelizeObj.query(
            `select ifnull(vehicleNo, -1) as vehicleNo, startDateTime, endDateTime
            from hoto 
            where ((? >= startDateTime AND ? <= endDateTime) 
            OR (? >= startDateTime AND ? <= endDateTime) 
            OR (? < startDateTime AND ? > endDateTime))
            and vehicleNo not in (select vehicleNo from hoto 
                where ? >= startDateTime AND ? <= endDateTime 
                and vehicleNo is not null  and status = 'Approved'
            ) and status = 'Approved'
            and vehicleNo is not null
            group by vehicleNo
            `,
        {
            type: QueryTypes.SELECT
            , replacements: [startDate, startDate, endDate, endDate, startDate, endDate, startDate, endDate]
        })
        hotoVehicleListByNotScope = hotoVehicleListByNotScope.map(item => item.vehicleNo)
        log.warn(`hoto Not within the specified range vehicleList ${ JSON.stringify(hotoVehicleListByNotScope) }`)

        let hotoVehicle = []
        if(dataType) {
            // 2023-08-08 hoto vehicle
            // hotoVehicle = await sequelizeObj.query(`
            //     SELECT vehicleNo FROM hoto 
            //     WHERE vehicleNo IS NOT NULL 
            //     AND (('${ startDate }' >= startDateTime AND '${ startDate }' <= endDateTime) 
            //     OR ('${ endDate }' >= startDateTime AND '${ endDate }' <= endDateTime) 
            //     OR ('${ startDate }' < startDateTime AND '${ endDate }' > endDateTime)
            //     OR '${ startDate }' >= startDateTime) and status = 'Approved'
            //     GROUP BY vehicleNo
            // `, { type: QueryTypes.SELECT })
            // hotoVehicle = hotoVehicle.map(item => item.vehicleNo)
            // log.warn(`mb type task hoto vehicleList ${ JSON.stringify(hotoVehicle) }`)
        }
        let excludeVehicle = leaveVehicle.concat(loanOutVehicle).concat(hotoVehicle).concat(hotoVehicleListByNotScope)    
        excludeVehicle = excludeVehicle.map(item => item);
        excludeVehicle = Array.from(new Set(excludeVehicle))  
        log.warn(`Need to exclude the vehicle ${ JSON.stringify(excludeVehicle) }`)
        //2023-06-29 add vehicle onhold = 1 exclude
        let sql = `
            select vv.vehicleNo, vv.unitId  from (
                SELECT a.vehicleNo, IF(h.unitId is NULL, a.unitId, h.unitId) as unitId, a.groupId FROM vehicle a
                left join (
                    select ho.vehicleNo, ho.unitId from hoto ho where ((? >= ho.startDateTime AND ? <= ho.endDateTime)) and ho.status = 'Approved'
                ) h ON h.vehicleNo = a.vehicleNo 
                where a.groupId is null
        `
        let replacements = [startDate, endDate]
        if(vehicleType){
            sql += ` and FIND_IN_SET(a.vehicleType, ?)`
            replacements.push(vehicleType)
        }
        if(purposeList.indexOf(purpose?.toLowerCase()) == -1 || purpose?.toLowerCase() == 'mpt'){
            sql += ` AND (a.nextAviTime > ? OR a.nextAviTime IS NULL)`
            replacements.push(moment(endDate).format('YYYY-MM-DD'))
        }
        sql += ` ) vv where vv.unitId is not null`
        if(unitIdList.length > 0){
            sql += ` and vv.unitId in (?)`
            replacements.push(unitIdList)
        }
        if(excludeVehicle.length > 0){
            sql += ` and vv.vehicleNo not in (?)`
            replacements.push(`'${ excludeVehicle.join("','") }'`)
        }
        sql += ` GROUP BY vv.vehicleNo`
        console.log(sql)
        let vehicleList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return vehicleList;
    },
    getDriverByGroup: async function(leaveStatus, unitId, vehicleType, startDate, endDate, dateData, totalStatus) {
        startDate = startDate ? moment(startDate).format('YYYY-MM-DD HH:mm:ss') : null;
        endDate = endDate ? moment(endDate).format('YYYY-MM-DD HH:mm:ss') : null;
        //2023-07-14 dv/loa group
        let sql = `
            select d.driverId, d.driverName, d.contactNumber, u.unitId
            from driver d
            LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            LEFT JOIN user u ON u.driverId = d.driverId
            where d.driverId is not null and u.role in ('DV', 'LOA') and d.unitId is null
        `
        let replacements = []
        if(!totalStatus){
            sql += `  and d.permitStatus != 'invalid'`
        }
        if(vehicleType){
            sql += ` and dc.vehicleType = ?`
            replacements.push(vehicleType)
        }
        if(unitId){
            if(unitId.length > 0){
                sql +=  ` and d.groupId in(?)  `
                replacements.push(unitId.join(","))
            } else {
                sql += ` and d.groupId = ?`
                replacements.push(unitId)
            }
        }
        if(dateData){
            sql += ` and (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)`
            replacements.push(moment(dateData).format('YYYY-MM-DD'))
        } else if (endDate){
            sql += ` and (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)`
            replacements.push(moment(endDate).format('YYYY-MM-DD'))
        } else {
            sql += ' and (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate is null)'
        }
        if(leaveStatus) {
            sql += `
            and d.driverId not in (
                SELECT ifnull(dl.driverId, -1) FROM driver_leave_record dl WHERE dl.status = 1  
                AND ( (? >= dl.startTime AND ? <= dl.endTime) 
                OR (? >= dl.startTime AND ? <= dl.endTime) 
                OR (? < dl.startTime AND ? > dl.endTime)
                ) GROUP BY dl.driverId
            )
            ` 
            replacements.push(startDate)
            replacements.push(startDate)
            replacements.push(endDate)
            replacements.push(endDate)
            replacements.push(startDate)
            replacements.push(endDate)
        }
        sql += ` GROUP BY d.driverId`
        //2023-07-14 loan out group
        let sql2 = `
        SELECT dd.driverId, dd.driverName, dd.contactNumber, dd.unitId FROM (
        `
        let replacements2 = []
        if(dateData || startDate){
            sql2 += `
                SELECT d.driverName, d.contactNumber,
                IF(l.driverId IS NULL, lr.driverId, l.driverId) AS driverId,
                IF(l.groupId IS NULL, lr.groupId, l.groupId) AS unitId
                FROM driver d
                LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            `
            if(dateData) {
                sql2 += `
                    LEFT JOIN (SELECT lo.driverId, lo.groupId FROM loan lo WHERE 
                        ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                    ) l ON l.driverId = d.driverId
                    LEFT JOIN (SELECT lr.driverId, lr.groupId FROM loan_record lr WHERE 
                        ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                    ) lr ON lr.driverId = d.driverId
                    WHERE (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)
                `
                replacements2.push(moment(dateData).format('YYYY-MM-DD'))
                replacements2.push(moment(dateData).format('YYYY-MM-DD'))
                replacements2.push(moment(dateData).format('YYYY-MM-DD'))
            } else {
                sql2 += `
                    LEFT JOIN (SELECT lo.driverId, lo.groupId FROM loan lo WHERE ? BETWEEN lo.startDate AND lo.endDate) l ON l.driverId = d.driverId
                    LEFT JOIN (SELECT lr.driverId, lr.groupId FROM loan_record lr WHERE 
                        ? >= lr.startDate AND ? <= lr.returnDate
                    ) lr ON lr.driverId = d.driverId
                    WHERE (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)
                `
                replacements2.push(startDate)
                replacements2.push(startDate)
                replacements2.push(endDate)
                replacements2.push(moment(endDate).format('YYYY-MM-DD'))
            }
           
        } else {
            sql2 += `
                select l.driverId, d.driverName, d.contactNumber, l.groupId as unitId
                from driver d
                LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
                LEFT JOIN (SELECT lo.driverId, lo.groupId FROM loan lo WHERE now() BETWEEN lo.startDate AND lo.endDate) l ON l.driverId = d.driverId
                WHERE (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate is null)
        `
        }
        if(vehicleType){
            sql2 += ` and dc.vehicleType = ?`
            replacements2.push(vehicleType)
        }
        sql2 += `
            ${ !totalStatus ? ` and d.permitStatus != 'invalid'` : '' }   
            ) dd WHERE dd.driverId IS NOT NULL 
        `
        if(unitId){
            if(unitId.length > 0){
                sql2 += ` and dd.unitId in(?)`
                replacements2.push(unitId.join(","))
            } else {
                sql2 += ` and dd.unitId = ?`
                replacements2.push(unitId)
            }
        } else {
            sql2 += ' and dd.unitId is not null'
        }
        if(leaveStatus){
            sql2 += `
                and dd.driverId not in (
                    SELECT ifnull(dl.driverId, -1) FROM driver_leave_record dl WHERE dl.status = 1 
                    AND ( (? >= dl.startTime AND ? <= dl.endTime) 
                    OR (? >= dl.startTime AND ? <= dl.endTime) 
                    OR (? < dl.startTime AND ? > dl.endTime)
                    ) GROUP BY dl.driverId
                )
            ` 
            replacements2.push(startDate)
            replacements2.push(startDate)
            replacements2.push(endDate)
            replacements2.push(endDate)
            replacements2.push(startDate)
            replacements2.push(endDate)
        }
        sql2 += ` GROUP BY dd.driverId`
        console.log(sql)
        let driverList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        let driverListByLoan = await sequelizeObj.query(sql2, { type: QueryTypes.SELECT, replacements: replacements2 });
        driverList = driverList.concat(driverListByLoan)
        return driverList
    }
}
module.exports.TaskUtils = TaskUtils

module.exports.getUnitId = async function (req, res) {
    try {
         let hub = req.body.unit && req.body.unit != '' && req.body.unit != 'null'  ? req.body.unit : null;
        let node = req.body.subUnit && req.body.subUnit != '' && req.body.subUnit != 'all' && req.body.subUnit != 'null' ? req.body.subUnit : null;
        let result
        if(hub){
            result = await Unit.findOne({ where: { unit: hub, subUnit:{ [Op.is]: null } } });
            if(node) result = await Unit.findOne({ where: { unit: hub, subUnit: node } });
        } else {
            let user = await User.findOne({ where: { userId: req.cookies.userId } })
            result = await Unit.findOne({ where: { id: user.unitId } });
        }
        return res.json(utils.response(1, result));      
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
    
}

module.exports.getHubNode = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        // let unitIdList = await TaskUtils.findOutHubNodeIDByUserId(userId);
        let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
        let unitIdList = dataList.unitIdList
        let result = await Unit.findAll({ where: { id: unitIdList }, group: 'id', order: ['unit', 'subUnit'] });

        log.info(`Request URL Result => ${ JSON.stringify(result) }`)
        return res.json(utils.response(1, result));    
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
    
}
//2023-06-15  It is used to distinguish the purpose of CUSTOMER from other users (created)
module.exports.getPurposeModelist = async function (req, res) {
    try {
        let userType = req.cookies.userType;
        let creator = req.body.creator;
        if(creator) {
            let user = await User.findOne({ where: { userId: creator } })
            if(user) userType = user.userType;
        }
        let purposeMode = []
        if(!userType) return res.json(utils.response(1, [])); 
        if(userType.toUpperCase() == 'CUSTOMER'){
            purposeMode = [{"id": 1, "purposeName": "Ops"}, 
            {"id": 2, "purposeName": "Training"}, 
            {"id": 3, "purposeName": "WPT"}, 
            {"id": 4, "purposeName": "MPT"},
            {"id": 5, "purposeName": "Admin"}]
        } else {
            purposeMode = [{"id": 1, "purposeName": "Duty"}, 
            {"id": 2, "purposeName": "Driving Training"}, 
            // {"id": 3, "purposeName": "Maintenance"}, 
            {"id": 4, "purposeName": "Others"},
            {"id": 5, "purposeName": "Familiarisation"},
            {"id": 6, "purposeName": "AVI"},
            {"id": 7, "purposeName": "PM"},
            {"id": 8, "purposeName": "WPT"},
            {"id": 9, "purposeName": "MPT"}]
        }  
        return res.json(utils.response(1, purposeMode));   
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
    
}
//2023-06-15  It is used to distinguish the purpose of CUSTOMER from other users (select Sizer)
module.exports.getPurposeModeType = async function (req, res) {
    try {
        let userType = req.cookies.userType;
        let purposeMode = []
        if(!userType) return res.json(utils.response(1, [])); 
        if(userType.toUpperCase() == 'HQ' || userType.toUpperCase() == 'ADMINISTRATOR') {
            purposeMode = await PurposeMode.findAll();
        } else if(userType.toUpperCase() == 'CUSTOMER'){
            purposeMode = [{"id": 1, "purposeName": "Ops"}, 
            {"id": 2, "purposeName": "Training"}, 
            {"id": 3, "purposeName": "WPT"}, 
            {"id": 4, "purposeName": "MPT"},
            {"id": 5, "purposeName": "Admin"}]
        } else {
            purposeMode = [{"id": 1, "purposeName": "Duty"}, 
            {"id": 2, "purposeName": "Driving Training"}, 
            // {"id": 3, "purposeName": "Maintenance"}, 
            {"id": 4, "purposeName": "Others"},
            {"id": 5, "purposeName": "Familiarisation"},
            {"id": 6, "purposeName": "AVI"},
            {"id": 7, "purposeName": "PM"},
            {"id": 8, "purposeName": "WPT"},
            {"id": 9, "purposeName": "MPT"}]
        }
        return res.json(utils.response(1, purposeMode));   
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
    
}

module.exports.getUnitIdByUserId = async function (req, res) {
    try {
      let userId = req.body.userId
        let user = await TaskUtils.getUnitIdByUserId(userId);
        return res.json(utils.response(1, user));      
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
    
}

module.exports.GetDestination = async function (req, res) {
    try {
        let locationList = await TaskUtils.GetDestination('');
        return res.json(utils.response(1, locationList)); 
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
   
}

module.exports.getVehicleType = async function (req, res) {
    try {
        let purpose = req.body.purpose;
        let startDate = req.body.startDate;
        let endDate = req.body.endDate;
        let userId = req.cookies.userId;
        let hub = req.body.unit && req.body.unit != '' && req.body.unit != 'null'  ? req.body.unit : null;
        let node = req.body.subUnit && req.body.subUnit != '' && req.body.subUnit != 'all' && req.body.subUnit != 'null' ? req.body.subUnit : null;
        let unitId = req.body.unitId;
        if(unitId){
            let unit = await Unit.findOne({ where: { id: unitId } });
            if(unit){
                hub = unit.unit;
                node = unit.subUnit;
            }
        }
        let unitIdList = []
        if(hub){
            if(node){
                let unit2 = await Unit.findOne({ where: { unit: hub, subUnit: node } })
                unitIdList.push(unit2.id)
            } else {
                let unit = await Unit.findAll({ where: { unit: hub } })
                unitIdList = unit.map(item => item.id);
            }
        } else {
            let user = await TaskUtils.getUnitIdByUserId(userId);
            if (user.unitId) {
                unitIdList.push(user.unitId)
                let unit = await TaskUtils.getUnitByUnitId(user.unitId)
                if (unit) {
                    if (!unit.subUnit) {
                        let newUnit = await Unit.findAll({ where: { unit: unit.unit } })
                        unitIdList = newUnit.map(item => item.id);
                    }
                } else {
                    unitIdList = []
                }
            } else {
                unitIdList = [];
            }
        }
        log.info(`vehicleTypeList unitIdList ${ JSON.stringify(unitIdList) }`)
        let replacements = []
        let sql = `
             select * from (
                SELECT v.vehicleType, IF(hh.toHub is NULL, v.unitId, hh.unitId) as unitId, hh.startDateTime, hh.endDateTime
                FROM vehicle v
                LEFT JOIN unit u ON u.id = v.unitId
                left join (
                select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId, ho.startDateTime, ho.endDateTime  from hoto ho where 
                ((? >= ho.startDateTime AND ? <= ho.endDateTime) 
                OR (? >= ho.startDateTime AND ? <= ho.endDateTime) 
                OR (? < ho.startDateTime AND ? > ho.endDateTime))
                and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = v.vehicleNo
             ) vv WHERE 1=1 and ((? >= vv.startDateTime and ? <= vv.endDateTime)
             OR (vv.startDateTime is null)
             )
        `
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        if (unitIdList.length > 0) {
            sql += ` and vv.unitId IN (?)`
            replacements.push(unitIdList)
        } 

        console.log(sql)
    
        let vehicleList = await sequelizeObj.query(
            sql,
            {
                replacements: replacements,
                type: QueryTypes.SELECT
            }
        )
        
        if (vehicleList) {
            let newVehicleList = vehicleList.map(vehicle => { return vehicle.vehicleType });
            newVehicleList = Array.from(new Set(newVehicleList));
            return res.json(utils.response(1, newVehicleList));
        } else {
            return res.json(utils.response(1, []));
        }
    } catch(error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }

};

//2023-08-08 getDriverList function merge
module.exports.getDriverList = async function (req, res) {
    try {
        let { purpose, unitId, vehicleType, startDate, endDate, dataType } = req.body
        if(purpose.toLowerCase() == 'driving training'){
            vehicleType = null
        } 
        if(purpose.toLowerCase() == 'familiarisation') {
            vehicleType = null
            unitId = null
        }
        let unitIdList = []
        if (unitId) {
            let unit = await TaskUtils.getUnitByUnitId(unitId)
            if (unit && !unit.subUnit) {
                let newUnit = await Unit.findAll({ where: { unit: unit.unit } })
                for (let newUnitId of newUnit) {
                    unitIdList.push(newUnitId.id)
                }
            } else if (unit && unit.subUnit) {
                unitIdList.push(unitId)
            }
        }
        log.info(`driverList unitIDList ${ JSON.stringify(unitIdList) }`)
        let indentStartTime = moment(startDate).format('YYYY-MM-DD HH:mm:ss');
        let indentEndTime = moment(endDate).format('YYYY-MM-DD HH:mm:ss');
        // leave driver
        let leaveDriver = await sequelizeObj.query(`
            SELECT ifnull(dl.driverId, -1) as driverId FROM driver_leave_record dl WHERE dl.status = 1 
            AND ( (? >= dl.startTime AND ? <= dl.endTime) 
            OR (? >= dl.startTime AND ? <= dl.endTime) 
            OR (? < dl.startTime AND ? > dl.endTime)
            ) GROUP BY dl.driverId
        `, { type: QueryTypes.SELECT, replacements: [indentStartTime, indentStartTime, indentEndTime, indentEndTime, indentStartTime, indentEndTime] })
        leaveDriver = leaveDriver.map(item => item.driverId)
        log.warn(`leave driverList ${ JSON.stringify(leaveDriver) }`)

        // 2023-07-13 loan out driver
        let loanOutDriver = await sequelizeObj.query(`
            SELECT ifnull(driverId, -1) as driverId FROM loan 
            WHERE ((? >= startDate AND ? <= endDate) 
            OR (? >= startDate AND ? <= endDate) 
            OR (? < startDate AND ? > endDate))
            and driverId is not null
            group by driverId
       `, { type: QueryTypes.SELECT, replacements: [indentStartTime, indentStartTime, indentEndTime, indentEndTime, indentStartTime, indentEndTime] })
        loanOutDriver = loanOutDriver.map(item => item.driverId)
        log.warn(`loan out driverList ${ JSON.stringify(loanOutDriver) }`)

        //Not within the specified range hoto driver
        let hotoDriverListByNotScope = await sequelizeObj.query(
            `select ifnull(driverId, -1) as driverId, startDateTime, endDateTime
            from hoto 
            where ((? >= startDateTime AND ? <= endDateTime) 
            OR (? >= startDateTime AND ? <= endDateTime) 
            OR (? < startDateTime AND ? > endDateTime))
            and driverId not in (select driverId from hoto 
                where ? >= startDateTime AND ? <= endDateTime
                and driverId is not null and status = 'Approved'
            ) and status = 'Approved'
            and driverId is not null group by driverId`,
        {
            type: QueryTypes.SELECT
            , replacements: [
                indentStartTime,
                indentStartTime,
                indentEndTime,
                indentEndTime,
                indentStartTime,
                indentEndTime,
                indentStartTime,
                indentEndTime
            ]
        })
        hotoDriverListByNotScope = hotoDriverListByNotScope.map(item => item.driverId)
        log.warn(`hoto Not within the specified range driverList ${ JSON.stringify(hotoDriverListByNotScope) }`)

        let hotoDriver = []
        if(dataType){
            // 2023-08-08 hoto driver
            // hotoDriver = await sequelizeObj.query(`
            //     SELECT driverId FROM hoto 
            //     WHERE driverId IS NOT NULL 
            //     AND (('${ indentStartTime }' >= startDateTime AND '${ indentStartTime }' <= endDateTime) 
            //     OR ('${ indentEndTime }' >= startDateTime AND '${ indentEndTime }' <= endDateTime) 
            //     OR ('${ indentStartTime }' < startDateTime AND '${ indentEndTime }' > endDateTime)
            //     OR '${ indentStartTime }' >= startDateTime) and status = 'Approved'
            //     GROUP BY driverId
            // `, { type: QueryTypes.SELECT })
            // hotoDriver = hotoDriver.map(item => item.driverId)
            // log.warn(`mb type task hoto driverList ${ JSON.stringify(hotoDriver) }`)
        }

        let excludeDriver = leaveDriver.concat(loanOutDriver).concat(hotoDriver).concat(hotoDriverListByNotScope)  
        excludeDriver = excludeDriver.map(item => item);
        excludeDriver = Array.from(new Set(excludeDriver))  
        log.warn(`Need to exclude the driver ${ JSON.stringify(excludeDriver) }`)
        let sql = `
            select dd.driverId, dd.driverName, dd.vehicleType, dd.contactNumber, dd.unitId, dd.overrideStatus from (
                select d.driverId, d.driverName,dc.vehicleType,d.contactNumber, IF(h.unitId is NULL,d.unitId, h.unitId) as unitId, d.overrideStatus 
                from driver d
                LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
                left join (
                    select ho.driverId, ho.unitId from hoto ho where ((? >= ho.startDateTime AND ? <= ho.endDateTime)) and ho.status = 'Approved'
                ) h ON h.driverId = d.driverId 
                where d.permitStatus != 'invalid'
                and (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)
        `
        let replacements = [indentStartTime, indentEndTime, moment(indentEndTime).format('YYYY-MM-DD')]
        if(vehicleType){
            sql += ` and FIND_IN_SET(?, dc.vehicleType)`
            replacements.push(vehicleType)
        }
        sql += ` ) dd where dd.unitId is not null`
        if(unitIdList.length > 0){
            sql += ` and dd.unitId in (?)`
            replacements.push(unitIdList)
        }
        if(excludeDriver.length > 0){
            sql += ` and dd.driverId not in (?)`
            replacements.push(`'${ excludeDriver.join("','") }'`)
        }
        sql += ` GROUP BY dd.driverId`
        console.log(sql)
        let driverList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return res.json(utils.response(1, driverList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
};

// 2023-11-28
module.exports.getVehicleListByTaskId = async function (req, res) {
    let userId = req.cookies.userId;
    let user = await User.findOne({ where: { userId: userId } })
    if(!user) {
        return res.json(utils.response(0, `User ${ userId } does not exist.`));
    }

    let taskId = req.body.taskId;
    let task = await Task.findByPk(taskId);
    if (task) {
        let dataFrom = task.dataFrom;
        let vehicleType = '';
        if (dataFrom == 'SYSTEM') {
            let systemTaskId = task.taskId;
            if(systemTaskId.includes('AT-')) systemTaskId = task.taskId.slice(3)
            let taskVehicleType = await sequelizeSystemObj.query(`
                SELECT j.vehicleType
                FROM job_task jt
                LEFT JOIN job j ON jt.tripId = j.id
                where jt.id=?
            `, { replacements: [systemTaskId], type: QueryTypes.SELECT });
            if (taskVehicleType && taskVehicleType.length > 0) {
                vehicleType = taskVehicleType[0].vehicleType;
            }
        } else if (dataFrom == 'MT-ADMIN') {
            let mtAdmin = await MtAdmin.findByPk(task.indentId);
            if (mtAdmin) {
                vehicleType = mtAdmin.vehicleType;
            }
        }
        let taskGroupId = task.groupId;
        let vehicleList = null;
        if (taskGroupId) {
            //mobile task Or customer task has groupId, mobile task no vehicleType
            vehicleList = await TaskUtils.getVehicleByGroup(task.purpose, true, user.unitId, vehicleType, task.indentStartTime, task.indentEndTime);
        } else {
            vehicleList = await TaskUtils.getVehicleList(task.purpose, vehicleType, task.indentStartTime, task.indentEndTime, null, task.hub, task.node,  null );
        }

        return res.json(utils.response(1, vehicleList));
    } else {
        return res.json(utils.response(0, `Task[${taskId}] does not exist.`));
    }
}

//2023-05-23 optimize sql
module.exports.getVehicleList = async function (req, res) {
    try {
        let { purpose, vehicleType, startDate, endDate, unitId, unit, subUnit, dataType } = req.body
        let vehicleList = await TaskUtils.getVehicleList(purpose, vehicleType, startDate, endDate, unitId, unit, subUnit,  dataType );
        return res.json(utils.response(1, vehicleList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
};

module.exports.getMtAdminList = async function (req, res) {
    try {
        let { userId, pageNum, pageLength, purpose, execution_date, created_date, hub, node, taskId, vehicleNo, driverName, endDateOrder, taskIdDateOrder, groupId } = req.body;
        let user = await User.findOne({ where: { userId: userId } })
        if(!user) return res.json(utils.response(0, `UserID ${ userId } does not exist!.`));  

        let pageList = await userService.getUserPageList(userId, 'MT-Admin')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let unitId = []
        let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
        unitId = dataList.unitIdList
        if((user.userType).toUpperCase() == 'CUSTOMER'){
            unitId = []
        } else {
            if(hub) {
                if(node) {
                    let unit = await Unit.findOne({ where: { unit: hub, subUnit: node } })
                    unitId = [unit.id]
                } else {
                    let unit = await Unit.findAll({ where: { unit: hub } })
                    let hubUnitIdList = unit.map(item => item.id)
                    unitId = unitId.filter(item => hubUnitIdList.includes(item))
                }
            } 
            // else {
            //     // let users = await TaskUtils.getUnitIdByUserId(userId);
            //     // if (users.unitId) {
            //     //     unitId.push(users.unitId)
            //     //     let unit = await TaskUtils.getUnitByUnitId(users.unitId)
            //     //     if (!unit.subUnit) {
            //     //         let newUnit = await Unit.findAll({ where: { unit: unit.unit } })
            //     //         unitId = newUnit.map(item => item.id)
            //     //     }
            //     // }
            //     let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
            //     unitId = dataList.unitIdList
            // }
        }
    
        log.info(`MtAdminList unitIdList ${ JSON.stringify(unitId) }`)
        let replacements = []
        let replacements2 = []
        let sql = `SELECT ? as operation, m.id, m.purpose, m.activityName, m.remarks, m.vehicleType, t.vehicleNumber, m.category, m.serviceMode, m.unitId, 
        m.reportingLocation, m.destination, m.startDate, m.endDate, d.contactNumber, d.driverName as driver_name, t.taskId, m.cancelledCause, m.cancelledDateTime, m.amendedBy, uu.fullName as amendedByUsername ,
        t.driverStatus, t.mobileStartTime, t.hub, t.node, t.groupId 
        from task t
        LEFT JOIN mt_admin m ON t.indentId = m.id
        LEFT JOIN (select driverId, nric, driverName, contactNumber from driver union all select driverId, nric, driverName, contactNumber from driver_history) d ON d.driverId = t.driverId 
        LEFT JOIN user u on u.userId = m.creator 
        LEFT JOIN user uu on uu.userId = m.amendedBy 
        where m.dataType != 'mb' `//t.indentId is not null and 
        replacements.push(operationList)
        let sql2 = `SELECT COUNT(DISTINCT m.id) total from task t 
        LEFT JOIN mt_admin m ON t.indentId = m.id
        LEFT JOIN (select driverId, nric, driverName, contactNumber from driver union all select driverId, nric, driverName, contactNumber from driver_history) d ON d.driverId = t.driverId 
        LEFT JOIN user u on u.userId = m.creator 
        LEFT JOIN user uu on uu.userId = m.amendedBy 
        where m.dataType != 'mb' `

        if (unitId.length > 0) {
            sql += ` and m.unitId in(?)`
            sql2 += ` and m.unitId in(?)`
            replacements.push(unitId)
            replacements2.push(unitId)
        }

        if(groupId) {
            // sql += ` and (u.unitId = ? and u.userType = 'CUSTOMER')`
            // sql2 += ` and (u.unitId = ? and u.userType = 'CUSTOMER')`
            sql += ` and t.groupId = ?`
            sql2 += ` and t.groupId = ?`
            replacements.push(groupId)
            replacements2.push(groupId)
        }

        if((user.userType).toUpperCase() == 'CUSTOMER') {
            //2023-06-15 Same groupId as the logged-in person
            // let newUser = await User.findAll({ where: { unitId: user.unitId, userType: 'CUSTOMER' } })
            // let userIdList = newUser.map(item => item.userId);
            // userIdList = Array.from(new Set(userIdList))
            // sql += ` and m.creator in(?)`
            // sql2 += ` and m.creator in(?)`
            sql += ` and t.groupId IS NOT NULL and t.groupId = ? `
            sql2 += ` and t.groupId IS NOT NULL and t.groupId = ? `
            replacements.push(user.unitId)
            replacements2.push(user.unitId)
            // replacements.push(userIdList)
        }

        if (created_date) {
            sql += ` and DATE_FORMAT(m.createdAt,'%Y-%m-%d') = ?`
            sql2 += ` and DATE_FORMAT(m.createdAt,'%Y-%m-%d') = ?`
            replacements.push(created_date)
            replacements2.push(created_date)
        }

        if(purpose) {
            sql += ` and m.purpose = ?`
            sql2 += ` and m.purpose = ?`
            replacements.push(purpose)
            replacements2.push(purpose)
        }

        if (execution_date) {
            if (execution_date.indexOf('~') != -1) {
                const dates = execution_date.split(' ~ ')
                sql += ` and (m.endDate >= ? and m.endDate <= ?)`
                sql2 += ` and (m.endDate >= ? and m.endDate <= ?)`
                replacements.push(dates[0])
                replacements.push(dates[1])
                replacements2.push(dates[0])
                replacements2.push(dates[1])
            } else {
                sql += ` and m.endDate = ?`
                sql2 += ` and m.endDate = ?`
                replacements.push(execution_date)
                replacements2.push(execution_date)
            }
        }
        if(taskId) {
            sql += ` and t.taskId like ?`
            sql2 += ` and t.taskId like ?`
            replacements.push('%' + taskId + '%')
            replacements2.push('%' + taskId + '%')
        }

        if(vehicleNo) {
            sql += ` and t.vehicleNumber like ?`
            sql2 += ` and t.vehicleNumber like ?`
            replacements.push('%' + vehicleNo + '%')
            replacements2.push('%' + vehicleNo + '%')
        }

        if(driverName) {
            sql += ` and d.driverName like ?`
            sql2 += ` and d.driverName like ?`
            replacements.push('%' + driverName + '%')
            replacements2.push('%' + driverName + '%')
        }
        let orderSql = [];
        if (endDateOrder) {
            orderSql.push(' m.endDate ' + `${ endDateOrder }`)
        } 
        if(taskIdDateOrder) {
            orderSql.push(' m.id ' + `${ taskIdDateOrder }`)
        }
        if(orderSql.length > 0) {
            sql += ` group by m.id ORDER BY ${ orderSql.join(' , ') }`
        } else {
            sql += ` group by m.id ORDER BY m.id desc`
        }
        let countResult = await sequelizeObj.query(sql2, { replacements: replacements2, type: QueryTypes.SELECT })
        let totalRecord = countResult[0].total
        pageNum = pageNum ?? 0
        pageLength = pageLength ?? 10
        sql += ` limit ? , ?`
        replacements.push(Number(pageNum))
        replacements.push(Number(pageLength))
        let pageResult = await sequelizeObj.query(
            sql,
            {
                type: QueryTypes.SELECT
                , replacements: replacements,
            }
        );
        return res.json({ data: pageResult, recordsFiltered: totalRecord, recordsTotal: totalRecord });
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
};

module.exports.getMtAdminByMtAdminId = async function (req, res) {
    try {
        let MtAdminID = req.body.mtAdminId;
        let list = await sequelizeObj.query(
            ` SELECT m.id, m.purpose, m.activityName, m.remarks, m.vehicleType, t.vehicleNumber, m.category, m.serviceMode, m.unitId,t.hub, t.node, m.creator,
            m.reportingLocation, m.destination, m.startDate, m.endDate, d.contactNumber, d.driverId, d.driverName, t.taskId, t.driverStatus, t.mobileStartTime from mt_admin m 
            LEFT JOIN task t ON t.indentId = m.id
            LEFT JOIN driver d ON d.driverId = t.driverId 
            LEFT JOIN vehicle v ON t.vehicleNumber = v.vehicleNo 
            where m.dataType != 'mb' and t.indentId is not null and t.mobileStartTime is null and m.id = ?`,
            {
                type: QueryTypes.SELECT
                , replacements: [MtAdminID]
            }
        );
        if (list && list.length > 0) {
            return res.json(utils.response(1, list[0]));
        } else {
            return res.json(utils.response(0, false));
        }
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
};

module.exports.createMtAdmin = async function (req, res) {
    try {
        let mtAdmin = req.body.mtAdmin;
        let unit = null
        let driverState = await TaskUtils.verifyDriverLeave(mtAdmin.driverId, mtAdmin.startDate, mtAdmin.endDate);
        if(!driverState) return res.json(utils.response(0, 'The creation failed. The driver is in the leave state.'));
        let vehicleState = await TaskUtils.verifyVehicleLeave(mtAdmin.vehicleNumber, mtAdmin.startDate, mtAdmin.endDate);
        if(!vehicleState) return res.json(utils.response(0, 'The creation failed. The vehicle is in the leave state.'));
        let pickupDestination = await TaskUtils.GetDestination(mtAdmin.reportingLocation);
        let dropoffDestination = await TaskUtils.GetDestination(mtAdmin.destination);
        if(!pickupDestination[0] || !dropoffDestination[0]) return res.json(utils.response(0, 'Please reselect. The location does not exist.'));
        mtAdmin.reportingLocationLat = pickupDestination[0].lat ? pickupDestination[0].lat : 0;
        mtAdmin.reportingLocationLng = pickupDestination[0].lng ? pickupDestination[0].lng : 0;

        mtAdmin.destinationLat = dropoffDestination[0].lat ? dropoffDestination[0].lat : 0;
        mtAdmin.destinationLng = dropoffDestination[0].lng ? dropoffDestination[0].lng : 0;
        mtAdmin.creator = req.cookies.userId;

        let state = await TaskUtils.verifyLocation(mtAdmin.reportingLocation, mtAdmin.destination);
        if(!state) return res.json(utils.response(0, 'Reporting Location, Destination does not exist, please select again.'));
        let __taskId = null;
        await sequelizeObj.transaction(async transaction => {
            let _user = await User.findOne({ where: { userId: req.cookies.userId } })
            if((_user.userType).toUpperCase() != 'CUSTOMER') unit = await Unit.findOne({ where: { id: mtAdmin.unitId } })
            let state = await MtAdmin.create(mtAdmin);
            let groupId = null;
            // Create task by mt-admin
            let taskIdPrefix = 'MT-'
            if((_user.userType).toUpperCase() == 'CUSTOMER') {
                taskIdPrefix = 'CU-'
                let userByDriver = await User.findOne({ where: { driverId: mtAdmin.driverId } })
                if((userByDriver.role).toUpperCase() == 'DV' || (userByDriver.role).toUpperCase() == 'LOA'){
                    groupId = userByDriver.unitId
                } else {
                    groupId = _user.unitId
                }
                log.warn(`mt_admin groupId ${ groupId }`)
            }
            let task = {
                taskId: taskIdPrefix + state.id,
                dataFrom: 'MT-ADMIN',
                driverId: mtAdmin.driverId,
                vehicleNumber: mtAdmin.vehicleNumber,
                driverStatus: 'waitcheck',
                vehicleStatus: 'waitcheck',
                indentId: state.id,
                purpose: mtAdmin.purpose,
                activity: mtAdmin.activityName,
                creator: req.cookies.userId,
                indentStartTime: mtAdmin.startDate,
                indentEndTime: mtAdmin.endDate,
                pickupDestination: mtAdmin.reportingLocation,
                dropoffDestination: mtAdmin.destination,
                pickupGPS: `${mtAdmin.reportingLocationLat ? mtAdmin.reportingLocationLat : '0.0'},${mtAdmin.reportingLocationLng ? mtAdmin.reportingLocationLng : '0.0'}`,
                dropoffGPS: `${mtAdmin.destinationLat ? mtAdmin.destinationLat : '0.0'},${mtAdmin.destinationLng ? mtAdmin.destinationLng : '0.0'}`,
                hub: unit ? unit.unit : '-',
                node: unit ? unit.subUnit : '-',
                groupId
            }
            await Task.create(task);  
            __taskId = task.taskId
            await TaskUtils.initOperationRecord(req.cookies.userId, task.taskId, '', mtAdmin.driverId, '', mtAdmin.vehicleNumber, 'mt-admin')

            // Update vehicle-relation db
            if(mtAdmin.vehicleNumber) await vehicleService.createVehicleRelation(mtAdmin.driverId, mtAdmin.vehicleNumber)
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'MT-Admin',
                businessId: __taskId,
                optType: 'New',
                afterData: `${ JSON.stringify([task, mtAdmin]) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'create the mtadmin task.'
            })
        }).catch(error => {
            throw error
        })
        await FirebaseService.createFirebaseNotification2([{
            taskId: __taskId,
            token: '',
            driverId: mtAdmin.driverId,
            vehicleNo: mtAdmin.vehicleNumber
        }], 'INFO', 'New task assigned!')
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error('createMtAdmin fail: ' + error);
        return res.json(utils.response(0, error));
    }
};

module.exports.deleteMtAdminByMtAdminId = async function (req, res) {
    try {
        let mtAdminID = req.body.mtAdminId;
        let cancelledCause = req.body.cancelledCause;
        let taskId = req.body.taskId;
        let task = await Task.findOne({where: { indentId: mtAdminID }})
        if(task.mobileStartTime || (task.driverStatus).toLowerCase() == 'cancelled') return res.json(utils.response(0, `The operation failed because the state of the data has changed.`));
        let taskObj = await TaskUtils.getDriverIdAndVehicleNoByTaskId(mtAdminID);
        if(taskObj) {
            if(taskObj.driverId){
                await FirebaseService.createFirebaseNotification2([{
                    taskId: taskId,
                    token: '',
                    driverId: taskObj.driverId,
                    vehicleNo: taskObj.vehicleNumber
                }], 'INFO', 'Task cancelled!')
            }
        } 
        await sequelizeObj.transaction(async transaction => {
            let mtAdmin = await MtAdmin.findOne({ where: { id: mtAdminID  } })
            await Task.update({ driverStatus: 'Cancelled', vehicleStatus: 'Cancelled' }, { where: { indentId: mtAdminID  } });
            await MtAdmin.update({ cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), cancelledCause: cancelledCause, amendedBy: req.cookies.userId }, { where: { id: mtAdminID  } });
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'MT-Admin',
                businessId: taskId,
                optType: 'Cancel',
                beforeData: `${ JSON.stringify([{driverStatus: task.driverStatus, vehicleStatus: task.vehicleStatus},{cancelledDateTime: mtAdmin.cancelledDateTime, cancelledCause: mtAdmin.cancelledCause, amendedBy: mtAdmin.amendedBy}]) }`,
                afterData: `${ JSON.stringify([{driverStatus: 'Cancelled', vehicleStatus: 'Cancelled'},{cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), cancelledCause: cancelledCause, amendedBy: req.cookies.userId}]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'cancel the mtadmin task.'
            })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
};

module.exports.updateMtAdminByMtAdminId = async function (req, res) {
    try {
        let mtAdmin = req.body.mtAdmin;
        let taskId = req.body.taskId;
        let businessType = req.body.businessType;
        let unit = null;
        let mtAdminId = mtAdmin.id
        let loanObjStatus = await loan.findOne({where: { taskId: 'AT-'+mtAdminId }});
        let loanObj2Status = await loanRecord.findOne({where: { taskId: 'AT-'+mtAdminId }});
        if(loanObjStatus || loanObj2Status) {
            if(loanObjStatus) {
                if(loanObjStatus.actualStartTime || loanObjStatus.actualEndTime) {
                    return res.json(utils.response(0, 'The operation failed because the current data status has changed.'));
                }
            }
            if(loanObj2Status) {
                return res.json(utils.response(0, 'The operation failed because the current data status has changed.'));
            }
        }
        let task = await Task.findOne({where: { indentId: mtAdminId }})
        if(task) {
            if(task.mobileStartTime || (task.vehicleStatus).toLowerCase() == 'cancelled') return res.json(utils.response(0, `Operation failed.The status of the task has changed.`));
        } 
        let driverState = await TaskUtils.verifyDriverLeave(mtAdmin.driverId, mtAdmin.startDate, mtAdmin.endDate);
        if(!driverState) return res.json(utils.response(0, 'Fail to modify. The driver is in the leave state.'));
        let vehicleState = await TaskUtils.verifyVehicleLeave(mtAdmin.vehicleNumber, mtAdmin.startDate, mtAdmin.endDate);
        if(!vehicleState) return res.json(utils.response(0, 'The creation failed. The vehicle is in the leave state.'));
        let taskObj = await TaskUtils.getDriverIdAndVehicleNoByTaskId(mtAdminId);
        let pickupDestination = await TaskUtils.GetDestination(mtAdmin.reportingLocation);
        let dropoffDestination = await TaskUtils.GetDestination(mtAdmin.destination);
        if(!pickupDestination[0] || !dropoffDestination[0]) return res.json(utils.response(0, 'Please reselect. The location does not exist.'));
        mtAdmin.reportingLocationLat = pickupDestination[0].lat ? pickupDestination[0].lat : 0;
        mtAdmin.reportingLocationLng = pickupDestination[0].lng ? pickupDestination[0].lng : 0;
        mtAdmin.destinationLat = dropoffDestination[0].lat ? dropoffDestination[0].lat : 0;
        mtAdmin.destinationLng = dropoffDestination[0].lng ? dropoffDestination[0].lng : 0;
        mtAdmin.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss');
        
        let state = await TaskUtils.verifyLocation(mtAdmin.reportingLocation, mtAdmin.destination);
        if(!state) return res.json(utils.response(0, 'Reporting Location, Destination does not exist, please select again.'));
        if(taskObj) {
            if(taskObj.driverId != mtAdmin.driverId) {
                await FirebaseService.createFirebaseNotification2([{
                    taskId,
                    token: '',
                    driverId: taskObj.driverId,
                    vehicleNo: taskObj.vehicleNumber
                }], 'INFO', 'Task cancelled!')
            }
        }
        if(!taskObj){
            if(mtAdmin.dataType == 'mb') {
                taskId = 'AT-' + mtAdminId
            } else {
                taskId = 'MT-' + mtAdminId
                mtAdmin.driverNum = 1;
                mtAdmin.needVehicle = 1;
            } 
        } 

        await sequelizeObj.transaction(async transaction => {
            let _user = await User.findOne({ where: { userId: req.cookies.userId } })
            if((_user.userType).toUpperCase() != 'CUSTOMER') unit = await Unit.findOne({ where: { id: mtAdmin.unitId } })
            let oldMtAdminObj = await MtAdmin.findOne({ where: { id: mtAdminId } })
            await MtAdmin.update(mtAdmin, { where: { id: mtAdmin.id } });
            let mtAdminObj = await MtAdmin.findOne({ where: { id: mtAdminId } })
            let task = null;
            if((mtAdminObj.driverNum > 0 && mtAdminObj.needVehicle > 0)) {
                let loanByTaskId = await loan.findOne({ where: { [Op.or]: [
                    { taskId: `AT-${ mtAdminId }` },
                    { taskId: `MT-${ mtAdminId }` }
                  ] } })
                if(loanByTaskId) {
                    await loan.destroy({ where: { [Op.or]: [
                        { taskId: `AT-${ mtAdminId }` },
                        { taskId: `MT-${ mtAdminId }` }
                        ] } });
                }
                task = {
                    taskId: taskId,
                    dataFrom: 'MT-ADMIN',
                    driverId: mtAdmin.driverId ? mtAdmin.driverId : null, 
                    vehicleNumber: mtAdmin.vehicleNumber ? mtAdmin.vehicleNumber : null, 
                    activity: mtAdmin.activityName,
                    indentId: mtAdmin.id,
                    purpose: mtAdmin.purpose,
                    // creator: req.cookies.userId,
                    indentStartTime: mtAdmin.startDate,
                    indentEndTime: mtAdmin.endDate,
                    pickupDestination: mtAdmin.reportingLocation,
                    dropoffDestination: mtAdmin.destination,
                    pickupGPS: `${mtAdmin.reportingLocationLat ? mtAdmin.reportingLocationLat : '0.0'},${mtAdmin.reportingLocationLng ? mtAdmin.reportingLocationLng : '0.0'}`,
                    dropoffGPS: `${mtAdmin.destinationLat ? mtAdmin.destinationLat : '0.0'},${mtAdmin.destinationLng ? mtAdmin.destinationLng : '0.0'}`,
                    hub: unit ? unit.unit : '-',
                    node: unit ? unit.subUnit : '-',
                    updatedAt: moment().format('YYYY-MM-DD HH:mm:ss')
                }
                if(taskObj) {
                    await Task.update(task, { where: { taskId: task.taskId } });
                } else {
                    task.driverStatus = task.driverId ? 'waitcheck' : null;
                    task.vehicleStatus = 'waitcheck';
                    task.creator = req.cookies.userId;
                    await Task.create(task);
                }
                await OperationRecord.create({
                    id: null,
                    operatorId: req.cookies.userId,
                    businessType: businessType,
                    businessId: task.taskId,
                    optType: 'Edit',
                    beforeData: `${ JSON.stringify([taskObj ? taskObj : '',oldMtAdminObj]) }`,
                    afterData: `${ JSON.stringify([task,mtAdminObj]) }`,
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: 'edit the task.'
                })
            } else {
                let checkTask2 = await Task.findOne({ where: { [Op.or]: [
                    { taskId: `AT-${ mtAdminId }` },
                    { taskId: `MT-${ mtAdminId }` }
                  ] } })
                if(checkTask2) {
                await Task.destroy({ where: { [Op.or]: [
                    { taskId: `AT-${ mtAdminId }` },
                    { taskId: `MT-${ mtAdminId }` }
                    ] } });
                }
                if(mtAdminObj.driverId || mtAdminObj.vehicleNumber){
                    let loanByTaskId = await loan.findOne({ where: { [Op.or]: [
                        { taskId: `AT-${ mtAdminId }` },
                        { taskId: `MT-${ mtAdminId }` }
                      ] } })
                    if(loanByTaskId) {
                        if(loanByTaskId.driverId && !mtAdminObj.driverId) {
                            await loan.destroy({ where: { [Op.or]: [
                                { taskId: `AT-${ mtAdminId }` },
                                { taskId: `MT-${ mtAdminId }` }
                              ] } });
                        }
                        if(loanByTaskId.vehicleNo && !mtAdminObj.vehicleNumber) {
                            await loan.destroy({ where: { [Op.or]: [
                                { taskId: `AT-${ mtAdminId }` },
                                { taskId: `MT-${ mtAdminId }` }
                              ] } });
                        }
                    }
                    let loanByTaskId2 = await loan.findOne({ where: { [Op.or]: [
                        { taskId: `AT-${ mtAdminId }` },
                        { taskId: `MT-${ mtAdminId }` }
                      ] } })
                    if(loanByTaskId2) {
                        await loan.update({  
                            taskId: 'AT-'+mtAdminId,
                            indentId: mtAdminObj.indentId, 
                            driverId: mtAdminObj.driverId ? mtAdminObj.driverId : null,
                            vehicleNo: mtAdminObj.vehicleNumber ? mtAdminObj.vehicleNumber : null, 
                            startDate: mtAdminObj.startDate, 
                            endDate: mtAdminObj.endDate, 
                            groupId: -1,
                            unitId: mtAdmin.unitId,
                            activity: mtAdmin.activityName,
                            purpose: mtAdmin.purpose,
                            creator: req.cookies.userId
                        }, { where: { taskId } })
                    } else {
                        await loan.create({  
                            taskId: 'AT-'+mtAdminId,
                            indentId: mtAdminObj.indentId, 
                            driverId: mtAdminObj.driverId ? mtAdminObj.driverId : null,
                            vehicleNo: mtAdminObj.vehicleNumber ? mtAdminObj.vehicleNumber : null, 
                            startDate: mtAdminObj.startDate, 
                            endDate: mtAdminObj.endDate, 
                            groupId: -1,
                            unitId: mtAdmin.unitId,
                            activity: mtAdmin.activityName,
                            purpose: mtAdmin.purpose,
                            creator: req.cookies.userId
                        })
                    }
                    let newLoanByTaskId2 = await loan.findOne({ where: { [Op.or]: [
                        { taskId: `AT-${ mtAdminId }` },
                        { taskId: `MT-${ mtAdminId }` }
                      ] } })
                    await TaskUtils.initOperationRecord(req.cookies.userId, 'AT-'+mtAdminId, loanByTaskId ? loanByTaskId.driverId : null, mtAdmin.driverId, loanByTaskId ? loanByTaskId.vehicleNo : null, mtAdmin.vehicleNumber, 'atms task assign')
                    await OperationRecord.create({
                        id: null,
                        operatorId: req.cookies.userId,
                        businessType: businessType,
                        businessId: 'AT-'+mtAdminId,
                        optType: 'Edit',
                        beforeData: `${ JSON.stringify([loanByTaskId ? loanByTaskId : '', oldMtAdminObj]) }`,
                        afterData: `${ JSON.stringify([newLoanByTaskId2, mtAdminObj]) }`,
                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remarks: 'edit the loan.'
                    })
                }
            }
        
            if(!taskObj){
                if(mtAdminObj.driverNum > 0 && mtAdminObj.needVehicle > 0) await TaskUtils.initOperationRecord(req.cookies.userId, taskId, null, mtAdmin.driverId, null, mtAdmin.vehicleNumber, businessType)
            }
            // Update vehicle-relation db
            if(mtAdmin.vehicleNumber) await vehicleService.createVehicleRelation(mtAdmin.driverId, mtAdmin.vehicleNumber)
        }).catch(error => {
            throw error
        })
        if(task) {
            if(mtAdmin.driverId != task.driverId) {
                await FirebaseService.createFirebaseNotification2([{
                    taskId,
                    token: '',
                    driverId: mtAdmin.driverId,
                    vehicleNo: mtAdmin.vehicleNumber
                }], 'INFO', 'New task assigned!')
            }
        }
        if(taskObj){
            if(mtAdmin.driverId) {
                await FirebaseService.createFirebaseNotification2([{
                    taskId,
                    token: '',
                    driverId: mtAdmin.driverId,
                    vehicleNo: mtAdmin.vehicleNumber
                }], 'INFO', 'Task update!')
                if(taskObj.driverId != mtAdmin.driverId || taskObj.vehicleNumber != mtAdmin.vehicleNumber) TaskUtils.initOperationRecord(req.cookies.userId, taskId, taskObj.driverId, mtAdmin.driverId, taskObj.vehicleNumber, mtAdmin.vehicleNumber, businessType)
            }
        } 
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error('updateMtAdmin fail: ' + error);
        return res.json(utils.response(0, error));
    }
};

module.exports.verifyVehicleType = async function (req, res) {
    try {
        let startDate = req.body.startDate;
        let endDate = req.body.endDate;
        let vehicleType = req.body.vehicleType;
        let hub = req.body.hub && req.body.hub != '' && req.body.hub != 'null'  ? req.body.hub : null;
        let node = req.body.node && req.body.node != '' && req.body.node != 'all' && req.body.node != 'null' ? req.body.node : null;
        let unitId = req.body.unitId;
        if(unitId){
            let unit = await Unit.findOne({ where: { id: unitId } });
            if(unit){
                hub = unit.unit;
                node = unit.subUnit;
            }
        }
        let unitIdList = []
        if(hub){
            if(node){
                let unit2 = await Unit.findOne({ where: { unit: hub, subUnit: node } })
                unitIdList.push(unit2.id)
            } else {
                let unit = await Unit.findAll({ where: { unit: hub } })
                unitIdList = unit.map(item => item.id)
            }
        }   
        let replacements = []
        let sql = `
             select * from (
                SELECT v.vehicleType, IF(hh.toHub is NULL, v.unitId, hh.unitId) as unitId, hh.startDateTime, hh.endDateTime
                FROM vehicle v
                LEFT JOIN unit u ON u.id = v.unitId
                left join (
                select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId, ho.startDateTime, ho.endDateTime  from hoto ho where 
                ((? >= ho.startDateTime AND ? <= ho.endDateTime) 
                OR (? >= ho.startDateTime AND ? <= ho.endDateTime) 
                OR (? < ho.startDateTime AND ? > ho.endDateTime))
                and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = v.vehicleNo
                where v.vehicleType = ?
             ) vv WHERE 1=1 and ((? >= vv.startDateTime and ? <= vv.endDateTime)
             OR (vv.startDateTime is null)
             ) 
        `
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(vehicleType)
        replacements.push(moment(startDate).format('YYYY-MM-DD HH:mm:ss'))
        replacements.push(moment(endDate).format('YYYY-MM-DD HH:mm:ss'))
        if (unitIdList.length > 0) {
            sql += ` and vv.unitId IN (?)`
            replacements.push(unitIdList)
        } 
        let vehicleList = await sequelizeObj.query(
            sql,
            {
                replacements: replacements,
                type: QueryTypes.SELECT
            }
        )
        if(vehicleList.length > 0) {
            return res.json(utils.response(1, true));
        } else {
            return res.json(utils.response(1, false));
        }
    } catch(error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
};

module.exports.getVehicleTypeByGroup = async function (req, res) {
    try {
        let { purpose, editUserId, startDate, endDate } = req.body;
        let newUserId = editUserId ? editUserId : req.cookies.userId;
        let user = await User.findOne({ where: { userId: newUserId } })
        if(!user) return res.json(utils.response(0, `User ${ newUserId } does not exist.`));
        log.info(`user groupId ${ JSON.stringify(user.unitId) }`)
        let vehicleList = await TaskUtils.getVehicleByGroup(purpose, true, user.unitId, null, startDate, endDate);
        let vehicleType = vehicleList.map(item => item.vehicleType);
        vehicleType = Array.from(new Set(vehicleType))
        return res.json(utils.response(1, vehicleType));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getVehicleNoByGroup = async function (req, res) {
    try {
        let { purpose, editUserId, vehicleType, startDate, endDate } = req.body;
        let newUserId = editUserId ? editUserId : req.cookies.userId;
        let user = await User.findOne({ where: { userId: newUserId } })
        if(!user) return res.json(utils.response(0, `User ${ newUserId } does not exist.`));
        log.info(`user groupId ${ JSON.stringify(user.unitId) }`)
        let vehicleList = await TaskUtils.getVehicleByGroup(purpose, true, user.unitId, vehicleType, startDate, endDate);
        let vehicleNoList = vehicleList.map(item => item);
        vehicleNoList = Array.from(new Set(vehicleNoList))
        return res.json(utils.response(1, vehicleNoList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverDatatByGroup = async function (req, res) {
    try {
        let { editUserId, vehicleType, startDate, endDate } = req.body;
        let newUserId = editUserId ? editUserId : req.cookies.userId;
        let user = await User.findOne({ where: { userId: newUserId } })
        if(!user) return res.json(utils.response(0, `User ${ newUserId } does not exist.`));
        log.info(`user groupId ${ JSON.stringify(user.unitId) }`)
        let driverList = await TaskUtils.getDriverByGroup(true, user.unitId, vehicleType, startDate, endDate)
        return res.json(utils.response(1, driverList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getUserByUserId = async function (req, res) {
    try {
        let creator = req.body.creator;
        let user = await User.findOne({ where: { userId: creator } })
        if(!user) return res.json(utils.response(0, `User ${ creator } does not exist.`));
        return res.json(utils.response(1, user));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}