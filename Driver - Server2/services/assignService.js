const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');
const SOCKET = require('../socket/socket');
const ACTIVEMQ = require('../activemq/activemq');

const moment = require('moment');
let axios = require('axios');
const _ = require('lodash');

const FirebaseService = require('../firebase/firebase');

const { Sequelize, Op, QueryTypes, where } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { Route } = require('../model/route');
const { RouteWaypoint } = require('../model/routeWaypoint');
const { Driver } = require('../model/driver');
const { CheckList } = require('../model/checkList');
const { DriverTask } = require('../model/driverTask');
const { DriverPosition } = require('../model/driverPosition');
const { VehicleRelation } = require('../model/vehicleRelation.js');
const { User } = require('../model/user.js');
const { UserGroup } = require('../model/userGroup.js');
const { Unit } = require('../model/unit.js');
const { Friend } = require('../model/friend.js');

const { loan } = require('../model/loan.js');

const groupService = require('./groupService');
const userService = require('./userService');
const unitService = require('./unitService');
const vehicleService = require('./vehicleService');
const reportCreatorService = require('./reportCreatorService');
const assignService2 = require('./assignService2');
const { UserZone } = require('../model/userZone.js');
const { Vehicle } = require('../model/vehicle.js');
const { Task } = require('../model/task.js');

const _SystemDriver = require('../model/system/driver');
const _SystemVehicle = require('../model/system/vehicle');
const _SystemLocation = require('../model/system/location');
const _SystemTask = require('../model/system/task');
const _SystemRequest = require('../model/system/request');
const _SystemGroup = require('../model/system/group');

const { MtAdmin } = require('../model/mtAdmin');
const { OperationRecord } = require('../model/operationRecord');
const { loanRecord } = require('../model/loanRecord');
const { MobileTrip } = require('../model/mobileTrip.js');
const { UserBase } = require('../model/userBase.js');
const { NGTSResp } = require('../model/system/ngtsResp.js');

const purposeList = ['avi', 'wpt', 'mpt', 'pm']

let TaskUtils = {
    getDriverIdAndVehicleNoByTaskId: async function (taskId) {
        let taskObj = await Task.findOne({where: { taskId: taskId }}) 
        return taskObj;
    },
    getLoanByTaskId: async function(taskId) {
        let loanObj = await loan.findOne({ where: { taskId: taskId } });
        return loanObj;
    },
    getUnitByUnitId: async function (unitId) {
        let unitHubList = await Unit.findAll({where: { id: unitId }});
        return unitHubList
    },
    getUnitByUnitId2: async function (unitId) {
        let unit = await Unit.findAll({ where: { id: unitId } })
        return unit[0];
    },
    getTypeOfVehicle: async function () {
        const typeOfVehicleList = await sequelizeSystemObj.query(
            `SELECT DISTINCT typeOfVehicle from contract_rate order by typeOfVehicle`,
            {
                type: QueryTypes.SELECT
            }
        );
        return typeOfVehicleList
    },    
    assignTaskBySystem: async function (body) {
        let { taskId, driver, vehicle, status, systemStatus } = body;
        try {
            await sequelizeSystemObj.transaction(async transaction => {
                if (driver && driver !='') {
                    if(status){
                        await _SystemTask.Task.update({ driverId: driver.driverId }, { where: { id: taskId } })
                    } else {
                        await _SystemTask.Task.update({ taskStatus: 'Assigned', driverId: driver.driverId }, { where: { id: taskId } })
                    }
                    let sysDriver = await _SystemDriver.Driver.findOne({ where: { taskId: taskId } })
                    let driverObj = {
                        taskId,
                        driverId: driver.driverId,
                        name: driver.driverName,
                        nric: driver.nric,
                        contactNumber: driver.contactNumber,
                        permitType: driver.permitType,
                        driverFrom: 'transport'
                    }
                    if(!sysDriver) {
                        driverObj.status = systemStatus ? systemStatus : 'Assigned'
                    }
                    await _SystemDriver.Driver.upsert(driverObj)
                } else {
                    if(!status){
                        await _SystemTask.Task.update({ taskStatus: 'Assigned' }, { where: { id: taskId } })   
                    } 
                }
                if(vehicle && vehicle != ''){
                    let sysVehicle = await _SystemVehicle.Vehicle.findOne({ where: { taskId: taskId } })
                    let vehicleObj = {
                        taskId,
                        vehicleNumber: vehicle.vehicleNo,
                        vehicleType: vehicle.vehicleType,
                        permitType: vehicle.permitType,
                    }
                    if(!sysVehicle) {
                        vehicleObj.vehicleStatus = systemStatus ? systemStatus : 'available'
                    }
                    await _SystemVehicle.Vehicle.upsert(vehicleObj)
                }
            }).catch(error => {
                throw error
            })
            return {
                code: 1
            };
        } catch(error) {
         log.error(error)
            return {
                code: 0,
                message: error
            }; 
        }        
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
    verifyLocation: async function (reportingLocation, destination) {
        let location = [reportingLocation, destination];
        let newLocation = location.map(loc => { return loc.toLowerCase() });
        newLocation = Array.from(new Set(newLocation));
        let locationList = await TaskUtils.GetDestination(newLocation);
        if(locationList.length > 0) locationList = Array.from(new Set(locationList.map(loc2 => { return loc2.locationName.toLowerCase() })));
        if(newLocation.length == 1){
            if(locationList.length > 0) {
                return true
            } else {
                return false
            }
        } else if(newLocation.length > 1) {
            if(locationList.length > 1) {
                return true
            } else {
                return false
            }
        } else {
            return false
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
        return unitList.map(unit => unit.id);
    },
    verifyDriverLeave: async function (driverId, startDate, endDate) {
        let params = []; 
        if (endDate == null || endDate == '' || endDate == 'null') {
            params = [startDate, startDate];
        } else {
            params = [startDate, startDate, endDate, endDate,startDate, endDate];
        }
        let leaveDriverIds = await sequelizeObj.query(`
            SELECT * FROM driver_leave_record WHERE status = 1 and driverId = '${ driverId }'   
            AND ( ${endDate == null || endDate == '' || endDate == 'null' ? '(? >= startTime AND ? <= endTime)' 
            : '(? >= startTime AND ? <= endTime) OR (? >= startTime AND ? <= endTime) OR (? < startTime AND ? > endTime) '}
            )
        `, { type: QueryTypes.SELECT, replacements: params });
        log.info(`leave driver ${ JSON.stringify(leaveDriverIds.length) }`)
        if(leaveDriverIds.length > 0) {
            return false
        } else {
            return true
        }
    },
    verifyVehicleLeave: async function (vehicleNo, startDate, endDate) {
        let params = []; 
        if (endDate == null || endDate == '' || endDate == 'null') {
            params = [startDate, startDate];
        } else {
            params = [startDate, startDate, endDate, endDate,startDate, endDate];
        }
        let leaveVehicleNos = await sequelizeObj.query(`
            SELECT * FROM vehicle_leave_record WHERE status = 1 and vehicleNo = '${ vehicleNo }' 
            AND ( ${endDate == null || endDate == '' || endDate == 'null' ? '(? >= startTime AND ? <= endTime)' 
            : '(? >= startTime AND ? <= endTime) OR (? >= startTime AND ? <= endTime) OR (? < startTime AND ? > endTime) '}
            )
        `, { type: QueryTypes.SELECT, replacements: params });
        if(leaveVehicleNos.length > 0) {
            return false
        } else {
            return true
        }
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
    getClosestHubNodeListByLocation: async function (locationName, groupName) {
        const checkParams = async function (locationName, groupName) {
            // Check params null
            if (!locationName || !groupName) {
                let errorMessage = `getClosestHubNodeListByLocation: Location Name or Group Name is empty! (LocationName: ${ locationName }, groupName: ${ groupName })`
                log.error(errorMessage)
                throw errorMessage
            }
    
            // check locationName exist
            let location = await sequelizeSystemObj.query(` SELECT locationName, lat, lng FROM location WHERE locationName = ? `, { type: QueryTypes.SELECT, replacements: [ locationName ] })
            if (!location || !location.length) {
                let errorMessage = `Location Name ${ locationName } does not exist!`
                log.error(errorMessage)
                throw errorMessage
            }

            location = location[0]
            return location
        }
        const checkOutHubNodeByDistance = async function (location) {
            let result = []
            let hubNodeList = await sequelizeObj.query(` SELECT id, unit AS hub, subUnit AS node, lat, lng, 'location' AS dataFrom FROM unit WHERE lat IS NOT NULL AND lng IS NOT NULL `, { type: QueryTypes.SELECT })
            for (let hubNode of hubNodeList) {
                let distance = utils.getPointDistance(location, { lat: hubNode.lat, lng: hubNode.lng })
                hubNode.distance = distance
            }

            // sort by distance asc
            result = _.sortBy(hubNodeList, function(o) { 
				return o.distance;
			});
            return result
        }

        try {
            // [ { hub: null, node: null }, ... ]
            let hubNodeList = [ ]

            // let location = await checkParams(locationName, groupName);
            // log.info(`Location => ${ JSON.stringify(location, null, 4) }`)
            // if (location.lat && location.lng && location.lat != '0' && location.lng != '0') {
            //     // find out hub/node list by gps distance
            //     hubNodeList = await checkOutHubNodeByDistance(location)
            //     log.info(`Find out hub/node by locationName "${ locationName }".`)
            // } else {
                // log.warn(`Location Name ${ locationName } has no lat or lng.(Lat: ${ location.lat }, Lng: ${ location.lng })`)
                // find out hub/node list by groupName
                let result = await sequelizeObj.query(` SELECT id, unit AS hub, subUnit AS node , \`group\`, 'group' AS dataFrom FROM unit WHERE FIND_IN_SET( ?, \`group\` ) `, { type: QueryTypes.SELECT, replacements: [ groupName ] })
                if (result && result.length) {
                    log.info(`Find out hub/node by groupName "${ groupName }".`)
                    hubNodeList = result
                } else {
                    log.info(`Find out hub/node by groupName "${ groupName }" failed, do not find groupName from unit table`)
                }
            // }

            // if (!hubNodeList.length) {
            //     hubNodeList = await sequelizeObj.query(` SELECT id, unit AS hub, subUnit AS node, \`group\` FROM unit `, { type: QueryTypes.SELECT })
            //     log.info(`Find out hub/node failed, will use all hub/node`)
            //     log.info(JSON.stringify(hubNodeList, null, 4))
            // }
            
            log.info(`getClosestHubNodeListByLocation => ${ JSON.stringify(hubNodeList, null, 4) }`)
            return hubNodeList
        } catch (error) {
            throw error
        }
    },
    getVehicleList: async function(unitIdList, purpose, vehicleType, noOfDriver, startDate, endDate){
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss');
        endDate = endDate && endDate != '' ? moment(endDate).format('YYYY-MM-DD HH:mm:ss') : null;
        // leave vehicle
        let leaveVehicleSql = `
            select ifnull(vl.vehicleNo, -1) as vehicleNo from vehicle_leave_record vl 
            where vl.status = 1 
        `
        let replacements1 = [];
        if(endDate == null || endDate == ''){
            leaveVehicleSql += `
                and ((? >= vl.startTime AND ? <= vl.endTime) or (? <= vl.startTime))
            `
            replacements1.push(startDate, startDate, startDate)
        } else {
            leaveVehicleSql += `
                and ((? >= vl.startTime AND ? <= vl.endTime) 
                OR (? >= vl.startTime AND ? <= vl.endTime) 
                OR (? < vl.startTime AND ? > vl.endTime))
            `
            replacements1.push(startDate, startDate, endDate, endDate, startDate, endDate)
        }
        leaveVehicleSql += ` GROUP BY vl.vehicleNo`
        let leaveVehicle = await sequelizeObj.query(leaveVehicleSql, { type: QueryTypes.SELECT, replacements: replacements1 })
        leaveVehicle = leaveVehicle.map(item => item.vehicleNo)
        log.warn(`leave vehicleList ${ JSON.stringify(leaveVehicle) }`)

        let loanOutVehicleSql = `
            SELECT ifnull(vehicleNo, -1) as vehicleNo FROM loan 
            WHERE vehicleNo is not null
        `
        let replacements2 = [];
        if(startDate && endDate){
            loanOutVehicleSql += `
                and ((? >= startDate AND ? <= endDate) 
                OR (? >= startDate AND ? <= endDate) 
                OR (? < startDate AND ? > endDate))
            `
            replacements2.push(startDate, startDate, endDate, endDate, startDate, endDate)
        }
        loanOutVehicleSql += `  group by vehicleNo`
        //2023-07-13 exclude loan out vehicle
        let loanOutVehicle = await sequelizeObj.query(loanOutVehicleSql, { replacements: replacements2, type: QueryTypes.SELECT })
        loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)
        log.warn(`loan out vehicleList ${ JSON.stringify(loanOutVehicle) }`)

        // Not within the specified range hoto vehicle
        let hotoVehicleListByNotScopeSql = `
            select ifnull(vehicleNo, -1) as vehicleNo, startDateTime, endDateTime from hoto
            where vehicleNo is not null
        `
        let replacements3 = []
        if(endDate == null || endDate == ''){
            hotoVehicleListByNotScopeSql += `
                and ((? >= startDateTime AND ? <= endDateTime)  OR (? <= startDateTime))
            `
            replacements3.push(startDate, startDate, startDate)
        } else {
            hotoVehicleListByNotScopeSql += `
                and ((? >= startDateTime AND ? <= endDateTime) 
                OR (? >= startDateTime AND ? <= endDateTime) 
                OR (? < startDateTime AND ? > endDateTime))
            `
            replacements3.push(startDate, startDate, endDate, endDate, startDate, endDate)
        }
        if(startDate && endDate) {
            hotoVehicleListByNotScopeSql += `
                and vehicleNo not in (select vehicleNo from hoto 
                    where ? >= startDateTime AND ? <= endDateTime 
                    and vehicleNo is not null and status = 'Approved'
                ) and status = 'Approved'
            `
            replacements3.push(startDate, endDate)
        }
        hotoVehicleListByNotScopeSql += ` group by vehicleNo`
        let hotoVehicleListByNotScope = await sequelizeObj.query(hotoVehicleListByNotScopeSql,
        {
            replacements: replacements3,
            type: QueryTypes.SELECT
        })
        hotoVehicleListByNotScope = hotoVehicleListByNotScope.map(item => item.vehicleNo)
        log.warn(`hoto Not within the specified range vehicleList ${ JSON.stringify(hotoVehicleListByNotScope) }`)

        let taskVehicle = []
        let hotoVehicle = []
        if(noOfDriver == 0){
            // 2023-07-07 Tasks that do not require a driver need to exclude vehicles that are not completed within the task time frame
            taskVehicle = await sequelizeObj.query(`
                SELECT ifnull(t.vehicleNumber, -1) as vehicleNo FROM task t
                LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
                WHERE v.vehicleType = ?
                and t.vehicleStatus not in ('completed', 'cancelled')
                AND (((? >= t.indentStartTime AND ? <= t.indentEndTime) 
                OR (? >= t.indentStartTime AND ? <= t.indentEndTime) 
                OR (? < t.indentStartTime AND ? > t.indentEndTime))
                OR t.vehicleStatus = 'started'
                )
            `, { type: QueryTypes.SELECT, replacements: [vehicleType, startDate, startDate, endDate, endDate, startDate, endDate] })
            taskVehicle = taskVehicle.map(item => item.vehicleNo)
            log.warn(`task vehicleList ${ JSON.stringify(taskVehicle) }`)
        }
        if(noOfDriver == 0){
            // 2023-07-14 hoto vehicle not loan out
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
            // log.warn(`hoto vehicelList ${ JSON.stringify(hotoVehicle) }`)
        }
        let excludeVehicle = leaveVehicle.concat(taskVehicle).concat(loanOutVehicle).concat(hotoVehicle).concat(hotoVehicleListByNotScope)    
        excludeVehicle = excludeVehicle.map(item => item);
        excludeVehicle = Array.from(new Set(excludeVehicle))
        log.warn(`Need to exclude the vehicle ${ JSON.stringify(excludeVehicle) }`)
        let vehicleListSql = `
            select vv.vehicleNo, vv.unitId, vv.hub, vv.node  from (
                SELECT a.vehicleNo, IF(h.unitId is NULL, a.unitId, h.unitId) as unitId, a.groupId,
                IF(h.toHub is NULL, u.unit, h.toHub) as hub,
                IF(h.toHub is NULL, u.subUnit, h.toNode) as node 
                FROM vehicle a
                left join unit u on u.id = a.unitId
                left join (
                    select ho.vehicleNo, ho.unitId, ho.toHub, ho.toNode from hoto ho where ho.status = 'Approved'
        `
        let replacementsByvehicleList = []
        if(endDate == null || endDate == '') {
            vehicleListSql += `
                and (? >= ho.startDateTime AND ? <= ho.endDateTime)
            `
            replacementsByvehicleList.push(startDate, startDate)
        } else {
            vehicleListSql += `
                and (? >= ho.startDateTime AND ? <= ho.endDateTime)
            `
            replacementsByvehicleList.push(startDate, endDate)
        }
        vehicleListSql += ` ) h ON h.vehicleNo = a.vehicleNo 
        where a.groupId is null
        `
        if(vehicleType){
            vehicleListSql += `
                AND a.vehicleType = ? 
            `
            replacementsByvehicleList.push(vehicleType)
        }
        if(purposeList.indexOf(purpose?.toLowerCase()) == -1 || purpose?.toLowerCase() == 'mpt'){
            vehicleListSql += `
                AND (a.nextAviTime > ? OR a.nextAviTime IS NULL)
            `
            replacementsByvehicleList.push(moment(endDate).format('YYYY-MM-DD'))
        }
        vehicleListSql += ` ) vv where 1=1`
        if(unitIdList.length > 0) {
            vehicleListSql += ` and vv.unitId in (?)`
            replacementsByvehicleList.push(unitIdList)
        }
        if(excludeVehicle.length > 0){
            vehicleListSql += ` and vv.vehicleNo not in (?)`
            replacementsByvehicleList.push(`'${ excludeVehicle.join("','") }'`)
        }
        vehicleListSql += ` GROUP BY vv.vehicleNo`
        let vehicleList = await sequelizeObj.query(vehicleListSql,
        {
            replacements: replacementsByvehicleList,
            type: QueryTypes.SELECT
        }
        );
        return vehicleList;
    },
    getDriverList: async function(unitIdList, vehicleType, noOfVehicle, startDate, endDate, dataType){
        let indentStartTime;
        let indentEndTime;
        indentStartTime = startDate ? moment(startDate).format('YYYY-MM-DD HH:mm:ss') : null;
        indentEndTime = endDate && endDate != '' ? moment(endDate).format('YYYY-MM-DD HH:mm:ss') : null;
        
        // leave driver
        let leaveDriverSql = `
            SELECT ifnull(dl.driverId, -1) as driverId FROM driver_leave_record dl 
            WHERE dl.status = 1
        `
        let replacementsByleaveDriver = [];
        if(indentEndTime == null || indentEndTime == ''){
            leaveDriverSql += `
                and ((? >= dl.startTime AND ? <= dl.endTime) or (? <= dl.startTime))
            `
            replacementsByleaveDriver.push(indentStartTime, indentStartTime, indentStartTime)
        } else {
            leaveDriverSql += `
                and (
                    (? >= dl.startTime AND ? <= dl.endTime) 
                    OR (? >= dl.startTime AND ? <= dl.endTime) 
                    OR (? < dl.startTime AND ? > dl.endTime)
                )
            `
            replacementsByleaveDriver.push(indentStartTime, indentStartTime, indentEndTime, indentEndTime, indentStartTime, indentEndTime)
        }
        leaveDriverSql += ` GROUP BY dl.driverId`
        let leaveDriver = await sequelizeObj.query(leaveDriverSql, { type: QueryTypes.SELECT, replacements: replacementsByleaveDriver })
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
                where (? >= startDateTime AND ? <= endDateTime)
                and driverId is not null and status = 'Approved'
            ) and status = 'Approved'
            and driverId is not null group by driverId`,
        {
            type: QueryTypes.SELECT
            , replacements: [indentStartTime, indentStartTime, indentEndTime, indentEndTime, indentStartTime, indentEndTime, indentStartTime, indentEndTime]
        })
        hotoDriverListByNotScope = hotoDriverListByNotScope.map(item => item.driverId)
        log.warn(`hoto Not within the specified range driverList ${ JSON.stringify(hotoDriverListByNotScope) }`)

        let taskDriver = []
        let hotoDriver = []
        if(noOfVehicle) {
            // 2023-07-13 Tasks that do not require a driver need to exclude vehicles that are not completed within the task time frame
            taskDriver = await sequelizeObj.query(`
                SELECT ifnull(t.driverId, -1) as driverId FROM task t
                WHERE t.driverStatus not in ('completed', 'cancelled')
                AND (((? >= t.indentStartTime AND ? <= t.indentEndTime) 
                OR (? >= t.indentStartTime AND ? <= t.indentEndTime) 
                OR (? < t.indentStartTime AND ? > t.indentEndTime))
                OR t.driverStatus = 'started'
                ) and t.driverId is not null
            `, { type: QueryTypes.SELECT, replacements: [indentStartTime, indentStartTime, indentEndTime, indentEndTime, indentStartTime, indentEndTime] })
            taskDriver = taskDriver.map(item => item.driverId)
            log.warn(`task driverList ${ JSON.stringify(taskDriver) }`)
        }
        if(noOfVehicle) {
            // 2023-07-14 hoto driver not loan out
            // hotoDriver = await sequelizeObj.query(`
            //     SELECT driverId FROM hoto 
            //     WHERE driverId IS NOT NULL 
            //     AND (('${ indentStartTime }' >= startDateTime AND '${ indentStartTime }' <= endDateTime) 
            //     OR ('${ indentEndTime }' >= startDateTime AND '${ indentEndTime }' <= endDateTime) 
            //     OR ('${ indentStartTime }' < startDateTime AND '${ indentEndTime }' > endDateTime)
            //     OR '${ indentStartTime }' >= startDateTime) and status = 'Approved' and driverId is not null
            //     GROUP BY driverId
            // `, { type: QueryTypes.SELECT })
            // hotoDriver = hotoDriver.map(item => item.driverId)
            // log.warn(`hoto driverList ${ JSON.stringify(hotoDriver) }`)
        }
        // 2023-12-29 Drivers who were not 24 hours away from the start of the task were excluded
        let finishDriverList = []
        if(dataType) {
            finishDriverList = await sequelizeObj.query(`
                SELECT ifnull(driverId, -1) as driverId FROM task 
                WHERE driverStatus != 'cancelled' and dataFrom != 'MT-ADMIN'
                and (mobileEndTime >= ? or indentEndTime >= ?)
                group by driverId
            `, { 
                type: QueryTypes.SELECT, 
                replacements: [moment(moment(indentStartTime).subtract(1, 'days')).format('YYYY-MM-DD HH:mm:ss'), 
                moment(moment(indentStartTime).subtract(1, 'days')).format('YYYY-MM-DD HH:mm:ss')] 
            })
            finishDriverList = finishDriverList.map(item => item.driverId)
            log.warn(`A driver who is not 24 hours away from the task: ${ JSON.stringify(finishDriverList) }`)
        }
        let excludeDriver = leaveDriver.concat(taskDriver).concat(loanOutDriver).concat(hotoDriver).concat(hotoDriverListByNotScope).concat(finishDriverList) 
        excludeDriver = excludeDriver.map(item => item);
        excludeDriver = Array.from(new Set(excludeDriver))
        log.warn(`Need to exclude the driver ${ JSON.stringify(excludeDriver) }`)
        //2023-07-20 driver permitStatus != 'invalid'
        let driverListSql = `
            select dd.driverId, dd.driverName, dd.vehicleType, dd.contactNumber, dd.unitId, dd.overrideStatus, dd.hub, dd.node, dd.assessmentType, dd.violationNo from (
                select d.driverId, d.driverName, dc.vehicleType, d.contactNumber, 
                IF(h.unitId is NULL,d.unitId, h.unitId) as unitId, d.overrideStatus, dr.assessmentType, th.violationNo,
                IF(h.toHub is NULL, u.unit, h.toHub) as hub,
                IF(h.toHub is NULL, u.subUnit, h.toNode) as node
                from driver d
                left join unit u on u.id = d.unitId
                LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
                left join driver_assessment_record dr on dr.driverId = d.driverId and dr.approveStatus = 'Approved'
                left join (
                    select count(deviceId) as violationNo, deviceId from track_history 
                    where dataFrom = 'mobile' and violationType in ('Speeding', 'Hard Braking', 'Rapid Acc') 
                    group by deviceId
                ) th on d.driverId = th.deviceId
                left join (
                    select ho.driverId, ho.unitId, ho.toHub, ho.toNode from hoto ho 
                    where (? >= ho.startDateTime AND ? <= ho.endDateTime)
                    and ho.status = 'Approved'
                    ) h ON h.driverId = d.driverId 
                where d.permitStatus != 'invalid' 
                and (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)
        `
        let driverListByReplacements = [indentStartTime, indentEndTime, moment(indentEndTime).format('YYYY-MM-DD')]
        if(vehicleType){
            driverListSql += ` and FIND_IN_SET(?, dc.vehicleType)`
            driverListByReplacements.push(vehicleType)
        }
        driverListSql += `
                GROUP BY d.driverId
            ) dd where 1=1
        `
        if(unitIdList.length > 0){
            driverListSql += ` and dd.unitId in (?)`
            driverListByReplacements.push(unitIdList)
        }
        if(excludeDriver.length > 0){
            driverListSql += ` and dd.driverId not in (?)`
            driverListByReplacements.push(excludeDriver.join(","))
        }
        driverListSql += ` GROUP BY dd.driverId `
        if(dataType){
            driverListSql += ` ORDER BY isnull(dd.assessmentType), dd.assessmentType, dd.violationNo`
        }
        let driverList = await sequelizeObj.query(driverListSql,
        {
            type: QueryTypes.SELECT
            , replacements: driverListByReplacements
        });
        return driverList;
    },
    getSystemTaskByTripId: async function(tripId){
        if(!tripId) return null
        let cvTask = await sequelizeSystemObj.query(`
            select * from (
                SELECT jt.id, jt.pickupDestination, jt.dropoffDestination, j.tripNo, jt.driverNo, r.groupId, j.referenceId,
                CONCAT(l.lat, ',', l.lng) AS pickupGPS, CONCAT(l2.lat, ',', l2.lng) AS dropoffGPS, 
                j.noOfDriver, r.purposeType, j.vehicleType, g.groupName,
                DATE_FORMAT(jt.startDate, '%Y-%m-%d %H:%i:%s') as periodStartDate, 
                DATE_FORMAT(jt.endDate, '%Y-%m-%d %H:%i:%s') as periodEndDate,
                if(j.vehicleType = '-', 'toOnly', if(j.noOfDriver >= jt.driverNo, 'both', 'vehicleOnly')) as taskType
                FROM job j
                LEFT JOIN job_task jt ON jt.tripId = j.id
                LEFT JOIN request r ON r.id = jt.requestId
                LEFT JOIN location l ON l.locationName = jt.pickupDestination
                LEFT JOIN location l2 ON l2.locationName = jt.dropoffDestination
                LEFT JOIN \`group\` g on g.id = r.groupId
                WHERE j.id = ? and j.approve = 1
            ) jj
        `, { type: QueryTypes.SELECT, replacements: [tripId] })
        return cvTask
    }, 
    assignTaskByTaskId: async function(job_taskId, driverId, vehicleNo, hub, node, unitId, userId, systemStatus, serverTaskId){
        try {
            if(!serverTaskId) serverTaskId = job_taskId
            let loan2ByTaskId = await loanRecord.findOne({ where: { taskId: serverTaskId } })
            if(loan2ByTaskId) throw 'The operation failed because the current data status has changed.';
            let checkTask = await Task.findOne({ where: { taskId: serverTaskId } })
            if(checkTask) {
                if(checkTask.mobileStartTime) throw 'The task has started disabling operations.'
            }        
            if(node == '-') node = null
            let driver = null;
            let vehicle = null;
            let vehicleType;
            let taskObj = await this.getDriverIdAndVehicleNoByTaskId(serverTaskId);
            let loanObj = await this.getLoanByTaskId(serverTaskId);

            let systemTask = await sequelizeSystemObj.query(`
                SELECT jt.*, CONCAT(l.lat, ',', l.lng) AS pickupGPS, CONCAT(l2.lat, ',', l2.lng) AS dropoffGPS, j.tripNo,
                j.vehicleType, j.noOfDriver, jt.driverNo,
                DATE_FORMAT(jt.startDate, '%Y-%m-%d %H:%i:%s') as periodStartDate, 
                DATE_FORMAT(jt.endDate, '%Y-%m-%d %H:%i:%s') as periodEndDate,
                r.groupId
                FROM job_task jt
                LEFT JOIN job j ON jt.tripId = j.id
                LEFT JOIN request r ON r.id = jt.requestId
                LEFT JOIN location l ON l.locationName = jt.pickupDestination
                LEFT JOIN location l2 ON l2.locationName = jt.dropoffDestination
                WHERE jt.id = ?
            `, { type: QueryTypes.SELECT, replacements: [ job_taskId ] })
            systemTask = systemTask[0]
            log.warn(`task startDate: ${ systemTask.periodStartDate }, endDate: ${ systemTask.periodEndDate } `)
            //if driver/vehicle leave
            // if(driverId){
            //     let driverState = await this.verifyDriverLeave(driverId, systemTask.periodStartDate, systemTask.periodEndDate ? systemTask.periodEndDate : null);
            //     if(!driverState) throw 'The creation failed. The driver is in the leave state.';
            // }
            // if(vehicleNo) {
            //     let vehicleState = await this.verifyVehicleLeave(vehicleNo, systemTask.periodStartDate, systemTask.periodEndDate ? systemTask.periodEndDate : null);
            //     if(!vehicleState) throw 'The creation failed. The vehicle is in the leave state.';
            // }
            //if location 
            let state = await this.verifyLocation(systemTask.pickupDestination, systemTask.dropoffDestination);
            if(!state) throw `Location does not exist, allocation failed.`;
            let systemRequest = await _SystemRequest.Request.findByPk(systemTask.requestId);

            await sequelizeSystemObj.transaction(async transaction => {
                await sequelizeSystemObj.query(`
                    UPDATE job_task SET mobiusUnit = ? WHERE id = ?
                `, { type: QueryTypes.UPDATE, replacements: [ unitId, job_taskId ] })
            }).catch(error => {
               throw error
            })
           
            if (driverId){
                driver = await Driver.findByPk(driverId)
                if(driver.nric) {
                    if(driver.nric.length > 9) driver.nric = utils.decodeAESCode(driver.nric);
                } 
            } 
            if(vehicleNo) vehicle = await Vehicle.findByPk(vehicleNo)
            let option = { taskId: job_taskId, driver, vehicle, status: false, systemStatus: systemStatus ?? null }
            let result = await this.assignTaskBySystem(option)
            if (result.code != 1)  throw result.message;
            if(taskObj){
                if(taskObj.driverId){
                    if(taskObj.driverId != driverId) {
                        await FirebaseService.createFirebaseNotification2([{
                            taskId: serverTaskId,
                            token: '',
                            driverId: taskObj.driverId,
                            vehicleNo: taskObj.vehicleNumber
                        }], 'INFO', 'Task cancelled.')
                    }
                }
            }
            await sequelizeObj.transaction(async transaction => {
                let checkTask2 = await Task.findOne({ where: { taskId: serverTaskId } })
                if((systemTask.noOfDriver >= systemTask.driverNo && systemTask.vehicleType != '-') || checkTask2){
                    let task = { 
                        taskId: serverTaskId, 
                        driverId: driverId ? driverId : null, 
                        vehicleNumber: vehicleNo ? vehicleNo : null, 
                        indentId: systemTask.tripNo, 
                        indentStartTime: systemTask.periodStartDate, 
                        indentEndTime: systemTask.periodEndDate ? systemTask.periodEndDate : '', 
                        purpose: (systemRequest ? systemRequest.purposeType : '' ),
                        activity: (systemRequest ? systemRequest.additionalRemarks : '' ),
                        pickupDestination: systemTask.pickupDestination, 
                        dropoffDestination: systemTask.dropoffDestination, 
                        pickupGPS: systemTask.pickupGPS ? systemTask.pickupGPS : '', 
                        dropoffGPS: systemTask.dropoffGPS ? systemTask.dropoffGPS : '',
                        hub: hub, 
                        node: node
                    }
                    if(!taskObj) {
                        task.driverStatus = 'waitcheck';
                        task.vehicleStatus = 'waitcheck';
                        task.creator = userId
                    }
                    await Task.upsert(task)
                } else {
                    if(driverId || vehicleNo){
                        let loanByTaskId = await loan.findOne({ where: { taskId: serverTaskId } })
                        if(loanByTaskId) {
                            if(loanByTaskId.driverId && !driverId) {
                                await loan.destroy({ where: { taskId: serverTaskId } });
                            }
                            if(loanByTaskId.vehicleNo && !vehicleNo) {
                                await loan.destroy({ where: { taskId: serverTaskId } });
                            }
                        }
                        let loanByTaskId2 = await loan.findOne({ where: { taskId: serverTaskId } })
                        if(loanByTaskId2) {
                            await loan.update({  
								taskId: serverTaskId,
                                indentId: systemTask.tripNo, 
                                driverId: driverId ? driverId : null,
                                vehicleNo: vehicleNo ? vehicleNo : null, 
                                startDate: systemTask.periodStartDate, 
                                endDate: systemTask.periodEndDate, 
                                groupId: systemTask.groupId,
                                unitId: unitId,
                                purpose: (systemRequest ? systemRequest.purposeType : '' ),
                                activity: (systemRequest ? systemRequest.additionalRemarks : '' ),
                            }, { where: { taskId: serverTaskId } })
                        } else {
                            await loan.create({  
                                taskId: serverTaskId,
                                indentId: systemTask.tripNo, 
                                driverId: driverId ? driverId : null,
                                vehicleNo: vehicleNo ? vehicleNo : null, 
                                startDate: systemTask.periodStartDate, 
                                endDate: systemTask.periodEndDate, 
                                groupId: systemTask.groupId,
                                unitId: unitId,
                                purpose: (systemRequest ? systemRequest.purposeType : '' ),
                                activity: (systemRequest ? systemRequest.additionalRemarks : '' ),
                                creator: userId
                            })
                        }
                    }
                }
                
                if(taskObj || loanObj){
                    if(taskObj){
                        if(taskObj.driverId != driverId || taskObj.vehicleNumber != vehicleNo){
                            await this.initOperationRecord(userId, serverTaskId, taskObj.driverId, driverId, taskObj.vehicleNumber, vehicleNo, 'sys task assign')
                        } 
                    }
                    if(loanObj){
                        if(loanObj.driverId != driverId || loanObj.vehicleNo != vehicleNo){
                            await this.initOperationRecord(userId, serverTaskId, loanObj.driverId, driverId, loanObj.vehicleNo, vehicleNo, 'sys task assign')
                        } 
                    }
                } else {
                    await this.initOperationRecord(userId, serverTaskId, '', driverId, '', vehicleNo, 'sys task assign')
                }
                // MQ for create route
                if (systemTask.pickupDestination.toLowerCase() == systemTask.dropoffDestination.toLowerCase()) {
                    log.warn(`TaskId (${ serverTaskId }) pickupDestination = dropoffDestination, no need ask for route`)
                }
                // Update vehicle-relation db
                if(vehicleNo) await vehicleService.createVehicleRelation(driverId, vehicleNo)
            }).catch(error => {
               throw error
            })
            if(taskObj) {
                if(taskObj.driverId != driverId) {
                    await FirebaseService.createFirebaseNotification2([{
                        taskId: serverTaskId,
                        token: '',
                        driverId: driverId,
                        vehicleNo: vehicleNo
                    }], 'INFO', 'New task assigned!')
                }
            }
            return {
                code: 1
            };
        } catch (error) {
            log.error(error)
            return {
                code: 0,
                message: error
            }; 
        }
    },
    // 2024-02-20 atms indent resp
    initAtmsIndentResp: async function (taskId, userId){
        try {
            let userBase = null;
            if(userId) userBase = await UserBase.findOne({ where: { mvUserId: userId } })
            let operatorId = userBase ? userBase.id : null;
            let atmsIndent = await sequelizeSystemObj.query(`
                SELECT * FROM (
                    SELECT b.id as atmsTaskId, a.tripNo as ngtsTripId, a.referenceId,  
                    a.resourceId, a.noOfVehicle as resourceQuantity, sm.\`name\` as serviceMode,
                    DATE_FORMAT(b.startDate, '%Y-%m-%d %H:%i:%s') as startDateTime, 
                    DATE_FORMAT(b.endDate, '%Y-%m-%d %H:%i:%s') as endDateTime,
                    a.pocUnitCode, a.poc as pocName, a.pocNumber as pocMobileNumber, 
                    l.id as reportingLocationId, l2.id as destinationLocationId,
                    if(a.preParkDate is null, null, '1') as preparkQuantity,
                    a.preParkDate as preparkDateTime, a.id as ngtsJobId, b.trackingId,
                    c.driverId, c.\`name\` as driverName, c.contactNumber as driverMobileNumber, d.vehicleNumber
                    FROM (SELECT * from job_task where (taskStatus = 'unassigned' OR taskStatus = 'assigned')) b
                    LEFT JOIN  job a ON a.id = b.tripId
                    LEFT JOIN driver c ON b.id = c.taskId
                    LEFT JOIN vehicle d ON b.id = d.taskId
                    LEFT JOIN request r ON a.requestId = r.id 
                    LEFT JOIN location l on l.locationName = a.pickupDestination
                    LEFT JOIN location l2 on l2.locationName = a.dropoffDestination
                    LEFT JOIN service_mode sm ON a.serviceModeId = sm.id
                    LEFT JOIN service_type st ON st.id = sm.service_type_id
                    where a.approve = 1 and r.purposeType != 'Urgent' and lower(st.category) = 'mv' and a.referenceId is not null
                ) jj where jj.atmsTaskId = ?
            `, { type: QueryTypes.SELECT, replacements: [ taskId ] })
            log.warn('ATMS INDENT ===> '+JSON.stringify(atmsIndent))
            let respStatus = atmsIndent ? atmsIndent.length > 0 ? true : false : false;
            if(respStatus) {
                atmsIndent = atmsIndent[0]
                let ngtsRespObj = {
                    atmsTaskId: atmsIndent.atmsTaskId, 
                    ngtsTripId: atmsIndent.ngtsTripId, 
                    referenceId: atmsIndent.referenceId, 
                    transacationType: 'U', 
                    transacationDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), 
                    responseStatus: 'A', 
                    serviceMode: atmsIndent.serviceMode, 
                    resourceId: atmsIndent.resourceId, 
                    resourceQuantity: atmsIndent.resourceQuantity, 
                    startDateTime: atmsIndent.startDateTime, 
                    endDateTime: atmsIndent.endDateTime, 
                    pocUnitCode: atmsIndent.pocUnitCode, 
                    pocName: atmsIndent.pocName, 
                    pocMobileNumber: atmsIndent.pocMobileNumber, 
                    reportingLocationId: atmsIndent.reportingLocationId, 
                    destinationLocationId: atmsIndent.destinationLocationId, 
                    preparkQuantity: atmsIndent.preparkQuantity, 
                    preparkDateTime: atmsIndent.preparkDateTime && atmsIndent.preparkDateTime != '' ? atmsIndent.preparkDateTime : null, 
                    ngtsJobId: atmsIndent.ngtsJobId, 
                    ngtsJobStatus: 'A', 
                    driverId: atmsIndent.driverId, 
                    driverName: atmsIndent.driverName, 
                    driverMobileNumber: atmsIndent.driverMobileNumber, 
                    vehicleNumber: atmsIndent.vehicleNumber, 
                    operatorId: operatorId, 
                    isSend: 'N', 
                    trackingId: atmsIndent.trackingId
                }
                await NGTSResp.create(ngtsRespObj);
            }
        } catch (error) {
            log.error(error)
            return error; 
        }
    },
    getTaskTypeByTaskId: async function(taskId){
        try {
            if(!taskId) return null
            let systemIndent = await sequelizeSystemObj.query(`
                select jt.id, j.referenceId from job j 
                left join job_task jt on jt.tripId = j.id
                where jt.id  = ?
            `, { type: QueryTypes.SELECT, replacements: [ taskId ] })
            log.warn('system INDENT type ===> '+JSON.stringify(systemIndent))
            return systemIndent ? systemIndent[0] ? systemIndent[0].referenceId : null : null;
        } catch (error) {
            log.error(error)
        }
    }
}

module.exports = {
    getHubNode: async function (req, res) {
        try {
            let hub = req.cookies.hub;
            let node = req.cookies.node;
            let result
            if(hub && node) {
                result = await Unit.findAll({ where: { unit: hub, subUnit: node } });
                log.info(`Request URL Result => ${ JSON.stringify(result) }`)
                return res.json(utils.response(1, result));
            } 
            
            result = await Unit.findAll({ group: 'id', order: ['unit', 'subUnit'] });
            log.info(`Request URL Result => ${ JSON.stringify(result) }`)
            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
        
    },
    getVehicleType: async function (req, res) {
        try {
            let result = await TaskUtils.getTypeOfVehicle()
            return res.json(utils.response(1, result));    
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },    
    getTaskIdDriverAndVehicle: async function (req, res) {
        try {
            let taskId = req.body.taskId;
            let dataType = req.body.dataType;
            let newTaskId = dataType.toLowerCase() == 'atms' ? `AT-${ taskId }` : taskId;
            let data = []
            data = await sequelizeObj.query(`
                SELECT t.taskId, t.driverId, d.driverName AS name, d.contactNumber, t.vehicleNumber
                FROM task t 
                LEFT JOIN driver d ON d.driverId = t.driverId
                WHERE t.taskId = ?
            `, { type: QueryTypes.SELECT, replacements: [newTaskId] })
            // if(dataType.toLowerCase() == 'sys'){
            //     data = await sequelizeSystemObj.query(`
            //         SELECT
            //         b.id, c.driverId, c.name, c.contactNumber, d.vehicleNumber
            //         FROM
            //             job_task b 
            //         LEFT JOIN driver c ON b.id = c.taskId
            //         LEFT JOIN vehicle d ON b.id = d.taskId
            //         WHERE b.id = ?
            //     `, { type: QueryTypes.SELECT, replacements: [ taskId ] })
            // } else {
                // data = await sequelizeObj.query(`
                //     SELECT t.taskId, t.driverId, d.driverName AS name, d.contactNumber, t.vehicleNumber
                //     FROM task t 
                //     LEFT JOIN driver d ON d.driverId = t.driverId
                //     WHERE t.taskId = '${ taskId }'
                // `, { type: QueryTypes.SELECT })
            // }
            return res.json(utils.response(1, data[0]));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    //2023-05-23 optimize
    getDriverListByTaskId: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let vehicleType = req.body.vehicleType && req.body.vehicleType != '-' ? req.body.vehicleType : null;
            let hub = req.body.hub;
            let node = req.body.node;
            let unitId = req.body.unitId;
            let noOfVehicle =  req.body.noOfVehicle
            let dataType = req.body.dataType;
            if (!userId) return res.json(utils.response(0, `UserID ${ userId } does not exist!.`));
            if(unitId){
                let unitOjb = await Unit.findOne({ where: { id: unitId } })
                if(unitOjb) {
                    hub = unitOjb.unit;
                    node = unitOjb.subUnit;
                }
            }
            let unitIdList = await TaskUtils.findOutHubNode(hub, node == '-' ? null : node)
            log.info(`driverList unitIDList ${ JSON.stringify(unitIdList) }`)
            let driverList = await TaskUtils.getDriverList(unitIdList, vehicleType, noOfVehicle, req.body.startDate, req.body.endDate)
            return res.json(utils.response(1, driverList));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    //2023-05-23 optimize
    getVehicleList: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let vehicleType = req.body.vehicleType;
            let hub = req.body.hub;
            let node = req.body.node;
            let unitId = req.body.unitId;
            let taskId = req.body.taskId;
            let noOfDriver = req.body.noOfDriver;
            let dataType = req.body.dataType;
            let purpose = req.body.purpose;
            if (!userId) return res.json(utils.response(0, `UserID ${ userId } does not exist!.`));  
            if(node == '-' || !node) node = null
            if(unitId){
                let unit = await Unit.findOne({ where: { id: unitId } });
                if(unit){
                    hub = unit.unit;
                    node = unit.subUnit;
                }
            }
            let unitIdList = await TaskUtils.findOutHubNode(hub, node == '-' ? null : node)
            log.info(`vehicleList unitIDList ${ JSON.stringify(unitIdList) }`)
            let vehicleList = await TaskUtils.getVehicleList(unitIdList, purpose, vehicleType, noOfDriver, req.body.startDate, req.body.endDate)
            return res.json(utils.response(1, vehicleList));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    preAssign: async function (req, res) {
        try {
            let { taskId, hub, tableNode} = req.body;
            if(tableNode == '-') tableNode = null
            let unit = null
            if(tableNode) {
                unit = await Unit.findOne({where: { unit: hub, subUnit: tableNode }})
            } else {
                unit = await Unit.findOne({where: { unit: hub, subUnit: { [Op.is]: null } }})
            }
            if(!unit) return res.json(utils.response(0, `The selected hub/node does not exist`));
            await sequelizeSystemObj.transaction(async transaction => {
                await sequelizeSystemObj.query(`
                    UPDATE job_task SET mobiusUnit = ? WHERE id = ?
                `, { type: QueryTypes.UPDATE, replacements: [ unit.id, taskId ] })
            }).catch(error => {
               throw error
            })
            // 2024-02-20 atms indent resp
            // await TaskUtils.initAtmsIndentResp(taskId, req.cookies.userId)
            return res.json(utils.response(1, true));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    // preMbAssign: async function (req, res) {
    //     try {
    //         let { taskId, hub, tableNode} = req.body;
    //         if(tableNode == '-') tableNode = null
    //         let unit = null
    //         if(tableNode) {
    //             unit = await Unit.findOne({where: { unit: hub, subUnit: tableNode }})
    //         } else {
    //             unit = await Unit.findOne({where: { unit: hub, subUnit: { [Op.is]: null } }})
    //         }
    //         if(!unit) return res.json(utils.response(0, `The selected hub/node does not exist`));
    //         await sequelizeObj.transaction(async transaction => {
    //             await MtAdmin.update({ unitId: unit.id }, { where: { id: taskId } }); 
    //         }).catch(error => {
    //            throw error
    //         })
    //         return res.json(utils.response(1, true));
    //     } catch (error) {
    //         log.error(error)
    //         return res.json(utils.response(0, error));
    //     }
    // },
    assignTask: async function (req, res) {
        try {
            let { taskId, driverId, vehicleNo, hub, node } = req.body;
            if(node == '-') node = null
            let unit = null
            if(node) {
                unit = await Unit.findOne({where: { unit: hub, subUnit: node }})
            } else {
                unit = await Unit.findOne({where: { unit: hub, subUnit: { [Op.is]: null } }})
            }
            if(!unit) throw `The hub/node does not exist.`;
            let result = await TaskUtils.assignTaskByTaskId(taskId, driverId, vehicleNo, hub, node, unit.id, req.cookies.userId);
            if (result.code == 0)  return res.json(utils.response(0, result.message));
            return res.json(utils.response(1, true));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    // assignMbTask: async function (req, res) {
    //     try {
    //         let { driverId, vehicleNo, hub, node, mtAdminId } = req.body;
    //         if(node == '-') node = null
    //         let driver = null;
    //         let vehicle = null;
    //         let taskId = 'AT-'+mtAdminId
    //         let checkTask = await Task.findOne({ where: { taskId: `MT-${ mtAdminId }` } })
    //         if(!checkTask) checkTask = await Task.findOne({ where: { taskId: `AT-${ mtAdminId }` } })
    //         if(checkTask) {
    //             if(checkTask.mobileStartTime) return res.json(utils.response(0, 'The task has started disabling operations.'));
    //             taskId = checkTask.taskId
    //         }
    //         let taskObj = await TaskUtils.getDriverIdAndVehicleNoByTaskId(`${ taskId }`);
    //         let loanObj = await TaskUtils.getLoanByTaskId(`${ taskId }`);
    //         let loanObjStatus = await loan.findOne({ where: { [Op.or]: [
    //             { taskId: `AT-${ mtAdminId }` },
    //             { taskId: `MT-${ mtAdminId }` }
    //           ] } })
    //         let loanObj2Status = await loanRecord.findOne({ where: { [Op.or]: [
    //             { taskId: `AT-${ mtAdminId }` },
    //             { taskId: `MT-${ mtAdminId }` }
    //           ] } })
    //         if(loanObjStatus || loanObj2Status) {
    //             if(loanObjStatus) {
    //                 if(loanObjStatus.actualStartTime || loanObjStatus.actualEndTime) {
    //                     return res.json(utils.response(0, 'The operation failed because the current data status has changed.'));
    //                 }
    //             }
    //             if(loanObj2Status) {
    //                 return res.json(utils.response(0, 'The operation failed because the current data status has changed.'));
    //             }
    //         }
            
    //         let mt_admin = await MtAdmin.findOne({ where: { id: mtAdminId } })
    //         let state = await TaskUtils.verifyLocation(mt_admin.destination, mt_admin.reportingLocation);
    //         if(!state) return res.json(utils.response(0, `Location does not exist, allocation failed.`));
    //         if(taskObj){
    //             if(driverId){
    //                 let driverState = await TaskUtils.verifyDriverLeave(driverId, taskObj.indentStartTime, taskObj.indentEndTime);
    //                 if(!driverState) return res.json(utils.response(0, 'The creation failed. The driver is in the leave state.')); 
    //             }
    //             let vehicleState = await TaskUtils.verifyVehicleLeave(vehicleNo, taskObj.indentStartTime, taskObj.indentEndTime);
    //             if(!vehicleState) return res.json(utils.response(0, 'The creation failed. The vehicle is in the leave state.')); 
    //             if(taskObj.driverId){
    //                 if(taskObj.driverId != driverId) {
    //                     await FirebaseService.createFirebaseNotification2([{
    //                         taskId,
    //                         token: '',
    //                         driverId: taskObj.driverId,
    //                         vehicleNo: taskObj.vehicleNumber
    //                     }], 'INFO', 'Task cancelled!')
    //                 }

    //             }
    //         }
    //         await sequelizeObj.transaction(async transaction => {
    //             let unit = null
    //             if(node) {
    //                 unit = await Unit.findOne({where: { unit: hub, subUnit: node }})
    //             } else {
    //                 unit = await Unit.findOne({where: { unit: hub, subUnit: { [Op.is]: null } }})
    //             }
    //             if(!unit) throw `The hub/node does not exist.`
                
    //             await MtAdmin.update({ unitId: unit.id, vehicleNumber: vehicleNo, driverId: driverId }, { where: { id: mtAdminId } });
    //             if(driverId) driver = await Driver.findByPk(driverId)
    //             if(vehicleNo) vehicle = await Vehicle.findByPk(vehicleNo)
    //             let checkTask2 = await Task.findOne({ where: { [Op.or]: [
    //                 { taskId: `AT-${ mtAdminId }` },
    //                 { taskId: `MT-${ mtAdminId }` }
    //               ] } })
    //             if((mt_admin.driverNum > 0 && mt_admin.needVehicle > 0) || checkTask2) {
    //                 let task = { 
    //                     taskId: taskId, 
    //                     driverId: driverId ? driverId : null, 
    //                     vehicleNumber: vehicleNo ? vehicleNo : null, 
    //                     dataFrom: 'MT-ADMIN',
    //                     indentStartTime: mt_admin.startDate, 
    //                     indentEndTime: mt_admin.endDate, 
    //                     creator: req.cookies.userId,
    //                     purpose: (mt_admin.purpose ? mt_admin.purpose : '' ),
    //                     activity: (mt_admin.activityName ? mt_admin.activityName : '' ),
    //                     pickupDestination: mt_admin.reportingLocation,
    //                     dropoffDestination: mt_admin.destination,
    //                     pickupGPS: `${mt_admin.reportingLocationLat ? mt_admin.reportingLocationLat : '0.0'},${mt_admin.reportingLocationLng ? mt_admin.reportingLocationLng : '0.0'}`,
    //                     dropoffGPS: `${mt_admin.destinationLat ? mt_admin.destinationLat : '0.0'},${mt_admin.destinationLng ? mt_admin.destinationLng : '0.0'}`,
    //                     hub: hub, 
    //                     node: node,
    //                     indentId: mtAdminId
    //                 }
    //                 if(!taskObj) {
    //                     task.driverStatus = driverId ? 'waitcheck' : null;
    //                     task.vehicleStatus = 'waitcheck';
    //                 }
    //                 await Task.upsert(task)
    //             } else {
    //                 if(driverId || vehicleNo) {
    //                     let loanByTaskId = await loan.findOne({ where: { [Op.or]: [
    //                         { taskId: `AT-${ mtAdminId }` },
    //                         { taskId: `MT-${ mtAdminId }` }
    //                       ] } })
    //                     if(loanByTaskId) {
    //                         if(loanByTaskId.driverId && !driverId) {
    //                             await loan.destroy({ where: { [Op.or]: [
    //                                 { taskId: `AT-${ mtAdminId }` },
    //                                 { taskId: `MT-${ mtAdminId }` }
    //                               ] } });
    //                         }
    //                         if(loanByTaskId.vehicleNo && !vehicleNo) {
    //                             await loan.destroy({ where: { [Op.or]: [
    //                                 { taskId: `AT-${ mtAdminId }` },
    //                                 { taskId: `MT-${ mtAdminId }` }
    //                               ] } });
    //                         }
    //                     }
    //                     let loanByTaskId2 = await loan.findOne({ where: { [Op.or]: [
    //                         { taskId: `AT-${ mtAdminId }` },
    //                         { taskId: `MT-${ mtAdminId }` }
    //                       ] } })
    //                     if(loanByTaskId2) {
    //                         await loan.update({  
    //                             taskId: `AT-${ mtAdminId }`,
    //                             indentId: mt_admin.indentId, 
    //                             driverId: driverId ? driverId : null,
    //                             vehicleNo: vehicleNo ? vehicleNo : null, 
    //                             startDate: mt_admin.startDate, 
    //                             endDate: mt_admin.endDate, 
    //                             groupId: -1,
    //                             unitId: unit.id,
    //                             purpose: (mt_admin.purpose ? mt_admin.purpose : '' ),
    //                             activity: (mt_admin.activityName ? mt_admin.activityName : '' ),
    //                             creator: req.cookies.userId
    //                         }, { where: { taskId } })
    //                     } else {
    //                         await loan.create({  
    //                             taskId: `AT-${ mtAdminId }`,
    //                             indentId: mt_admin.indentId, 
    //                             driverId: driverId ? driverId : null,
    //                             vehicleNo: vehicleNo ? vehicleNo : null, 
    //                             startDate: mt_admin.startDate, 
    //                             endDate: mt_admin.endDate, 
    //                             groupId: -1,
    //                             purpose: (mt_admin.purpose ? mt_admin.purpose : '' ),
    //                             activity: (mt_admin.activityName ? mt_admin.activityName : '' ),
    //                             unitId: unit.id,
    //                             creator: req.cookies.userId
    //                         })
    //                     }
    //                 }
    //             }
    //             if(taskObj || loanObjStatus){
    //                 if(taskObj){
    //                     if(taskObj.driverId != driverId || taskObj.vehicleNumber != vehicleNo){
    //                         await TaskUtils.initOperationRecord(req.cookies.userId, taskId, taskObj.driverId, driverId, taskObj.vehicleNumber, vehicleNo, 'atms task assign')
    //                     } 
    //                 }
    //                 if(loanObjStatus){
    //                     if(loanObjStatus.driverId != driverId || loanObjStatus.vehicleNo != vehicleNo){
    //                         await TaskUtils.initOperationRecord(req.cookies.userId, taskId, loanObjStatus.driverId, driverId, loanObjStatus.vehicleNo, vehicleNo, 'atms task assign')
    //                     } 
    //                 }
    //             } else {
    //                 await TaskUtils.initOperationRecord(req.cookies.userId, taskId, '', driverId, '', vehicleNo, 'atms task assign')
    //             }
    //             // MQ for create route
    //             if (mt_admin.reportingLocation.toLowerCase() == mt_admin.destination.toLowerCase()) {
    //                 log.warn(`TaskId (${ taskId }) pickupDestination = dropoffDestination, no need ask for route`)
    //             }
    //             // Update vehicle-relation db
    //             if(vehicleNo) await vehicleService.createVehicleRelation(driverId, vehicleNo)
    //         }).catch(error => {
    //             throw error
    //         })
    //         if(taskObj) {
    //             if(taskObj.driverId != driverId) {
    //                 await FirebaseService.createFirebaseNotification2([{
    //                     taskId,
    //                     token: '',
    //                     driverId: driverId,
    //                     vehicleNo: vehicleNo
    //                 }], 'INFO', 'New task assigned!')
    //             }
    //         }
    //         return res.json(utils.response(1, true)); 
    //     } catch (error) {
    //         log.error(error)
    //         return res.json(utils.response(0, error));
    //     }
    // },
    reassignMvTask: async function(req, res) {
        let { dataType, taskId, driverId, vehicleNo, hub, node } = req.body;
        if(node == '-') node = null
        let preTaskId = taskId;
        if(dataType.toLowerCase() == 'atms') preTaskId = 'AT-'+preTaskId;
        let loan2ByTaskId = await loanRecord.findOne({ where: { taskId: preTaskId } })
        if(loan2ByTaskId) return res.json(utils.response(0, 'The operation failed because the current data status has changed.'));
        let checkTask = await Task.findOne({ where: { taskId: preTaskId } })
        if(checkTask) {
            if(checkTask.mobileStartTime) return res.json(utils.response(0, 'The task has started disabling operations.'));
        }        
        
        // just auto match resource task need approve when reassign
        let needApprove = false;
        let taskObj = await TaskUtils.getDriverIdAndVehicleNoByTaskId(preTaskId);
        let loanObj = await TaskUtils.getLoanByTaskId(preTaskId);
        let oldData = { driverId: "", vehicleNo: "" };
        let newData = { driverId: driverId, vehicleNo: vehicleNo, hub, node };
        if (taskObj && taskObj.creator == 0) {
            needApprove = true;
            oldData.driverId = taskObj.driverId;
            oldData.vehicleNo = taskObj.vehicleNumber;
        } else if (loanObj && loanObj.creator == 0) {
            needApprove = true;
            oldData.driverId = loanObj.driverId;
            oldData.vehicleNo = loanObj.vehicleNo;
        }

        // assign and approve 
        let pageType = dataType.toLowerCase() == 'atms' ? 'ATMS Task Assign' : 'Sys Task Assign';

        let pageList = await userService.getUserPageList(req.cookies.userId, 'Task Assign', pageType)
        let operationList = pageList.map(item => `${ item.action }`).join(',')
        operationList = operationList.split(',')
        if(operationList.includes('Assign') && operationList.includes("Reassign Approve")) {
            needApprove = false
        }

        if (needApprove) {
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'sys auto match task reassign',
                businessId: preTaskId,
                optType: 'apply',
                beforeData: JSON.stringify(oldData),
                afterData: JSON.stringify(newData),
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'auto match resource task need approve when reassign.'
            });
        } else {
            let unit = null
            if(node) {
                unit = await Unit.findOne({where: { unit: hub, subUnit: node }})
            } else {
                unit = await Unit.findOne({where: { unit: hub, subUnit: { [Op.is]: null } }})
            }
            if(!unit) {
                return res.json(utils.response(0, 'The hub/node does not exist.'));
            }
            let result = await TaskUtils.assignTaskByTaskId(taskId, driverId, vehicleNo, hub, node, unit.id, req.cookies.userId, null, preTaskId);
            if (result.code == 0) {
                return res.json(utils.response(0, result.message));
            }
            // 2024-02-20 atms indent resp
            await TaskUtils.initAtmsIndentResp(taskId, req.cookies.userId)
        }
        return res.json(utils.response(1, 'success'));
    },
    reassignMvTaskApprove: async function(req, res) {
        let { dataType, taskId, optType, remarks } = req.body;
        let preTaskId = taskId;
        if(dataType.toLowerCase() == 'atms') preTaskId = 'AT-'+preTaskId;
        let loan2ByTaskId = await loanRecord.findOne({ where: { taskId: preTaskId } })
        if(loan2ByTaskId) return res.json(utils.response(0, 'The operation failed because the current data status has changed.'));
        let checkTask = await Task.findOne({ where: { taskId: preTaskId } })
        if(checkTask && checkTask.mobileStartTime) {
            return res.json(utils.response(0, 'The task has started disabling operations.'));
        }        
        
        // just auto match resource task need approve when reassign
        let reassignApplyData = await sequelizeObj.query(`
            SELECT
                oo.businessId,
                oo.beforeData,
                oo.afterData,
                oo.optType
            FROM operation_record oo
            WHERE oo.businessId=? and oo.businessType = 'sys auto match task reassign'
            ORDER BY oo.optTime DESC LIMIT 1;
        `, { type: QueryTypes.SELECT, replacements: [preTaskId]});
        if (reassignApplyData && reassignApplyData.length == 1) {
            if (reassignApplyData[0].optType == 'apply') {
                let reassignApproveOpt = {
                    id: null,
                    operatorId: req.cookies.userId,
                    businessType: 'sys auto match task reassign',
                    businessId: preTaskId,
                    beforeData: reassignApplyData[0].beforeData,
                    afterData: reassignApplyData[0].afterData,
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: 'auto match resource task approve.'
                }
                if (optType == 'pass') {
                    reassignApproveOpt.optType = 'approve pass';

                    let reassignData = reassignApplyData[0].afterData ? JSON.parse(reassignApplyData[0].afterData) : null;
                    if (reassignData) {
                        let hub = reassignData.hub;
                        let node = reassignData.node;
                        if (node == '-') {
                            node = null;
                        }
                        //confirm reasign task.
                        let unit = null
                        if(node) {
                            unit = await Unit.findOne({where: { unit: hub, subUnit: node }})
                        } else {
                            unit = await Unit.findOne({where: { unit: hub, subUnit: { [Op.is]: null } }})
                        }
                        if(!unit) {
                            return res.json(utils.response(0, 'The hub/node does not exist.'));
                        }
                        let result = await TaskUtils.assignTaskByTaskId(taskId, reassignData.driverId, reassignData.vehicleNo, reassignData.hub, reassignData.node, unit.id, req.cookies.userId, null, preTaskId);
                        if (result.code == 0) {
                            return res.json(utils.response(0, result.message));
                        }
                        // 2024-02-20 atms indent resp
                        await TaskUtils.initAtmsIndentResp(preTaskId, req.cookies.userId)
                    }
                } else {
                    reassignApproveOpt.optType = 'reject';
                }
                await OperationRecord.create(reassignApproveOpt);
            }
        }

        return res.json(utils.response(1, 'success'));
    },
    reassignTaskVehicle: async function(req, res) {
        let userId = req.cookies.userId;
        let user = await User.findOne({ where: { userId: userId } })
        if(!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let taskId = req.body.taskId;
        let newVehicleNo = req.body.vehicleNo;
        let task = await Task.findByPk(taskId);
        if (task) {
            let cvTransaction = null;
            let mvTransaction = await sequelizeObj.transaction();
            try {
                let dataFrom = task.dataFrom;
                if (dataFrom == 'SYSTEM') {
                    cvTransaction = await sequelizeSystemObj.transaction();
                    // update system vehicle.vehicleNumber
                    let systemTaskId = taskId;
                    if(taskId.includes('AT-')) systemTaskId = taskId.slice(3)
                    await _SystemVehicle.Vehicle.update({ vehicleNumber: newVehicleNo }, {where: {taskId: systemTaskId}, transaction: cvTransaction });
                } else if (dataFrom == 'MT-ADMIN') {
                    await MtAdmin.update({ vehicleNumber: newVehicleNo }, { where: { id: task.indentId }, transaction: mvTransaction });
                } else if (dataFrom == 'MOBILE') {
                    await MobileTrip.update({ vehicleNumber: newVehicleNo }, { where: { id: task.indentId }, transaction: mvTransaction });
                }
                await Task.update({ vehicleNumber: newVehicleNo }, { where: { taskId: taskId }, transaction: mvTransaction });
                await OperationRecord.create({
                    id: null,
                    operatorId: userId,
                    businessType: 'task',
                    businessId: taskId,
                    optType: 'assign vehicle',
                    beforeData: '',
                    afterData: newVehicleNo,
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: `reassign without vehicle task.`,
                }, { transaction: mvTransaction });

                if (cvTransaction) {
                    await cvTransaction.commit();
                }
                if (mvTransaction) {
                    await mvTransaction.commit();
                }
                return res.json(utils.response(1, 'success'));
            } catch (error) {
                log.error(error)
                if (cvTransaction) {
                    await cvTransaction.rollback();
                }
                if (mvTransaction) {
                    await mvTransaction.rollback();
                }
                return res.json(utils.response(0, `Assign task[${taskId}] vehicle fail!`,));
            }
        } else {
            return res.json(utils.response(0, `Task[${taskId}] does not exist.`));
        }
    },
    checkTaskRoute: async function (req, res) {
        try {
          let task = await Task.findOne({ order: [ [ 'createdAt', 'DESC' ] ] })
        if (task) {
            if(task.routePoints) {
                return res.json(utils.response(1, 'success'));
            } else {
                return res.json(utils.response(0, 'Still waiting...'));
            }
        }
        return res.json(utils.response(1, 'success'));  
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
        
    },
    //2023-07-14 loan out driver/vehicle task  
    loanOutTaskByTaskId: async function (req, res) {
        try {
            let taskId = req.body.taskId;
            let startDate = req.body.startDate;
            let endDate = req.body.endDate;
            let loanByTaskId = await loan.findOne({ where: { taskId: taskId } })
            if(!loanByTaskId) return res.json(utils.response(1, false));
            let taskByVehicleOrDriverSql = `
                SELECT taskId, driverId, vehicleNumber, vehicleStatus 
                FROM task 
                WHERE taskId LIKE 'CU-%'
                AND vehicleStatus NOT IN ('Cancelled', 'completed') 
                AND (((? >= indentStartTime AND ? <= indentEndTime) 
                OR (? >= indentStartTime AND ? <= indentEndTime) 
                OR (? < indentStartTime AND ? > indentEndTime))
                OR vehicleStatus = 'started')
            `
            let taskByVehicleOrDriverByReplacements = [startDate, startDate, endDate, endDate, startDate, endDate]
            if(loanByTaskId.driverId){
                taskByVehicleOrDriverSql += ` and driverId = ?`
                taskByVehicleOrDriverByReplacements.push(loanByTaskId.driverId)
            }
            if(loanByTaskId.vehicleNo){
                taskByVehicleOrDriverSql += ` and vehicleNumber = ?`
                taskByVehicleOrDriverByReplacements.push(loanByTaskId.vehicleNo)
            }
            taskByVehicleOrDriverSql += ` group by taskId`
            let taskByVehicleOrDriver = await sequelizeObj.query(taskByVehicleOrDriverSql, 
                { type: QueryTypes.SELECT, replacements: taskByVehicleOrDriverByReplacements }
            );
            // log.warn(`task driver/vehilce ${ JSON.stringify(taskByVehicleOrDriver ? taskByVehicleOrDriver[0] : '') }`)
            if(taskByVehicleOrDriver[0]){
                return res.json(utils.response(1, true));
            } else {
                return res.json(utils.response(1, false));
            }
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        } 
    },
    cancalTaskByMb: async function (req, res) {
        try {
            let mtAdminID = req.body.mtAdminId;
            let cancelledCause = req.body.cancelledCause;
            let taskObj = await Task.findOne({where: { indentId: mtAdminID }})
            let taskId = null;
            if(taskObj){
                taskId = taskObj.taskId;
                if(taskObj.mobileStartTime || (taskObj.vehicleStatus).toLowerCase() == 'cancelled') return res.json(utils.response(0, `The operation failed because the state of the data has changed.`));
            } 
            let loanObj = await TaskUtils.getLoanByTaskId(`${ taskId }`);
            let loanObjStatus = await loan.findOne({where: { taskId: 'AT-'+mtAdminID }});
            let loanObj2Status = await loanRecord.findOne({where: { taskId: 'AT-'+mtAdminID }});
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
                let mtAdminObj = await MtAdmin.findOne({ where: { id: mtAdminID } })
                await MtAdmin.update({ cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), cancelledCause: cancelledCause, amendedBy: req.cookies.userId }, { where: { id: mtAdminID  } });
                if(taskObj) {
                    await Task.update({ vehicleStatus: 'Cancelled', driverStatus: 'Cancelled' }, { where: { indentId: (mtAdminID).toString() } });
                    await OperationRecord.create({
                        id: null,
                        operatorId: req.cookies.userId,
                        businessType: 'atms task assign',
                        businessId: taskId,
                        optType: 'Cancel',
                        beforeData: `${ JSON.stringify([{driverStatus: taskObj.driverStatus, vehicleStatus: taskObj.vehicleStatus},{cancelledDateTime: mtAdminObj.cancelledDateTime, cancelledCause: mtAdminObj.cancelledCause, amendedBy: mtAdminObj.amendedBy}]) }`,
                        afterData: `${ JSON.stringify([{driverStatus: 'Cancelled', vehicleStatus: 'Cancelled'},{cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss'), cancelledCause: cancelledCause, amendedBy: req.cookies.userId}]) }`,
                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remarks: 'cancel atms task'
                    })
                }
                if(mtAdminObj.needVehicle == 0 || mtAdminObj.driverNum == 0){
                    let loanOut = await loan.findOne({ where: { taskId: 'AT-'+mtAdminID } })
                    if(loanOut) {
                        await loanRecord.create({
                            driverId: loanOut.driverId,
                            vehicleNo: loanOut.vehicleNo,
                            indentId: loanOut.indentId, 
                            taskId: loanOut.taskId,
                            startDate: loanOut.startDate,
                            endDate: loanOut.endDate, 
                            groupId: loanOut.groupId,
                            returnDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                            returnBy: req.cookies.userId,
                            creator: loanOut.creator,
                            returnRemark: cancelledCause,
                            actualStartTime: loanOut.actualStartTime,
                            actualEndTime: loanOut.actualEndTime,
                            unitId: loanOut.unitId,
                            activity: loanOut.activity,
                            purpose: loanOut.purpose,
                            createdAt: loanOut.createdAt
                        });
                        await loan.destroy({ where: { taskId: 'AT-'+mtAdminID } });
                        await OperationRecord.create({
                            id: null,
                            operatorId: req.cookies.userId,
                            businessType: 'atms task assign',
                            businessId: 'AT-'+mtAdminID,
                            optType: 'cancel loan',
                            beforeData: `${ loanOut.driverId && loanOut.driverId != '' ? `driverId:${ loanOut.driverId },` : '' }${ loanOut.vehicleNo && loanOut.vehicleNo != '' ? `vehicleNo:${ loanOut.vehicleNo }` : '' }`,
                            afterData: `${ loanOut.driverId && loanOut.driverId != '' ? `driverId:${ loanOut.driverId },` : '' }${ loanOut.vehicleNo && loanOut.vehicleNo != '' ? `vehicleNo:${ loanOut.vehicleNo }` : '' }`,
                            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                            remarks: `cancel loan ${ loanOut.driverId ? 'driver' : '' }${ loanOut.vehicleNo ? 'vehicle' : '' }` 
                        })
                    }
                } 
            }).catch(error => {
                throw error
            })
            return res.json(utils.response(1, true));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getMBTaskById: async function (req, res) {
        try {
            let mtAdminId = req.body.mtAdminId;
            let taskMtAdmin = await sequelizeObj.query(
                `
                    SELECT m.id, m.purpose, m.activityName, m.remarks, t.taskId, m.vehicleType, m.destination,  
                    m.driverNum, m.needVehicle,
                    DATE_FORMAT(m.startDate, '%Y-%m-%d %H:%i:%s') as startDate, 
                    DATE_FORMAT(m.endDate, '%Y-%m-%d %H:%i:%s') as endDate,
                    m.driverId, d.driverName,d.contactNumber, m.unitId, m.vehicleNumber, m.reportingLocation,
                    IF(t.hub IS NULL, u.unit, t.hub) AS hub,
                    IF(t.node IS NULL, u.subUnit, t.node) AS node
                    FROM mt_admin m
                    LEFT JOIN unit u ON u.id = m.unitId 
                    LEFT JOIN task t ON t.indentId = m.id
                    LEFT JOIN driver d ON d.driverId = m.driverId
                    WHERE m.id = ? AND m.cancelledDateTime IS NULL
                `, { type: QueryTypes.SELECT, replacements: [mtAdminId] }
            );
            return res.json(utils.response(1, taskMtAdmin[0]));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        } 
    },
    TaskUtils,
    initSystemTaskByTripId: async function(req, res){
        try{
            // let userId = req.cookies.userId
            let { tripId } = req.body;
            let taskData = await TaskUtils.getSystemTaskByTripId(tripId);
            let bothTaskList = taskData.filter(itemObj => itemObj.taskType.toLowerCase() == 'both')
            let toOnlyTaskList = taskData.filter(itemObj => itemObj.taskType.toLowerCase() == 'toonly')
            let vehicleOnlyTaskList = taskData.filter(itemObj => itemObj.taskType.toLowerCase() == 'vehicleonly')
            let bothNum = bothTaskList.length;
            let toOnlyNum = toOnlyTaskList.length;
            let vehicleOnlyNum = vehicleOnlyTaskList.length;
            // let driverNum = bothNum > 0 ? bothNum : toOnlyNum;
            // let vehicleNum = bothNum > 0 ? vehicleOnlyNum > 0 ? bothNum + vehicleOnlyNum : bothNum : vehicleOnlyNum;
            if(taskData.length <= 0) return res.json(utils.response(1, `trip does not exist.`));
            let hubNodeList = await TaskUtils.getClosestHubNodeListByLocation(taskData[0].pickupDestination, taskData[0].groupName)
            if(hubNodeList.length <= 0) return res.json(utils.response(1, `No hub, node available.`));
            log.warn(`hubnodeList =>${ JSON.stringify(hubNodeList) }`)
            let unitIdList = hubNodeList.map(item => item.id);
            let vehicleList = []
            let driverList = []
            if(bothTaskList.length > 0 || vehicleOnlyTaskList.length > 0) vehicleList = await TaskUtils.getVehicleList(unitIdList, taskData[0].purposeType, taskData[0].vehicleType == '-' ? null : taskData[0].vehicleType, '0', taskData[0].periodStartDate, taskData[0].periodEndDate)
            if(bothTaskList.length > 0 || toOnlyTaskList.length > 0) driverList = await TaskUtils.getDriverList(unitIdList, taskData[0].vehicleType == '-' ? null : taskData[0].vehicleType, '0', taskData[0].periodStartDate, taskData[0].periodEndDate, 'system')
            if(vehicleList.length <= 0 && driverList.length <= 0) return res.json(utils.response(1, `No driver or vehicle is available.`));
            log.warn(`vehicleList =>${ JSON.stringify(vehicleList) }`)
            log.warn(`driverList =>${ JSON.stringify(driverList) }`)
            let newRequestList = []
            if(vehicleList.length > 0 || driverList.length > 0){
                if(bothNum > 0) {
                    let requestList = []
                    for(let item of unitIdList){
                        let newVehicleList = vehicleList.filter(itemObj => itemObj.unitId == item)
                        let newDriverList = driverList.filter(itemObj => itemObj.unitId == item)
                        if(newVehicleList.length <= 0 || newDriverList.length <= 0){
                            continue;
                        }
                        let maxList = null;
                        let minList = null;
                        if(newVehicleList.length >=  newDriverList.length){
                            maxList = newVehicleList.slice(0, newDriverList.length)
                            minList = newDriverList;
                        } else {
                            maxList = newDriverList.slice(0, newVehicleList.length)
                            minList = newVehicleList;
                        }
                        // Take the arrays corresponding to driver and vehicle
                        for (let index = 0; index < maxList.length; index++) {
                            for(let index2 = 0; index2 < minList.length; index2++){
                                if(index == index2){
                                    let obj = {
                                        hub: maxList[index].hub,
                                        node: maxList[index].node,
                                        unitId: item,
                                        vehicleNo: maxList[index].vehicleNo ?? minList[index].vehicleNo,
                                        driverId: maxList[index].driverId ?? minList[index].driverId
                                    }
                                    requestList.push(obj)
                                } else {
                                    continue;
                                }
                            }
                        }
                    }
                    let __NewRequestList = requestList.slice(0, bothNum)
                    if(__NewRequestList.length <= 0) __NewRequestList = requestList
                    //The indent of both requires a different number of drivers and vehicles
                    if(vehicleOnlyNum > 0){
                        let newVehicleList = vehicleList.filter(item => !requestList.some(itemObj => itemObj.vehicleNo == item.vehicleNo))
                        let __NewVehicleList = newVehicleList.slice(0, vehicleOnlyNum)
                        if(__NewVehicleList.length <= 0) __NewVehicleList = newVehicleList
                        newRequestList = [...__NewRequestList, ...__NewVehicleList];
                    } else {
                        newRequestList = __NewRequestList
                    }
                } else {
                    //Intercept a specified number of cars and drivers
                    if(toOnlyNum > 0){
                        newRequestList = driverList.slice(0, toOnlyNum)
                        if(newRequestList.length <= 0) newRequestList = driverList
                    }
                    if(vehicleOnlyNum > 0){
                        newRequestList = vehicleList.slice(0, vehicleOnlyNum)
                        if(newRequestList.length <= 0) newRequestList = vehicleList
                    }
                }
            }
            log.warn(`newRequestList =>${ JSON.stringify(newRequestList) }`)
            if(newRequestList.length > 0){
                for (let index = 0; index < taskData.length; index++) {
                    // id and vehicle/driver corresponding to do assign operation
                    // creator 0 to indicate that the system matches by default
                   for (let index2 = 0; index2 < newRequestList.length; index2++) {
                        if(index == index2){
                            let preMvTaskId = taskData[index].id;
                            if(taskData[index].referenceId) {
                                preMvTaskId = `AT-${ taskData[index].id }`
                            }
                            if(taskData[index].taskType == 'both'){
                                if(newRequestList[index2].driverId && newRequestList[index2].vehicleNo){
                                    let result = await TaskUtils.assignTaskByTaskId(taskData[index].id, 
                                        newRequestList[index2].driverId, 
                                        newRequestList[index2].vehicleNo, 
                                        newRequestList[index2].hub, 
                                        newRequestList[index2].node,
                                        newRequestList[index2].unitId,
                                        0, 'Assigned (System)', preMvTaskId
                                        );
                                    if (result.code == 0)  return res.json(utils.response(0, result.message));
                                    // 2024-02-20 atms indent resp
                                    await TaskUtils.initAtmsIndentResp(taskData[index].id, req.cookies ? req.cookies.userId : null)
                                }
                            } else {
                                if(newRequestList[index2].driverId || newRequestList[index2].vehicleNo){
                                    let result = await TaskUtils.assignTaskByTaskId(taskData[index].id, 
                                        newRequestList[index2].driverId ?? null, 
                                        newRequestList[index2].vehicleNo ?? null, 
                                        newRequestList[index2].hub, 
                                        newRequestList[index2].node,
                                        newRequestList[index2].unitId,
                                        0, 'Assigned (System)', preMvTaskId
                                        );
                                    if (result.code == 0)  return res.json(utils.response(0, result.message));
                                    // 2024-02-20 atms indent resp
                                    await TaskUtils.initAtmsIndentResp(taskData[index].id, req.cookies ? req.cookies.userId : null)
                                }
                            }
                        } else {
                            continue;
                        }
                   }
                }
            }
            return res.json(utils.response(1, true));
        } catch(error){
            log.error(error)
            return res.json(utils.response(0, error));
        }
    }
}
