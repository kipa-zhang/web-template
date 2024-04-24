const log = require('../log/winston').logger('Hoto Service');
const FirebaseService = require('../firebase/firebase');
const utils = require('../util/utils');
const moment = require('moment');
const CONTENT = require('../util/content');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { Unit } = require('../model/unit.js');
const { HOTO } = require('../model/hoto');
const { Driver } = require('../model/driver');
const { HOTORecord } = require('../model/hotoRecord');
const { HOTORequest } = require('../model/hotoRequest');
const { OperationRecord } = require('../model/operationRecord');
const { User } = require('../model/user');

const userService = require('../services/userService');
const urgentService = require('../services/urgentService');
const { Task } = require('../model/task');
const { MtAdmin } = require('../model/mtAdmin');
const vehicleService = require('../services/vehicleService.js');
const { Vehicle } = require('../model/vehicle');
const _SystemTask = require('../model/system/task');
const _SystemDriver = require('../model/system/driver');
const _SystemVehicle = require('../model/system/vehicle');
const { DriverHistory } = require('../model/driverHistory');
const { UrgentConfig } = require('../model/urgent/urgentConfig');
const { UrgentDuty } = require('../model/urgent/urgentDuty');
const { UrgentIndent } = require('../model/urgent/urgentIndent.js');
const { UnitUtils } = require('./unitService');

let TaskUtils = {
    getUnitIdByUnitAndSubUnit: async function (hub, node) {
        let unitId
        if(!hub) {
            let unit = await Unit.findAll()
            unitId = unit.map(item => { return item.id });
            unitId = Array.from(new Set(unitId));
            unitId = (unitId.toString()).split(',')
        } else if(node){
            if(node.toLowerCase() == 'null'){
                let unit = await Unit.findOne({ where: { unit: hub, subUnit: { [Op.is]: null } } })
                unitId = []
                unitId.push(unit.id);
            } else {
                let unit = await Unit.findOne({ where: { unit: hub, subUnit: node } })
                unitId = []
                unitId.push(unit.id);
            }
            
        } else {
            let unit = await Unit.findAll({ where: { unit: hub } })
            unitId = unit.map(item => { return item.id });
            unitId = Array.from(new Set(unitId));
            unitId = (unitId.toString()).split(',')
        }
        
        return unitId
    },
    verifyDriverOrVehicleByDate: async function (vehicleNo, driverId, startDate, endDate, unfinishedTask) {
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss')
        endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss')
        let sql = `SELECT taskId, vehicleNumber, driverId from task where 1=1 `
        let replacements = []
        let groupSql = ''
        if(vehicleNo) {
            sql += ` and vehicleStatus not in ('completed', 'cancelled') and vehicleNumber = ?`
            groupSql = ` group by vehicleNumber`
            replacements.push(vehicleNo)
        }
        if(driverId) {
            sql += ` and driverStatus not in ('completed', 'cancelled') and driverId = ?`
            groupSql = ` group by driverId`
            replacements.push(driverId)
        }
        
        if(unfinishedTask){
             //Unfinished task (In this time frame started but not completed)
            sql += ` and (mobileStartTime is not null and mobileEndTime is null)
                    and (indentStartTime >= ? and indentEndTime <= ?)
            `
            replacements.push(startDate, endDate)
        } else {
             //Valid task (during this time period or started but not ended)
            sql += `
            and (((? >= indentStartTime AND ? <= indentEndTime) 
            OR (? >= indentStartTime AND ? <= indentEndTime) 
            OR (? < indentStartTime AND ? > indentEndTime))
            OR vehicleStatus = 'started')
            `
            replacements.push(startDate, startDate, endDate, endDate, startDate, endDate)
        }
        sql += groupSql;
        let result = await sequelizeObj.query(sql, {  type: QueryTypes.SELECT, replacements: replacements })
        return result;
    },
    // verifyDriverOrVehicleByDate2: async function (vehicleNo, driverId, startDate, endDate) {
    //     startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss')
    //     endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss')
    //     let sql = `SELECT taskId, vehicleNumber, driverId from task where 1=1 `
    //     let groupSql = ''
    //     if(vehicleNo) {
    //         sql += ` and vehicleStatus not in ('completed', 'cancelled') and vehicleNumber = '${ vehicleNo }'`
    //         groupSql = ` group by vehicleNumber`
    //     }
    //     if(driverId) {
    //         sql += ` and driverStatus not in ('completed', 'cancelled') and driverId = ${ driverId }`
    //         groupSql = ` group by driverId`
    //     }

    //     sql += `
    //         and ((('${ startDate }' >= indentStartTime AND '${ startDate }' <= indentEndTime) 
    //         OR ('${ endDate }' >= indentStartTime AND '${ endDate }' <= indentEndTime) 
    //         OR ('${ startDate }' < indentStartTime AND '${ endDate }' > indentEndTime))
    //         ) and mobileStartTime is not null
    //     `
    //     sql += groupSql;
    //     let result = await sequelizeObj.query(sql, {  type: QueryTypes.SELECT })
    //     return result;
    // },
    getUrgentStatusByDate: async function(vehicleNo, driverId, startDate, endDate, unfinishedUrgent){
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss')
        endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss')
        let sql = `
        select ud.id as dutyId, ud.configId, ui.indentId as taskId, ud.indentStartDate AS startDateTime, ud.indentEndDate AS endDateTime, 
        ug.vehicleType, ui.status as indentStatus,
        ud.driverId, ud.vehicleNo,
        ui.startTime, ui.endTime, ug.purpose 
        from urgent_duty ud
        left join urgent_config ug on ug.id = ud.configId
        left join (select * from urgent_indent where status not in ('completed', 'cancelled')) ui on ud.id = ui.dutyId
        where ud.status not in ('completed', 'cancelled')
        `
        let replacements = []
        if(vehicleNo){
            sql += ` and ud.vehicleNo = ?`
            replacements.push(vehicleNo)
        }
        if(driverId){
            sql += ` and ud.driverId = ?`
            replacements.push(driverId)
        }
        if(unfinishedUrgent) {
            if(unfinishedUrgent.toLowerCase() == 'true') {
                //Unfinished task (In this time frame started but not completed)
                sql += ` and (ud.mobileStartTime is not null and ud.mobileEndTime is null)
                and (ud.indentStartDate >= ? and ud.indentEndDate <= ?) `
                replacements.push(startDate, endDate)
            } else {
                sql += ` and (ud.mobileStartTime is null and ud.mobileEndTime is null)
                and (ud.indentStartDate >= ? and ud.indentEndDate <= ?) `
                replacements.push(startDate, endDate)
            }
            
        } else {
            //Valid task (during this time period or started but not ended)
            sql += `
            and (((? >= ud.indentStartDate AND ? <= ud.indentEndDate) 
            OR (? >= ud.indentStartDate AND ? <= ud.indentEndDate) 
            OR (? < ud.indentStartDate AND ? > ud.indentEndDate))
            OR ud.status = 'started')
            `
            replacements.push(startDate, startDate, endDate, endDate, startDate, endDate)
        }
        let result = await sequelizeObj.query(sql, {  type: QueryTypes.SELECT, replacements: replacements })
        return result;
    },
    getLoanOutDriverOrVehicle: async function (vehicleNo, driverId, startDate, endDate) {
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss')
        endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss')
        let sql = `
            SELECT vehicleNo, driverId, startDate, endDate
            FROM loan 
            WHERE ((? >= startDate AND ? <= endDate) 
            OR (? >= startDate AND ? <= endDate) 
            OR (? < startDate AND ? > endDate))
        `
        let replacements = [startDate, startDate, endDate, endDate, startDate, endDate];
        if(vehicleNo) {
            sql += ` and vehicleNo = ? group by vehicleNo`
            replacements.push(vehicleNo)
        }
        if(driverId) {
            sql += ` and driverId = ? group by driverId`
            replacements.push(driverId)
        }
        let loanOutDriverOrVehicle = await sequelizeObj.query(sql,
        {
            type: QueryTypes.SELECT
            , replacements: replacements
        })
        return loanOutDriverOrVehicle[0]
    },
    getHotoDriverOrVehicle: async function (vehicleNo, driverId, startDate, endDate){
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss')
        endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss')
        let sql = `
            select vehicleNo, driverId, startDateTime, endDateTime
            from hoto 
            where ((? >= startDateTime AND ? <= endDateTime) 
            OR (? >= startDateTime AND ? <= endDateTime) 
            OR (? < startDateTime AND ? > endDateTime))
            and status = 'Approved'
        `
        let replacements = [startDate, startDate, endDate, endDate, startDate, endDate]
        if(vehicleNo){
            sql += ` and vehicleNo = ? group by vehicleNo`
            replacements.push(vehicleNo)
        }
        if(driverId){
            sql += ` and driverId = ? group by driverId`
            replacements.push(driverId)
        }
        let hotoVehicleList = await sequelizeObj.query(sql,
        {
            type: QueryTypes.SELECT
            , replacements: replacements
        })
        return hotoVehicleList[0]
    },
    getStatusDriverOrVehicle: async function (vehicleNo, driverId, endDate) {
        let statusDriver = null
        let statusVehicle = null
        if(vehicleNo) {
            statusVehicle = await sequelizeObj.query(`
            SELECT groupId, vehicleNo FROM vehicle
            WHERE vehicleNo = ? and groupId IS NOT NULL group by vehicleNo
            `, 
            { type: QueryTypes.SELECT, replacements: [vehicleNo] });
            statusVehicle = statusVehicle[0]
        }
        if(driverId) {
            statusDriver = await sequelizeObj.query(`
            SELECT us.role, d.permitStatus FROM driver d
            LEFT JOIN USER us ON us.driverId = d.driverId 
            WHERE (us.role IN ('dv', 'loa') OR d.permitStatus = 'invalid') AND d.driverId = ? group by d.driverId
            `, 
            { type: QueryTypes.SELECT, replacements: [driverId] });
            statusDriver = statusDriver[0]
        }
        if(statusDriver || statusVehicle){
            return true
        } else {
            return false
        }
    },
    initOperationRecord: async function(operatorId, requestId, optType, remarks, dataList) {
        let obj = {
            id: null,
            operatorId: operatorId,
            businessType: 'hoto',
            businessId: requestId,
            optType: optType,
            afterData: dataList.length > 0 ? dataList.join(',') : '',
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            remarks: remarks
        }
        await OperationRecord.create(obj)
    },
    getFiltrationDriverOrVehicle: async function(indentStartTime, indentEndTime, type, requestId, taskState){
        if(indentStartTime && indentEndTime) {
            let sql = `
                SELECT vehicleNo, driverId FROM hoto 
                WHERE ((? >= startDateTime AND ? <= endDateTime) 
                OR (? >= startDateTime AND ? <= endDateTime) 
                OR (? < startDateTime AND ? > endDateTime)
                OR ? >= startDateTime) and status = 'Approved'
            `
            let replacements = [indentStartTime, indentStartTime, indentEndTime, 
                indentEndTime, indentStartTime, indentEndTime, indentStartTime]
            if(requestId){
                sql += ` and requestId != ?`
                replacements.push(requestId)
            }
            if(type == 'vehicle'){
                sql += ` and vehicleNo IS NOT NULL GROUP BY vehicleNo`
            }
            if(type == 'driver'){
                sql += ` and driverId IS NOT NULL GROUP BY driverId`
            }
            
            let hotoData = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
             if(type == 'vehicle'){
                hotoData = hotoData.map(item => item.vehicleNo)
             } else {
                hotoData = hotoData.map(item => item.driverId)
             }
             log.warn(`hoto assign hoto data ${ JSON.stringify(hotoData) }`)
             
             let taskDataSql = `
                SELECT t.vehicleNumber, t.driverId FROM task t
                WHERE t.vehicleStatus not in ('completed', 'cancelled')
                AND (((? >= t.indentStartTime AND ? <= t.indentEndTime) 
                OR (? >= t.indentStartTime AND ? <= t.indentEndTime) 
                OR (? < t.indentStartTime AND ? > t.indentEndTime))
                OR t.vehicleStatus = 'started')
             `
             let replacements2 = [indentStartTime, indentStartTime, indentEndTime, 
                indentEndTime, indentStartTime, indentEndTime]
             if(type == 'vehicle'){
                taskDataSql += ` and t.vehicleNumber IS NOT NULL GROUP BY t.vehicleNumber`
             }
             if(type == 'driver'){
                taskDataSql += ` and t.driverId IS NOT NULL GROUP BY t.driverId`
             }
             console.log(taskDataSql)
             let taskData = await sequelizeObj.query(taskDataSql, { type: QueryTypes.SELECT, replacements: replacements2 })
             if(type == 'vehicle'){
                taskData = taskData.map(item => item.vehicleNumber)
             } else {
                taskData = taskData.map(item => item.driverId)
             }
             log.warn(`hoto assign task data ${ JSON.stringify(taskData) }`)
            let excludeData = hotoData
            if(taskState) {
                excludeData = hotoData.concat(taskData);    
            }
            excludeData = excludeData.map(item => item);
            excludeData = Array.from(new Set(excludeData))
            log.warn(`Need to exclude the data ${ JSON.stringify(excludeData) }`)
            return excludeData
        }
        return []
    },
    getTaskByHotoVehicleOrDriver: async function (vehicleNo, driverId, startDate, endDate) {
        startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss')
        endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss')
        let sql = `SELECT t.*,
        t.indentStartTime AS startDateTime, t.indentEndTime AS endDateTime, v.vehicleType
        FROM task t
        LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
        WHERE 1=1 `
        let replacements = []
        if(vehicleNo) {
            sql += ` and t.vehicleStatus not in ('completed', 'cancelled') and t.vehicleNumber = ?`
            replacements.push(vehicleNo)
        }
        if(driverId) {
            sql += ` and t.driverStatus not in ('completed', 'cancelled') and t.driverId = ?`
            replacements.push(driverId)
        }

        sql += `
            and (t.mobileStartTime is null and t.mobileEndTime is null)
            AND (t.indentStartTime >= ? AND t.indentEndTime <= ?)
        `
        replacements.push(startDate, endDate)
        sql += ` group by t.taskId`;
        let result = await sequelizeObj.query(sql, {  type: QueryTypes.SELECT, replacements: replacements })
        return result;
    },
    driverTypeByVehicleType: async function (driverId, vehicleType) {
        let result = await sequelizeObj.query(`SELECT d.driverId, dc.vehicleType FROM driver d
        LEFT JOIN driver_platform_conf dc ON dc.driverId = d.driverId AND dc.approveStatus='Approved'
        WHERE d.driverId = ? AND dc.vehicleType = ?`, 
        {  type: QueryTypes.SELECT, replacements: [driverId, vehicleType] })
        return result;
    }
}

module.exports.getHubNode = async function (req, res) {
    try {
        let hub = req.body.hub;
        let node = req.body.node;
        let result = null
        if(hub){
            if(node) {
                result = await Unit.findAll({ where: { unit: hub, subUnit: node } });
                return res.json(utils.response(1, result));
            } else {
                result = await Unit.findAll({ where: { unit: hub } });
                return res.json(utils.response(1, result));
            }
        }
        result = await Unit.findAll();
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getVehicleTypeList = async function (req, res) {
    try {
        let vehicleTypeList = await sequelizeObj.query(`SELECT vehicleType FROM vehicle GROUP BY vehicleType order by vehicleType asc;`, { type: QueryTypes.SELECT })
        vehicleTypeList = vehicleTypeList.map(item => item.vehicleType)
        return res.json(utils.response(1, vehicleTypeList));
    } catch(error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getVehicleList = async function (req, res) {
    try {
        let { formHub, formNode, dataType, requestId, vehicleType, vehicleNo, requestState, pageNum, pageLength } = req.body;
        let unitIdList = []
        let dataList = await UnitUtils.getPermitUnitList(req.cookies.userId);
        unitIdList = dataList.unitIdList;
        if(formHub) {
            if(formNode){
                let unit2 = await Unit.findOne({ where: { unit: formHub, subUnit: formNode } })
                unitIdList = [unit2.id]
            } else {
                let unit = await Unit.findAll({ where: { unit: formHub } })
                let hubUnitIdList = unit.map(item => item.id)
                unitIdList = unitIdList.filter(item => hubUnitIdList.includes(item))
            }
        } 
        
        let hotoRequest = await HOTORequest.findOne({ where: { id: requestId } });
        let indentStartTime = hotoRequest.startTime ? moment(hotoRequest.startTime).format('YYYY-MM-DD HH:mm:ss') : null;
        let indentEndTime = hotoRequest.endTime ? moment(hotoRequest.endTime).format('YYYY-MM-DD HH:mm:ss') : null;

        let notUnitId = []
        let newhub = hotoRequest.hub;
        let newnode = hotoRequest.node;
        if(newnode) {
            let unit = await Unit.findOne({ where: { unit: newhub, subUnit: newnode } })
            notUnitId.push(unit.id)
        } else {
           let unitList = await Unit.findAll({ where: { unit: newhub } })
           notUnitId = unitList.map(item => item.id)
        }
        let excludeData = await TaskUtils.getFiltrationDriverOrVehicle(indentStartTime, indentEndTime, 'vehicle', requestId, true);
        let replacements = [];
        let replacements2 = [];
        let sql = `
        select vv.vehicleNo, vv.unit, vv.subUnit, vv.groupId, vv.toHub, vv.toNode,  vv.id, vv.vehicleType,
        vv.returnDateTime, vv.hotoDateTime, vv.startDateTime, vv.endDateTime, vv.hotoId, vv.status, vv.updatedAt
        from (
            SELECT v.vehicleNo, u.unit, u.subUnit, u.id, v.groupId, hr.returnDateTime, v.vehicleType, h.updatedAt, 
            IF(h.toHub IS NULL, hr.toHub, h.toHub) toHub,
            IF(h.toHub IS NULL, hr.toNode, h.toNode) toNode, 
            IF(h.hotoDateTime IS NULL, hr.createdAt, h.hotoDateTime) hotoDateTime, 
            IF(h.startDateTime IS NULL, hr.startDateTime, h.startDateTime) startDateTime, 
            IF(h.endDateTime IS NULL, hr.endDateTime, h.endDateTime) endDateTime,
            IF(h.id IS NULL, hr.id, h.id) hotoId,
            if(h.requestId is null, hr.requestId, h.requestId) requestId,
            if(h.status is null, 'Completed', h.status) status
            FROM (
                SELECT vehicleNo, nextMptTime, nextAviTime, nextWpt2Time, nextWpt3Time, nextWpt1Time, unitId, groupId, vehicleType FROM vehicle
                UNION ALL 
                SELECT vehicleNo, nextMptTime, nextAviTime, nextWpt2Time, nextWpt3Time, nextWpt1Time, unitId, groupId, vehicleType FROM vehicle_history
            ) v
            LEFT JOIN unit u ON u.id = v.unitId
            LEFT JOIN (select ho.* from hoto ho WHERE ho.requestId = ?) h ON h.vehicleNo = v.vehicleNo
            LEFT JOIN (select hr.* from hoto_record hr WHERE hr.requestId = ? ORDER BY hr.returnDateTime DESC) hr ON hr.vehicleNo = v.vehicleNo
        ) vv where vv.groupId IS NULL and vv.unit is not null        
        `
        replacements.push(requestId, requestId)
        let sql2 = `
        select COUNT(DISTINCT vv.vehicleNo) total, vv.unit, vv.subUnit, vv.groupId, vv.requestId, vv.id, vv.vehicleType, vv.status, vv.endDateTime from (
            SELECT v.vehicleNo, u.unit, u.subUnit, v.groupId, u.id, v.vehicleType,
            if(h.requestId is null, hr.requestId, h.requestId) requestId,
            IF(h.endDateTime IS NULL, hr.endDateTime, h.endDateTime) endDateTime,
            if(h.status is null, 'Completed', h.status) status
            FROM (
                SELECT vehicleNo, nextMptTime, nextAviTime, nextWpt2Time, nextWpt3Time, nextWpt1Time, unitId, groupId, vehicleType FROM vehicle
                UNION ALL 
                SELECT vehicleNo, nextMptTime, nextAviTime, nextWpt2Time, nextWpt3Time, nextWpt1Time, unitId, groupId, vehicleType FROM vehicle_history
            ) v
            LEFT JOIN unit u ON u.id = v.unitId
            LEFT JOIN (select ho.* from hoto ho WHERE ho.requestId = ?) h ON h.vehicleNo = v.vehicleNo
            LEFT JOIN (select hr.* from hoto_record hr WHERE hr.requestId = ? ORDER BY hr.returnDateTime DESC) hr ON hr.vehicleNo = v.vehicleNo
        ) vv where vv.groupId IS NULL and vv.unit is not null   
        `  
        replacements2.push(requestId, requestId)
        if(dataType != 'assign'){
            sql += ` and vv.requestId = ?`
            sql2 += ` and vv.requestId = ?`
            if(dataType == 'return') {
                sql += ` and vv.status in('Approved', 'Completed')`
                sql2 += ` and vv.status in('Approved', 'Completed')`
            }
            replacements.push(requestId)
            replacements2.push(requestId)
        } else {
            if(excludeData.length > 0){
                sql += ` and vv.vehicleNo not in (?)`
                sql2 += ` and vv.vehicleNo not in (?)`
                replacements.push(`'${ excludeData.join("','") }'`)
                replacements2.push(`'${ excludeData.join("','") }'`)
            }
            if(unitIdList.length > 0){
                if(notUnitId.length > 0) unitIdList = unitIdList.filter(item => !notUnitId.includes(item));
                if(unitIdList.length > 0) {
                    sql += ` and vv.id in(?)`
                    sql2 += ` and vv.id in(?)`
                    replacements.push(unitIdList.join(","))
                    replacements2.push(unitIdList.join(","))
                } else {
                    sql += ` and 1=2`
                    sql2 += ` and 1=2`
                }
            } else if(notUnitId.length > 0){
                sql += ` and vv.id not in (?)`
                sql2 += ` and vv.id not in (?)`
                replacements.push(notUnitId.join(','))
                replacements2.push(notUnitId.join(','))
            }
            
            if(vehicleNo) {
                sql += ` and vv.vehicleNo like ?`
                sql2 += ` and vv.vehicleNo like ?`
                replacements.push(`'%${ vehicleNo }%'`)
                replacements2.push(`'%${ vehicleNo }%'`)
            }
        }
        if(vehicleType){
            sql += ` and vv.vehicleType = ?`
            sql2 += ` and vv.vehicleType = ?`
            replacements.push(vehicleType)
            replacements2.push(vehicleType)
        }
        if(requestState || dataType == 'approve') {
            sql += ` and vv.status != 'Completed'`
            sql2 += ` and vv.status != 'Completed'`
        }
        if(dataType.toLowerCase() == 'replace'){
            sql += ` and vv.endDateTime > ?`
            sql += ` and vv.endDateTime > ?`
            replacements.push(moment().format('YYYY-MM-DD HH:mm'))
            replacements2.push(moment().format('YYYY-MM-DD HH:mm'))
        }
        let countResult = await sequelizeObj.query(sql2, { type: QueryTypes.SELECT, replacements: replacements2 })
        let totalRecord = countResult[0].total
        pageLength = pageLength || 10
        pageNum = pageNum || 0
        pageNum = Number(pageNum)
        pageLength = Number(pageLength)
        sql += ` GROUP BY vv.vehicleNo order by vv.hotoDateTime desc, vv.updatedAt desc `
        if((pageNum || pageNum == 0) && pageLength){
            sql += ` limit ?,?`
            replacements.push(pageNum)
            replacements.push(pageLength)
        }
        let pageResult = await sequelizeObj.query(sql,
            {
                type: QueryTypes.SELECT
                , replacements: replacements
            }
        );
        if(dataType == 'view'){
            for(let item of pageResult){
                if(item.status == 'Completed'){
                    if(moment(item.hotoDateTime).format('YYYY-MM-DD HH:mm') == moment(item.returnDateTime).format('YYYY-MM-DD HH:mm')){
                        let operationRecordList = await OperationRecord.findAll({ where: { businessId: requestId, businessType: 'hoto', optType: 'assign', afterData: { [Op.not]: null } } })
                        if(operationRecordList.length > 0){
                            for(let item2 of operationRecordList){
                                let newDataList = null;
                                let dataList = item2.afterData.split(',')
                                if(dataList.length > 0) {
                                    newDataList = dataList.filter(data => data == item.vehicleNo)
                                    if(newDataList.length > 0){
                                        item.hotoDateTime = item2.optTime;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return res.json({ data: pageResult, recordsFiltered: totalRecord, recordsTotal: totalRecord })
    } catch(error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getVehicleListByReplace = async function (req, res) {
    try {
        let { formHub, formNode, dataType, requestId, vehicleType, vehicleNo } = req.body;

        let unitIdList = []
        let userUnit = await UnitUtils.getPermitUnitList(req.cookies.userId);
        unitIdList = userUnit.unitIdList;

        if(formHub) {
            if(formNode){
                let unit2 = await Unit.findOne({ where: { unit: formHub, subUnit: formNode } })
                unitIdList = [unit2.id]
            } else {
                let unit = await Unit.findAll({ where: { unit: formHub } })
                let hubUnitIdList = unit.map(item => item.id)
                unitIdList = unitIdList.filter(item => hubUnitIdList.includes(item))
            }
        } 
        
        let hotoRequest = await HOTORequest.findOne({ where: { id: requestId } });
        let indentStartTime = hotoRequest.startTime ? moment(hotoRequest.startTime).format('YYYY-MM-DD HH:mm:ss') : null;
        let indentEndTime = hotoRequest.endTime ? moment(hotoRequest.endTime).format('YYYY-MM-DD HH:mm:ss') : null;

        let notUnitId = []
        let newhub = hotoRequest.hub;
        let newnode = hotoRequest.node;
        if(newnode) {
            let unit = await Unit.findOne({ where: { unit: newhub, subUnit: newnode } })
            notUnitId.push(unit.id)
        } else {
           let unitList = await Unit.findAll({ where: { unit: newhub } })
           notUnitId = unitList.map(item => item.id)
        }
        let excludeData = await TaskUtils.getFiltrationDriverOrVehicle(indentStartTime, indentEndTime, 'vehicle', requestId, false);
        let sql = `
        select vv.vehicleNo, vv.unit, vv.subUnit, vv.groupId, vv.toHub, vv.toNode,  vv.id, vv.vehicleType,
        vv.returnDateTime, vv.hotoDateTime, vv.startDateTime, vv.endDateTime, vv.hotoId, vv.status
        from (
            SELECT v.vehicleNo, u.unit, u.subUnit, u.id, v.groupId, hr.returnDateTime, v.vehicleType,
            IF(h.toHub IS NULL, hr.toHub, h.toHub) toHub,
            IF(h.toHub IS NULL, hr.toNode, h.toNode) toNode, 
            IF(h.hotoDateTime IS NULL, hr.hotoDateTime, h.hotoDateTime) hotoDateTime, 
            IF(h.startDateTime IS NULL, hr.startDateTime, h.startDateTime) startDateTime, 
            IF(h.endDateTime IS NULL, hr.endDateTime, h.endDateTime) endDateTime,
            IF(h.id IS NULL, hr.id, h.id) hotoId,
            if(h.requestId is null, hr.requestId, h.requestId) requestId,
            if(h.status is null, 'Completed', h.status) status
            FROM vehicle v
            LEFT JOIN unit u ON u.id = v.unitId
            LEFT JOIN (select ho.* from hoto ho WHERE ho.requestId = ?) h ON h.vehicleNo = v.vehicleNo
            LEFT JOIN (select hr.* from hoto_record hr WHERE hr.requestId = ? ORDER BY hr.returnDateTime DESC) hr ON hr.vehicleNo = v.vehicleNo
        ) vv where vv.groupId IS NULL and vv.unit is not null        
        `
        let replacements = [requestId, requestId]
        if(dataType != 'assign'){
            sql += ` and vv.requestId = ?`
            replacements.push(requestId)
            if(dataType == 'return') {
                sql += ` and vv.status = 'Approved'`
            }
        } else {
            if(excludeData.length > 0){
                sql += ` and vv.vehicleNo not in (?)`
                replacements.push(`'${ excludeData.join("','") }'`)
            }
            if(unitIdList.length > 0){
                if(notUnitId.length > 0) unitIdList = unitIdList.filter(item => !notUnitId.includes(item));
                if(unitIdList.length > 0) {
                    sql += ` and vv.id in(?)`
                    replacements.push(unitIdList.join(","))
                } else {
                    sql += ` and 1=2`
                }
            } else if(notUnitId.length > 0){
                sql += ` and vv.id not in (?)`
                replacements.push(notUnitId.join(','))
            }

            if(vehicleNo) {
                sql += ` and vv.vehicleNo like ?`
                replacements.push(`%${ vehicleNo }%`)
            }
        }
        if(vehicleType){
            sql += ` and vv.vehicleType = ?`
            replacements.push(vehicleType)
        }
        let dataList = await sequelizeObj.query(
            sql + ` GROUP BY vv.vehicleNo order by vv.hotoDateTime desc; `,
            {
                type: QueryTypes.SELECT
                , replacements: replacements
            }
        );
        return res.json(utils.response(1, dataList));
    } catch(error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverList = async function (req, res) {
    try {
        let { purpose, formHub, formNode, dataType, requestId, vehicleType, driverName, permitType, requestState, pageNum, pageLength } = req.body;

        let unitIdList = []
        let userUnit = await UnitUtils.getPermitUnitList(req.cookies.userId);
        unitIdList = userUnit.unitIdList;

        if(formHub) {
            if(formNode){
                let unit2 = await Unit.findOne({ where: { unit: formHub, subUnit: formNode } })
                unitIdList = [unit2.id]
            } else {
                let unit = await Unit.findAll({ where: { unit: formHub } })
                let hubUnitIdList = unit.map(item => item.id)
                unitIdList = unitIdList.filter(item => hubUnitIdList.includes(item))
            }
        } 

        let hotoRequest = await HOTORequest.findOne({ where: { id: requestId } });
        let indentStartTime = hotoRequest.startTime ? moment(hotoRequest.startTime).format('YYYY-MM-DD HH:mm:ss') : null;
        let indentEndTime = hotoRequest.endTime ? moment(hotoRequest.endTime).format('YYYY-MM-DD HH:mm:ss') : null;

        let notUnitId = []
        let newhub = hotoRequest.hub;
        let newnode = hotoRequest.node;
        if(newnode) {
            let unit = await Unit.findOne({ where: { unit: newhub, subUnit: newnode } })
            notUnitId.push(unit.id)
        } else {
           let unitList = await Unit.findAll({ where: { unit: newhub } })
           notUnitId = unitList.map(item => item.id)
        }
        let excludeData = await TaskUtils.getFiltrationDriverOrVehicle(indentStartTime, indentEndTime, 'driver', requestId, true);
        if(purpose.toLowerCase() == 'driving training'){
            vehicleType = null
        } 
        if(purpose.toLowerCase() == 'familiarisation') {
            vehicleType = null
            unitIdList = []
        }
        let replacements = []
        let replacements2 = []
        let sql = `
        SELECT dd.driverId, dd.driverName, dd.unit, dd.subUnit, dd.toHub, dd.toNode, dd.requestId, dd.permitType, dd.updatedAt,
        dd.returnDateTime, dd.status, dd.hotoDateTime, dd.startDateTime, dd.endDateTime, dd.hotoId, dd.id, dd.vehicleType
        FROM (
            SELECT d.driverId, d.driverName, u.unit, u.subUnit, u.id, hr.returnDateTime, d.permitType, dc.vehicleType, h.updatedAt,
            IF(h.toHub IS NULL, hr.toHub, h.toHub) toHub,
            IF(h.toHub IS NULL, hr.toNode, h.toNode) toNode, 
            IF(h.hotoDateTime IS NULL, hr.createdAt, h.hotoDateTime) hotoDateTime, 
            IF(h.startDateTime IS NULL, hr.startDateTime, h.startDateTime) startDateTime, 
            IF(h.endDateTime IS NULL, hr.endDateTime, h.endDateTime) endDateTime,
            IF(h.id IS NULL, hr.id, h.id) hotoId,
            if(h.requestId is null, hr.requestId, h.requestId) requestId,
            if(h.status is null, 'Completed', h.status) status
            FROM (
                SELECT driverId, driverName, unitId, permitType, permitStatus, operationallyReadyDate  FROM driver 
                UNION ALL 
                SELECT driverId, driverName, unitId, permitType, permitStatus, operationallyReadyDate FROM driver_history
            ) d 
            LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN USER us ON us.driverId = d.driverId
            LEFT JOIN (SELECT ho.* FROM hoto ho WHERE ho.requestId = ?) h ON h.driverId = d.driverId
            LEFT JOIN (select hr.* from hoto_record hr WHERE hr.requestId = ? ORDER BY hr.returnDateTime DESC) hr ON hr.driverId = d.driverId
            WHERE d.permitStatus != 'invalid' AND us.role IN('TO', 'TL')
        `
        replacements.push(requestId)
        replacements.push(requestId)
        if(indentEndTime){
            sql += ` AND (d.operationallyReadyDate > ? OR d.operationallyReadyDate IS NULL)`
            replacements.push(moment(indentEndTime).format('YYYY-MM-DD'))
        }
        sql += `  ) dd WHERE dd.unit is not null`
        let sql2 = `
        select COUNT(DISTINCT dd.driverId) total, dd.driverName, dd.unit, dd.subUnit, dd.requestId, dd.id, dd.permitType, dd.vehicleType, dd.status, dd.endDateTime from (
            SELECT d.driverId, d.driverName, u.unit, u.subUnit, u.id, d.permitType, dc.vehicleType,
            if(h.requestId is null, hr.requestId, h.requestId) requestId,
            IF(h.endDateTime IS NULL, hr.endDateTime, h.endDateTime) endDateTime,
            if(h.status is null, 'Completed', h.status) status
            FROM (
                SELECT driverId, driverName, unitId, permitType, permitStatus, operationallyReadyDate  FROM driver 
                UNION ALL 
                SELECT driverId, driverName, unitId, permitType, permitStatus, operationallyReadyDate FROM driver_history
            ) d 
            LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN USER us ON us.driverId = d.driverId
            LEFT JOIN (SELECT ho.* FROM hoto ho WHERE ho.requestId = ?) h ON h.driverId = d.driverId
            LEFT JOIN (select hr.* from hoto_record hr WHERE hr.requestId = ? ORDER BY hr.returnDateTime DESC) hr ON hr.driverId = d.driverId
            WHERE d.permitStatus != 'invalid' AND us.role IN('TO', 'TL')
        `  
        replacements2.push(requestId)
        replacements2.push(requestId)
        if(indentEndTime){
            sql2 += ` AND (d.operationallyReadyDate > ? OR d.operationallyReadyDate IS NULL)`
            replacements2.push(moment(indentEndTime).format('YYYY-MM-DD'))
        }
        sql2 += ` ) dd where dd.unit is not null   `
        if(dataType != 'assign') {
            sql += ` and dd.requestId = ?`
            sql2 += ` and dd.requestId = ?`
            replacements.push(requestId)
            replacements2.push(requestId)
            if(dataType == 'return') {
                sql += ` and dd.status in('Approved', 'Completed')`
                sql2 += ` and dd.status in('Approved', 'Completed')`
            }
        } else {
            if(excludeData.length > 0) {
                sql += ` and dd.driverId not in (?)`
                sql2 += ` and dd.driverId not in (?)`
                replacements.push(excludeData.join(","))
                replacements2.push(excludeData.join(","))
            }
            if(unitIdList.length > 0){
                if(notUnitId.length > 0) unitIdList = unitIdList.filter(item => !notUnitId.includes(item));
                if(unitIdList.length > 0) {
                    sql += ` and dd.id in (?)`
                    sql2 += ` and dd.id in (?)`
                    replacements.push(unitIdList.join(','))
                    replacements2.push(unitIdList.join(','))
                } else {
                    sql += ` and 1=2`
                    sql2 += ` and 1=2`
                }
            } else if(notUnitId.length > 0){
                sql += ` and dd.id not in (?)`
                sql2 += ` and dd.id not in (?)`
                replacements.push(notUnitId.join(','))
                replacements2.push(notUnitId.join(','))
            }
            if (permitType) {
                let permitTypeSql = []
                let list = permitType.split(',')
                for (let item of list) {
                    permitTypeSql.push(` FIND_IN_SET(?, dd.permitType) `)
                    replacements.push(item)
                    replacements2.push(item)
                }
                if(permitTypeSql.length > 0){
                    sql += `  and ( ${ permitTypeSql.join(' OR ') } )`
                    sql2 += `  and ( ${ permitTypeSql.join(' OR ') } )`
                }
            }
            if(driverName){
                sql += ` and dd.driverName like ?`
                sql2 += ` and dd.driverName like ?`
                replacements.push(`'%${ driverName }%'`)
                replacements2.push(`'%${ driverName }%'`)
            }
        }
        if(vehicleType){
            sql += ` and dd.vehicleType = ?`
            sql2 += ` and dd.vehicleType = ?`
            replacements.push(vehicleType)
            replacements2.push(vehicleType)
        }
        if(requestState || dataType == 'approve') {
            sql += ` and dd.status != 'Completed'`
            sql2 += ` and dd.status != 'Completed'`
        }
        if(dataType.toLowerCase() == 'replace'){
            sql += ` and dd.endDateTime > ?`
            sql += ` and dd.endDateTime > ?`
            replacements.push(moment().format('YYYY-MM-DD HH:mm'))
            replacements.push(moment().format('YYYY-MM-DD HH:mm'))
        }
        let countResult = await sequelizeObj.query(sql2, { type: QueryTypes.SELECT, replacements: replacements2 })
        let totalRecord = countResult[0].total
        pageLength = pageLength || 10
        pageNum = pageNum || 0
        sql += ` GROUP BY dd.driverId order by dd.hotoDateTime desc, dd.updatedAt desc`
        if((pageNum || pageNum == 0) && pageLength){
            sql += ` limit ?,?`
            replacements.push(...[Number(pageNum), Number(pageLength)])
        }

        let pageResult = await sequelizeObj.query(sql,
            {
                type: QueryTypes.SELECT
                , replacements: replacements
            }
        );
        if(dataType == 'view'){
            for(let item of pageResult){
                if(item.status == 'Completed'){
                    if(moment(item.hotoDateTime).format('YYYY-MM-DD HH:mm') == moment(item.returnDateTime).format('YYYY-MM-DD HH:mm')){
                        let operationRecordList = await OperationRecord.findAll({ where: { businessId: requestId, businessType: 'hoto', optType: 'assign', afterData: { [Op.not]: null } } })
                        if(operationRecordList.length > 0){
                            for(let item2 of operationRecordList){
                                let newDataList = null;
                                let dataList = item2.afterData.split(',')
                                if(dataList.length > 0) {
                                    newDataList = dataList.filter(data => data == item.driverId)
                                    if(newDataList.length > 0){
                                        item.hotoDateTime = item2.optTime;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return res.json({ data: pageResult, recordsFiltered: totalRecord, recordsTotal: totalRecord })
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverListByReplace = async function (req, res) {
    try {
        let { purpose, formHub, formNode, dataType, requestId, vehicleType, driverName, permitType } = req.body;
        let unitIdList = []
        let userUnit = await UnitUtils.getPermitUnitList(req.cookies.userId);
        unitIdList = userUnit.unitIdList;

        if(formHub) {
            if(formNode){
                let unit2 = await Unit.findOne({ where: { unit: formHub, subUnit: formNode } })
                unitIdList = [unit2.id]
            } else {
                let unit = await Unit.findAll({ where: { unit: formHub } })
                let hubUnitIdList = unit.map(item => item.id)
                unitIdList = unitIdList.filter(item => hubUnitIdList.includes(item))
            }
        } 

        let hotoRequest = await HOTORequest.findOne({ where: { id: requestId } });
        let indentStartTime = hotoRequest.startTime ? moment(hotoRequest.startTime).format('YYYY-MM-DD HH:mm:ss') : null;
        let indentEndTime = hotoRequest.endTime ? moment(hotoRequest.endTime).format('YYYY-MM-DD HH:mm:ss') : null;

        let notUnitId = []
        let newhub = hotoRequest.hub;
        let newnode = hotoRequest.node;
        if(newnode) {
            let unit = await Unit.findOne({ where: { unit: newhub, subUnit: newnode } })
            notUnitId.push(unit.id)
        } else {
           let unitList = await Unit.findAll({ where: { unit: newhub } })
           notUnitId = unitList.map(item => item.id)
        }
        let excludeData = await TaskUtils.getFiltrationDriverOrVehicle(indentStartTime, indentEndTime, 'driver', requestId, false);
        if(purpose.toLowerCase() == 'driving training'){
            vehicleType = null
        } 
        if(purpose.toLowerCase() == 'familiarisation') {
            vehicleType = null
        }
        let sql = `
        SELECT dd.driverId, dd.driverName, dd.unit, dd.subUnit, dd.toHub, dd.toNode, dd.requestId, dd.permitType,
        dd.returnDateTime, dd.status, dd.hotoDateTime, dd.startDateTime, dd.endDateTime, dd.hotoId, dd.id, dd.vehicleType
        FROM (
            SELECT d.driverId, d.driverName, u.unit, u.subUnit, u.id, hr.returnDateTime, d.permitType, dc.vehicleType,
            IF(h.toHub IS NULL, hr.toHub, h.toHub) toHub,
            IF(h.toHub IS NULL, hr.toNode, h.toNode) toNode, 
            IF(h.hotoDateTime IS NULL, hr.hotoDateTime, h.hotoDateTime) hotoDateTime, 
            IF(h.startDateTime IS NULL, hr.startDateTime, h.startDateTime) startDateTime, 
            IF(h.endDateTime IS NULL, hr.endDateTime, h.endDateTime) endDateTime,
            IF(h.id IS NULL, hr.id, h.id) hotoId,
            if(h.requestId is null, hr.requestId, h.requestId) requestId,
            if(h.status is null, 'Completed', h.status) status
            FROM driver d
            LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN USER us ON us.driverId = d.driverId
            LEFT JOIN (SELECT ho.* FROM hoto ho WHERE ho.requestId = ?) h ON h.driverId = d.driverId
            LEFT JOIN (select hr.* from hoto_record hr WHERE hr.requestId = ? ORDER BY hr.returnDateTime DESC) hr ON hr.driverId = d.driverId
            WHERE d.permitStatus != 'invalid' AND us.role IN('TO', 'TL')
        ) dd WHERE dd.unit is not null
        `
        let replacements = [requestId, requestId]
        if(indentEndTime){
            sql += ` AND (d.operationallyReadyDate > ? OR d.operationallyReadyDate IS NULL)`
            replacements.push(moment(indentEndTime).format('YYYY-MM-DD'))
        }
        if(dataType != 'assign') {
            sql += ` and dd.requestId = ?`
            replacements.push(requestId)
            if(dataType == 'return') {
                sql += ` and dd.status = 'Approved'`
            }
        } else {
            if(excludeData.length > 0) {
                sql += ` and dd.driverId not in (?)`
                replacements.push(excludeData.join(","))
            }
            if(unitIdList.length > 0){
                if(notUnitId.length > 0) unitIdList = unitIdList.filter(item => !notUnitId.includes(item));
                if(unitIdList.length > 0) {
                    sql += ` and dd.id in (?)`
                    replacements.push(unitIdList.join(','))
                } else {
                    sql += ` and 1=2`
                }
            } else if(notUnitId.length > 0){
                sql += ` and dd.id not in (?)`
                replacements.push(notUnitId.join(','))
            }

            if (permitType) {
                let permitTypeSql = []
                let list = permitType.split(',')
                for (let item of list) {
                    permitTypeSql.push(` FIND_IN_SET(?, dd.permitType) `)
                    replacements.push(item)
                }
                if(permitTypeSql.length > 0){
                    sql += `  and ( ${ permitTypeSql.join(' OR ') } )`
                }
            }
            if(driverName){
                sql += ` and dd.driverName like ?`
                replacements.push(`'%${ driverName }%'`)
            }
        }
        if(vehicleType){
            sql += ` and dd.vehicleType = ?`
            replacements.push(vehicleType)
        }

        let pageResult = await sequelizeObj.query(
            sql + ` GROUP BY dd.driverId order by dd.hotoDateTime desc; `,
            {
                type: QueryTypes.SELECT
                , replacements: replacements
            }
        );
        return res.json(utils.response(1, pageResult));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getRequestListByHistory = async function (req, res) {
    try {
        let { requestId, pageNum, pageLength } = req.body;
        let dataList = [];
        let operationRecord = await OperationRecord.findAll({ where: { businessId: requestId, optType: 'replace' } })
        for(let item of operationRecord){
            let oldDriver = null;
            let newDriver = null;
            let optUser = null;
            if(item.afterData) item.afterData = JSON.parse(item.afterData)
            if(item.beforeData) item.beforeData = JSON.parse(item.beforeData)
            if(item.afterData[0].driverId) {
                oldDriver = await Driver.findOne({ where: { driverId: item.beforeData[0].driverId } })
                if(!oldDriver) oldDriver = await DriverHistory.findOne({ where: { driverId: item.beforeData[0].driverId } })
                newDriver = await Driver.findOne({ where: { driverId: item.afterData[0].driverId } })
            }
            if(item.operatorId) {
                optUser = await User.findOne({ where: { userId: item.operatorId } })
            }
            dataList.push({ 
                newResource: newDriver ? newDriver.driverName : item.afterData[0].vehicleNo,
                oldResource: oldDriver ? oldDriver.driverName : item.beforeData[0].vehicleNo,
                startDateTime: item.afterData[0].startDateTime,
                endDateTime: item.afterData[0].endDateTime,
                optDate: item.optTime,
                optby: optUser ? optUser.fullName : ''
            })
        }
        let data = dataList.slice(pageNum, Number(pageLength)+Number(pageNum))
        return res.json({ data: data, recordsFiltered: dataList.length, recordsTotal: dataList.length })
    } catch(error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.operateRequestById = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let { operateType, requestId, hotoIdList } = req.body;
        // 2023-09-18 If hoto has returned data, it cannot cancel.
        if(operateType.toLowerCase() == 'cancelled' || operateType.toLowerCase() == 'rejected'){
            let hotoRecordList = await HOTORecord.findAll({ where: { requestId: requestId } })
            if(hotoRecordList.length > 0) return res.json(utils.response(0, [` Data has been returned and cannot be canceled.`]));
        }

        await sequelizeObj.transaction(async transaction => {
            let oldHotoRequest = await HOTORequest.findOne({ where: { id: requestId } })
            let oldHotoList = await HOTO.findAll({ where: { requestId: requestId } })
            let hotoDriverOrVehicleListSql = `
                SELECT driverId, vehicleNo, startDateTime, endDateTime FROM hoto WHERE requestId = ?
            `
            let replacementsByhotoDriverOrVehicleList = [requestId]
            if(hotoIdList?.length > 0){
                hotoDriverOrVehicleListSql += ` and id in(?)`
                replacementsByhotoDriverOrVehicleList.push(hotoIdList.join(','))
            }
            if(operateType.toLowerCase() == 'cancelled' || operateType.toLowerCase() == 'rejected'){
                hotoDriverOrVehicleListSql += ` and status = 'Approved'`
            }
            hotoDriverOrVehicleListSql += ` group by id`
            let hotoDriverOrVehicleList = await sequelizeObj.query(hotoDriverOrVehicleListSql, { type: QueryTypes.SELECT, replacements: replacementsByhotoDriverOrVehicleList })
            let errorList = []
            for(let item of hotoDriverOrVehicleList){
                let driver = null;
                if(item.driverId) driver = await Driver.findOne({ where: { driverId: item.driverId } })
                if(operateType.toLowerCase() != 'rejected' && operateType.toLowerCase() != 'cancelled'){
                    let hotoByDriverOrVehicle = await TaskUtils.getHotoDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                    if(hotoByDriverOrVehicle) {
                        errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } In this time frame has been hoto.`)
                    }
                }
                let state = await TaskUtils.verifyDriverOrVehicleByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(state.length > 0){
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } The operation cannot be performed because some tasks are not completed.`)
                }
                let state2 = await TaskUtils.getUrgentStatusByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(state2.length > 0){
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } Some Urgent tasks were not completed, so the operation could not be completed.`)
                }
                let loanOutByDriverOrVehicle = await TaskUtils.getLoanOutDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(loanOutByDriverOrVehicle) {
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } It's been loaned out.`);
                }
            }
            if(errorList.length > 0) {
                throw errorList
            }
            
            let remarksName = null;
            let statusName = null;
            if(operateType.toLowerCase() == 'rejected'){
                statusName = 'reject'
            } else if(operateType.toLowerCase() == 'cancelled'){
                statusName = 'cancel'
            } else if(operateType.toLowerCase() == 'approved'){
                statusName = 'approve'
            } else if(operateType.toLowerCase() == 'endorsed'){
                statusName = 'endorse'
            }
            if(hotoIdList) {
                if(hotoIdList.length > 0) {
                    if(operateType.toLowerCase() != 'endorsed') {
                        await HOTO.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: hotoIdList } })
                    }
                    for(let item of hotoIdList){
                        let hoto = await HOTO.findOne({ where: { id: item } })
                        if(hoto.driverId){
                            remarksName = 'hoto driver'
                        } else {
                            if(hoto.vehicleNo) remarksName = 'hoto vehicle'
                        }
                        dataList.push(hoto.driverId ? hoto.driverId : hoto.vehicleNo)
                    }
                }
            } else {
                if(operateType.toLowerCase() != 'endorsed') {
                    await HOTO.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { requestId: requestId } })
                }
                remarksName = `hoto request ${ statusName ?? '' }`
            }
            if(operateType.toLowerCase() == 'approved'){
                let HotoApproved = await HOTO.findAll({ where: { requestId: requestId, status: 'Approved' } })
                let hotoRequestList = await HOTORequest.findOne({ where: { id: requestId } })
                if(hotoRequestList.resourceQty == HotoApproved.length) {
                    await HOTORequest.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: requestId } })
                }
            } else {
                await HOTORequest.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: requestId } })
            }
            let newHotoRequest = await HOTORequest.findOne({ where: { id: requestId } })
            let newHotoList = await HOTO.findAll({ where: { requestId: requestId } })
            await OperationRecord.create({
                id: null,
                operatorId: userId,
                businessType: 'hoto',
                businessId: requestId,
                optType: `${ statusName }`,
                beforeData: `[{hoto_request:${ JSON.stringify(oldHotoRequest) }},{hoto:${ JSON.stringify(oldHotoList) }}]`,
                afterData: `[{hoto_request:${ JSON.stringify(newHotoRequest) }},{hoto:${ JSON.stringify(newHotoList) }}]`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: `${ remarksName }`
            })
        }).catch(error => {
           throw error
        })
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const operateRequestById2 = async function (operateType, requestId, hotoIdList, userId) {
    try {
        // 2023-09-18 If hoto has returned data, it cannot cancel.
        if(operateType.toLowerCase() == 'cancelled'){
            let hotoRecordList = await HOTORecord.findAll({ where: { requestId: requestId } })
            if(hotoRecordList.length > 0) throw new RangeError([` Data has been returned and cannot be canceled.`])
        }
        await sequelizeObj.transaction(async transaction => {
            let hotoDriverOrVehicleListSql = `
            SELECT driverId, vehicleNo, startDateTime, endDateTime FROM hoto WHERE requestId = ?
            `
            let hotoDriverOrVehicleListByReplacements = [requestId]
            if(hotoIdList?.length > 0){
                hotoDriverOrVehicleListSql += ` and id in(?)`
                hotoDriverOrVehicleListByReplacements.push(hotoIdList.join(','))
            }
            if(operateType.toLowerCase() == 'cancelled' || operateType.toLowerCase() == 'rejected'){
                hotoDriverOrVehicleListSql += ` and status = 'Approved'`
            }
            hotoDriverOrVehicleListSql += `  group by id`
            let hotoDriverOrVehicleList = await sequelizeObj.query(hotoDriverOrVehicleListSql, { type: QueryTypes.SELECT, replacements: hotoDriverOrVehicleListByReplacements })
            let errorList = []
            for(let item of hotoDriverOrVehicleList){
                let driver = null;
                if(item.driverId) driver = await Driver.findOne({ where: { driverId: item.driverId } })
                if(operateType.toLowerCase() != 'rejected' && operateType.toLowerCase() != 'cancelled'){
                    let hotoByDriverOrVehicle = await TaskUtils.getHotoDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                    if(hotoByDriverOrVehicle) {
                        errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } In this time frame has been hoto.`)
                    }
                }
                let state = await TaskUtils.verifyDriverOrVehicleByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(state.length > 0){
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } The operation cannot be performed because some tasks are not completed.`)
                }
                let state2 = await TaskUtils.getUrgentStatusByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(state2.length > 0){
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } Some Urgent tasks were not completed, so the operation could not be completed.`)
                }
                let loanOutByDriverOrVehicle = await TaskUtils.getLoanOutDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(loanOutByDriverOrVehicle) {
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } It's been loaned out.`);
                }
            }
            if(errorList.length > 0) {
                throw errorList
            }
            
            let dataList = [];
            let remarksName = null;
            let statusName = null;
            if(operateType.toLowerCase() == 'rejected'){
                statusName = 'reject'
            } else if(operateType.toLowerCase() == 'cancelled'){
                statusName = 'cancel'
            } else if(operateType.toLowerCase() == 'approved'){
                statusName = 'approve'
            } else if(operateType.toLowerCase() == 'endorsed'){
                statusName = 'endorse'
            }
            if(hotoIdList) {
                if(hotoIdList.length > 0) {
                    if(operateType.toLowerCase() != 'endorsed') {
                        await HOTO.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: hotoIdList } })
                    }
                    for(let item of hotoIdList){
                        let hoto = await HOTO.findOne({ where: { id: item } })
                        if(hoto.driverId){
                            remarksName = 'hoto driver'
                        } else if(hoto.vehicleNo) {
                            remarksName = 'hoto vehicle'
                        }
                        dataList.push(hoto.driverId ? hoto.driverId : hoto.vehicleNo)
                    }
                }
            } else {
                if(operateType.toLowerCase() != 'endorsed') {
                    await HOTO.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { requestId: requestId } })
                }
                remarksName = `hoto request ${ statusName ?? '' }`
                dataList = [requestId]
            }
            if(operateType.toLowerCase() == 'approved'){
                let HotoApproved = await HOTO.findAll({ where: { requestId: requestId, status: 'Approved' } })
                let hotoRequestList = await HOTORequest.findOne({ where: { id: requestId } })
                if(hotoRequestList.resourceQty == HotoApproved.length) {
                    await HOTORequest.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: requestId } })
                }
            } else {
                await HOTORequest.update({ status: operateType, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: requestId } })
            }
            await TaskUtils.initOperationRecord(userId, requestId, statusName, remarksName, dataList)
        }).catch(error => {
           throw error
        })
    } catch (error) {
        log.error(error)
    }
}

module.exports.createHoto = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let hotoList = req.body.hotoList;
        let newHotoList = hotoList;
        let hotoDriverOrVehicle = null;
        let ultimatelyHotoList = [];
        let ultimatelyHotoIdList = [];
        let errorName = [];
        let errorMessageList = [];
        if(hotoList[0].requestId) {
            let hotoRequest = await HOTORequest.findOne({ where: { id: hotoList[0].requestId } })
            let hotoTotal = await HOTO.count({ where: { requestId: hotoList[0].requestId } })
            let hotoStillNeedNo = Number(hotoRequest.resourceQty) - Number(hotoTotal);
            if(hotoStillNeedNo > 0){
                newHotoList = hotoList.slice(0, hotoStillNeedNo)
                hotoDriverOrVehicle = hotoList.slice(hotoStillNeedNo, hotoList.length)
                if(hotoDriverOrVehicle) {
                    if(hotoDriverOrVehicle.length > 0) {
                        if(hotoDriverOrVehicle[0].driverId) {
                            errorName.push(hotoDriverOrVehicle.map(item => item.driverName))
                        } else {
                            errorName.push(hotoDriverOrVehicle.map(item => item.vehicleNo))
                        }
                    }
                }
            } 
            if(hotoStillNeedNo <= 0){
                newHotoList = []
                hotoDriverOrVehicle = hotoList.slice(hotoStillNeedNo, hotoList.length)
                if(hotoDriverOrVehicle) {
                    if(hotoDriverOrVehicle.length > 0) {
                        if(hotoDriverOrVehicle[0].driverId) {
                            errorName.push(hotoDriverOrVehicle.map(item => item.driverName))
                        } else {
                            errorName.push(hotoDriverOrVehicle.map(item => item.vehicleNo))
                        }
                    }
                }
            }
            if(errorName.length > 0) {
                errorMessageList.push(`${ errorName.join(", ") } Assigned Qty cannot be greater than resource Qty.`)
            } 
        }
        if(newHotoList.length <= 0) return res.json(utils.response(0, errorMessageList));
        for(let item of newHotoList){
            let unitId = null
            if(item.toNode) {
                let unit = await Unit.findOne({ where: { unit: item.toHub, subUnit: item.toNode }})
                unitId = unit.id
            } else {
                let unit = await Unit.findOne({ where: { unit: item.toHub }})
                unitId = unit.id
            }
            item.startDateTime = moment(item.startDateTime).format("YYYY-MM-DD HH:mm")
            item.endDateTime = moment(item.endDateTime).format("YYYY-MM-DD HH:mm")
            item.unitId = unitId
            item.creator = userId;
            let driver
            if(item.driverId) driver = await Driver.findOne({where: { driverId: item.driverId }})
            //2023-07-14 loan out driver/vehicle not hoto
            let loanOutByDriverOrVehicle = await TaskUtils.getLoanOutDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            let hotoByDriverOrVehicle = await TaskUtils.getHotoDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            let statusDriverOrVehicle = await TaskUtils.getStatusDriverOrVehicle(item.vehicleNo, item.driverId, item.endDateTime)
            if(statusDriverOrVehicle) {
                errorMessageList.push(`Operation failed. The ${ item.vehicleNo ? `vehicle(vehicleNo: ${ item.vehicleNo })` : `driver(driverName: ${ driver.driverName })` }'s status has changed.`);
            }
            if(loanOutByDriverOrVehicle) {
                errorMessageList.push(`${ item.vehicleNo ? item.vehicleNo : driver.driverName } It's been loaned out.`);
            }
            if(hotoByDriverOrVehicle) {
                errorMessageList.push(`${ item.vehicleNo ? item.vehicleNo : driver.driverName } In this time frame has been hoto.`);
            }
            let state = await TaskUtils.verifyDriverOrVehicleByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            if(state.length > 0){
                errorMessageList.push(`${ item.vehicleNo ? item.vehicleNo : driver.driverName } The operation cannot be performed because some tasks are not completed.`);
            } 
            let state2 = await TaskUtils.getUrgentStatusByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            if(state2.length > 0){
                errorMessageList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } Some Urgent tasks were not completed, so the operation could not be completed.`)
            }
            if(!statusDriverOrVehicle && !loanOutByDriverOrVehicle && !hotoByDriverOrVehicle && state.length <= 0 && state2.length <= 0){
                ultimatelyHotoList.push(item)
            }
        }    
        if(ultimatelyHotoList.length <= 0) return res.json(utils.response(0, errorMessageList));
        await sequelizeObj.transaction(async transaction => {
            let dataList = [];
            let remarksName = null
            let requestId = null;
            for(let item of ultimatelyHotoList){
                const newHoto = await HOTO.create(item)
                ultimatelyHotoIdList.push(newHoto.id)
                dataList.push(item.driverId ? item.driverId : item.vehicleNo)
                requestId = item.requestId;
                if(item.driverId){
                    remarksName = 'hoto driver'
                } else if(item.vehicleNo) {
                    remarksName = 'hoto vehicle'
                }
            } 
            let oldHOTORequestObj = await HOTORequest.findOne({ where: { id: ultimatelyHotoList[0].requestId } });
            let newHotoObj = await HOTO.findAll({ where: { requestId: ultimatelyHotoList[0].requestId } });
            if(oldHOTORequestObj.resourceQty == newHotoObj.length) {
                await HOTORequest.update({ status: ultimatelyHotoList[0].status, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: ultimatelyHotoList[0].requestId } })
            }
            await TaskUtils.initOperationRecord(userId, requestId, 'assign', remarksName, dataList)
        }).catch(error => {
           throw error
        })
        let pageList = await userService.getUserPageList(req.cookies.userId, 'HOTO', 'HOTO')
        let operationList = pageList.map(item => `${ item.action }`).join(',')
        operationList = operationList.split(',')
        if(operationList.includes('Assign') && operationList.includes("Approve")) {
            if(ultimatelyHotoIdList.length > 0) {
                await operateRequestById2('Approved', hotoList[0].requestId, ultimatelyHotoIdList, userId)
            }
        }
        if(errorMessageList.length > 0) {
            return res.json(utils.response(0, errorMessageList));
        } 
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.createHotoRecord = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let hotoIdList = req.body.hotoIdList;
        let newHotoIdList = [];
        let errorMessageList = [];
        for(let item of hotoIdList){
            let hotoItem = await HOTO.findOne({ where: { id: item } });
            if (!hotoItem) {
                continue;
            }
            log.warn(`hoto ${ JSON.stringify(hotoItem) }`)
            let driver
            if(hotoItem.driverId) driver = await Driver.findOne({where: { driverId: hotoItem.driverId }})
            let state = await TaskUtils.verifyDriverOrVehicleByDate(hotoItem.vehicleNo, hotoItem.driverId, hotoItem.startDateTime, hotoItem.endDateTime)
            if(state.length > 0){
                errorMessageList.push(`${ hotoItem.vehicleNo ? hotoItem.vehicleNo : driver.driverName } The operation cannot be performed because some tasks are not completed.`);
            }
            let state3 = await TaskUtils.getUrgentStatusByDate(hotoItem.vehicleNo, hotoItem.driverId, hotoItem.startDateTime, hotoItem.endDateTime)
            if(state3.length > 0){
                errorMessageList.push( `${ hotoItem.vehicleNo ? hotoItem.vehicleNo : driver.driverName } Some Urgent tasks were not completed, so the operation could not be completed.`)
            }
            let state2 = await TaskUtils.getLoanOutDriverOrVehicle(hotoItem.vehicleNo, hotoItem.driverId, hotoItem.startDateTime, hotoItem.endDateTime)
            if(state2){
                errorMessageList.push(`${ hotoItem.vehicleNo ? hotoItem.vehicleNo : driver.driverName } It's been loaned out.`);
            } 
            if(state.length <= 0 && !state2 && state3.length <= 0){
                newHotoIdList.push(item)
            }
        }
        if(newHotoIdList.length <= 0) return res.json(utils.response(0, errorMessageList));
        await sequelizeObj.transaction(async transaction => {
            let requestId = null;
            let dataList = [];
            let remarksName = null;
            for(let item of newHotoIdList){
                let hotoItem = await HOTO.findOne({ where: { id: item } });
                await HOTO.destroy({ where: { id: item } });
                let obj = {
                    vehicleNo: hotoItem.vehicleNo, 
                    driverId: hotoItem.driverId,
                    fromHub: hotoItem.fromHub,
                    fromNode: hotoItem.fromNode,
                    toHub: hotoItem.toHub,
                    toNode: hotoItem.toNode,
                    returnDateTime: moment().format('YYYY-MM-DD HH:mm'),
                    startDateTime:  moment(hotoItem.startDateTime).format("YYYY-MM-DD HH:mm"),
                    endDateTime: moment(hotoItem.endDateTime).format("YYYY-MM-DD HH:mm"),
                    creator: hotoItem.creator,
                    returnBy: userId,
                    status: hotoItem.status,
                    requestId: hotoItem.requestId,
                    createdAt: hotoItem.createdAt
                }
                await HOTORecord.create(obj)
                dataList.push(hotoItem.driverId ? hotoItem.driverId : hotoItem.vehicleNo)
                requestId = hotoItem.requestId;
                if(hotoItem.driverId){
                    remarksName = 'return driver'
                } else if(hotoItem.vehicleNo) {
                    remarksName = 'return vehicle'
                }
            }
            await TaskUtils.initOperationRecord(userId, requestId, 'return', remarksName, dataList)
        }).catch(error => {
           throw error
       })
       if(errorMessageList.length > 0) {
        return res.json(utils.response(0, errorMessageList));
       }
       return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getHubNodeByUser = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let hubNodeList = await sequelizeObj.query(`
        SELECT u.unit, u.subUnit FROM unit u
        LEFT JOIN USER us ON us.unitId = u.id
        WHERE us.userId = ? and us.userType != 'CUSTOMER'
        GROUP BY u.id
        `,
        {
            type: QueryTypes.SELECT
            , replacements: [userId]
        })
        return res.json(utils.response(1, hubNodeList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.createHotoRequest = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let hotoRequest = req.body.hotoRequest;
        hotoRequest.status = 'Submitted'
        hotoRequest.creator = userId
        hotoRequest.createdAt = moment().format('YYYY-MM-DD HH:mm:ss')
        hotoRequest.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss')
        await sequelizeObj.transaction(async transaction => {
            const request = await HOTORequest.create(hotoRequest)
            log.warn(`HOTO request ${ JSON.stringify(request) }`)
            await TaskUtils.initOperationRecord(userId, request.id, 'submitt', `hoto request submitt`, [JSON.stringify(request)])
        }).catch(error => {
           throw error
        })
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.editHotoRequest = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let hotoRequest = req.body.hotoRequest;
        let requestId = req.body.requestId;
        await sequelizeObj.transaction(async transaction => {    
            hotoRequest.status = 'Submitted'
            hotoRequest.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss')
            let oldHotoRequest = await HOTORequest.findOne({ where: { id: requestId } })
            await HOTORequest.update(hotoRequest, { where: { id: requestId } })
            let newHotoRequest = await HOTORequest.findOne({ where: { id: requestId } })
            await OperationRecord.create({
                id: null,
                operatorId: userId,
                businessType: 'hoto',
                businessId: requestId,
                optType: 'edit',
                beforeData: `${ [JSON.stringify(oldHotoRequest)] }`,
                afterData: `${ [JSON.stringify(newHotoRequest)] }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'edit request'
            })
        }).catch(error => {
           throw error
        })
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getHotoRequestById = async function (req, res) {
    try {
        let requestId = req.body.requestId;
        let request = await HOTORequest.findOne({ where: { id: requestId } })
        let hotoRecordList = await HOTORecord.findAll({ where: { requestId: request.id } })
        if(request.status.toLowerCase() == 'approved') {
            if(request.resourceQty == hotoRecordList.length) {
                request.status = 'Completed'
            }
        }
        return res.json(utils.response(1, request));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getHotoRequest = async function (req, res) {
    try {
        let { idOrder, purpose, type, resource, execution_date, createDate, selectHub, selectNode, status, tableType, pageNum, pageLength } = req.body;
        let userId = req.cookies.userId;
        let user = await User.findOne({ where: { userId: userId } })
        if(user.userType == CONTENT.USER_TYPE.CUSTOMER) return res.json({ data: [], recordsFiltered: 0, recordsTotal: 0 });
        if(!user)  return res.json(utils.response(0, `The user does not exist.`));
        let unitIdList = []
        let dataList = await UnitUtils.getPermitUnitList(req.cookies.userId);
        unitIdList = dataList.unitIdList;
        if(selectHub) {
            if(selectNode){
                let unit2 = await Unit.findOne({ where: { unit: selectHub, subUnit: selectNode } })
                unitIdList = [unit2.id]
            } else {
                let unit = await Unit.findAll({ where: { unit: selectHub } })
                let hubUnitIdList = unit.map(item => item.id)
                unitIdList = unitIdList.filter(item => hubUnitIdList.includes(item))
            }
        } 
        let pageList = await userService.getUserPageList(userId, 'HOTO')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let sql = `
        SELECT hr.*, us.fullName FROM hoto_request hr
        LEFT JOIN unit un on un.unit = hr.hub and (if(hr.node = '', un.subUnit is null, un.subUnit <=> hr.node))
        LEFT JOIN hoto h ON h.requestId = hr.id
        LEFT JOIN hoto_record he ON he.requestId = hr.id and hr.status = 'Approved'
        LEFT JOIN USER us ON us.userId = hr.creator
        where 1=1
        `

         let sql2 = `
        SELECT COUNT(DISTINCT hr.id) as total FROM hoto_request hr
        LEFT JOIN unit un on un.unit = hr.hub and (if(hr.node = '', un.subUnit is null, un.subUnit <=> hr.node))
        LEFT JOIN hoto h ON h.requestId = hr.id
        LEFT JOIN hoto_record he ON he.requestId = hr.id and hr.status = 'Approved'
        LEFT JOIN USER us ON us.userId = hr.creator
        where 1=1
        `
        let replacements = [];
        let replacements2 = [];
        if(unitIdList.length > 0){
            sql += ` and un.id in(?)`
            sql2 += ` and un.id in(?)`
            replacements.push(unitIdList.join(","))
            replacements2.push(unitIdList.join(","))
        }
        if(purpose) {
            sql += ` and hr.purpose = ?`
            sql2 += ` and hr.purpose = ?`
            replacements.push(purpose)
            replacements2.push(purpose)
        }
        if(type){
            sql += ` and hr.vehicleType = ?`
            sql2 += ` and hr.vehicleType = ?`
            replacements.push(type)
            replacements2.push(type)
        }
        if(resource){
            sql += ` and hr.resource = ?`
            sql2 += ` and hr.resource = ?`
            replacements.push(resource)
            replacements2.push(resource)
        }
        if(createDate){
            sql += ` and hr.createdAt like ?`
            sql2 += ` and hr.createdAt like ?`
            replacements.push(`'${ createDate }%'`)
            replacements2.push(`'${ createDate }%'`)
        }
        if (execution_date) {
            if (execution_date.indexOf('~') != -1) {
                const dates = execution_date.split(' ~ ')
                sql += ` and (hr.startTime >= ? and hr.startTime <= ?)`
                sql2 += ` and (hr.startTime >= ? and hr.startTime <= ?)`
                replacements.push(moment(`${ dates[0] } 00:00:00`).format('YYYY-MM-DD HH:mm:ss'))
                replacements.push(moment(`${ dates[1] } 23:59:59`).format('YYYY-MM-DD HH:mm:ss'))
                replacements2.push(moment(`${ dates[0] } 00:00:00`).format('YYYY-MM-DD HH:mm:ss'))
                replacements2.push(moment(`${ dates[1] } 23:59:59`).format('YYYY-MM-DD HH:mm:ss'))
            } else {
                sql += ` and hr.startTime = ?`
                sql2 += ` and hr.startTime = ?`
                replacements.push(moment(execution_date).format('YYYY-MM-DD HH:mm:ss'))
                replacements2.push(moment(execution_date).format('YYYY-MM-DD HH:mm:ss'))
            }
        }

        if(!status){
            if(tableType) {
                if(tableType == 'current'){
                    sql += ` and (hr.status not in ('Rejected', 'Cancelled') and (h.id is not null or hr.status != 'Approved'))`
                    sql2 += ` and (hr.status not in ('Rejected', 'Cancelled') and (h.id is not null or hr.status != 'Approved'))`
                }
                if(tableType == 'history'){
                    sql += ` and (hr.status in ('Rejected', 'Cancelled') or (hr.status = 'Approved' and he.id is not null and h.id is null))`
                    sql2 += ` and (hr.status in ('Rejected', 'Cancelled') or (hr.status = 'Approved' and he.id is not null and h.id is null))`
                }
            }
        }
        

        if(status) {
            if(status.toLowerCase() == 'completed') {
                sql += ` and (hr.status = 'Approved' and he.id is not null and h.id is null)`
                sql2 += ` and (hr.status = 'Approved' and he.id is not null and h.id is null)`
            } else if(status.toLowerCase() == 'approved') {
                sql += ` and (hr.status = 'Approved' and h.id is not null)`
                sql2 += ` and (hr.status = 'Approved' and h.id is not null)`
            } else {
                let newStatus = null;
                if(status.toLowerCase() == 'pending endorse') {
                    newStatus = `Submitted`
                } else if(status.toLowerCase() == 'pending assign') {
                    newStatus = `Endorsed`
                } else if(status.toLowerCase() == 'pending approval') {
                    newStatus = `Assigned`
                } else {
                    newStatus = status;
                }
                sql += ` and hr.status = ?`
                sql2 += ` and hr.status = ?`
                replacements.push(newStatus)
                replacements2.push(newStatus)
            }
        }
        if(idOrder) {
            if(idOrder.toLowerCase() == 'desc'){
                sql += '  GROUP BY hr.id order by hr.id desc ' 
            }
            if(idOrder.toLowerCase() == 'asc'){
                sql += '  GROUP BY hr.id order by hr.id asc ' 
            }
        } else {
            sql += ` GROUP BY hr.id order by hr.id desc`
        }
        pageNum = pageNum ?? 0
        pageLength = pageLength ?? 10
        if((pageNum || pageNum == 0) && pageLength){
            sql += ` limit ?,?`
            replacements.push(...[Number(pageNum), Number(pageLength)])
        }
       
        let hotoRecord = await sequelizeObj.query(sql,{ 
            type: QueryTypes.SELECT
            , replacements: replacements,
        })
        let hotoRecordTotal = await sequelizeObj.query(sql2, { 
            replacements: replacements2,
            type: QueryTypes.SELECT 
        })
        let data = []
        for(let item of hotoRecord){
            let hotoList = await HOTO.findAll({ where: { requestId: item.id } })
            let hotoListByReplace = await HOTO.findAll({ where: { requestId: item.id, endDateTime: { [Op.gt]: moment().format('YYYY-MM-DD HH:mm:ss') } } })
            let hotoRecordList = await HOTORecord.findAll({ where: { requestId: item.id } })
            if(item.status.toLowerCase() == 'approved') {
                if(item.resourceQty == hotoRecordList.length || hotoList.length <= 0) {
                    item.newStatus = 'Completed'
                }
            }
            let hotoAndRecordTotal = 0;
            let hotoRecordTotal = await HOTORecord.count({ where: { requestId: item.id } })
            hotoAndRecordTotal = hotoList.length + hotoRecordTotal;
            item.taskStatus = false
            item.hotoExist = 0
            item.approverExist = false
            item.assignQtyStatus = false
            item.replaceStatus = false
            // 2023-09-18 If hoto has returned data, it cannot cancel.
            item.cancelBanStatus = true
            if(hotoListByReplace.length > 0) item.replaceStatus = true
            if(hotoRecordList.length > 0)  item.cancelBanStatus = false
            if(item.resourceQty != hotoAndRecordTotal) item.assignQtyStatus = true
            for(let hoto of hotoList){
                if(hoto) {
                    item.hotoExist = 1
                    if((hoto.status).toLowerCase() == 'assigned') item.approverExist = true
                    let state = await TaskUtils.verifyDriverOrVehicleByDate(hoto.vehicleNo, hoto.driverId, hoto.startDateTime, hoto.endDateTime)
                    let state3 = await TaskUtils.getUrgentStatusByDate(hoto.vehicleNo, hoto.driverId, hoto.startDateTime, hoto.endDateTime)
                    let state2 = await TaskUtils.getLoanOutDriverOrVehicle(hoto.vehicleNo, hoto.driverId, hoto.startDateTime, hoto.endDateTime)
                    if(state.length > 0 || state2 || state3.length > 0) item.taskStatus = true
                }
            }
            let hotoIdList = hotoList.map(option => option.id);
            hotoIdList = Array.from(new Set(hotoIdList))
            item.assignStatus = false
            if(item.resourceQty == hotoIdList.length) item.assignStatus = true

            item.operation = operationList

            data.push(item)
        }
        return res.json({ data: data, recordsFiltered: hotoRecordTotal[0].total, recordsTotal: hotoRecordTotal[0].total })
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.cancelAssignHotoById = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let { hotoIdList, requestId } = req.body;
        let newHotoIdList = [];
        let errorList = [];
        let vehicleOrDriver = null;
        let dataList = []
        await sequelizeObj.transaction(async transaction => {
            let hotoDriverOrVehicleList = await sequelizeObj.query(`
            SELECT id, driverId, vehicleNo, startDateTime, endDateTime, status FROM hoto WHERE id in(?) group by id
            `, { type: QueryTypes.SELECT, replacements: [hotoIdList.join(',')] })

            for(let item of hotoDriverOrVehicleList){
                let driver = null;
                if(item.driverId) driver = await Driver.findOne({ where: { driverId: item.driverId } })
                let state = await TaskUtils.verifyDriverOrVehicleByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(state.length > 0){
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } The operation cannot be performed because some tasks are not completed.`)
                }
                let state3 = await TaskUtils.getUrgentStatusByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(state3.length > 0){
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } Some Urgent tasks were not completed, so the operation could not be completed.`)
                }
                let loanOutByDriverOrVehicle = await TaskUtils.getLoanOutDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
                if(loanOutByDriverOrVehicle) {
                    errorList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } It's been loaned out.`);
                }
                if(item.status != 'Assigned') {
                    errorList.push( `The ${ item.vehicleNo ? 'vehicle: '+item.vehicleNo : 'driver: '+driver.driverName } has been ${ item.status } and cannot be cancelled.`);
                }
                if(state.length <= 0 && !loanOutByDriverOrVehicle && item.status == 'Assigned' && state3.length <= 0) {
                    newHotoIdList.push(item.id)
                    if(item.driverId){
                        vehicleOrDriver = 'driver'
                        dataList.push(item.driverId)
                    } 
                    if(item.vehicleNo) {
                        vehicleOrDriver = 'vehicle'
                        dataList.push(item.vehicleNo)
                    }
                }
            }
            if(errorList.length > 0 && (newHotoIdList.length <= 0 || !newHotoIdList)) {
                throw errorList
            }
        
            await HOTORequest.update({ status: 'Endorsed', updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: requestId } })
            await HOTO.destroy({ where: { id: { [Op.in]: newHotoIdList } } });
            await OperationRecord.create({
                id: null,
                operatorId: userId,
                businessType: 'hoto',
                businessId: `${ newHotoIdList.join(',') }`,
                optType: 'assign cancel',
                afterData: dataList.length > 0 ? dataList.join(',') : '',
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: `assign cancel ${ vehicleOrDriver }` 
            })
        }).catch(error => {
           throw error
        })
        if(errorList.length) {
            return res.json(utils.response(0, errorList)); 
        }
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.replaceHotoByResource = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let hotoList = req.body.hotoList;
        let ultimatelyHotoList = [];
        let errorMessageList = [];
        for(let item of hotoList){
            let unitId = null
            if(item.toNode) {
                let unit = await Unit.findOne({ where: { unit: item.toHub, subUnit: item.toNode }})
                unitId = unit.id
            } else {
                let unit = await Unit.findOne({ where: { unit: item.toHub }})
                unitId = unit.id
            }
            item.startDateTime = moment(item.startDateTime).format("YYYY-MM-DD HH:mm")
            item.endDateTime = moment(item.endDateTime).format("YYYY-MM-DD HH:mm")
            item.unitId = unitId
            item.creator = userId;
            let driver
            if(item.driverId){
                driver = await Driver.findOne({ where: { driverId: item.driverId } })
                if(driver) {
                    if(driver.unitId) {
                        let unit = await Unit.findOne({ where: { id: driver.unitId } })
                        item.fromHub = unit ? unit.unit : null
                        item.fromNode = unit ? unit.subUnit ?? null : null
                    }
                }
            } 
            if(item.vehicleNo) {
                let vehicle = await Vehicle.findOne({ where: { vehicleNo: item.vehicleNo } })
                if(vehicle) {
                    if(vehicle.unitId) {
                        let unit = await Unit.findOne({ where: { id: vehicle.unitId } })
                        item.fromHub = unit ? unit.unit : null
                        item.fromNode = unit ? unit.subUnit ?? null : null
                    }
                }
            }
            //2023-07-14 loan out driver/vehicle not hoto
            let loanOutByDriverOrVehicle = await TaskUtils.getLoanOutDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            let hotoByDriverOrVehicle = await TaskUtils.getHotoDriverOrVehicle(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            let statusDriverOrVehicle = await TaskUtils.getStatusDriverOrVehicle(item.vehicleNo, item.driverId, item.endDateTime)
            if(statusDriverOrVehicle) {
                errorMessageList.push(`Operation failed. The ${ item.vehicleNo ? `vehicle(vehicleNo: ${ item.vehicleNo })` : `driver(driverName: ${ driver.driverName })` }'s status has changed.`);
            }
            if(loanOutByDriverOrVehicle) {
                errorMessageList.push(`${ item.vehicleNo ? item.vehicleNo : driver.driverName } It's been loaned out.`);
            }
            if(hotoByDriverOrVehicle) {
                errorMessageList.push(`${ item.vehicleNo ? item.vehicleNo : driver.driverName } In this time frame has been hoto.`);
            }
            let state = await TaskUtils.verifyDriverOrVehicleByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            if(state.length > 0){
                errorMessageList.push(`${ item.vehicleNo ? item.vehicleNo : driver.driverName } The operation cannot be performed because some tasks are not completed.`);
            } 
            let state3 = await TaskUtils.getUrgentStatusByDate(item.vehicleNo, item.driverId, item.startDateTime, item.endDateTime)
            if(state3.length > 0){
                errorMessageList.push( `${ item.vehicleNo ? item.vehicleNo : driver.driverName } Some Urgent tasks were not completed, so the operation could not be completed.`)
            }
            if(!statusDriverOrVehicle && !loanOutByDriverOrVehicle && !hotoByDriverOrVehicle && state.length <= 0 && state3.length <= 0){
                ultimatelyHotoList.push(item)
            }
        }    
        if(ultimatelyHotoList.length <= 0) return res.json(utils.response(0, errorMessageList));
        for(let item of ultimatelyHotoList){
            let remarksName = null
            let oldHotoByReplaceList = null
            let newHotoByReplaceList = null
            let driver = null;
            let hotoObj = [];
            if(item.driverId){
                driver = await Driver.findOne({where: { driverId: item.driverId }})
                if(driver.nric) {
                    if(driver.nric.length > 9) driver.nric = utils.decodeAESCode(driver.nric);
                } 
                hotoObj = await HOTO.findAll({ where: { requestId: item.requestId, driverId: item.driverId  } })
            } else {
                hotoObj = await HOTO.findAll({ where: { requestId: item.requestId, vehicleNo: item.vehicleNo } })
            }
            let oldHoto = await HOTO.findOne({ where: { id: item.id } })
            log.warn(`old hoto ${ JSON.stringify(oldHoto) }`)
            let oldHotoByDriver = null;
            if(oldHoto.driverId) {
                oldHotoByDriver = await Driver.findOne({where: { driverId: oldHoto.driverId }})
            }
            //2023-07-14 loan out driver/vehicle not hoto
            let loanOutByDriverOrVehicle = await TaskUtils.getLoanOutDriverOrVehicle(oldHoto.vehicleNo, oldHoto.driverId, oldHoto.startDateTime, oldHoto.endDateTime)
            if(loanOutByDriverOrVehicle) {
                errorMessageList.push(`${ oldHoto.vehicleNo ? oldHoto.vehicleNo : oldHotoByDriver.driverName } It's been loaned out.`);
            }
            let state = await TaskUtils.verifyDriverOrVehicleByDate(oldHoto.vehicleNo, oldHoto.driverId, oldHoto.startDateTime, oldHoto.endDateTime, 'true')
            if(state.length > 0){
                errorMessageList.push(`${ oldHoto.vehicleNo ? oldHoto.vehicleNo : oldHotoByDriver.driverName } The task has started and cannot be replaced.`);
            } 
            let state3 = await TaskUtils.getUrgentStatusByDate(oldHoto.vehicleNo, oldHoto.driverId, oldHoto.startDateTime, oldHoto.endDateTime, 'true')
            if(state3.length > 0){
                errorMessageList.push( `${ oldHoto.vehicleNo ? oldHoto.vehicleNo : oldHotoByDriver.driverName } Some urgent tasks have been started, causing operations to fail.`)
            }
            if(loanOutByDriverOrVehicle || state.length > 0 || state3.length > 0){
                return res.json(utils.response(0, errorMessageList));
            }
            let result = await TaskUtils.getTaskByHotoVehicleOrDriver(oldHoto.vehicleNo, oldHoto.driverId, oldHoto.startDateTime, oldHoto.endDateTime);
            let urgentDutyList = await TaskUtils.getUrgentStatusByDate(oldHoto.vehicleNo, oldHoto.driverId, oldHoto.startDateTime, oldHoto.endDateTime, 'false')
            if(result.length > 0) {
                for(let resultItem of result){
                    if(item.driverId){
                        let driverTypeByVehicleType = await TaskUtils.driverTypeByVehicleType(item.driverId, resultItem.vehicleType)
                        if(driverTypeByVehicleType.length == 0) {
                            errorMessageList.push(`The driver(${ driver.driverName }) type must be included '${ resultItem.vehicleType }'`)
                            return res.json(utils.response(0, errorMessageList));
                        }
                    }
                    
                    if(!(moment(item.startDateTime).format('YYYY-MM-DD HH:mm:ss') <= moment(resultItem.startDateTime).format('YYYY-MM-DD HH:mm:ss') 
                    && moment(item.endDateTime).format('YYYY-MM-DD HH:mm:ss') >= moment(resultItem.endDateTime).format('YYYY-MM-DD HH:mm:ss'))){
                        errorMessageList.push(`The ${ driver ? `driver(${ driver.driverName })` : `vehicle(${ item.vehicleNo })` } from/to time must include the period (${ moment(resultItem.startDateTime).format('YYYY-MM-DD HH:mm:ss') } ~ ${ moment(resultItem.endDateTime).format('YYYY-MM-DD HH:mm:ss') })`)
                        return res.json(utils.response(0, errorMessageList));
                    }
                }
            }
            if(urgentDutyList.length > 0) {
                for(let resultItem of urgentDutyList){
                    if(item.driverId){
                        let driverTypeByVehicleType = await TaskUtils.driverTypeByVehicleType(item.driverId, resultItem.vehicleType)
                        if(driverTypeByVehicleType.length == 0) {
                            errorMessageList.push(`The driver(${ driver.driverName }) type must be included '${ resultItem.vehicleType }'`)
                            return res.json(utils.response(0, errorMessageList));
                        }
                    }
                    
                    if(!(moment(item.startDateTime).format('YYYY-MM-DD HH:mm:ss') <= moment(resultItem.startDateTime).format('YYYY-MM-DD HH:mm:ss') 
                    && moment(item.endDateTime).format('YYYY-MM-DD HH:mm:ss') >= moment(resultItem.endDateTime).format('YYYY-MM-DD HH:mm:ss'))){
                        errorMessageList.push(`The ${ driver ? `driver(${ driver.driverName })` : `vehicle(${ item.vehicleNo })` } from/to time must include the period (${ moment(resultItem.startDateTime).format('YYYY-MM-DD HH:mm:ss') } ~ ${ moment(resultItem.endDateTime).format('YYYY-MM-DD HH:mm:ss') })`)
                        return res.json(utils.response(0, errorMessageList));
                    }
                }
            }
            if(hotoObj.length == 0 && (moment(item.startDateTime).format('YYYY-MM-DD HH:mm:ss') >= moment(oldHoto.startDateTime).format('YYYY-MM-DD HH:mm:ss') 
            && moment(item.endDateTime).format('YYYY-MM-DD HH:mm:ss') <= moment(oldHoto.endDateTime).format('YYYY-MM-DD HH:mm:ss'))) {
                if(oldHoto.vehicleNo != item.vehicleNo || oldHoto.driverId != item.driverId){
                    await sequelizeObj.transaction(async transaction => {
                        if(oldHoto.vehicleNo){
                            oldHotoByReplaceList = [{ id: oldHoto.id, vehicleNo: oldHoto.vehicleNo, startDateTime: moment(oldHoto.startDateTime).format('YYYY-MM-DD HH:mm:ss'), endDateTime: moment(oldHoto.endDateTime).format('YYYY-MM-DD HH:mm:ss') }]
                            await HOTO.update({ vehicleNo: item.vehicleNo, fromHub: item.fromHub, fromNode: item.fromNode, startDateTime: item.startDateTime, endDateTime: item.endDateTime }, { where: { id: item.id } })
                            newHotoByReplaceList = [{ id: item.id, vehicleNo: item.vehicleNo, startDateTime: moment(item.startDateTime).format('YYYY-MM-DD HH:mm:ss'), endDateTime: moment(item.endDateTime).format('YYYY-MM-DD HH:mm:ss') }]
                            remarksName = 'hoto replace vehicle'
                        }
                        if(oldHoto.driverId){
                            oldHotoByReplaceList = [{ id: oldHoto.id, driverId: oldHoto.driverId, startDateTime: moment(oldHoto.startDateTime).format('YYYY-MM-DD HH:mm:ss'), endDateTime: moment(oldHoto.endDateTime).format('YYYY-MM-DD HH:mm:ss') }]
                            await HOTO.update({ driverId: item.driverId, fromHub: item.fromHub, fromNode: item.fromNode, startDateTime: item.startDateTime, endDateTime: item.endDateTime }, { where: { id: item.id } })
                            newHotoByReplaceList = [{ id: item.id, driverId: item.driverId, startDateTime: moment(item.startDateTime).format('YYYY-MM-DD HH:mm:ss'), endDateTime: moment(item.endDateTime).format('YYYY-MM-DD HH:mm:ss') }]
                            remarksName = 'hoto replace driver'
                        }
                        await OperationRecord.create({
                            id: null,
                            operatorId: userId,
                            businessType: 'hoto',
                            businessId: item.requestId,
                            optType: 'replace',
                            beforeData: `${ JSON.stringify(oldHotoByReplaceList) }`,
                            afterData: `${ JSON.stringify(newHotoByReplaceList) }`,
                            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                            remarks: remarksName 
                        })
                    }).catch(error => {
                        throw error
                    })
                   
                    if(result.length > 0) {
                        for(let resultItem of result){
                            if((moment(item.startDateTime).format('YYYY-MM-DD HH:mm:ss') <= moment(resultItem.startDateTime).format('YYYY-MM-DD HH:mm:ss') 
                            && moment(item.endDateTime).format('YYYY-MM-DD HH:mm:ss') >= moment(resultItem.endDateTime).format('YYYY-MM-DD HH:mm:ss'))){
                                let remarksName = null;
                                if(resultItem.driverId){
                                    await FirebaseService.createFirebaseNotification2([{
                                        taskId: resultItem.taskId,
                                        token: '',
                                        driverId: resultItem.driverId,
                                        vehicleNo: resultItem.vehicleNumber
                                    }], 'INFO', 'Task cancelled!')
                                }
                                if(item.vehicleNo) {
                                    let vehicle = await Vehicle.findOne({ where: { vehicleNo: item.vehicleNo } })
                                    await Task.update({ vehicleNumber: item.vehicleNo }, { where: { taskId: resultItem.taskId } })
                                    if(resultItem.dataFrom == 'MT-ADMIN') {
                                        await MtAdmin.update({ vehicleNumber: item.vehicleNo }, { where: { id: resultItem.indentId } })
                                    }
                                    if(resultItem.dataFrom == 'SYSTEM'){
                                        let systemTaskId = resultItem.taskId;
                                        if(systemTaskId.includes('AT-')) systemTaskId = resultItem.taskId.slice(3)
                                        await _SystemVehicle.Vehicle.upsert({
                                            taskId: systemTaskId,
                                            vehicleNumber: vehicle.vehicleNo,
                                            vehicleType: vehicle.vehicleType,
                                            permitType: vehicle.permitType,
                                            vehicleStatus: 'available'
                                        })
                                    }
                                    remarksName = 'hoto replaces the vehicle in the task'
                                }
                                if(item.driverId) {
                                    await Task.update({ driverId: item.driverId }, { where: { taskId: resultItem.taskId } })
                                    if(resultItem.dataFrom == 'MT-ADMIN') {
                                        await MtAdmin.update({ driverId: item.driverId, driverName: driver.driverName }, { where: { id: resultItem.indentId } })
                                    }
                                    if(resultItem.dataFrom == 'SYSTEM'){
                                        let systemTaskId = resultItem.taskId;
                                        if(systemTaskId.includes('AT-')) systemTaskId = resultItem.taskId.slice(3)
                                        await _SystemTask.Task.update({ driverId: driver.driverId }, { where: { id: systemTaskId } })
                                        await _SystemDriver.Driver.upsert({
                                            taskId: systemTaskId,
                                            driverId: driver.driverId,
                                            status: 'Assigned',
                                            name: driver.driverName,
                                            nric: driver.nric,
                                            contactNumber: driver.contactNumber,
                                            permitType: driver.permitType,
                                            driverFrom: 'transport'
                                        })
                                    }
                                    remarksName = 'hoto replaces the driver in the task'
                                }
                                let newTask = await Task.findOne({ where: { taskId: resultItem.taskId } })
                                if(newTask.vehicleNumber && newTask.driverId) await vehicleService.createVehicleRelation(newTask.driverId, newTask.vehicleNumber)
                                if(newTask.driverId){
                                    await FirebaseService.createFirebaseNotification2([{
                                        taskId: newTask.taskId,
                                        token: '',
                                        driverId: newTask.driverId,
                                        vehicleNo: newTask.vehicleNumber
                                    }], 'INFO', 'New task assigned!')
                                }
                                await OperationRecord.create({
                                    id: null,
                                    operatorId: userId,
                                    businessType: 'hoto',
                                    businessId: item.requestId,
                                    optType: 'task replace',
                                    beforeData: `${ JSON.stringify([resultItem]) }`,
                                    afterData: `${ JSON.stringify([newTask]) }`,
                                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                                    remarks: remarksName 
                                })
                            }
                        }
                    }
                    if(urgentDutyList.length > 0) {
                        for(let resultItem of urgentDutyList){
                            let oldUrgentConfig = await UrgentConfig.findOne({ where: { id: resultItem.configId } })
                            let oldUrgentDuty = await UrgentDuty.findOne({ where: { id: resultItem.dutyId } })
                            let oldUrgentIndent = await UrgentIndent.findOne({ where: { indentId: resultItem.taskId } })
                            if((moment(item.startDateTime).format('YYYY-MM-DD HH:mm:ss') <= moment(resultItem.startDateTime).format('YYYY-MM-DD HH:mm:ss') 
                            && moment(item.endDateTime).format('YYYY-MM-DD HH:mm:ss') >= moment(resultItem.endDateTime).format('YYYY-MM-DD HH:mm:ss'))){
                                let remarksName = null;
                                if(item.vehicleNo) {
                                    let vehicle = await Vehicle.findOne({ where: { vehicleNo: item.vehicleNo } })
                                    await UrgentConfig.update({ vehicleNo: item.vehicleNo }, { where: { id: resultItem.configId } });
                                    await UrgentDuty.update({ vehicleNo: item.vehicleNo }, { where: { id: resultItem.dutyId } });
                                    await UrgentIndent.update({ vehicleNo: item.vehicleNo }, { where: { indentId: resultItem.taskId } })
                                    if(resultItem.taskId && resultItem.indentStatus){
                                        if(resultItem.indentStatus.toLowerCase() != 'completed' && resultItem.indentStatus.toLowerCase() != 'cancelled'){
                                            await _SystemVehicle.Vehicle.upsert({
                                                taskId: resultItem.taskId,
                                                vehicleNumber: vehicle.vehicleNo,
                                                vehicleType: vehicle.vehicleType,
                                                permitType: vehicle.permitType,
                                                vehicleStatus: 'available'
                                            })
                                        }
                                    }
                                    remarksName = 'hoto replaces the vehicle in the urgent'
                                }
                                if(item.driverId) {
                                    await UrgentConfig.update({ driverId: item.driverId }, { where: { id: resultItem.configId } });
                                    await UrgentDuty.update({ driverId: item.driverId }, { where: { id: resultItem.dutyId } });
                                    await UrgentIndent.update({ driverId: item.driverId }, { where: { indentId: resultItem.taskId } })
                                    if(resultItem.taskId && resultItem.indentStatus){
                                        if(resultItem.indentStatus.toLowerCase() != 'completed' && resultItem.indentStatus.toLowerCase() != 'cancelled'){
                                            await _SystemTask.Task.update({ driverId: driver.driverId }, { where: { id: resultItem.taskId } })
                                            await _SystemDriver.Driver.upsert({
                                                taskId: resultItem.taskId,
                                                driverId: driver.driverId,
                                                status: 'Assigned',
                                                name: driver.driverName,
                                                nric: driver.nric,
                                                contactNumber: driver.contactNumber,
                                                permitType: driver.permitType,
                                                driverFrom: 'transport'
                                            })
                                        }
                                    }
                                    remarksName = 'hoto replaces the driver in the urgent'
                                }
                                let newUrgentConfig = await UrgentConfig.findOne({ where: { id: resultItem.configId } })
                                let newUrgentDuty = await UrgentDuty.findOne({ where: { id: resultItem.dutyId } })
                                let newUrgentIndent = await UrgentIndent.findOne({ where: { indentId: resultItem.taskId } })
                                await urgentService.UrgentUtil.updateDutyNotice(oldUrgentConfig, newUrgentConfig)
                                if(item.driverId && resultItem.taskId && resultItem.indentStatus){
                                    if(resultItem.driverId != item.driverId) {
                                        let oldIdent = { purpose: resultItem.purpose, startTime: resultItem.startTime, dutyId: 'DUTY-'+resultItem.dutyId, driverId: resultItem.driverId, vehicleNo: resultItem.vehicleNo }
                                        let newIdent = { purpose: resultItem.purpose, startTime: resultItem.startTime, dutyId: 'DUTY-'+resultItem.dutyId, driverId: item.driverId, vehicleNo: item.vehicleNo ? item.vehicleNo : resultItem.vehicleNo }
                                        await urgentService.UrgentUtil.reAssignIndentNotice(oldIdent, newIdent)
                                    }
                                }
                                await OperationRecord.create({
                                    id: null,
                                    operatorId: userId,
                                    businessType: 'hoto',
                                    businessId: item.requestId,
                                    optType: 'task replace',
                                    beforeData: `${ JSON.stringify(oldUrgentConfig) }`,
                                    afterData: `${ JSON.stringify(newUrgentConfig) }`,
                                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                                    remarks: remarksName + ' config'
                                })
                                await OperationRecord.create({
                                    id: null,
                                    operatorId: userId,
                                    businessType: 'hoto',
                                    businessId: item.requestId,
                                    optType: 'task replace',
                                    beforeData: `${ JSON.stringify(oldUrgentDuty) }`,
                                    afterData: `${ JSON.stringify(newUrgentDuty) }`,
                                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                                    remarks: remarksName + ' duty'
                                })
                                if(oldUrgentIndent){
                                    await OperationRecord.create({
                                        id: null,
                                        operatorId: userId,
                                        businessType: 'hoto',
                                        businessId: item.requestId,
                                        optType: 'task replace',
                                        beforeData: `${ JSON.stringify(oldUrgentIndent) }`,
                                        afterData: `${ JSON.stringify(newUrgentIndent) }`,
                                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                                        remarks: remarksName + ' indent'
                                    })
                                }
                            }
                        }
                    }
                }
            } else if(hotoObj.length > 0) {
                errorMessageList.push(`Hoto already has ${ driver ? 'driver('+driver.driverName+')' : 'vehicle('+item.vehicleNo+')' }.`)
            } else {
                errorMessageList.push(`The ${ driver ? 'driver('+driver.driverName+')' : 'vehicle('+item.vehicleNo+')' } was out of time.`)
            }
        } 
        if(errorMessageList.length > 0) {
            return res.json(utils.response(0, errorMessageList));
        } 
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

