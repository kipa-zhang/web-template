const log = require('../log/winston').logger('Report Creator Service');
const utils = require('../util/utils');
const userService = require('./userService')
const moment = require('moment');
const fs = require('fs');
const path = require('path');
const xlsx = require('node-xlsx');
const CONTENT = require('../util/content');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const fmt = 'YYYY-MM-DD HH:mm'
const fmt2 = 'YYYY-MM-DD HH:mm:ss'
const downloadFolder = './public/download/'

const { UnitUtils } = require('./unitService');
const { PermitType } = require('../model/permitType');

let reportUtils = {
    getDriverMileageByClass: async function (startDate, endDate) {
        let statResult = [];
        let driverTotalMileage = 0;
        // Real-time mileage statistics
        let sql = `
        SELECT m.driverId, veh.permitType, sum(m.mileageTraveled) as permitMileage
        FROM mileage m
        LEFT JOIN vehicle veh ON m.vehicleNo = veh.vehicleNo
        WHERE m.endMileage IS NOT NULL and m.mileageTraveled is not null and m.mileageTraveled > 0
        `
        let replacements = []
        if(startDate){
            sql += `
            (? >= DATE_FORMAT(t.mobileStartTime, '%Y-%m-%d') AND ? <= DATE_FORMAT(t.mobileEndTime, '%Y-%m-%d'))
            `
            replacements.push(startDate)
            replacements.push(endDate)
        }
        sql += ` GROUP BY m.driverId, veh.permitType`
        let driverPermitTaskMileageList = await sequelizeObj.query(sql, { 
            type: QueryTypes.SELECT, replacements
        });
            
        let driverMileageStatList = await sequelizeObj.query(`
            SELECT
                dm.driverId,
                dm.permitType,
                dm.baseMileage
            FROM driver_permittype_detail dm
            where dm.approveStatus='Approved' and dm.baseMileage > 0
            group by dm.driverId, dm.permitType ORDER BY dm.driverId asc
        `, { type: QueryTypes.SELECT });
        for(let itemDriver of driverMileageStatList){
            let driverPermitTypeTaskMileage = driverPermitTaskMileageList.find(item => item.permitType == itemDriver.permitType && itemDriver.driverId == item.driverId);
            let driverPermitTypeBaseMileage = driverMileageStatList.find(item => item.permitType == itemDriver.permitType && itemDriver.driverId == item.driverId);
    
            let totalMileage = 0;
            if (driverPermitTypeTaskMileage) {
                totalMileage += driverPermitTypeTaskMileage.permitMileage || 0;
            }
            if (driverPermitTypeBaseMileage) {
                totalMileage += driverPermitTypeBaseMileage.baseMileage || 0;
            }
    
            driverTotalMileage += totalMileage;
    
            let permitTypeConf = await PermitType.findOne({ where: { permitType : itemDriver.permitType } });
            if (permitTypeConf?.parent) {
                let parentPermitType = permitTypeConf.parent;
                let parentMileageObj = statResult.find(item => item.permitType == parentPermitType && item.driverId == itemDriver.driverId);
                if (parentMileageObj) {
                    parentMileageObj.totalMileage += totalMileage;
                    continue;
                } 
                statResult.push({ driverId: itemDriver.driverId, permitType: permitTypeConf.parent, totalMileage: totalMileage });
                continue;
            }
            statResult.push({ driverId: itemDriver.driverId, permitType: itemDriver.permitType, totalMileage: totalMileage });
        }
        return statResult
    },
    getTaskPurposeByVehicle: async function(startDate, endDate){
        let sql = `
        select vehicleNumber, purpose, MAX(mobileEndTime) as mobileEndTime from task 
        where purpose in ('mpt', 'avi', 'pm') and driverStatus = 'completed' and mobileEndTime is not null
        `
        let replacements = []
        if(startDate){
            sql += `
            (? >= DATE_FORMAT(t.mobileStartTime, '%Y-%m-%d') AND ? <= DATE_FORMAT(t.mobileEndTime, '%Y-%m-%d'))
            `
            replacements.push(startDate)
            replacements.push(endDate)
        }
        sql += ` group by vehicleNumber, purpose`
        let taskPurpose = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return taskPurpose
    },
    getLastDrivenDate: async function(){
        let taskList = await sequelizeObj.query(` 
            select driverId, mobileEndTime from task where driverId is not null 
            and mobileEndTime is not null group by taskId ORDER BY mobileEndTime desc;
        `, { type: QueryTypes.SELECT });
        return taskList
    },
    getVehicleList: async function(option){
        try {
            let { startDate, endDate, user, newGroup, unitIdList, vehicleCategory, permitType, vehicleStatus, WPT1CompletionDateRange, WPT2CompletionDateRange, WPT3CompletionDateRange, hub, node } = option
            let taskVehicleSql = `
                SELECT tt.vehicleNumber FROM task tt
                WHERE tt.vehicleStatus not in ('Cancelled', 'completed') 
            `
            let replacementsTaskVehicle = []

            const checkStartDate1 = function () {
                if(startDate){
                    taskVehicleSql += `
                        and ((? >= DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and ? <= DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
                    `
                    replacementsTaskVehicle.push(startDate)
                    replacementsTaskVehicle.push(endDate)
                } else {
                    taskVehicleSql +=  `
                    and ((NOW() BETWEEN tt.indentStartTime and tt.indentEndTime)
                    `
                }
            }
            checkStartDate1()

            if( user.userType.toUpperCase() == 'CUSTOMER' && user.unitId ) {
                taskVehicleSql += ` and tt.groupId = ?`
                replacementsTaskVehicle.push(user.unitId)
            }
            if(newGroup){
                taskVehicleSql += ` and tt.groupId in(?)`
                replacementsTaskVehicle.push(newGroup)
            }
            taskVehicleSql += ` 
                OR tt.vehicleStatus = 'started')
                group by tt.vehicleNumber 
            `
            let taskVehicle = await sequelizeObj.query(taskVehicleSql, { type: QueryTypes.SELECT, replacements: replacementsTaskVehicle })
            taskVehicle = taskVehicle.map(item => item.vehicleNumber)

            let loanOutVehicleSql = `
            SELECT l.vehicleNo FROM loan l
            WHERE 
            `
            let replacementsLoanOutVehicle = []
            
            const checkStartDate2 = function () {
                if(startDate){
                    loanOutVehicleSql += ` (? >= DATE_FORMAT(l.startDate, '%Y-%m-%d') AND ? <= DATE_FORMAT(l.endDate, '%Y-%m-%d'))`
                    replacementsLoanOutVehicle.push(startDate)
                    replacementsLoanOutVehicle.push(endDate)
                } else {
                    loanOutVehicleSql += ` (now() BETWEEN l.startDate AND l.endDate) `
                }
            }
            checkStartDate2()

            loanOutVehicleSql += ` 
            AND l.vehicleNo IS NOT NULL
            GROUP BY l.vehicleNo
            `
            let loanOutVehicle = await sequelizeObj.query(loanOutVehicleSql, { type: QueryTypes.SELECT, replacements: replacementsLoanOutVehicle })
            loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)
            let sql = `
            select vv.* from(
                select v.vehicleNo, v.vehicleType, v.vehicleCategory, v.totalMileage as odometer, v.dimensions as description, v.limitSpeed,
                GROUP_CONCAT(distinct v.permitType) as permitType, v.nextAviTime, v.nextPmTime, v.nextMptTime,
                v.nextWpt1Time, v.wpt1CompleteTime, v.nextWpt2Time, v.wpt2CompleteTime, v.nextWpt3Time, v.wpt3CompleteTime,
            `
            let replacements = []

            const checkLimitSql0 = function () {
                if (user.userType.toUpperCase() == 'CUSTOMER' || newGroup) {
                    sql += ` 
                        ${
                            startDate ? `
                            IF(l.groupId IS NULL, IF(lr.groupId IS NULL, v.groupId, lr.groupId), l.groupId) AS groupId,
                            `: `
                            IF(l.groupId IS NULL, v.groupId, l.groupId) AS groupId,
                            `
                        } 
                        IF(ll.reason != '' and ll.reason is not null, ll.reason,
                            IF(FIND_IN_SET(v.vehicleNo, ?), 'Deployed',
                                IF(v.nextAviTime IS NOT NULL && v.nextAviTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending AVI',
                                    IF(v.nextMptTime IS NOT NULL && v.nextMptTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending MPT',
                                        IF((
                                            (v.nextWpt3Time IS NOT NULL && v.nextWpt3Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                            || (v.nextWpt2Time IS NOT NULL && v.nextWpt2Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                            || (v.nextWpt1Time IS NOT NULL && v.nextWpt1Time < DATE_FORMAT(NOW(),'%y-%m-%d'))
                                        ), 'Pending WPT', 'Deployable')
                                    )
                                )
                            )
                        ) as status
                    `;
                    replacements.push(taskVehicle)
                } else if(user.userType.toUpperCase() != 'CUSTOMER') {
                    sql += `
                        ${
                            startDate ? `
                            IF(hh.unitId IS NULL AND  hr.unitId IS NULL, u.id, IF(hh.unitId IS NULL, hr.unitId, hh.unitId)) AS unitIds, 
                            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.unit, IF(hh.toHub IS NULL, hr.toHub, hh.toHub)) AS unit, 
                            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.subUnit, IF(hh.toHub IS NULL, hr.toNode, hh.toNode)) AS subUnit,
                            `: `
                            IF(hh.unitId IS NULL, u.id, hh.unitId) AS unitIds, 
                            IF(hh.toHub IS NULL, u.unit, hh.toHub) AS unit, 
                            IF(hh.toHub IS NULL, u.subUnit, hh.toNode) AS subUnit,
                            `
                        } 
                        IF(FIND_IN_SET(v.vehicleNo, ?), 'LOAN OUT', 
                            IF(ll.reason != '' and ll.reason is not null, ll.reason,
                                IF(FIND_IN_SET(v.vehicleNo, ?), 'Deployed',
                                    IF(v.nextAviTime IS NOT NULL && v.nextAviTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending AVI',
                                        IF(v.nextMptTime IS NOT NULL && v.nextMptTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending MPT',
                                            IF((
                                                (v.nextWpt3Time IS NOT NULL && v.nextWpt3Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                                || (v.nextWpt2Time IS NOT NULL && v.nextWpt2Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                                || (v.nextWpt1Time IS NOT NULL && v.nextWpt1Time < DATE_FORMAT(NOW(),'%y-%m-%d'))
                                            ), 'Pending WPT', 'Deployable')
                                        )
                                    )
                                )
                            ) 
                        ) as status
                    `;
                    replacements.push(loanOutVehicle.join(","))
                    replacements.push(taskVehicle)
                }
            }
            checkLimitSql0()

            sql += `
                    from vehicle v
                `

            const checkLimitSql1 = function () {
                if(user.userType.toUpperCase() == 'CUSTOMER' || newGroup){
                    if(startDate){
                        sql += `
                        LEFT JOIN (SELECT lo.vehicleNo, lo.groupId, lo.unitId FROM loan lo WHERE 
                            ? >= DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND ? <= DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                        ) l ON l.vehicleNo = v.vehicleNo
                        LEFT JOIN (SELECT lr.vehicleNo, lr.groupId, lr.unitId FROM loan_record lr WHERE 
                            ? >=  DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND ? <= DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                        ) lr ON lr.vehicleNo = v.vehicleNo
                        `
                        replacements.push(startDate)
                        replacements.push(endDate)
                        replacements.push(startDate)
                        replacements.push(endDate)
                    } else {
                        sql += `
                        LEFT JOIN (
                            SELECT lo.vehicleNo, lo.groupId, lo.unitId FROM loan lo 
                            WHERE now() BETWEEN lo.startDate AND lo.endDate
                        ) l ON l.vehicleNo = v.vehicleNo
                        `
                    }
                } else if(user.userType.toUpperCase() != 'CUSTOMER') {
                    if(startDate){
                        sql += `
                        left join unit u on u.id = v.unitId
                        left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho 
                            where ((? >= DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') 
                            AND ? <= DATE_FORMAT(ho.endDateTime, '%Y-%m-%d'))) 
                            and ho.status = 'Approved'
                        ) hh ON hh.vehicleNo = v.vehicleNo
                        LEFT JOIN (
                            select hr.vehicleNo, hr.toHub, hr.toNode  uni.id as unitId FROM hoto_record hr 
                            left join unit uni on uni.unit = hr.toHub and hr.toNode <=> uni.subUnit
                            WHERE hr.status = 'Approved'
                            and (? >= DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') 
                            AND ? <= DATE_FORMAT(hr.returnDateTime , '%Y-%m-%d'))
                        ) hr ON hr.vehicleNo = v.vehicleNo
                        `
                        replacements.push(startDate)
                        replacements.push(endDate)
                        replacements.push(startDate)
                        replacements.push(endDate)
                    } else {
                        sql += `
                        left join unit u on u.id = v.unitId
                        left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho 
                            where (now() between ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                        ) hh ON hh.vehicleNo = v.vehicleNo
                        `
                    }
                }
            }
            checkLimitSql1()

            const checkLimitSql2 = function () {
                if(startDate){
                    sql += `
                            left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl
                                where vl.status = 1 and (? >= DATE_FORMAT(vl.startTime, '%Y-%m-%d') AND ? <= DATE_FORMAT(vl.endTime, '%Y-%m-%d'))
                            ) ll ON ll.vehicleNo = v.vehicleNo
                            group by v.vehicleNo
                        ) vv where 1=1
                    `
                    replacements.push(startDate)
                    replacements.push(endDate)
                } else {
                    sql += `
                            left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl
                                where vl.status = 1 and (now() between vl.startTime AND vl.endTime)
                            ) ll ON ll.vehicleNo = v.vehicleNo
                            group by v.vehicleNo
                        ) vv where 1=1
                    `
                }
                if(user.userType.toUpperCase() == 'CUSTOMER' && user.unitId){
                    sql += ` and vv.groupId = ?`
                    replacements.push(user.unitId) 
                } else if(user.userType.toUpperCase() != 'ADMINISTRATOR' && unitIdList.length){
                    if(newGroup){
                        sql += ` and vv.groupId in(?)`
                        replacements.push(newGroup)
                    } else {
                        sql += ` and vv.unitIds in(?)`
                        replacements.push(unitIdList.join(","))
                    }
                }
            }
            checkLimitSql2()
            
            const checkSearchSql = function () {
                if(!newGroup) {
                    if(hub) {
                        sql += ` and vv.unit = ?`
                        replacements.push(hub)
                    }
                    if(node) {
                        sql += ` and vv.subUnit = ?`
                        replacements.push(node)
                    }
                }
               
                if(vehicleCategory) {
                    sql += ` and vv.vehicleCategory = ?`
                    replacements.push(vehicleCategory)
                }
                if(permitType) {
                    sql += ` and FIND_IN_SET(?, vv.permitType)`
                    replacements.push(permitType)
                }
    
                if(vehicleStatus){
                    sql += ` and vv.status = ?`
                    replacements.push(vehicleStatus)
                }
            }
            checkSearchSql()

            const checkWPT = function () {
                if (WPT1CompletionDateRange?.indexOf(' - ') != -1) {
                    let dates = WPT1CompletionDateRange.split(' - ')
                    sql += ` and vv.wpt1CompleteTime between ? and ?`
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                }
                if (WPT2CompletionDateRange?.indexOf(' - ') != -1) {
                    let dates = WPT2CompletionDateRange.split(' - ')
                    sql += ` and vv.wpt2CompleteTime between ? and ?`
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                }
                if (WPT3CompletionDateRange?.indexOf(' - ') != -1) {
                    let dates = WPT3CompletionDateRange.split(' - ')
                    sql += ` and vv.wpt3CompleteTime between ? and ?`
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                }
            }
            checkWPT()
            
            let vehicleList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
            return vehicleList
        } catch (error) {
            log.error(error)
            return []
        }
    },
    getDriverList: async function(option){
        try {
            let { startDate, endDate, user, newGroup, unitIdList, driverStatus, driverCategory, driverClass, driverType, enDateRange,ordRange, role, vocation, ordStart, hub, node } = option
            let taskDriversql = `
            SELECT tt.driverId FROM task tt
            WHERE tt.driverStatus not in ('Cancelled', 'completed') 
            `
            let replacementsTaskDriver = []

            const checkLimitSql1 = function () {
                if(startDate){
                    taskDriversql += `
                    and ((? >= DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and ? <= DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
                    `
                    replacementsTaskDriver.push(startDate)
                    replacementsTaskDriver.push(endDate)
                } else {
                    taskDriversql += `
                        and ((NOW() BETWEEN tt.indentStartTime and tt.indentEndTime)
                    `
                }
                if(user.userType.toUpperCase() == 'CUSTOMER' && user.unitId){
                    taskDriversql += ` and tt.groupId = ?`
                    replacementsTaskDriver.push(user.unitId)
                }
                if(newGroup){
                    taskDriversql += ` and tt.groupId in(?)`
                    replacementsTaskDriver.push(newGroup)
                }
            }
            checkLimitSql1()

            taskDriversql += `
            OR tt.driverStatus = 'started')
            group by tt.driverId 
            `
            let taskDriver = await sequelizeObj.query(taskDriversql, { type: QueryTypes.SELECT, replacements: replacementsTaskDriver })
            taskDriver = taskDriver.map(item => item.driverId)

            let loanOutDriverSql = `
            SELECT l.driverId FROM loan l
            LEFT JOIN driver d ON d.driverId = l.driverId
            WHERE 
            `
            let replacementsLoanOutDriver = []

            if(startDate){
                loanOutDriverSql += ` (? >= DATE_FORMAT(l.startDate, '%Y-%m-%d') AND ? <= DATE_FORMAT(l.endDate, '%Y-%m-%d'))`
                replacementsLoanOutDriver.push(startDate)
                replacementsLoanOutDriver.push(endDate)
            } else {
                loanOutDriverSql += ` (now() BETWEEN l.startDate AND l.endDate) `
            }
            loanOutDriverSql += `
            and d.permitStatus != 'invalid' 
            AND l.driverId IS NOT NULL
            GROUP BY l.driverId
            `
            let loanOutDriver = await sequelizeObj.query(loanOutDriverSql, { type: QueryTypes.SELECT, replacements: replacementsLoanOutDriver })
            loanOutDriver = loanOutDriver.map(item => item.driverId)
            let sql = `
                select dd.* from(
                    select d.driverId, d.driverName, us.role, d.vocation, 
                    d.enlistmentDate, d.operationallyReadyDate, d.nric, d.birthday,
                    d.contactNumber, d.permitNo, d.permitIssueDate, ds.driverDemeritPoints,
                    GROUP_CONCAT(distinct dr.assessmentType) as assessmentType, GROUP_CONCAT(distinct dd.permitType) as permitType,
                    GROUP_CONCAT(distinct dc.vehicleType) as vehicleType,
                    ${
                        startDate ? `
                            IF(l.groupId IS NULL, if(lr.groupId IS NULL, d.groupId, lr.groupId), l.groupId) AS groupId,
                            IF(hh.unitId IS NULL AND  hr.unitId IS NULL, u.id, IF(hh.unitId IS NULL, hr.unitId, hh.unitId)) AS unitIds, 
                            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.unit, IF(hh.toHub IS NULL, hr.toHub, hh.toHub)) AS unit, 
                            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.subUnit, IF(hh.toHub IS NULL, hr.toNode, hh.toNode)) AS subUnit,
                        `: `
                            IF(l.groupId IS NULL, d.groupId, l.groupId) AS groupId,
                            IF(hh.unitId IS NULL, u.id, hh.unitId) AS unitIds, 
                            IF(hh.toHub IS NULL, u.unit, hh.toHub) AS unit, 
                            IF(hh.toHub IS NULL, u.subUnit, hh.toNode) AS subUnit,
                        `
                    }
                
            `
            let replacements = []

            
            const checkLimitSql2 = function () {
                if(user.userType.toUpperCase() == 'CUSTOMER'){
                    sql += `
                        IF(d.permitStatus = 'invalid', 'permitInvalid',
                            IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                    IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                            ) 
                        ) as status
                    `;
                    replacements.push(taskDriver.join(","))
                } else if (user.userType.toUpperCase() != 'CUSTOMER') {
                    sql += `
                        IF(d.permitStatus = 'invalid', 'permitInvalid',
                            IF(FIND_IN_SET(d.driverId, ?), 'LOAN OUT', 
                                IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                        IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                                )
                            ) 
                        ) as status
                    `;
                    replacements.push(loanOutDriver.join(","))
                    replacements.push(taskDriver.join(","))
                } 
                sql += `
                    from driver d
                    left join user us on us.driverId = d.driverId
                    left join unit u on u.id = d.unitId
                `
                if(startDate){
                    sql += `
                    LEFT JOIN (SELECT lo.driverId, lo.groupId, lo.unitId FROM loan lo WHERE 
                        ? >= DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND ? <= DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                    ) l ON l.driverId = d.driverId
                    LEFT JOIN (SELECT lr.driverId, lr.groupId, lr.unitId FROM loan_record lr WHERE 
                        ? >= DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND ? <= DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                    ) lr ON lr.driverId = d.driverId
                    left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho 
                        where ((? >= DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND ? <= DATE_FORMAT(ho.endDateTime, '%Y-%m-%d'))) and ho.status = 'Approved'
                    ) hh ON hh.driverId = d.driverId
                    LEFT JOIN (
                        select hr.driverId, hr.toHub, hr.toNode, uni.id as unitId FROM hoto_record hr 
                        left join unit uni on uni.unit = hr.toHub and hr.toNode <=> uni.subUnit
                        WHERE hr.status = 'Approved'
                        and (? >= DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND ? <= DATE_FORMAT(hr.returnDateTime , '%Y-%m-%d')) 
                    ) hr ON hr.driverId = d.driverId
                    left join (select dl.driverId, dl.reason from driver_leave_record dl 
                        where dl.status = 1 and (? >= DATE_FORMAT(dl.startTime, '%Y-%m-%d') AND ? <= DATE_FORMAT(dl.endTime, '%Y-%m-%d'))
                    ) ll ON ll.driverId = d.driverId
                    left join (SELECT sum(demeritPoint) as driverDemeritPoints, driverId
                                FROM sos
                                WHERE demeritPoint > 0 and optAt IS NOT NULL 
                                and (DATE_FORMAT(optAt, '%Y-%m-%d') between ? and ?)
                    ) ds on ds.driverId = d.driverId
                    `
                    replacements.push(startDate)
                    replacements.push(endDate)
                    replacements.push(startDate)
                    replacements.push(endDate)
                    replacements.push(startDate)
                    replacements.push(endDate)
                    replacements.push(startDate)
                    replacements.push(endDate)
                    replacements.push(startDate)
                    replacements.push(endDate)
                    replacements.push(startDate)
                    replacements.push(endDate)
                } else {
                    sql += `
                    LEFT JOIN (SELECT lo.driverId, lo.groupId, lo.unitId FROM loan lo 
                        WHERE now() BETWEEN lo.startDate AND lo.endDate) l ON l.driverId = d.driverId
                    left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho 
                        where (now() between ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                    ) hh ON hh.driverId = d.driverId
                    left join (select dl.driverId, dl.reason from driver_leave_record dl 
                        where dl.status = 1 and (now() between dl.startTime AND dl.endTime)
                    ) ll ON ll.driverId = d.driverId
                    left join (SELECT sum(demeritPoint) as driverDemeritPoints, driverId
                                FROM sos
                                WHERE demeritPoint > 0 and optAt IS NOT NULL 
                                and DATE_FORMAT(optAt, '%Y-%m-%d') >= ?
                    ) ds on ds.driverId = d.driverId
                    `
                    replacements.push(moment().subtract(1, 'year').format('YYYY-MM-DD'))
                }
            }
            checkLimitSql2()

            sql += `
            left join driver_assessment_record dr on dr.driverId = d.driverId and dr.approveStatus = 'Approved'
            left join driver_permittype_detail dd on dd.driverId = d.driverId and dd.approveStatus = 'Approved'
            left join driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus = 'Approved'
            `
            if(driverStatus){
                sql += `
                where d.permitStatus = ?
                `
                replacements.push(driverStatus)
            }
            sql += `
                    group by d.driverId
                ) dd where 1=1
            `

            const checkSearchSql = function () {
                if(!newGroup) {
                    if(unitIdList.length) {
                        sql += ` and dd.unitIds in(?)`
                        replacements.push(unitIdList.join(","))
                    }
                    if(hub) {
                        sql += ` and dd.unit = ?`
                        replacements.push(hub)
                        if(node) {
                            sql += ` and dd.subUnit = ?`
                            replacements.push(node)
                        }
                    } 
                    sql += ` and dd.groupId is null`
                } else {
                    sql += ` and dd.groupId in(?)`
                    replacements.push(newGroup)
                }
                if(driverCategory) {
                    sql += ` and FIND_IN_SET(?, dd.assessmentType)`
                    replacements.push(driverCategory)
                }
                if(driverClass) {
                    sql += ` and FIND_IN_SET(?, dd.permitType)`
                    replacements.push(driverClass)
                }
                if(driverType) {
                    sql += ` and FIND_IN_SET(?, dd.vehicleType)`
                    replacements.push(driverType)
                }
                
            }
            checkSearchSql()

            const checkSearchSq2 = function () {
                if(enDateRange?.indexOf('-') != -1) {
                    let dates = enDateRange.split(' - ')
                    sql += ` and (dd.enlistmentDate between ? and ?)`
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                }
                if (ordRange?.indexOf('-') != -1) {
                    let dates = ordRange.split(' - ')
                    sql += ` and (dd.operationallyReadyDate between ? and ?)`
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                }
                
                if(role){
                    sql += ` and dd.role = ?`
                    replacements.push(role)
                }
                if(vocation){
                    sql += ` and dd.vocation = ?`
                    replacements.push(vocation)
                }
                if (ordStart?.toLowerCase() == 'effective') {
                    sql += ` and (dd.operationallyReadyDate is null OR dd.operationallyReadyDate > DATE_FORMAT(NOW(), '%Y-%m-%d'))`;
                } else if (ordStart) {
                    sql += ` and (dd.operationallyReadyDate is not null and dd.operationallyReadyDate <= DATE_FORMAT(NOW(), '%Y-%m-%d'))`;
                }
            }
            checkSearchSq2()
            
            console.log(sql)
            let driverList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
            return driverList
        } catch (error) {
            log.error(error)
            return []
        }
    }
}

const getSystemGroup = async function () {
    let groupList = await sequelizeSystemObj.query(
        `select id, groupName from \`group\``,
        { type: QueryTypes.SELECT }
    )
    return groupList
}

const getTaskReportList = async function (req, res) {
    try {
        let reportGroupSelectionTitle = req.body.reportGroupSelectionTitle
        let filter = req.body.filter
        let { taskType, reportDateRange, group } = req.body.filter

        group = group ? Number.parseInt(group) : null

        // check hub, node
        let userId = req.cookies.userId;
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            log.error(`UserId ${userId} does not exist!`)
            return res.json(utils.response(0, `UserId ${userId} does not exist!`));
        }

        let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(userId)
        let hubNodeIdList = unitIdList
        let result = []
        if (taskType == "") {
            let taskList = await getTaskList(user, group, filter, hubNodeIdList, groupIdList)
            let urgentIndentList = await getUrgentIndentList(user, group, filter, hubNodeIdList, groupIdList)

            let loanList = await getLoanList(user, group, filter, hubNodeIdList)
            result = taskList.concat(urgentIndentList, loanList)
        } else if (taskType == "Urgent Indent") {
            result = await getUrgentIndentList(user, group, filter, hubNodeIdList, groupIdList)
        } else {
            let taskList = await getTaskList(user, group, filter, hubNodeIdList)
            let loanList = []
            if (taskType == 'Sys Task') {
                loanList = await getLoanList(user, group, filter, hubNodeIdList)
            }
            result = taskList.concat(loanList)
        }
        for(let item of result){
            if (item.driverStatus.toLowerCase() == 'waitcheck') {
                if (moment().isAfter(item.indentEndTime)) {
                    item.driverStatus = 'System Expired'
                }
            }
            if(item?.length > 9) {
                item.driverNric = utils.decodeAESCode(item.driverNric);
                item.driverNric= ((item.driverNric).toString()).substr(0, 1) + '****' + ((item.driverNric).toString()).substr(((item.driverNric).toString()).length-4, 4)
            }
        }
        let filename = await ExportDataToExcel(reportGroupSelectionTitle, result, reportDateRange)
        return res.json(utils.response(1, filename));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}
module.exports.getTaskReportList = getTaskReportList

const getTaskList = async function (user, group, filter, hubNodeIdList, groupIdList) {
    let { taskType, taskStatus, taskID, hub, node, activity, actualTime, driverName,
        mobileNumber, executionTime, purpose, remarks, reportDateRange, vehicleNumber,
        vehicleType, indentID, mileageCaptured, startOdometer, endOdometer, supportedUnit } = filter

    let taskSql = `
        SELECT t.taskId, t.groupId, t.dataFrom, d.driverName, d.nric as driverNric, t.driverStatus, d.contactNumber, t.vehicleNumber, v.vehicleType,
        t.indentStartTime, t.indentEndTime, t.purpose, t.activity, t.mobileStartTime, t.mobileEndTime, t.indentId, m.cancelledCause,
        t.hub, t.node, ml.startMileage, ml.endMileage, ml.mileageTraveled
        FROM task t 
        LEFT JOIN mt_admin m ON t.indentId = m.id AND t.dataFrom = 'MT-ADMIN'
        LEFT JOIN unit u ON u.unit = t.hub AND u.subUnit <=> t.node
        LEFT JOIN (
            SELECT * FROM (
                SELECT driverId, unitId, nric, driverName, contactNumber FROM driver  
                UNION ALL 
                SELECT driverId, unitId, nric, driverName, contactNumber FROM driver_history  
            ) dd GROUP BY dd.driverId
        ) d ON d.driverId = t.driverId
        LEFT JOIN (
            SELECT * FROM (
                SELECT vehicleNo, unitId, vehicleType FROM vehicle 
                UNION ALL 
                SELECT vehicleNo, unitId, vehicleType FROM vehicle_history
            ) vv GROUP BY vv.vehicleNo
        ) v ON v.vehicleNo = t.vehicleNumber
        left join mileage ml on t.taskId = ml.taskId
    `;

    let limitCondition = [], replacements = []

    const getLimitSql = function () {
        if (user.userType.toLowerCase() == 'customer') {
            limitCondition.push(` t.groupId = ? `)
            replacements.push(user.unitId)
        } else if (user.userType.toLowerCase() != 'administrator') {
            if(supportedUnit){
                limitCondition.push(` t.groupId = ? `)
                replacements.push(supportedUnit)
            } else if (hubNodeIdList.length) {
                if(groupIdList){
                    limitCondition.push(` (u.id IN (?) or t.groupId in(?)) `)
                    replacements.push(hubNodeIdList, groupIdList);
                } else {
                    limitCondition.push(` (u.id IN (?) and t.groupId is null) `)
                    replacements.push(hubNodeIdList);
                }
            }
        }
    }
    getLimitSql()
    
    const getSearchSql1 = function () {
        if (reportDateRange) {
            let reportDateRangeArr = reportDateRange.split(' ~ ')
            limitCondition.push(` (DATE(t.indentStartTime) >= ? AND DATE(t.indentStartTime) <= ? ) `)
            replacements.push(reportDateRangeArr[0])
            replacements.push(reportDateRangeArr[1])
        }
    
        if (taskType && taskType == 'MT-Admin') {
            limitCondition.push(` (t.dataFrom = 'MT-ADMIN' and (t.taskId like 'CU-%' or t.taskId like 'MT-%')) `)
        } else if (taskType && taskType == 'ATMS') {
            limitCondition.push(` (t.taskId like 'AT-%') `)
        } else if (taskType && taskType == 'Sys Task') {
            limitCondition.push(` (t.dataFrom = 'SYSTEM' and t.taskId not like 'AT-%') `)
        } else if (taskType && taskType == 'Mobile') {
            limitCondition.push(` t.dataFrom = 'MOBILE' `)
        }
    
        if (hub) {
            limitCondition.push(` t.hub=? `)
            replacements.push(hub)
        }
        if (node) {
            limitCondition.push(` t.node=? `)
            replacements.push(node)
        }
        if (taskID) {
            limitCondition.push(` t.taskId LIKE ? `)
            replacements.push('%'+taskID+'%')
        }
        if (taskStatus) {
            let taskStatusList = taskStatus.split(',').map(val => {
                if (val.toLowerCase() == 'waitcheck') {
                    return ` t.driverStatus LIKE '%waitcheck%' and now() < t.indentEndTime `
                } else if (val.toLowerCase() == 'system expired') {
                    return ` t.driverStatus LIKE '%waitcheck%' and now() > t.indentEndTime `
                } else {
                    replacements.push('%'+val+'%')
                    return ` t.driverStatus LIKE ? `
                }
            }).join('or')
            limitCondition.push(` (${taskStatusList}) `)
        }
    }
    getSearchSql1()

    const getSearchSql2 = function () {
        if (activity) {
            limitCondition.push(` t.activity LIKE ? `)
            replacements.push('%'+activity+'%')
        }
        if (purpose) {
            limitCondition.push(` t.purpose LIKE ? `)
            replacements.push('%'+purpose+'%')
        }
    
        if (actualTime) {
            let actualTimeArr = actualTime.split(' ~ ')
            limitCondition.push(` (DATE(t.mobileStartTime) >= ? AND DATE(t.mobileEndTime) <= ? ) `)
            replacements.push(actualTimeArr[0])
            replacements.push(actualTimeArr[1])
        }
        if (executionTime) {
            let executionTimeArr = executionTime.split(' ~ ')
            limitCondition.push(` (DATE(t.indentStartTime) >= ? AND DATE(t.indentStartTime) <= ? ) `)
            replacements.push(executionTimeArr[0])
            replacements.push(executionTimeArr[1])
        }
    
        if (driverName) {
            limitCondition.push(` d.driverName like ? `)
            replacements.push('%'+driverName+'%')
        }
        if (vehicleNumber) {
            limitCondition.push(` t.vehicleNumber like ? `)
            replacements.push('%'+vehicleNumber+'%')
        }
        if (vehicleType) {
            limitCondition.push(` v.vehicleType like ? `)
            replacements.push('%'+vehicleType+'%')
        }
        if (mobileNumber) {
            limitCondition.push(` d.contactNumber like ? `)
            replacements.push('%'+mobileNumber+'%')
        }
        if (remarks) {
            limitCondition.push(` m.cancelledCause like ? `)
            replacements.push('%'+remarks+'%')
        }
        if (indentID) {
            limitCondition.push(` t.indentId like ? `)
            replacements.push('%'+indentID+'%')
        }
        if (mileageCaptured) {
            limitCondition.push(` ml.mileageTraveled >= ? `)
            replacements.push(mileageCaptured)
        }
        if (startOdometer) {
            limitCondition.push(` ml.startMileage >= ? `)
            replacements.push(startOdometer)
        }
        if (endOdometer) {
            limitCondition.push(` ml.endOdometer <= ? `)
            replacements.push(endOdometer)
        }
    }
    getSearchSql2()    

    if (limitCondition.length) {
        taskSql += ' WHERE ' + limitCondition.join(' AND ');
    }
    console.log(taskSql)
    let taskList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements });
    return taskList
}

module.exports.getKeyPressReportList = async function (req, res) {
    try {
        let reportGroupSelectionTitle = req.body.reportGroupSelectionTitle;
        let { reportDateRange, boxName, location, transactionType } = req.body.filter
        if(req.cookies.userType == 'CUSTOMER')  return res.json(utils.response(0, 'Report data is empty!'));
        let userId = req.cookies.userId;
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            log.error(`UserId ${userId} does not exist!`)
            return res.json(utils.response(0, `UserId ${userId} does not exist!`));
        }
        let { unitIdList } = await UnitUtils.getPermitUnitList(userId)
        //get keypress transcation list
        let baseSQL = `
            SELECT
                ks.locationName,
                ks.boxName,
                ko.vehicleNo,
                v.vehicleType,
                ko.keyTagId,
                ko.slotId as keySlotLocation,
                uu.fullName as userName,
                d.driverName,
                ko.reason,
                ko.optType as transactionType,
                ko.optTime as transactionTime
            FROM key_opt_record ko
            LEFT JOIN keypress_site_info ks on ko.siteId=ks.siteId
            LEFT JOIN task t ON t.taskId = ko.taskId
            LEFT JOIN driver d ON t.driverId = d.driverId
            LEFT JOIN vehicle v on v.vehicleNo = ko.vehicleNo
            LEFT JOIN user uu on ko.optby = uu.userId
            where 1=1  
        `;
        let replacements = []
        if(unitIdList.length > 0){
            baseSQL += ` and ks.unitId in(?)`
            replacements.push(unitIdList.join(","))
        }
        if (reportDateRange) {
            let reportDateRangeArr = reportDateRange.split(' ~ ')
            baseSQL += ` and (DATE(ko.optTime) >= ? AND DATE(ko.optTime) <= ? ) `;
            replacements.push(reportDateRangeArr[0])
            replacements.push(reportDateRangeArr[1])
        }
        if (boxName) {
            baseSQL += ` and ks.boxName like ? `;
            replacements.push('%'+boxName+'%')
        }
        if (location) {
            baseSQL += ` and ks.locationName like ? `;
            replacements.push('%'+location+'%')
        }
        if (transactionType) {
            switch (transactionType) {
                case 'mobileReturn':
                    baseSQL += ` and ko.dataFrom = 'mobile' and ko.optType = 'returnConfirm' `;
                    break;
                case 'mobileWithdraw':
                    baseSQL += ` and ko.dataFrom = 'mobile' and ko.optType = 'withdrawConfirm' `;
                    break;
                case 'manualReturn':
                    baseSQL += ` and ko.dataFrom = 'server' and ko.optType = 'returnConfirm' `;
                    break;
                case 'manualWithdraw':
                    baseSQL += ` and ko.dataFrom = 'server' and ko.optType = 'withdrawConfirm' `;
                    break;
                default:
                    baseSQL += ` and ko.optType = ? `;
                    replacements.push(transactionType)
            }
        } else {
            baseSQL += ` and ko.optType in('withdrawConfirm', 'returnConfirm', 'withdrawConfirmUpload', 'returnConfirmUpload', 'withdrawConfirmQrcode', 'returnConfirmQrcode', 'Mustering') `;
        }

        baseSQL += ` ORDER BY ks.boxName, ko.slotId `;
        let keyTransactionsList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: replacements })
        if (!keyTransactionsList || keyTransactionsList.length == 0) {
            return res.json(utils.response(0, 'Report data is empty!'));
        }

        let filename = await ExportKeypressDataToExcel(reportGroupSelectionTitle, keyTransactionsList, reportDateRange)
        return res.json(utils.response(1, filename));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const getUrgentIndentList = async function (user, group, filter, hubNodeIdList, groupIdList) {
    let { taskStatus, taskID, hub, node, actualTime, driverName, mobileNumber, executionTime,
        purpose, remarks, reportDateRange, vehicleNumber, vehicleType, activity,
        indentID, mileageCaptured, startOdometer, endOdometer, supportedUnit } = filter
    if (purpose && 'urgent duty'.indexOf(purpose.toLowerCase()) == -1) {
        return []
    }

    let taskSql = `
            SELECT t.indentId as taskId, t.groupId, 'Urgent Indent' as dataFrom, d.driverName, d.nric as driverNric, t.status as driverStatus, d.contactNumber, t.vehicleNo as vehicleNumber, v.vehicleType,
            t.startTime as indentStartTime, t.endTime as indentEndTime, 'Urgent' as purpose, '' as activity, t.mobileStartTime, t.mobileEndTime, t.requestId as indentId, t.cancelledCause,
            t.hub, t.node, ml.startMileage, ml.endMileage, ml.mileageTraveled
            FROM urgent_indent t 
            LEFT JOIN unit u ON u.unit = t.hub AND u.subUnit <=> t.node
            LEFT JOIN (
                SELECT * FROM (
                    SELECT driverId, unitId, nric, driverName, contactNumber FROM driver  
                    UNION ALL 
                    SELECT driverId, unitId, nric, driverName, contactNumber FROM driver_history  
                ) dd GROUP BY dd.driverId
            ) d ON d.driverId = t.driverId
            LEFT JOIN (
                SELECT * FROM (
                    SELECT vehicleNo, unitId, vehicleType FROM vehicle 
                    UNION ALL 
                    SELECT vehicleNo, unitId, vehicleType FROM vehicle_history
                ) vv GROUP BY vv.vehicleNo
            ) v ON v.vehicleNo = t.vehicleNo
            left join mileage ml on CONCAT('DUTY-', t.dutyId, '-', t.id) = ml.taskId
        `;
    let limitCondition = [], replacements = []

    const getPermitSql = function () {
        if (user.userType.toLowerCase() == 'customer') {
            limitCondition.push(` t.groupId = ? `)
            replacements.push(user.unitId)
        } else if (user.userType.toLowerCase() != 'administrator') {
            if(supportedUnit){
                limitCondition.push(` t.groupId = ? `)
                replacements.push(supportedUnit)
            } else if (hubNodeIdList.length) {
                if(groupIdList){
                    limitCondition.push(` (u.id IN (?) or t.groupId in(?)) `)
                    replacements.push(hubNodeIdList, groupIdList);
                } else {
                    limitCondition.push(` (u.id IN (?) and t.groupId is null) `)
                    replacements.push(hubNodeIdList);
                }
            }
        }
    }
    getPermitSql()
    
    const getSearchSql1 = function () {
        if (reportDateRange) {
            let reportDateRangeArr = reportDateRange.split(' ~ ')
            limitCondition.push(` (DATE(t.startTime) >= ? AND DATE(t.startTime) <= ? ) `)
            replacements.push(reportDateRangeArr[0])
            replacements.push(reportDateRangeArr[1])
        }
    
        if (hub) {
            limitCondition.push(` t.hub=? `)
            replacements.push(hub)
        }
        if (node) {
            limitCondition.push(` t.node=? `)
            replacements.push(node)
        }
    
        if (taskID) {
            limitCondition.push(` t.indentId LIKE ? `)
            replacements.push('%'+taskID+'%')
        }
        if (taskStatus) {
            let taskStatusList = taskStatus.split(',').map(val => {
                if (val.toLowerCase() == 'waitcheck') {
                    return ` t.status LIKE '%waitcheck%' and now() < t.endTime `
                } else if (val.toLowerCase() == 'system expired') {
                    return ` t.status LIKE '%waitcheck%' and now() > t.endTime `
                } else {
                    replacements.push('%'+val+'%')
                    return ` t.status LIKE ? `
                }
            }).join('or')
            limitCondition.push(` (${taskStatusList}) `)
        }
    }
    getSearchSql1()

    const getSearchSql2 = function () {
        if (actualTime) {
            let actualTimeArr = actualTime.split(' ~ ')
            limitCondition.push(` (DATE(t.mobileStartTime) >= ? AND DATE(t.mobileEndTime) <= ? ) `)
            replacements.push(actualTimeArr[0])
            replacements.push(actualTimeArr[1])
        }
        if (executionTime) {
            let executionTimeArr = executionTime.split(' ~ ')
            limitCondition.push(` (DATE(t.startTime) >= ? AND DATE(t.startTime) <= ? ) `)
            replacements.push(executionTimeArr[0])
            replacements.push(executionTimeArr[1])
        }
    
        if (activity) {
            // no activity here
            limitCondition.push(` 1 = 2 `)
        }
        if (driverName) {
            limitCondition.push(` d.driverName like ? `)
            replacements.push('%'+driverName+'%')
        }
        if (vehicleNumber) {
            limitCondition.push(` t.vehicleNo like ? `)
            replacements.push('%'+vehicleNumber+'%')
        }
        if (vehicleType) {
            limitCondition.push(` t.vehicleType like ? `)
            replacements.push('%'+vehicleType+'%')
        }
        if (mobileNumber) {
            limitCondition.push(` d.contactNumber like ? `)
            replacements.push('%'+mobileNumber+'%')
        }
        if (remarks) {
            limitCondition.push(` t.cancelledCause like ? `)
            replacements.push('%'+remarks+'%')
        }
        if (indentID) {
            limitCondition.push(` t.requestId like ? `)
            replacements.push('%'+indentID+'%')
        }
        if (mileageCaptured) {
            limitCondition.push(` ml.mileageTraveled >= ? `)
            replacements.push(mileageCaptured)
        }
        if (startOdometer) {
            limitCondition.push(` ml.startMileage >= ? `)
            replacements.push(startOdometer)
        }
        if (endOdometer) {
            limitCondition.push(` ml.endOdometer <= ? `)
            replacements.push(endOdometer)
        }
    }
    getSearchSql2()

    if (limitCondition.length) {
        taskSql += ' WHERE ' + limitCondition.join(' AND ');
    }
    console.log(taskSql)
    let taskList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements });
    return taskList
}

const getLoanList = async function (user, group, filter, hubNodeIdList) {
    let { taskStatus, taskID, hub, node, actualTime, driverName, mobileNumber, executionTime,
        purpose, reportDateRange, vehicleNumber, vehicleType, activity,
        indentID, supportedUnit } = filter

    let taskSql = `
        SELECT l.taskId, l.indentId, 'SYSTEM' AS dataFrom, l.unitId, l.groupId, u.unit AS hub, IFNULL(u.subUnit, '-') AS node, d.driverName, d.nric AS driverNric, d.contactNumber, 
        l.purpose, l.activity, NULL AS startMileage, NULL AS endMileage, NULL AS mileageTraveled, NULL AS mobileStartTime, NULL AS mobileEndTime, NULL AS cancelledCause,
        l.startDate AS indentStartTime, l.endDate AS indentEndTime, IF(l.driverId IS NULL AND l.vehicleNo IS NULL, 'Unassigned', 'Assigned') AS driverStatus, 
        v.vehicleNo, v.vehicleNo AS vehicleNumber, v.vehicleType
        FROM loan l
        LEFT JOIN unit u ON l.unitId = u.id
        LEFT JOIN (
            SELECT * FROM (
                SELECT driverId, unitId, nric, driverName, contactNumber FROM driver  
                UNION ALL 
                SELECT driverId, unitId, nric, driverName, contactNumber FROM driver_history  
            ) dd GROUP BY dd.driverId
        ) d ON d.driverId = l.driverId
        LEFT JOIN (
            SELECT * FROM (
                SELECT vehicleNo, unitId, vehicleType FROM vehicle 
                UNION ALL 
                SELECT vehicleNo, unitId, vehicleType FROM vehicle_history
            ) vv GROUP BY vv.vehicleNo
        ) v ON v.vehicleNo = l.vehicleNo
        WHERE l.taskId NOT LIKE 'AT-%'

        union

        SELECT l.taskId, l.indentId, 'SYSTEM' AS dataFrom, l.unitId, l.groupId, u.unit AS hub, IFNULL(u.subUnit, '-') AS node, d.driverName, d.nric AS driverNric, d.contactNumber, 
        l.purpose, l.activity, NULL AS startMileage, NULL AS endMileage, NULL AS mileageTraveled, NULL AS mobileStartTime, NULL AS mobileEndTime, NULL AS cancelledCause,
        l.startDate AS indentStartTime, l.endDate AS indentEndTime, IF(l.driverId IS NULL AND l.vehicleNo IS NULL, 'Unassigned', 'Assigned') AS driverStatus, 
        v.vehicleNo, v.vehicleNo AS vehicleNumber, v.vehicleType
        FROM loan_record l
        LEFT JOIN unit u ON l.unitId = u.id
        LEFT JOIN (
            SELECT * FROM (
                SELECT driverId, unitId, nric, driverName, contactNumber FROM driver  
                UNION ALL 
                SELECT driverId, unitId, nric, driverName, contactNumber FROM driver_history  
            ) dd GROUP BY dd.driverId
        ) d ON d.driverId = l.driverId
        LEFT JOIN (
            SELECT * FROM (
                SELECT vehicleNo, unitId, vehicleType FROM vehicle 
                UNION ALL 
                SELECT vehicleNo, unitId, vehicleType FROM vehicle_history
            ) vv GROUP BY vv.vehicleNo
        ) v ON v.vehicleNo = l.vehicleNo
        WHERE l.taskId NOT LIKE 'AT-%'
        `;
    taskSql = `
                select * from (${ taskSql }) t
    `
    let limitCondition = [], replacements = []

    const getPermitSql = function () {
        if (user.userType.toLowerCase() == 'customer') {
            limitCondition.push(` t.groupId = ${user.unitId} `)
        } else if (user.userType.toLowerCase() == 'hq' || user.userType.toLowerCase() == 'administrator') {
            if (group) {
                limitCondition.push(` t.groupId = ? `)
                replacements.push(group)
            }
        } else if (user.userType.toLowerCase() == 'unit') {
            if (hubNodeIdList.length) {
                // Maybe not more than 1000 node in one hub
                limitCondition.push(` ( t.unitId IN (?) AND t.groupId IS NULL ) `)
                replacements.push(hubNodeIdList);
            }
        }
    }
    getPermitSql()
    
    const getSearchSql1 = function () {
        if (reportDateRange) {
            let reportDateRangeArr = reportDateRange.split(' ~ ')
            limitCondition.push(` (DATE(t.indentStartTime) >= ? AND DATE(t.indentStartTime) <= ? ) `)
            replacements.push(reportDateRangeArr[0])
            replacements.push(reportDateRangeArr[1])
        }
    
        if (hub) {
            limitCondition.push(` t.hub=? `)
            replacements.push(hub)
        }
        if (node) {
            limitCondition.push(` t.node=? `)
            replacements.push(node)
        }
    
        if (taskID) {
            limitCondition.push(` t.indentId LIKE ? `)
            replacements.push('%'+taskID+'%')
        }
        if (taskStatus) {
            limitCondition.push(` 1 = 2 `)
        }
    
        if (actualTime) {
            limitCondition.push(` 1 = 2 `)
        }
        if (executionTime) {
            let executionTimeArr = executionTime.split(' ~ ')
            limitCondition.push(` (DATE(t.indentStartTime) >= ? AND DATE(t.indentStartTime) <= ? ) `)
            replacements.push(executionTimeArr[0])
            replacements.push(executionTimeArr[1])
        }
    
        if (activity) {
            limitCondition.push(` t.activity LIKE ? `)
            replacements.push('%'+activity+'%')
        }
        if (purpose) {
            limitCondition.push(` t.purpose LIKE ? `)
            replacements.push('%'+purpose+'%')
        }
        if (driverName) {
            limitCondition.push(` t.driverName like ? `)
            replacements.push('%'+driverName+'%')
        }
    }
    getSearchSql1()

    const getSearchSql2 = function () {
        if (vehicleNumber) {
            limitCondition.push(` t.vehicleNo like ? `)
            replacements.push('%'+vehicleNumber+'%')
        }
        if (vehicleType) {
            limitCondition.push(` t.vehicleType like ? `)
            replacements.push('%'+vehicleType+'%')
        }
        if (mobileNumber) {
            limitCondition.push(` t.contactNumber like ? `)
            replacements.push('%'+mobileNumber+'%')
        }
        if (indentID) {
            limitCondition.push(` t.indentId like ? `)
            replacements.push('%'+indentID+'%')
        }
        if (supportedUnit) {
            limitCondition.push(` t.groupId = ? `)
            replacements.push(supportedUnit)
        }
    }
    getSearchSql2()

    if (limitCondition.length) {
        taskSql += ' WHERE ' + limitCondition.join(' AND ');
    }
    console.log(taskSql)
    let taskList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements });
    return taskList
}

const generateExcelDatas = async function (reportGroupSelectionTitle, datas) {
    let groupList = await getSystemGroup()
    let excelList = []
    let titleList = []
    reportGroupSelectionTitle.forEach(title => {
        if (title == 'Driver Name') {
            titleList.push('Driver Name')
        } else if (title == 'Mobile Number') {
            titleList.push('Mobile Number')
        } else if (title == 'Vehicle Type') {
            titleList.push('Vehicle Type')
        } else if (title == 'Vehicle Number') {
            titleList.push('Vehicle Number')
        } else if (title == 'Actual Time') {
            titleList.push('Actual Start Time')
            titleList.push('Actual End Time')
        } else if (title == 'Execution Time') {
            titleList.push('Execution Start Time')
            titleList.push('Execution End Time')
        } else if (title == 'Hub/Node') {
            titleList.push('Hub')
            titleList.push('Node')
        }
        else if (title == "Odometer") {
            titleList.push('Start Odometer')
            titleList.push('End Odometer')
        }
        else {
            titleList.push(title)
        }
    })

    excelList.push(titleList)
    datas.forEach((r, index) => {
        let row = []
        let { taskId, groupId, dataFrom, driverName, driverNric, contactNumber, driverStatus, vehicleNumber, vehicleType, purpose, activity,
            indentStartTime, indentEndTime, mobileStartTime, mobileEndTime, indentId, cancelledCause, hub, node,
            startMileage, endMileage, mileageTraveled } = r
        
        if (driverStatus == 'waitcheck') {
            driverStatus = 'Pending'
        }
        driverStatus = driverStatus.slice(0, 1).toUpperCase() + driverStatus.slice(1).toLowerCase()

        let groupName = ''
        if (groupId) {
            let item = groupList.find(o => Number(o.id) == Number(groupId))
            groupName = item ? item.groupName : ""
        }

        let taskType = ''
        if (dataFrom == 'MT-ADMIN' && (taskId.indexOf('CU-') || taskId.indexOf('MT-'))) {
            taskType = 'MT-Admin'
        } else if (taskId.includes('AT-')) {
            taskType = 'ATMS'
        } else if (dataFrom == 'SYSTEM' && !taskId.includes('AT-')) {
            taskType = 'Sys Task'
        } else if (dataFrom == 'MOBILE') {
            taskType = 'Mobile'
        } else if (dataFrom == 'Urgent Indent') {
            taskType = 'Urgent Indent'
        }

        titleList.forEach(title => {
            switch (title) {
                case 'Task Type': 
                    row.push(taskType)
                    break;
                case 'Hub':
                    row.push(hub)
                    break;
                case 'Node':
                    row.push(node || '-')
                    break;
                case 'Task ID':
                    row.push(taskId)
                    break;
                case 'Task Status': 
                    row.push(driverStatus)
                    break;
                case 'Driver Name': 
                    row.push(driverName || '-')
                    break;
                case 'Mobile Number':
                    row.push(contactNumber)
                    break;
                case 'Vehicle Type':
                    row.push(vehicleType)
                    break;
                case 'Vehicle Number':
                    row.push(vehicleNumber || '-')
                    break;
                case 'Purpose':
                    row.push(purpose)
                    break;
                case 'Activity':
                    row.push(activity)
                    break;
                case 'Actual Start Time':
                    row.push(getTimeFormat(mobileStartTime))
                    break;
                case 'Actual End Time':
                    row.push(getTimeFormat(mobileEndTime))
                    break;
                case 'Execution Start Time':
                    row.push(getTimeFormat(indentStartTime))
                    break;
                case 'Execution End Time':
                    row.push(getTimeFormat(indentEndTime))
                    break;
                case 'Remarks': 
                    row.push(cancelledCause)
                    break;
                case 'Indent ID':
                    row.push(indentId)
                    break;
                case 'Start Odometer':
                    row.push(startMileage || "")
                    break;
                case 'End Odometer':
                    row.push(endMileage || "")
                    break;
                case "Mileage Captured":
                    row.push(mileageTraveled)
                    break;
                case "Supported Unit": 
                    row.push(groupName)
                    break;
                case 'Driver Nric':
                    row.push(driverNric)
                    break;
            }
        })
        excelList.push(row)
    })
    return excelList
}

const getTimeFormat = function (datetime) {
    if (!datetime) {
        return ""
    }
    return moment(datetime).format(fmt)
}

const getTimeFormat2 = function (dateTime) {
    if (!dateTime) {
        return '-'
    }
    return moment(dateTime).format(fmt2)
}

const ExportDataToExcel = async function (reportGroupSelectionTitle, datas, reportDateRange) {
    let excelList = await generateExcelDatas(reportGroupSelectionTitle, datas)

    if (!fs.existsSync(downloadFolder)) {
        fs.mkdir(path.resolve(downloadFolder), { recursive: true }, (err) => {
            if (err) {
                log.error(err)
            }
        });
    }
    reportDateRange = reportDateRange.replaceAll('-', '')
    let filename = `Task Report(${reportDateRange}).xlsx`
    let filepath = downloadFolder + utils.getSafePath(filename);
    let buffer = xlsx.build([
        {
            name: 'sheet1',
            data: excelList
        }
    ]);
    fs.writeFileSync(filepath, buffer, { 'flag': 'w' });
    return filename
}

const ExportKeypressDataToExcel = async function (reportGroupSelectionTitle, datas, reportDateRange) {
    let excelList = []
    let titleList = reportGroupSelectionTitle

    excelList.push(titleList)
    datas.forEach((r, index) => {
        let row = []
        let { locationName, boxName, vehicleNo, vehicleType, keySlotLocation, userName, driverName, transactionType, transactionTime, reason } = r
        titleList.forEach(title => {
            switch (title) {
                case 'Keypress Location':
                    row.push(locationName);
                    break;
                case 'Keypress Box Name':
                    row.push(boxName)
                    break;
                case 'Vehicle Type':
                    row.push(vehicleType)
                    break;
                case 'Vehicle No.':
                    row.push(vehicleNo)
                    break;
                case 'User Name':
                    row.push(userName)
                    break;
                case 'Driver Name':
                    row.push(driverName)
                    break;
                case 'Transaction Type':
                    row.push(transactionType)
                    break;
                case 'Key Slot Location':
                    row.push(keySlotLocation)
                    break;
                case 'Transaction Range':
                    row.push(getTimeFormat(transactionTime))
                    break;
                case 'Reason':
                    row.push(reason)
                    break;
            }
        })
        excelList.push(row)
    })

    if (!fs.existsSync(downloadFolder)) {
        fs.mkdir(path.resolve(downloadFolder), { recursive: true }, (err) => {
            if (err) {
                log.error(err)
            }
        });
    }
    reportDateRange = reportDateRange.replaceAll('-', '')
    let filename = `KeypressReport(${reportDateRange}).xlsx`
    let filepath = downloadFolder + utils.getSafePath(filename);
    let buffer = xlsx.build([
        {
            name: 'sheet1',
            data: excelList
        }
    ]);
    fs.writeFileSync(filepath, buffer, { 'flag': 'w' });
    return filename
}

const ExportTelematicsDataToExcel = async function (reportGroupSelectionTitle, dataList, reportDateRange) {
    let excelList = []
    let titleList = reportGroupSelectionTitle
    
    let newTitle = []
    for (let title of titleList) {
        if (title == 'Hub/Node') {
            newTitle.push('Hub')
            newTitle.push('Node')
            newTitle.push('Group')
        } else {
            newTitle.push(title)
        }
    }
    excelList.push(newTitle)

    let groupList = await sequelizeSystemObj.query(` SELECT id, groupName FROM \`group\` `, { type: QueryTypes.SELECT });
    dataList.forEach((r, index) => {
        let row = []
        let { violationType, occurrenceDateTime, dataFrom, groupId, driverName, vehicleNo, vehicleType, hub, node, lat, lng, speed, startTime, endTime, startSpeed, endSpeed, taskId } = r

        if (violationType !== CONTENT.ViolationType.Speeding) {
            speed = '-'
        }
        
        let diffSpeed = '-'
        if ([ CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc ].includes(violationType)) {
            diffSpeed = startSpeed - endSpeed
        }

        titleList.forEach(title => {
            switch (title) {
                case 'Type of ARB':
                    row.push(violationType);
                    break;
                case 'Vehicle No':
                    row.push(vehicleNo)
                    break;
                case 'Vehicle Type':
                    row.push(vehicleType)
                    break;
                case 'Occurrence Date & Time':
                    row.push(getTimeFormat2(occurrenceDateTime))
                    break;
                case 'Hub/Node': {
                    if (groupId) {
                        let list = groupList.filter(item => item.id == groupId)
                        if (list.length) {
                            let groupName = list[0].groupName
                            row.push('')
                            row.push('')                        
                            row.push(`${ groupName }`)
                        } else {
                            row.push('')
                            row.push('')
                            row.push(``)
                        }
                    } else {
                        row.push(hub)
                        row.push(node)
                        row.push('')
                    }
                    break;
                }
                case 'Driver Name':
                    row.push(driverName)
                    break;
                case 'Highest Speed':
                    row.push(speed)
                    break
                case 'Duration':
                    row.push(moment(endTime).diff(moment(startTime), 'second'))
                    break;
                case 'Diff in speed': {
                    row.push(diffSpeed)
                    break;
                }
                case 'Task ID':
                    row.push(taskId)
                    break;
                case 'Telematics Device':
                    row.push(dataFrom.toUpperCase());
                    break;
                case 'Location Data':
                    row.push(`${ lat }/${ lng }`)
                    break;
            }
        })
        excelList.push(row)
    })

    if (!fs.existsSync(downloadFolder)) {
        fs.mkdir(path.resolve(downloadFolder), { recursive: true }, (err) => {
            if (err) {
                log.error(err)
            }
        });
    }

    if (!reportDateRange) {
        // 2023-12-09 00:00:00 ~ 2024-01-09 23:59:59
        reportDateRange = moment(dataList[0]).format(`YYYYMMDD`) +"~"+moment(dataList.at(-1)).format(`YYYYMMDD`);
    } else {
        reportDateRange = reportDateRange.split('~').map(item => item.trim())
        reportDateRange = moment(reportDateRange[0]).format(`YYYYMMDD`) + "~" + moment(reportDateRange.at(-1)).format(`YYYYMMDD`);
    }
    let filename = `Telematics Report(${ reportDateRange }).xlsx`
    let filepath = downloadFolder + utils.getSafePath(filename);
    let buffer = xlsx.build([
        {
            name: 'sheet1',
            data: excelList
        }
    ]);
    fs.writeFileSync(filepath, buffer, { 'flag': 'w' });
    return filename
}

const ExportOBDDataToExcel = async function (reportGroupSelectionTitle, dataList, reportDateRange) {
    let excelList = []
    let titleList = reportGroupSelectionTitle

    let newTitle = []
    for (let title of titleList) {
        if (title == 'Hub/Node') {
            newTitle.push('Hub')
            newTitle.push('Node')
            newTitle.push('Group')
        } else {
            newTitle.push(title)
        }
    }
    excelList.push(newTitle)

    let groupList = await sequelizeSystemObj.query(` SELECT id, groupName FROM \`group\` `, { type: QueryTypes.SELECT });
    dataList.forEach((r, index) => {
        let row = []
        let { deviceId, groupId, vehicleNo, vehicleType, hub, node, lat, lng, latestDeviceTime  } = r
        titleList.forEach(title => {
            if (title == 'Device ID') {
                row.push(deviceId);
            } else if (title == 'Vehicle No') {
                row.push(vehicleNo)
            } else if (title == 'Vehicle Type') {
                row.push(vehicleType)
            } else if (title == 'Last Transmitted Date') {
                row.push(getTimeFormat2(latestDeviceTime))
            } else if (title == 'Hub/Node') {
                if (groupId) {
                    let list = groupList.filter(item => item.id == groupId)
                    if (list.length) {
                        let groupName = list[0].groupName
                        row.push('')
                        row.push('')
                        row.push(groupName)
                    } else {
                        row.push('')
                        row.push('')
                        row.push(``)
                    }
                } else {
                    row.push(hub)
                    row.push(node || '')
                    row.push('')
                }
            } else if (title == 'Location Data') {
                row.push(`${ lat }/${ lng }`)
            }
        })
        excelList.push(row)
    })

    if (!fs.existsSync(downloadFolder)) {
        fs.mkdir(path.resolve(downloadFolder), { recursive: true }, (err) => {
            if (err) {
                log.error(err)
            }
        });
    }

   
    let filename = `OBD Report(${ moment().format('YYYY-MM-DD') }).xlsx`
    let filepath = downloadFolder + filename
    let buffer = xlsx.build([
        {
            name: 'sheet1',
            data: excelList
        }
    ]);
    fs.writeFileSync(filepath, buffer, { 'flag': 'w' });
    return filename
}

module.exports.DownloadExcel = async function (req, res) {
    let { filename } = req.query
    filename = utils.getSafePath(filename);
    let rs = fs.createReadStream(downloadFolder + filename);
    res.writeHead(200, {
        'Content-Type': 'application/force-download',
        'Content-Disposition': 'attachment; filename=' + filename
    });
    rs.pipe(res);
}

module.exports.getTelematicReportList = async function (req, res) {
    try {
        let reportGroupSelectionTitle = req.body.reportGroupSelectionTitle
        let { occTimeRange, dataFrom, violationType, vehicleNumber, vehicleType, hub, node, driverName, taskId } = req.body.filter

        let userId = req.cookies.userId;
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            log.error(`UserId ${ userId } does not exist!`)
            return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
        }

        let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(userId);

        let mobileSql = `
            SELECT th.violationType, th.vehicleNo, v.vehicleType, DATE_FORMAT(th.occTime, '%Y-%m-%d %H:%i') AS occurrenceDateTime, 
            th.dataFrom, th.lat, th.lng, th.startTime, th.endTime, th.speed, th.startSpeed, th.endSpeed, 
            IFNULL(t1.taskId, CONCAT('DUTY', t2.dutyId)) AS taskId, 
            IFNULL(un1.id, un2.id) as unitId,
            IFNULL(t1.hub, t2.hub) AS hub,
            IFNULL(t1.node, t2.node) AS node,
            IFNULL(t1.groupId, t2.groupId) AS groupId,
            d.driverName
            FROM track_history th
            LEFT JOIN vehicle v ON v.vehicleNo = th.vehicleNo
            LEFT JOIN task t1 ON t1.driverId = th.deviceId AND t1.vehicleNumber = th.vehicleNo AND t1.mobileStartTime < th.occTime AND IFNULL(t1.mobileEndTime, NOW()) > th.occTime
            LEFT JOIN unit un1 on un1.unit = t1.hub and un1.subUnit <=> t1.node
            LEFT JOIN urgent_indent t2 ON t2.driverId = th.deviceId AND t2.vehicleNo = th.vehicleNo AND t2.mobileStartTime < th.occTime AND IFNULL(t2.mobileEndTime, NOW()) > th.occTime AND t2.status NOT IN ('cancelled')
            LEFT JOIN unit un2 on un2.unit = t2.hub and un2.subUnit <=> t2.node
            LEFT JOIN driver d ON th.deviceId = d.driverId
            WHERE th.dataFrom = 'mobile' 
        `
        let obdSql = `
            SELECT th.violationType, v.vehicleNo, v.vehicleType, DATE_FORMAT(th.occTime, '%Y-%m-%d %H:%i') AS occurrenceDateTime, 
            th.dataFrom, th.lat, th.lng, th.startTime, th.endTime, th.speed, th.startSpeed, th.endSpeed, 
            IFNULL(t1.taskId, CONCAT('DUTY', t2.dutyId)) AS taskId, 
            IFNULL(un1.id, un2.id) as unitId,
            IFNULL(t1.hub, t2.hub) AS hub,
            IFNULL(t1.node, t2.node) AS node,
            IFNULL(t1.groupId, t2.groupId) AS groupId,
            d.driverName
            FROM track_history th
            LEFT JOIN vehicle v ON v.deviceId = th.deviceId
            LEFT JOIN task t1 ON t1.vehicleNumber = v.vehicleNo AND t1.mobileStartTime < th.occTime AND IFNULL(t1.mobileEndTime, NOW()) > th.occTime
            LEFT JOIN unit un1 on un1.unit = t1.hub and un1.subUnit <=> t1.node
            LEFT JOIN urgent_indent t2 ON t2.driverId = th.deviceId AND t2.vehicleNo = th.vehicleNo AND t2.mobileStartTime < th.occTime AND IFNULL(t2.mobileEndTime, NOW()) > th.occTime AND t2.status NOT IN ('cancelled')
            LEFT JOIN unit un2 on un2.unit = t2.hub and un2.subUnit <=> t2.node
            LEFT JOIN driver d ON (d.driverId = t1.driverId OR d.driverId = t2.driverId)
            WHERE th.dataFrom = 'obd' 
        `

        let limitCondition = []
        let replacements = []

        const getSearchSql1 = function () {
            if (violationType) {
                limitCondition.push(` AND th.violationType = ? `)
                replacements.push(violationType)
            }
            if (occTimeRange) {
                let occTimeZone = occTimeRange.split('~').map(item => item.trim())
                limitCondition.push(` AND (th.occTime BETWEEN ? AND ?) `)
                replacements.push(occTimeZone[0])
                replacements.push(occTimeZone[1])
            }
            if (vehicleNumber) {
                limitCondition.push(` AND v.vehicleNo LIKE ? `)
                replacements.push('%'+vehicleNumber+'%')
            }
            if (vehicleType) {
                limitCondition.push(` AND v.vehicleType = ? `)
                replacements.push(vehicleType)
            }
            if (taskId) {
                limitCondition.push(` AND (t1.taskId LIKE ? OR t2.dutyId LIKE ?) `)
                replacements.push('%'+taskId+'%')
                replacements.push('%'+taskId+'%')
            }
            if (hub) {
                if (node) {
                    limitCondition.push(` AND ((t1.hub = ? AND t1.node = ?) OR (t2.hub = ? AND t2.node = ?)) `)
                    replacements.push(hub)
                    replacements.push(node)
                    replacements.push(hub)
                    replacements.push(node)
                } else {
                    limitCondition.push(` AND (t1.hub = ? OR t2.hub = ?) `)
                    replacements.push(hub)
                    replacements.push(hub)
                }
            }
            
            if (driverName) {
                limitCondition.push(` AND d.driverName LIKE ? `)
                replacements.push('%'+driverName+'%')
            }
        }
        getSearchSql1()
        
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            limitCondition.push(` AND (t1.groupId = ? OR t2.groupId = ?) `)
            replacements.push(user.unitId)
            replacements.push(user.unitId)
        } else if (user.userType != CONTENT.USER_TYPE.ADMINISTRATOR) {
            if (unitIdList.length) {
                if(groupIdList){
                    let newGroup = groupIdList.length ? groupIdList.join(',') : groupIdList
                    limitCondition.push(` AND ((un1.id in(?) or t1.groupId in(?))
                    OR (un2.id in(?) or t2.groupId in(?))) `)
                    replacements.push(unitIdList.join(","))
                    replacements.push(newGroup)
                    replacements.push(unitIdList.join(","))
                    replacements.push(newGroup)
                } else {
                    limitCondition.push(` AND ((un1.id in(?) and t1.groupId is null) 
                    OR (un2.id in(?) and t2.groupId is null)) `)
                    replacements.push(unitIdList.join(","))
                    replacements.push(unitIdList.join(","))
                }
            }
        }
        
        let baseSQL;
        if (dataFrom.toLowerCase() == 'mobile') {
            baseSQL = mobileSql
            if (limitCondition.length) {
                baseSQL += limitCondition.join(' ')
            }
        } else if (dataFrom.toLowerCase() == 'obd') {
            baseSQL = obdSql
            if (limitCondition.length) {
                baseSQL += limitCondition.join(' ')
            }
        } else {
            if (limitCondition.length) {
                mobileSql += limitCondition.join(' ')
                obdSql += limitCondition.join(' ')
            }
            baseSQL = `
                ${ mobileSql } UNION ${ obdSql }
            `
            replacements = replacements.flatMap(item => new Array(2).fill(item))
        }

        log.info(baseSQL)
        let telematicsReportList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: replacements })
        
        let filename = await ExportTelematicsDataToExcel(reportGroupSelectionTitle, telematicsReportList, occTimeRange)
        return res.json(utils.response(1, filename));
    } catch (error) {
        log.error(`getTelematicReportList`, error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getOBDReportList = async function (req, res) {
    try {
        let reportGroupSelectionTitle = req.body.reportGroupSelectionTitle
        let { deviceId, vehicleNumber, vehicleType, hub, node, transmittedDate } = req.body.filter

        if (transmittedDate) {
            transmittedDate = Number.parseInt(transmittedDate)
        }

        let userId = req.cookies.userId;
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            log.error(`UserId ${ userId } does not exist!`)
            return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
        }
        let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(userId);
        let baseSQL = `
            SELECT d.*, vv.vehicleNo, vv.vehicleType, vv.hub, vv.node, vv.unitId, vv.groupId 
            FROM device d
            LEFT JOIN (
                SELECT v.vehicleNo, v.vehicleType, u.unit AS hub, u.subUnit AS node, v.unitId, v.groupId, v.deviceId 
                FROM vehicle v
                LEFT JOIN unit u ON u.id = v.unitId
                GROUP BY v.deviceId

                UNION

                SELECT v.vehicleNo, v.vehicleType, u.unit AS hub, u.subUnit AS node, v.unitId, v.groupId, v.deviceId 
                FROM vehicle_history v
                LEFT JOIN unit u ON u.id = v.unitId
                GROUP BY v.deviceId
            ) vv ON vv.deviceId = d.deviceId
            WHERE 1=1
        `

        let limitCondition = []
        let replacements = []
        if (deviceId) {
            limitCondition.push(` AND d.deviceId LIKE ? `)
            replacements.push('%'+deviceId+'%')
        }
        if (vehicleNumber) {
            limitCondition.push(` AND vv.vehicleNo LIKE ? `)
            replacements.push('%'+vehicleNumber+'%')
        }
        if (vehicleType) {
            limitCondition.push(` AND vv.vehicleType = ? `)
            replacements.push(vehicleType)
        }
        if (transmittedDate) {
            limitCondition.push(` AND (DATE(d.latestDeviceTime) BETWEEN ? AND ?) `)
            replacements.push(moment().format('YYYY-MM-DD'))
            replacements.push(moment().add(transmittedDate, 'days').format('YYYY-MM-DD'))
        }
        if (hub) {
            if (node) {
                limitCondition.push(` AND (vv.hub = ? AND vv.node = ?) `)
                replacements.push(hub)
                replacements.push(node)
            } else {
                limitCondition.push(` AND ( vv.hub = ?) `)
                replacements.push(hub)
            }
        }

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            limitCondition.push(` AND vv.groupId = ? `)
            replacements.push(user.unitId)
        // } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
        //     if (user.node) {
        //         limitCondition.push(` AND ((vv.hub = '${ user.hub }' AND vv.node = '${ user.node }') `)
        //     } else {
        //         limitCondition.push(` AND (vv.hub = '${ user.hub }') `)
        //     }
        // }

        } else if (user.userType != CONTENT.USER_TYPE.ADMINISTRATOR) {
            if (unitIdList.length > 0) {
                if(groupIdList){
                    let newGroup = groupIdList.length > 0 ? groupIdList.join(',') : groupIdList
                    limitCondition.push(` AND (vv.unitId in(?) or vv.groupId in (?) ) `)
                    replacements.push(unitIdList.join(','))
                    replacements.push(newGroup)
                } else {
                    limitCondition.push(` AND vv.unitId in(?) `)
                    replacements.push(unitIdList.join(','))
                }
            }
        }

        if (limitCondition.length) {
            baseSQL += limitCondition.join(' ')
        }

        log.info(baseSQL)
        let obdReportList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: replacements })
        
        let filename = await ExportOBDDataToExcel(reportGroupSelectionTitle, obdReportList)
        return res.json(utils.response(1, filename));
    } catch (error) {
        log.error(`getOBDReportList`, error)
        return res.json(utils.response(0, error));
    }
}

const driverReportExcel = async function (reportGroupSelectionTitle, datas, reportDateRange) {
    let excelList = []
    let titleList = reportGroupSelectionTitle
    let classStatus = false
    let newTitleList = []
    let classList = null
    for(let item of titleList){
        if(item == 'Accumulated Mileage') {
            classList = await sequelizeObj.query(`
                select * from (
                    SELECT if(p.parent is null or p.parent = '', dm.permitType, p.parent) parent
                    FROM permittype p 
                    left join driver_permittype_detail dm on dm.permitType = p.permitType
               ) dp group by dp.parent 
            `, { type: QueryTypes.SELECT });
            classList = classList.map(itemObj => itemObj.parent)
            classList = Array.from(new Set(classList))
          
            if(classList.length > 0) {
                classStatus = true
                for(let item2 of classList){
                    if(item2 && item2 != '') newTitleList.push(item2)
                }
            }
        } else if (item == 'Hub/Node') {
            newTitleList.push('Hub')
            newTitleList.push('Node')
        } else {
            newTitleList.push(item)
        }
    }
    excelList.push(newTitleList)
    datas.forEach((r, index) => {
        let row = []
        let {
            driverName, vocation, enlistmentDate, operationallyReadyDate, nric, birthday,
            contactNumber, permitNo, permitIssueDate, role, driverDemeritPoints, assessmentType,
            permitType, vehicleType, groupName, unit, subUnit, status, mileageList, lastDrivenDate
        } = r
        let newMileageList = []
        if(classList){
            if(classList.length > 0){
                for(let item of classList){
                    let nlist = mileageList.filter(obj => obj.permitType == item)
                    if(nlist && nlist.length > 0){
                        newMileageList.push(nlist[0])
                    } else {
                        newMileageList.push({
                            permitType: item,
                            totalMileage: 0
                        })
                    }
                }
            }
        }
        newTitleList.forEach(title => {
            if (title == 'Driver Name') {
                row.push(driverName);
            } else if (title == 'Driver Role') {
                row.push(role)
            } else if (title == 'Vocation') {
                row.push(vocation)
            } else if (title == 'Hub') {
                row.push(unit)
            } else if (title == 'Node') {
                row.push(subUnit)
            } else if (title == 'Unit'){
                row.push(groupName)
            } else if (title == 'Enlistment Date') {
                row.push(enlistmentDate)
            } else if (title == 'ORD') {
                row.push(operationallyReadyDate)
            } else if (title == 'Driver NRIC') {
                row.push(nric)
            } else if (title == 'DOB') {
                row.push(birthday)
            } else if (title == 'Contact No') {
                row.push(contactNumber)
            } else if (title == 'Permit No') {
                row.push(permitNo)
            } else if (title == 'Date of Issue') {
                row.push(permitIssueDate)
            } else if (title == 'Status') {
                row.push(status)
            } else if (title == 'Category') {
                row.push(assessmentType)
            } else if (title == 'Vehicle Class') {
                row.push(permitType)
            } else if (title == 'Platforms') {
                row.push(vehicleType)
            } else if (title == 'Demerit Points') {
                row.push(driverDemeritPoints)
            } else if(title == 'Last Driven Date'){
                row.push(lastDrivenDate)
            } else if(classStatus) {
                if(newMileageList.length > 0){
                    for(let item of newMileageList){
                        if(title == item.permitType){
                            row.push(item.totalMileage ?? 0)
                        } 
                    }
                } else {
                    row.push(0)
                }
            }
        })
        excelList.push(row)
    })

    if (!fs.existsSync(downloadFolder)) {
        fs.mkdir(path.resolve(downloadFolder), { recursive: true }, (err) => {
            if (err) {
                log.error(err)
            }
        });
    }

    let filename = reportDateRange ? `driverReport(${utils.getSafePath(reportDateRange)}).xlsx` : `Driver Report(${ moment().format('YYYY-MM-DD') }).xlsx`
    let filepath = downloadFolder + filename
    let buffer = xlsx.build([
        {
            name: 'sheet1',
            data: excelList
        }
    ]);
    fs.writeFileSync(filepath, buffer, { 'flag': 'w' });
    return filename
}

module.exports.getDriverReportList = async function (req, res) {
    try {
        let { reportGroupSelectionTitle, reportDateRange, driverStatus, driverCategory, driverClass, driverType,
        enDateRange, ordRange, groupId, hub, node, role, vocation, ordStart } =  req.body;
        let startDate = null;
        let endDate = null;
        if(reportDateRange) {
            let dates = reportDateRange.split(' ~ ')
            startDate = dates[0]
            endDate = dates[1]
            startDate = moment(startDate).format('YYYY-MM-DD')
            endDate = moment(endDate).format('YYYY-MM-DD')
        }
        let userId = req.cookies.userId;
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            log.error(`UserId ${userId} does not exist!`)
            return res.json(utils.response(0, `UserId ${userId} does not exist!`));
        }
        let unitIdList = []
        let dataList = await UnitUtils.getPermitUnitList(req.cookies.userId);
        if(user.userType.toUpperCase() == 'CUSTOMER') {
            dataList.groupIdList = [user.unitId];
        }
        log.warn(`user unit list ==> ${ JSON.stringify(dataList) }`)
        if (user.userType.toUpperCase() != 'CUSTOMER' && !groupId) {
            unitIdList = dataList.unitIdList;
        }  
        log.warn(`driverList unitIdList ==> ${ JSON.stringify(unitIdList) }`)
        let groupList = await sequelizeSystemObj.query(` SELECT id, groupName FROM \`group\` `, { type: QueryTypes.SELECT })
        let newGroup = dataList.groupIdList?.length > 0 ? dataList.groupIdList.join(',') : dataList.groupIdList;
        if(hub && hub != '') newGroup = null
        if(groupId) newGroup = groupId
        log.warn(`driverList Group ==> ${ JSON.stringify(newGroup) }`)
        let result = []
        let uninGroupList = [newGroup]
        if(unitIdList.length > 0 && newGroup) uninGroupList = [null, newGroup]
        for(let item of uninGroupList){
            let driverList = await reportUtils.getDriverList({ startDate, endDate, user, item, unitIdList, driverStatus, driverCategory, driverClass, driverType, enDateRange,ordRange, role, vocation, ordStart, hub, node });
            let mileageListData = await reportUtils.getDriverMileageByClass(startDate, endDate)
            let lastDrivenDateList = await reportUtils.getLastDrivenDate()
            let newDriverList = []
            for(let item of driverList){
                let groupObj = groupList.filter(groupItem => groupItem.id == item.groupId)
                item.groupName = groupObj[0] ? groupObj[0].groupName : null
                if(item.nric){
                    if(item.nric.length > 9) {
                        item.nric = utils.decodeAESCode(item.nric);
                        item.nric = ((item.nric).toString()).substr(0, 1) + '****' + ((item.nric).toString()).substr(((item.nric).toString()).length-4, 4)
                    } 
                }
                let mileageList = mileageListData.filter(mi => mi.driverId == item.driverId)
                item.mileageList = mileageList
                let lastList = lastDrivenDateList.filter(ld => ld.driverId == item.driverId)
                item.lastDrivenDate = lastList.length > 0 ? moment(lastList[0].mobileEndTime).format('YYYY-MM-DD HH:mm:ss') : ''
                newDriverList.push(item)
            }
            result = [...result, ...newDriverList]
        }
        
        let filename = await driverReportExcel(reportGroupSelectionTitle, result, reportDateRange)
        return res.json(utils.response(1, filename));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const vehicleReportExcel = async function (reportGroupSelectionTitle, datas, reportDateRange) {
    function apartDays (startDate){
        if(!startDate) return 0
        startDate = moment(startDate).format('YYYY-MM-DD')
        let start = moment(startDate);
        let end = moment();
        let days = end.diff(start, 'days');
        return days;
    }
    let excelList = []
    let titleList = reportGroupSelectionTitle
    let newTitleList = []
    for(let item of titleList){
        if(item == 'WPT completion date and due date'){
            newTitleList.push('WPT1 completion date')
            newTitleList.push('WPT1 due date')
            newTitleList.push('WPT1 lapsed days')
            newTitleList.push('WPT2 completion date')
            newTitleList.push('WPT2 due date')
            newTitleList.push('WPT2 lapsed days')
            newTitleList.push('WPT3 completion date')
            newTitleList.push('WPT3 due date')
            newTitleList.push('WPT3 lapsed days')
        } else if(item == 'MPT completion date and due date'){
            newTitleList.push('MPT completion date')
            newTitleList.push('MPT due date')
            newTitleList.push('MPT lapsed days')
        } else if(item == 'PM completion date and due date'){
            newTitleList.push('PM completion date')
            newTitleList.push('PM due date')
            newTitleList.push('PM lapsed days')
        } else if(item == 'AVI completion date and due date'){
            newTitleList.push('AVI completion date')
            newTitleList.push('AVI due date')
            newTitleList.push('AVI lapsed days')
        } else if (item == 'Hub/Node') {
            newTitleList.push('Hub')
            newTitleList.push('Node')
        } else {
            newTitleList.push(item)
        }
    }
    excelList.push(newTitleList)
    datas.forEach((r, index) => {
        let row = []
        let {
            vehicleNo, vehicleType, vehicleCategory, odometer, description, limitSpeed, permitType,
            nextAviTime, nextPmTime, nextMptTime, nextWpt1Time, wpt1CompleteTime,nextWpt2Time,wpt2CompleteTime,
            nextWpt3Time,wpt3CompleteTime, unit, subUnit, status, mptCompleteTime, aviCompleteTime, pmCompleteTime
          } = r

        newTitleList.forEach(title => {
            if (title == 'Vehicle Category') {
                row.push(vehicleCategory);
            } else if (title == 'Vehicle Number') {
                row.push(vehicleNo)
            } else if (title == 'Vehicle Type') {
                row.push(vehicleType)
            }  else if (title == 'Odometer') {
                row.push(`${ odometer ?? 0 }km`)
            } else if (title == 'Hub') {
                row.push(unit)
            } else if (title == 'Node') {
                row.push(subUnit)
            } else if (title == 'Description') {
                row.push(description)
            } else if (title == 'Speed Limit') {
                row.push(`${ limitSpeed ?? 0 }KM/hr`)
            } else if (title == 'Permit Type') {
                row.push(permitType)
            } else if (title == 'Status') {
                row.push(status)
            } else if (title == 'WPT1 completion date') {
                row.push(wpt1CompleteTime ? moment(wpt1CompleteTime).format('YYYY-MM-DD HH:mm') : null)
            } else if (title == 'WPT1 due date') {
                row.push(nextWpt1Time)
            } else if (title == 'WPT1 lapsed days') {
                let days = apartDays(nextWpt1Time)
                row.push(days > 0 ? days : '')
            } else if (title == 'WPT2 completion date') {
                row.push(wpt2CompleteTime ? moment(wpt2CompleteTime).format('YYYY-MM-DD HH:mm') : null)
            } else if (title == 'WPT2 due date') {
                row.push(nextWpt2Time)
            } else if (title == 'WPT2 lapsed days') {
                let days = apartDays(nextWpt2Time)
                row.push(days > 0 ? days : '')
            } else if (title == 'WPT3 completion date') {
                row.push(wpt3CompleteTime ? moment(wpt3CompleteTime).format('YYYY-MM-DD HH:mm') : null)
            } else if (title == 'WPT3 due date') {
                row.push(nextWpt3Time)
            } else if (title == 'WPT3 lapsed days') {
                let days = apartDays(nextWpt3Time)
                row.push(days > 0 ? days : '')
            } else if (title == 'MPT completion date') {
                row.push(mptCompleteTime ? moment(mptCompleteTime).format('YYYY-MM-DD HH:mm') : null)
            } else if (title == 'MPT due date') {
                row.push(nextMptTime)
            } else if (title == 'MPT lapsed days') {
                let days = apartDays(nextMptTime)
                row.push(days > 0 ? days : '')
            } else if (title == 'PM completion date') {
                row.push(pmCompleteTime ? moment(pmCompleteTime).format('YYYY-MM-DD HH:mm') : null)
            } else if (title == 'PM due date') {
                row.push(nextPmTime)
            } else if (title == 'PM lapsed days') {
                let days = apartDays(nextPmTime)
                row.push(days > 0 ? days : '')
            } else if (title == 'AVI completion date') {
                row.push(aviCompleteTime ? moment(aviCompleteTime).format('YYYY-MM-DD HH:mm') : null)
            } else if (title == 'AVI due date') {
                row.push(nextAviTime)
            } else if (title == 'AVI lapsed days') {
                let days = apartDays(nextAviTime)
                row.push(days > 0 ? days : '')
            } 
        })
        excelList.push(row)
    })

    if (!fs.existsSync(downloadFolder)) {
        fs.mkdir(path.resolve(downloadFolder), { recursive: true }, (err) => {
            if (err) {
                log.error(err)
            }
        });
    }
    let filename = reportDateRange ? `vehicleReport(${reportDateRange}).xlsx` : `Vehicle Report(${ moment().format('YYYY-MM-DD') }).xlsx`
    let filepath = downloadFolder + utils.getSafePath(filename);
    let buffer = xlsx.build([
        {
            name: 'sheet1',
            data: excelList
        }
    ]);
    fs.writeFileSync(filepath, buffer, { 'flag': 'w' });
    return filename
}

module.exports.getVehicleReportList = async function (req, res) {
    try {
        let { reportGroupSelectionTitle, reportDateRange, vehicleCategory, permitType, hub, node, vehicleStatus,
            WPT1CompletionDateRange, WPT2CompletionDateRange, WPT3CompletionDateRange,
            MPTCompletionDateRange, PMCompletionDateRange, AVICompletionDateRange
        } = req.body;
        let startDate = null;
        let endDate = null;
        if(reportDateRange) {
            let dates = reportDateRange.split(' ~ ')
            startDate = dates[0]
            endDate = dates[1]
            startDate = moment(startDate).format('YYYY-MM-DD')
            endDate = moment(endDate).format('YYYY-MM-DD')
        }
        let userId = req.cookies.userId;
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            log.error(`UserId ${userId} does not exist!`)
            return res.json(utils.response(0, `UserId ${userId} does not exist!`));
        }
        let dataList = await UnitUtils.getPermitUnitList(req.cookies.userId);
        let unitIdList = []
        if (user.userType.toUpperCase() != 'CUSTOMER') {
            unitIdList = dataList.unitIdList;
        }
        log.warn(` vehicle unitId ==> ${ unitIdList }`)
        let newGroup = dataList.groupIdList?.length > 0 ? dataList.groupIdList.join(',') : dataList.groupIdList
        if(hub) newGroup = null
        log.warn(` vehicle group ==> ${ newGroup }`)
        let vehicleData = []
        let uninGroupList = [newGroup]
        if(unitIdList && newGroup) uninGroupList = [null, newGroup]
        for(let item of uninGroupList){
            let newVehicleList = []
            let vehicleList = await reportUtils.getVehicleList({ startDate, endDate, user, item, unitIdList, vehicleCategory, permitType, vehicleStatus, WPT1CompletionDateRange, WPT2CompletionDateRange, WPT3CompletionDateRange, hub, node });
            let taskPurposList = await reportUtils.getTaskPurposeByVehicle(startDate, endDate)
            for(let item of vehicleList){
                let purposList = taskPurposList.filter(taskObj => taskObj.vehicleNumber == item.vehicleNo);
                for(let itemTask of purposList){
                    if (!itemTask.purpose) continue

                    switch (itemTask.purpose.toLowerCase()) {
                        case 'avi':
                            item.aviCompleteTime = moment(itemTask.mobileEndTime).format('YYYY-MM-DD HH:mm')
                            break;
                        case 'pm':
                            item.pmCompleteTime = moment(itemTask.mobileEndTime).format('YYYY-MM-DD HH:mm')
                            break;
                        case 'mpt':
                            item.mptCompleteTime = moment(itemTask.mobileEndTime).format('YYYY-MM-DD HH:mm')
                            break;
                    }
                }
                let screeningCondition = [];
                const checkScreenCondition = function () {
                    if (MPTCompletionDateRange?.indexOf(' - ') != -1) {
                        let dates = MPTCompletionDateRange.split(' - ')
                        if(item.mptCompleteTime){
                            let state = dates[1] >= moment(item.mptCompleteTime).format('YYYY-MM-DD') && dates[0] <= moment(item.mptCompleteTime).format('YYYY-MM-DD')
                            screeningCondition.push(state) 
                        } else {
                            screeningCondition.push(false)
                        }
                    }
                    if (PMCompletionDateRange?.indexOf(' - ') != -1) {
                        let dates = PMCompletionDateRange.split(' - ')
                        if(item.pmCompleteTime){
                            let state = dates[1] >= moment(item.pmCompleteTime).format('YYYY-MM-DD') && dates[0] <= moment(item.pmCompleteTime).format('YYYY-MM-DD')
                            screeningCondition.push(state)
                        } else {
                            screeningCondition.push(false)
                        }
                    }
                    if (AVICompletionDateRange?.indexOf(' - ') != -1) {
                        let dates = AVICompletionDateRange.split(' - ')
                        if(item.aviCompleteTime){
                            let state = dates[1] >= moment(item.aviCompleteTime).format('YYYY-MM-DD') && dates[0] <= moment(item.aviCompleteTime).format('YYYY-MM-DD')
                            screeningCondition.push(state)
                        } else {
                            screeningCondition.push(false)
                        }
                    }
                }
                checkScreenCondition()
                if(screeningCondition.indexOf(false) == -1 && screeningCondition.indexOf('false') == -1) {
                    newVehicleList.push(item)
                } 
            }
            if (MPTCompletionDateRange || PMCompletionDateRange || AVICompletionDateRange) {
                vehicleData = [...vehicleData, ...newVehicleList]
            } else {
                vehicleData = [...vehicleData, ...vehicleList]
            }
        }
        let filename = await vehicleReportExcel(reportGroupSelectionTitle, vehicleData, reportDateRange)
        return res.json(utils.response(1, filename));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}