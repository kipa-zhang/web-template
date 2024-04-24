const log = require('../log/winston').logger('resourcesDashboard Service');
const FirebaseService = require('../firebase/firebase');
const utils = require('../util/utils');
const hubNodeConf = require('../conf/hubNodeConf');
const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const unitService = require('../services/unitService');
const mtAdminService = require('../services/mtAdminService');
const userService = require('../services/userService');
const { Unit } = require('../model/unit.js');
const { Driver } = require('../model/driver');
const { Vehicle } = require('../model/vehicle');
const { User } = require('../model/user');
const { sequelizeSystemObj } = require('../db/dbConf_system')

const jsonfile = require('jsonfile')

let TaskUtils = {
    getUnitAndUnitIdByUserId: async function (userId) {
        let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
        let hubNodeList = []
        for(let item of dataList.subUnitList){
            hubNodeList.push({ unit: item.split('&&')[0], subUnit: item.split('&&')[1] ?? null }) 
        }
        let user = await User.findOne({ where: { userId } })
        if(user.userType.toUpperCase() == 'CUSTOMER' && user.unitId){
            dataList.groupIdList = [user.unitId]
        }
        if(dataList.groupIdList || user.userType.toUpperCase() == 'ADMINISTRATOR') {
            hubNodeList.push({ unit: null, subUnit: null })
            dataList.unitList.push(null)
        }
        dataList.subUnitList = hubNodeList;
        return dataList;
    },
    getMtRacByRiskLevel: async function () {
        let mtRacByRiskLevel = await sequelizeObj.query(`
        SELECT r.taskId, r.riskLevel, u.id as unitId, u.unit FROM mt_rac as r
        LEFT JOIN task t ON r.taskId = t.taskId
        LEFT JOIN unit u ON u.unit = t.hub AND u.subUnit <=> t.node
        WHERE t.driverStatus not in ('Cancelled', 'completed')
        and ((NOW() between t.indentStartTime and t.indentEndTime)
        OR t.driverStatus = 'started')
        AND r.riskLevel like 'high'
        `, { type: QueryTypes.SELECT });
        return mtRacByRiskLevel
    },
    getMtRacByRiskLevelByCustomer: async function (unitId) {
        let driverByGroup = await TaskUtils.getDriverByGroup(unitId)
        driverByGroup = driverByGroup.map(item => item.driverId)
        if(driverByGroup.length < 1){
            return []
        }
        let sql = `
            SELECT r.taskId, r.riskLevel 
            FROM mt_rac as r
            LEFT JOIN task t ON r.taskId = t.taskId
            LEFT JOIN user us ON us.driverId = t.driverId
            WHERE t.driverStatus not in ('Cancelled', 'completed') 
            and ((NOW() between t.indentStartTime and t.indentEndTime)
            OR t.driverStatus = 'started')
        `
        let replacements = []
        if(driverByGroup.length > 0){
            sql += `
                and t.driverId in(?)
            `
            replacements.push(driverByGroup)
        }
        sql += ` AND r.riskLevel = 'high'`
        let mtRacByRiskLevel = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return mtRacByRiskLevel
    },
    getDriverByState: async function () {
        let driverByState = await sequelizeObj.query(`
            SELECT t.taskId, d.driverName, d.state, 
            IF(hh.toHub IS NULL, IF(u.id IS NULL, d.unitId, u.id), hh.unitId) AS unitId, 
            IF(hh.toHub IS NULL, IF(u.id IS NULL, u2.unit, u.unit), hh.toHub) AS unit
            FROM driver AS d 
            LEFT JOIN (
                SELECT ta.taskId, ta.driverId, ta.hub, ta.node, ta.mobileStartTime, ta.mobileEndTime FROM task ta 
                WHERE ta.driverStatus NOT IN ('Cancelled', 'completed')
                AND ((NOW() BETWEEN ta.indentStartTime AND ta.indentEndTime)
                OR ta.driverStatus = 'started') 
            ) t ON t.driverId = d.driverId AND (d.lastSOSDateTime >= t.mobileStartTime AND (t.mobileEndTime IS NULL OR t.mobileEndTime >= d.lastSOSDateTime )) 
            LEFT JOIN unit u ON u.unit <=> t.hub AND u.subUnit <=> t.node
            LEFT JOIN unit u2 ON u2.id = d.unitId
            LEFT JOIN USER us ON us.driverId = d.driverId
            LEFT JOIN (
                SELECT ho.driverId, ho.toHub, ho.toNode, ho.unitId FROM hoto ho 
                WHERE (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) hh ON hh.driverId = d.driverId
            WHERE us.role != 'TL'
            AND (d.lastSOSDateTime IS NOT NULL AND CURDATE() = DATE_FORMAT(d.lastSOSDateTime, '%Y-%m-%d'))
            AND (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate IS NULL)
            AND d.state LIKE '%sos%' GROUP BY d.driverId
        `, { type: QueryTypes.SELECT });
        return driverByState
    },
    getDriverByStateByCustomer: async function (unitId) {
        let driverByGroup = await TaskUtils.getDriverByGroup(unitId)
        driverByGroup = driverByGroup.map(item => item.driverId)
        if(driverByGroup.length < 1) {
            return []
        }
        let sql = `
            select t.taskId, d.driverName, d.state from driver as d 
            LEFT JOIN (
                SELECT ta.taskId, ta.driverId, ta.hub, ta.node, ta.mobileStartTime, ta.mobileEndTime FROM task ta 
                WHERE ta.driverStatus NOT IN ('Cancelled', 'completed')
                AND ((NOW() BETWEEN ta.indentStartTime AND ta.indentEndTime)
                OR ta.driverStatus = 'started') 
            ) t ON t.driverId = d.driverId AND (d.lastSOSDateTime >= t.mobileStartTime AND (t.mobileEndTime IS NULL OR t.mobileEndTime >= d.lastSOSDateTime )) 
            LEFT JOIN user us ON us.driverId = d.driverId
            WHERE (d.lastSOSDateTime IS NOT NULL AND CURDATE() = DATE_FORMAT(d.lastSOSDateTime, '%Y-%m-%d'))
        `
        let replacements = [];
        if(driverByGroup.length > 0){
            sql += ` and d.driverId in (?)`
            replacements.push(driverByGroup)
        }
        sql += ` AND d.state LIKE '%sos%' GROUP BY d.driverId`
        let driverByState = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return driverByState
    },
    getDriverDeployable: async function (subUnit, unitId, driverData) {
        let taskDriver = await sequelizeObj.query(`
            SELECT tt.driverId FROM task tt
            WHERE tt.driverStatus not in ('Cancelled', 'completed') 
            and ((NOW() BETWEEN tt.indentStartTime and tt.indentEndTime)
            OR tt.driverStatus = 'started')
            ${ !driverData ? ` AND tt.taskId NOT like 'CU-%'` : ''}
            group by tt.driverId 
        `, { type: QueryTypes.SELECT })
        taskDriver = taskDriver.map(item => item.driverId)

        let loanOutDriver = await sequelizeObj.query(` 
            SELECT l.driverId FROM loan l
            LEFT JOIN driver d ON d.driverId = l.driverId
            WHERE now() BETWEEN l.startDate AND l.endDate and d.permitStatus != 'invalid' 
            AND l.driverId IS NOT NULL
            GROUP BY l.driverId
        `, { type: QueryTypes.SELECT })
        loanOutDriver = loanOutDriver.map(item => item.driverId)

        let sql = `
            select ${ unitId == '-1' ? ` dd.currentUnit,` : ''  } ${ subUnit == '' && unitId == '-1' ? ` dd.currentSubUnit,` : '' } dd.currentStatus, dd.role, count(*) as statusSum from (
                SELECT d.driverId, d.operationallyReadyDate, us.role,
                    IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit,
                    IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit,
        `
        let replacements = []
        if(unitId != '-1'){
            sql += `
                IF(d.permitStatus = 'invalid', 'permitInvalid',
                    IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                            IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                    ) 
                )as currentStatus
            `
            replacements.push(taskDriver.join(","))
        } else {
            sql += `
                IF(d.permitStatus = 'invalid', 'permitInvalid',
                    IF(FIND_IN_SET(d.driverId, ?), 'LOAN OUT', 
                        IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                        )
                    ) 
                )as currentStatus
            `
            replacements.push(loanOutDriver.join(","))
            replacements.push(taskDriver.join(","))
        }
        sql += ` 
            FROM driver d
            LEFT JOIN user us ON us.driverId = d.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            left join (
                select ho.driverId, ho.toHub, ho.toNode from hoto ho 
                where (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) hh ON hh.driverId = d.driverId
            left join (
                select dl.driverId, dl.reason from driver_leave_record dl 
                where dl.status = 1 and (NOW() BETWEEN dl.startTime AND dl.endTime)
            ) ll ON ll.driverId = d.driverId
            where (d.operationallyReadyDate is null OR d.operationallyReadyDate > CURDATE()) 
        `
        if(driverData){
            if(driverData.length > 0){
                sql += ` and d.driverId in (?)`
                replacements.push(driverData)
            } else {
                sql += ` and 1=2`
            }
        } else {
            sql += ` and us.role != 'TL'`
        }
        sql += `
                group by d.driverId
            ) dd where dd.currentStatus = 'Deployable' 
            group by ${ unitId  == '-1' ?  `dd.currentUnit,` : '' } ${ subUnit == '' && unitId == '-1' ? ` dd.currentSubUnit,` : '' }dd.currentStatus
        `
        let driverDeployable = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return driverDeployable
    },
    getDriverStatusOrDeployed: async function (subUnit, dateData, deployedStatus, unitId, driverData) {
        let taskDriver = await sequelizeObj.query(`
            SELECT tt.driverId FROM task tt
            WHERE tt.driverStatus not in ('Cancelled', 'completed') 
            and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
            OR tt.driverStatus = 'started')
            ${ !driverData ? ` AND tt.taskId NOT like 'CU-%'` : ''}
            group by tt.driverId
        `, { type: QueryTypes.SELECT, replacements: [moment(dateData).format('YYYY-MM-DD')] })
        taskDriver = taskDriver.map(item => item.driverId)

        let loanOutDriver = await sequelizeObj.query(` 
            SELECT ll.driverId FROM (
                SELECT IF(l.driverId IS NULL, lr.driverId, l.driverId) AS driverId
                FROM driver d
                LEFT JOIN (
                    SELECT lo.driverId FROM loan lo 
                    WHERE ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') and DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                ) l ON l.driverId = d.driverId
                LEFT JOIN (
                    SELECT lr.driverId FROM loan_record lr 
                    WHERE ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                ) lr ON lr.driverId = d.driverId
                where d.permitStatus != 'invalid' 
            )ll WHERE ll.driverId IS NOT NULL
            group by ll.driverId
        `, { type: QueryTypes.SELECT, replacements: [moment(dateData).format('YYYY-MM-DD'), moment(dateData).format('YYYY-MM-DD')] })
        loanOutDriver = loanOutDriver.map(item => item.driverId)

        let sql = `
            select ${ unitId == '-1' ? `dd.currentUnit,`:''   }${ subUnit == '' && unitId == '-1' ? ` dd.currentSubUnit,` : '' } dd.currentStatus, dd.role, count(*) as statusSum from (
                SELECT d.driverId, d.operationallyReadyDate, us.role,
                IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.unit, IF(hh.toHub IS NULL, hr.toHub, hh.toHub)) AS currentUnit, 
                IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.subUnit, IF(hh.toHub IS NULL, hr.toNode, hh.toNode)) AS currentSubUnit,
        `
        let replacements = []
        if(unitId != '-1'){
            sql += `
                IF(d.permitStatus = 'invalid', 'permitInvalid',
                    IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                            IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                    ) 
                )as currentStatus
            `
            replacements.push(taskDriver.join(","))
        } else {
            sql += ` 
                IF(d.permitStatus = 'invalid', 'permitInvalid',
                    IF(FIND_IN_SET(d.driverId, ?), 'LOAN OUT', 
                        IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                        )
                    ) 
                )as currentStatus
            `
            replacements.push(loanOutDriver.join(","))
            replacements.push(taskDriver.join(","))
        }
        sql += `
            FROM driver d
            LEFT JOIN user us ON us.driverId = d.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            left join (
                select ho.driverId, ho.toHub, ho.toNode from hoto ho 
                where (? BETWEEN DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(ho.endDateTime, '%Y-%m-%d')) and ho.status = 'Approved'
            ) hh ON hh.driverId = d.driverId
            LEFT JOIN (
                select hr.driverId, hr.toHub, hr.toNode FROM hoto_record hr WHERE 
                (? BETWEEN DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(hr.returnDateTime , '%Y-%m-%d'))
                and hr.status = 'Approved'
            ) hr ON hr.driverId = d.driverId
            left join (
                select dl.driverId, dl.reason from driver_leave_record dl 
                where dl.status = 1 and (? BETWEEN DATE_FORMAT(dl.startTime, '%Y-%m-%d') AND DATE_FORMAT(dl.endTime, '%Y-%m-%d'))
            ) ll ON ll.driverId = d.driverId
            where (d.operationallyReadyDate is null OR d.operationallyReadyDate > ? )
        `
        replacements.push(moment(dateData).format('YYYY-MM-DD'))
        replacements.push(moment(dateData).format('YYYY-MM-DD'))
        replacements.push(moment(dateData).format('YYYY-MM-DD'))
        replacements.push(moment(dateData).format('YYYY-MM-DD'))
        if(driverData){
            if(driverData.length > 0){
                sql += ` 
                    and d.driverId in (?)
                `
                replacements.push(driverData)
            } else {
                sql += ` and 1=2`
            }
        } else {
            sql += ` and us.role != 'TL'`
        }
        sql += `
                group by d.driverId
            ) dd where 1=1 ${ deployedStatus == '' ? ` and dd.currentStatus = 'Deployed'` : '' }
            group by ${ unitId  == '-1' ? `dd.currentUnit,` : '' } ${ subUnit == '' && unitId  == '-1' ? ` dd.currentSubUnit,` : '' }dd.currentStatus
        `
        let driverStatusOrDeployed = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return driverStatusOrDeployed
    },
    getVehicleDeployable: async function (subUnit, vehicleList, userType) {
        let taskVehicle = await sequelizeObj.query(`
        SELECT tt.vehicleNumber FROM task tt 
        WHERE tt.vehicleStatus not in ('Cancelled', 'completed')  
        and ((NOW() BETWEEN tt.indentStartTime and tt.indentEndTime)
        OR tt.vehicleStatus = 'started')
        ${ !vehicleList ? ` AND tt.taskId NOT like 'CU-%'` : ''}
        group by tt.vehicleNumber
        `, { type: QueryTypes.SELECT })
        taskVehicle = taskVehicle.map(item => item.vehicleNumber)

        let loanOutVehicle = await sequelizeObj.query(`
            SELECT l.vehicleNo FROM loan l
            LEFT JOIN vehicle v ON v.vehicleNo = l.vehicleNo
            WHERE now() BETWEEN l.startDate AND l.endDate
            AND l.vehicleNo IS NOT NULL
            GROUP BY l.vehicleNo
        `, { type: QueryTypes.SELECT })// AND v.onhold = 0
        loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)
        
        let sql = `
            select ${ vehicleList ? '' : ` vv.currentUnit,`} ${ subUnit == '' && !vehicleList ? ` vv.currentSubUnit,` : '' } vv.currentStatus, count(*) as statusSum from (
                SELECT veh.vehicleNo,IF(hh.toHub is NULL, un.unit, hh.toHub) as currentUnit, 
                IF(hh.toHub is NULL, un.subUnit, hh.toNode) as currentSubUnit,
        `
        let replacements = []
        if(vehicleList){
            sql += `
                IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                    IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                ) as currentStatus
            `
            replacements.push(taskVehicle.join(","))
        } else {
            sql += `
                IF(FIND_IN_SET(veh.vehicleNo, ?), 'LOAN OUT', 
                    IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                    )
                ) as currentStatus
            `
            replacements.push(loanOutVehicle.join(","))
            replacements.push(taskVehicle.join(","))
        }
        sql += `
            FROM vehicle veh
            left join unit un on un.id = veh.unitId
            left join (
                select ho.vehicleNo, ho.toHub, ho.toNode from hoto ho where 
                (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) hh ON hh.vehicleNo = veh.vehicleNo
            left join (
                select vl.vehicleNo, vl.reason from vehicle_leave_record vl 
                where vl.status = 1 and (NOW() BETWEEN vl.startTime AND vl.endTime)
            ) ll ON ll.vehicleNo = veh.vehicleNo
            where 1=1
        `
        if(vehicleList){
            if(vehicleList?.length > 0){
                sql += ` and veh.vehicleNo in (?)`
                replacements.push(`'${ vehicleList.join("','") }'`)
            } else {
                sql += ` and 1=2`
            }
        }
        
        sql += ` 
                GROUP BY veh.vehicleNo
            ) vv where vv.currentStatus = 'Deployable' 
            group by  ${ vehicleList ? '' : ` vv.currentUnit,` } ${ subUnit == '' && !vehicleList ? ` vv.currentSubUnit,` : '' }vv.currentStatus
        `
        let vehicleDeployable = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return vehicleDeployable
    },
    getVehicleStatusOrDeployed: async function (subUnit, dateData, deployedStatus, userType, vehicleList) {
        let taskVehicle = await sequelizeObj.query(`
        SELECT tt.vehicleNumber FROM task tt 
        WHERE tt.vehicleStatus not in ('Cancelled', 'completed')  
        ${ !vehicleList ? ` AND taskId NOT like 'CU-%'` : ''}
        and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
        OR tt.vehicleStatus = 'started')
        group by tt.vehicleNumber
        `, { type: QueryTypes.SELECT, replacements: [moment(dateData).format('YYYY-MM-DD')] })
        taskVehicle = taskVehicle.map(item => item.vehicleNumber)

        let loanOutVehicle = await sequelizeObj.query(`
            SELECT ll.vehicleNo FROM (
                SELECT IF(l.vehicleNo IS NULL, lr.vehicleNo, l.vehicleNo) AS vehicleNo
                FROM vehicle v
                LEFT JOIN (SELECT lo.vehicleNo FROM loan lo WHERE ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') and DATE_FORMAT(lo.endDate, '%Y-%m-%d')) l ON l.vehicleNo = v.vehicleNo
                LEFT JOIN (SELECT lr.vehicleNo FROM loan_record lr 
                    WHERE ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                ) lr ON lr.vehicleNo = v.vehicleNo
            )ll WHERE ll.vehicleNo IS NOT NULL
            GROUP BY ll.vehicleNo
        `, { type: QueryTypes.SELECT, replacements: [moment(dateData).format('YYYY-MM-DD'), moment(dateData).format('YYYY-MM-DD')] })//  WHERE v.onhold = 0   
        loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)

        let sql = `
            select ${ vehicleList ? '' : ` vv.currentUnit,` }${ subUnit == '' && !vehicleList ? ` vv.currentSubUnit,` : '' } vv.currentStatus, count(*) as statusSum from (
                SELECT veh.vehicleNo,
                IF(hh.toHub is NULL AND  hr.toHub IS NULL, un.unit, IF(hh.toHub is NULL, hr.toHub, hh.toHub)) AS currentUnit, 
                IF(hh.toHub is NULL AND  hr.toHub IS NULL, un.subUnit, IF(hh.toHub is NULL, hr.toNode, hh.toNode)) AS currentSubUnit,
        `
        let replacements = []
        if(vehicleList){
            sql += `
                IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                    IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                )as currentStatus
            ` 
            replacements.push(taskVehicle.join(","))
        } else {
            sql += `
                IF(FIND_IN_SET(veh.vehicleNo, ?), 'LOAN OUT', 
                    IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                    )
                ) as currentStatus
            `
            replacements.push(loanOutVehicle.join(","))
            replacements.push(taskVehicle.join(","))
        }
        sql += `
            FROM vehicle veh
            left join unit un on un.id = veh.unitId
            left join (select ho.vehicleNo, ho.toHub, ho.toNode from hoto ho where 
                (? BETWEEN DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(ho.endDateTime, '%Y-%m-%d'))
                and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = veh.vehicleNo
            LEFT JOIN (select hr.vehicleNo, hr.toHub, hr.toNode FROM hoto_record hr WHERE 
                (? BETWEEN DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(hr.returnDateTime , '%Y-%m-%d'))
                and hr.status = 'Approved'
            ) hr ON hr.vehicleNo = veh.vehicleNo
            left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status = 1 and (? BETWEEN DATE_FORMAT(vl.startTime, '%Y-%m-%d') AND DATE_FORMAT(vl.endTime, '%Y-%m-%d'))) ll ON ll.vehicleNo = veh.vehicleNo
            where 1=1
        `
        replacements.push(moment(dateData).format('YYYY-MM-DD'))
        replacements.push(moment(dateData).format('YYYY-MM-DD'))
        replacements.push(moment(dateData).format('YYYY-MM-DD'))
        if(vehicleList){
            if(vehicleList?.length > 0){
                sql += ` and veh.vehicleNo in (?)`
                replacements.push(`'${ vehicleList.join("','") }'`)
            } else {
                sql += ` and 1=2`
            }
        }
        
        sql += `
                GROUP BY veh.vehicleNo
            ) vv ${ deployedStatus == '' ? `where vv.currentStatus = 'Deployed'` : '' }
            group by ${ vehicleList ? '' : `vv.currentUnit,` }${ subUnit == '' && !vehicleList ? ` vv.currentSubUnit,` : '' }vv.currentStatus
        `
        let vehicleStatusOrDeployed = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements });
        return vehicleStatusOrDeployed
    },
    getTaskDriver: async function (cuTaskShow) {
       let sql = `SELECT tt.driverId
       FROM task tt
       WHERE tt.driverStatus not in ('Cancelled', 'completed')  
       and ((NOW() BETWEEN tt.indentStartTime and tt.indentEndTime)
       OR tt.driverStatus = 'started')
       and tt.driverId is not null
       ${ cuTaskShow ? ` AND tt.taskId NOT like 'CU-%'` : `  AND tt.taskId like 'CU-%'` }
       group by tt.driverId
       `
       let taskDriver = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
       taskDriver = taskDriver.map(item => item.driverId)
       return taskDriver;
    },
    getTaskVehicle: async function (cuTaskShow) {
       let sql = `
       SELECT tt.vehicleNumber FROM task tt
       WHERE tt.vehicleStatus not in ('Cancelled', 'completed') 
       and ((NOW() BETWEEN tt.indentStartTime and tt.indentEndTime)
       OR tt.vehicleStatus = 'started')
       and tt.vehicleNumber is not null
       ${ cuTaskShow ? ` AND tt.taskId NOT like 'CU-%'` : `  AND tt.taskId like 'CU-%'` }
       group by tt.vehicleNumber
       `
       let taskVehicle = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
       taskVehicle = taskVehicle.map(item => item.vehicleNumber)
       return taskVehicle;
    },
    getLoanOutVehicle: async function (totalStatus) {
        let loanOutVehicle = await sequelizeObj.query(` 
            SELECT l.vehicleNo FROM loan l
            LEFT JOIN vehicle v ON v.vehicleNo = l.vehicleNo
            WHERE now() BETWEEN l.startDate and l.endDate
            AND l.vehicleNo IS NOT NULL
            GROUP BY l.vehicleNo
        `, { type: QueryTypes.SELECT })//${ !totalStatus ? ` AND v.onhold = 0` : '' }
        loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)
        return loanOutVehicle;
    },
    getLoanOutDriver: async function (unitId, dateData, totalStatus) {
        let sql;
        let replacements = []
        if(dateData){
            const initLoanDriverSqlByDate = function (){
                sql = `
                SELECT ll.driverId, ll.groupId FROM (
                    SELECT IF(l.driverId IS NULL, lr.driverId, l.driverId) AS driverId,
                    IF(l.groupId IS NULL, lr.groupId, l.groupId) AS groupId
                    FROM driver d
                    LEFT JOIN (SELECT lo.driverId, lo.groupId FROM loan lo 
                        WHERE ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') and DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                    ) l ON l.driverId = d.driverId
                    LEFT JOIN (SELECT lr.driverId, lr.groupId FROM loan_record lr 
                        WHERE ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                    ) lr ON lr.driverId = d.driverId
                    where 1=1 ${ !totalStatus ? ` and d.permitStatus != 'invalid'` : '' } 
                    and (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null) 
                )ll WHERE ll.driverId IS NOT NULL            
                `
                replacements.push(moment(dateData).format('YYYY-MM-DD'))
                replacements.push(moment(dateData).format('YYYY-MM-DD'))
                replacements.push(moment(dateData).format('YYYY-MM-DD'))
                if(unitId){
                    if(unitId.length > 0){
                        sql += ` and ll.groupId in(?)`
                        replacements.push(unitId)
                    } else {
                        sql += ` and ll.groupId = ?`
                        replacements.push(unitId)
                    }
                }
                sql += ` GROUP BY ll.driverId`
            }
            initLoanDriverSqlByDate()
        } else {
            const initLoanDriverSqlByNotDate = function (){
                sql = `
                SELECT l.driverId FROM loan l
                LEFT JOIN driver d ON d.driverId = l.driverId
                WHERE 1=1 ${ !totalStatus ? ` and d.permitStatus != 'invalid'` : '' } 
                and NOW() BETWEEN l.startDate and l.endDate 
                and (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate is null) 
                `
                if(unitId){
                    if(unitId.length > 0){
                        sql += ` and l.groupId in(?)`
                        replacements.push(unitId)
                    } else {
                        sql += ` and l.groupId = ?`
                        replacements.push(unitId)
                    }
                }
                sql += `
                    AND l.driverId IS NOT NULL
                    GROUP BY l.driverId
                `
            }
            initLoanDriverSqlByNotDate()
        }
        let loanOutDriver = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        loanOutDriver = loanOutDriver.map(item => item.driverId)
        return loanOutDriver;
    },
    getDayAll: async function(starDay, endDay) {
        starDay = moment(starDay).format('YYYY-MM-DD')
        endDay = moment(endDay).format('YYYY-MM-DD')
        let middleDates = [];
        let currentDate = moment(starDay);
        while (currentDate.isSameOrBefore(moment(endDay))) {
            middleDates.push(currentDate.format('YYYY-MM-DD'));
            currentDate = currentDate.add(1, 'day');
        }
        return middleDates
    },
    getVehicleByGroup: async function(groupId, dateData, totalStatus) {
        let vehicleList = await mtAdminService.TaskUtils.getVehicleByGroup(null, false, groupId, null, null, null, dateData, totalStatus)
        return vehicleList
    },
    getDriverByGroup: async function(unitId, dateData, totalStatus) {
        let driverList = await mtAdminService.TaskUtils.getDriverByGroup(false, unitId, null, null, null, dateData, totalStatus)
        return driverList
    },
    getModulePageByUserId: async function(userId, pageType){
        let modulePage = await sequelizeObj.query(`
        SELECT mp.page FROM module_page mp
        LEFT JOIN ROLE ro ON FIND_IN_SET(mp.id, ro.pageList)
        LEFT JOIN USER us ON us.role = ro.roleName
        WHERE userId = ? AND mp.module = ?
        `, { type: QueryTypes.SELECT, replacements: [userId, pageType] })
        modulePage = modulePage.map(item => (item.page).toLowerCase())
        let driverShow;
        let vehicleShow;
        if(modulePage.indexOf('vehicle') != -1 && modulePage.indexOf('to') == -1){
            vehicleShow = true;
            driverShow = false;
        } else if(modulePage.indexOf('vehicle') == -1 && modulePage.indexOf('to') != -1){
            vehicleShow = false;
            driverShow = true;
        } else {
            driverShow = true;
            vehicleShow = true;
        }
        return { driverShow, vehicleShow };
    },
    initUnitListByUnitHub: async function (unitHub, userUnit){
        let unitList = []
        if(unitHub){
            if(unitHub.toLowerCase() == 'dv_loa' || unitHub.toLowerCase() == 'other'){
                unitList = []
                unitList.push({ id: 0, unit: null, subUnit: null })
            } else {
                let unit = await Unit.findAll({where: { unit: unitHub }, order: [['subUnit', 'DESC']]})
                unitList = unit.map(item => item)
            }
        } else {
            unitList = userUnit.subUnitList
        }
        return unitList
    },
    initUnitListByUnitHubNode: async function (unitHub, unitNode, userUnit){
        let unitList = []
        if(unitHub){
            if((unitHub.toLowerCase() == 'dv_loa' || unitHub.toLowerCase() == 'other')) {
                unitList = []
                unitList.push({ id: 0, unit: null, subUnit: null }) 
             } else if((unitNode.toLowerCase() == 'dv_loa' || unitNode.toLowerCase() == 'other')) {
                unitList = []
                unitList.push({ id: 0, unit: unitHub, subUnit: null }) 
             } else {
                unitList = []
                let unit = await Unit.findOne({where: { unit: unitHub, subUnit: unitNode }})
                unitList.push(unit)
             }
           
        } else {
            unitList = userUnit.subUnitList
        }
        return unitList
    },
    getUnitHubList: function (unitHub, userUnit){
        let unitList = []
        if(unitHub) {
            if((unitHub.toLowerCase() == 'dv_loa' || unitHub.toLowerCase() == 'other')) {
                unitList = []
                unitList.push(null) 
            } else {
                unitList = [unitHub]
            }
        } else {
            unitList = userUnit.unitList
        }
        return unitList
    }
}

module.exports.TaskUtils = TaskUtils;

module.exports.getDriverByRoleByHub = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = userUnit.unitList

        let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;

        // unit 
        let taskDriver = await TaskUtils.getTaskDriver(true);
        let loanOutDriver = await TaskUtils.getLoanOutDriver(groupId);
        let taskDriverTotal = await sequelizeObj.query(`
            select dd.currentUnit,dd.currentStatus,dd.role, count(*) as taskNum from (
                SELECT d.driverId, d.operationallyReadyDate, uu.role,
                IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit, 
                IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit,
                IF(d.permitStatus = 'invalid', 'permitInvalid',
                    IF(FIND_IN_SET(d.driverId, ?), 'LOAN OUT', 
                        IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                        )
                    ) 
                ) as currentStatus
                FROM driver d
                LEFT JOIN unit u ON u.id = d.unitId
                LEFT JOIN user uu on d.driverId =uu.driverId and uu.userType='MOBILE'
                left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                left join (select dl.driverId, dl.reason from driver_leave_record dl 
                    where dl.status = 1 and (NOW() BETWEEN dl.startTime AND dl.endTime)
                ) ll ON ll.driverId = d.driverId
                where (d.operationallyReadyDate is null OR d.operationallyReadyDate > CURDATE())
                group by d.driverId
            ) dd where dd.currentStatus = 'Deployed' 
            group by dd.currentUnit,dd.currentStatus,dd.role
        `, { type: QueryTypes.SELECT, replacements: [loanOutDriver.join(","), taskDriver.join(",")] })

        //group
        const initTaskDriverByGroup = async function (){
            let taskDriverByGroup = await TaskUtils.getTaskDriver(false);
            let driverByGroup = await TaskUtils.getDriverByGroup(groupId)
            driverByGroup = driverByGroup.map(item => item.driverId)
            let taskDriverTotal2Sql = `
                select dd.currentUnit,dd.currentStatus,dd.role, count(*) as taskNum from (
                    SELECT d.driverId, d.operationallyReadyDate, uu.role,
                    IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit, 
                    IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit,
                    IF(d.permitStatus = 'invalid', 'permitInvalid',
                    IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                        IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                    )
                    ) as currentStatus
                    FROM driver d
                    LEFT JOIN unit u ON u.id = d.unitId
                    LEFT JOIN user uu on d.driverId = uu.driverId and uu.userType='MOBILE'
                    left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                        (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                    ) hh ON hh.driverId = d.driverId
                    left join (select dl.driverId, dl.reason from driver_leave_record dl 
                        where dl.status = 1 and (NOW() BETWEEN dl.startTime AND dl.endTime)
                    ) ll ON ll.driverId = d.driverId
                    where (d.operationallyReadyDate is null OR d.operationallyReadyDate > CURDATE())
            `
            let replacements = [taskDriverByGroup.join(",")]
            if(driverByGroup){
                if(driverByGroup.length > 0){
                    taskDriverTotal2Sql += ` and d.driverId in(?)`
                    replacements.push(driverByGroup)
                }
            }
            
            taskDriverTotal2Sql += ` 
                    group by d.driverId
                ) dd where dd.currentStatus = 'Deployed' 
                group by dd.currentUnit,dd.currentStatus,dd.role
            `
            let taskDriverTotal2 = await sequelizeObj.query(taskDriverTotal2Sql, { type: QueryTypes.SELECT, replacements: replacements })
            return taskDriverTotal2
        }

        const getTaskDriverTotalDataArrayByUnitOrGroup = async function (item, taskDriverTotal){
            let taskDriverTotalDataArray = []
            if(item){
                taskDriverTotalDataArray = taskDriverTotal.filter(task => task.currentUnit == item && task.role);
            } else {
                let taskDriverTotal2 = await initTaskDriverByGroup();
                taskDriverTotalDataArray = taskDriverTotal2.filter(task => task && task.role);
            }
            return taskDriverTotalDataArray
        }
        let result = []
        for(let item of unitList){
            let hubData = { unit: item || 'Other', subunit: item || 'Other', driverPurposeData: [] }
            let taskDriverTotalDataArray = await getTaskDriverTotalDataArrayByUnitOrGroup(item, taskDriverTotal)
            
            let toStartedCount = 0;
            let tlStartedCount = 0;
            let dvStartedCount = 0;
            let loaStartedCount = 0;
            if(modulePage.driverShow && taskDriverTotalDataArray.length > 0){
                for (let temp of taskDriverTotalDataArray) {
                    switch (temp.role.toLowerCase()){
                        case 'to':  
                            toStartedCount += temp.taskNum
                            break; 
                        case 'tl':
                            tlStartedCount += temp.taskNum
                            break; 
                        case 'dv':
                            dvStartedCount += temp.taskNum
                            break; 
                        case 'loa':
                            loaStartedCount += temp.taskNum
                            break;
                    }
                }
            }
            hubData.driverPurposeData.push({purpose: 'TO', taskCount: 0, startedTaskCount: toStartedCount})
            hubData.driverPurposeData.push({purpose: 'TL', taskCount: 0, startedTaskCount: tlStartedCount})
            hubData.driverPurposeData.push({purpose: 'DV', taskCount: 0, startedTaskCount: dvStartedCount})
            hubData.driverPurposeData.push({purpose: 'LOA', taskCount: 0, startedTaskCount: loaStartedCount})
            
            result.push(hubData);
        }
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverByRoleByNode = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let unitHub = req.body.unit;
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHub(unitHub, userUnit)
        let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;

        // data by unit
        let taskDriver = await TaskUtils.getTaskDriver(true);
        let loanOutDriver = await TaskUtils.getLoanOutDriver(groupId);
        let taskDriverTotal = await sequelizeObj.query(`
            select dd.currentUnit,dd.currentSubUnit,dd.currentStatus,dd.role, count(*) as taskNum from (
                SELECT d.driverId, d.operationallyReadyDate, uu.role,
                IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit, 
                IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit,
                IF(d.permitStatus = 'invalid', 'permitInvalid',
                    IF(FIND_IN_SET(d.driverId, ?), 'LOAN OUT', 
                        IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                        )
                    ) 
                )as currentStatus
                FROM driver d
                LEFT JOIN unit u ON u.id = d.unitId
                LEFT JOIN user uu on d.driverId =uu.driverId and uu.userType='MOBILE'
                left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                left join (select dl.driverId, dl.reason from driver_leave_record dl 
                    where dl.status = 1 and (NOW() BETWEEN dl.startTime AND dl.endTime)
                ) ll ON ll.driverId = d.driverId
                where (d.operationallyReadyDate is null OR d.operationallyReadyDate > CURDATE()) 
                group by d.driverId
            ) dd where dd.currentStatus = 'Deployed' 
            group by dd.currentUnit, dd.currentSubUnit, dd.currentStatus, dd.role
        `, { type: QueryTypes.SELECT, replacements: [loanOutDriver.join(","), taskDriver.join(",")] })

        // data by group
        const initTaskDriverTotalByGroup = async function (){
            let taskDriverByGroup = await TaskUtils.getTaskDriver(false);
            let driverByGroup = await TaskUtils.getDriverByGroup(groupId)
            driverByGroup = driverByGroup.map(item => item.driverId)
            let sql = `
                select dd.currentUnit,dd.currentSubUnit,dd.currentStatus,dd.role, count(*) as taskNum from (
                    SELECT d.driverId, d.operationallyReadyDate, uu.role,
                    IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit, 
                    IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit,
                    IF(d.permitStatus = 'invalid', 'permitInvalid',
                        IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                            IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                        ) 
                    )as currentStatus
                    FROM driver d
                    LEFT JOIN unit u ON u.id = d.unitId
                    LEFT JOIN user uu on d.driverId =uu.driverId and uu.userType='MOBILE'
                    left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                        (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                        ) hh ON hh.driverId = d.driverId
                    left join (select dl.driverId, dl.reason from driver_leave_record dl 
                        where dl.status = 1 and (NOW() BETWEEN dl.startTime AND dl.endTime)
                    ) ll ON ll.driverId = d.driverId
                    where (d.operationallyReadyDate is null OR d.operationallyReadyDate > CURDATE()) 
            `
            let replacements = [taskDriverByGroup.join(",")]
            if(driverByGroup){
                if(driverByGroup.length > 0){
                    sql += ` and d.driverId in (?)`
                    replacements.push(driverByGroup)
                }
            }
            
            sql += `
                    group by d.driverId
                ) dd where dd.currentStatus = 'Deployed' 
                group by dd.currentUnit, dd.currentSubUnit, dd.currentStatus, dd.role
            `
            let taskDriverTotal2 = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
            return taskDriverTotal2
        }

        // task driver total by unit
        const initStartedTaskListStartedDataArray = async function (item){
            let startedTaskListStartedDataArray = []
            if(item.unit){
                startedTaskListStartedDataArray = taskDriverTotal.filter(task => (task.currentUnit == item.unit && task.currentSubUnit == item.subUnit) && task.role);
            } else {
                let taskDriverTotal2 = await initTaskDriverTotalByGroup()
                startedTaskListStartedDataArray = taskDriverTotal2.filter(task => task && task.role);
            }
            return startedTaskListStartedDataArray;
        }
        let result = []
        for(let item of unitList){
            let hubData = { unit: item.unit || 'Other', subunit: item.subUnit || 'Other', driverPurposeData: [] }
            let startedTaskListStartedDataArray = await initStartedTaskListStartedDataArray(item)
            let toStartedCount = 0;
            let tlStartedCount = 0;
            let dvStartedCount = 0;
            let loaStartedCount = 0;
            if(modulePage.driverShow && startedTaskListStartedDataArray.length > 0){
                for (let temp of startedTaskListStartedDataArray) {
                    switch (temp.role.toLowerCase()){
                        case 'to':
                            toStartedCount += temp.taskNum
                            break;
                        case 'tl':
                            tlStartedCount += temp.taskNum
                            break;
                        case 'dv':
                            dvStartedCount += temp.taskNum
                            break;
                        case 'loa':
                            loaStartedCount += temp.taskNum
                            break;
                    }
                }
            }
            hubData.driverPurposeData.push({purpose: 'TO', taskCount: 0, startedTaskCount: toStartedCount})
            hubData.driverPurposeData.push({purpose: 'TL', taskCount: 0, startedTaskCount: tlStartedCount})
            hubData.driverPurposeData.push({purpose: 'DV', taskCount: 0, startedTaskCount: dvStartedCount})
            hubData.driverPurposeData.push({purpose: 'LOA', taskCount: 0, startedTaskCount: loaStartedCount})
            
            result.push(hubData);
        }
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-06-15 Customer user,Add two purpose(WPT and MPT)
module.exports.getVehicleByPurposeByHub = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = userUnit.unitList;
        
        // data by unit
        let taskVehicle = await TaskUtils.getTaskVehicle(true);
        let loanOutVehicle = await TaskUtils.getLoanOutVehicle()
        let taskVehicleTotal = await sequelizeObj.query(`
            select vv.currentUnit,vv.currentStatus, vv.purpose, count(*) as taskNum from (
                SELECT veh.vehicleNo, t.purpose,
                IF(hh.toHub is NULL, un.unit, hh.toHub) as currentUnit, 
                IF(hh.toHub is NULL, un.subUnit, hh.toNode) as currentSubUnit,
                IF(FIND_IN_SET(veh.vehicleNo, ?), 'LOAN OUT', 
                    IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                    )
                ) as currentStatus
                FROM vehicle veh
                left join unit un on un.id = veh.unitId
                Left join task t on t.vehicleNumber = veh.vehicleNo
                left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = veh.vehicleNo
                left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status = 1 and (NOW() BETWEEN vl.startTime AND vl.endTime)) ll ON ll.vehicleNo = veh.vehicleNo
                GROUP BY veh.vehicleNo
            ) vv where vv.currentStatus = 'Deployed' group by vv.currentUnit,vv.currentStatus, vv.purpose
         `, { type: QueryTypes.SELECT, replacements: [loanOutVehicle.join(","), taskVehicle.join(",")] }) 
        
        // data by group
        const initTaskVehicleTotalByGroup = async function (){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let taskVehicleByGroup = await TaskUtils.getTaskVehicle(false);
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId);
            vehicleData = vehicleData.map(item => item.vehicleNo)
            let sql = `
            select vv.currentStatus, vv.purpose, count(*) as taskNum from (
                SELECT veh.vehicleNo, t.purpose,
                IF(hh.toHub is NULL, un.unit, hh.toHub) as currentUnit, 
                IF(hh.toHub is NULL, un.subUnit, hh.toNode) as currentSubUnit,
                IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                ) as currentStatus                    
                FROM vehicle veh
                left join unit un on un.id = veh.unitId
                Left join (SELECT tt.purpose, tt.vehicleNumber FROM task tt WHERE tt.vehicleStatus NOT IN ('Cancelled', 'completed') AND (NOW() BETWEEN tt.indentStartTime AND tt.indentEndTime)) t on t.vehicleNumber = veh.vehicleNo
                left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = veh.vehicleNo
                left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status = 1 and (NOW() BETWEEN vl.startTime AND vl.endTime)) ll ON ll.vehicleNo = veh.vehicleNo
            `
            let replacements = [taskVehicleByGroup.join(",")]
            if(vehicleData){
                if(vehicleData.length > 0){
                    sql += ` where veh.vehicleNo in (?)`
                    replacements.push(`'${ vehicleData.join("','") }'`)
                } else {
                    sql += ' and 1=2'
                }
            }
            sql += ` 
                GROUP BY veh.vehicleNo
            ) vv where vv.currentStatus = 'Deployed'  and vv.purpose is not null
            group by vv.currentStatus, vv.purpose
            `
            let taskVehicleTotalByGroup = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements }) 
            return taskVehicleTotalByGroup
        }

        // vehicle total by purpose
        const initStartedTaskListStartedDataArray = async function (item){
            let startedTaskListStartedDataArray = []
            if(item){
                startedTaskListStartedDataArray = taskVehicleTotal.filter(task => task.currentUnit == item && task.purpose);
            } else {
                startedTaskListStartedDataArray = await initTaskVehicleTotalByGroup()
            }
            return startedTaskListStartedDataArray
        }
        let result = []
        for(let item of unitList){
            let hubData = { unit: item || 'Other', subunit: item || 'Other', vehiclePurposeData: [] }            
            let startedTaskListStartedDataArray = await initStartedTaskListStartedDataArray(item);

            let vehiclePurposeDataList = [
                {purpose: 'ops', taskCount: 0, startedTaskCount: 0},
                {purpose: 'training', taskCount: 0, startedTaskCount: 0},
                {purpose: 'admin', taskCount: 0, startedTaskCount: 0},
                {purpose: 'exercise', taskCount: 0, startedTaskCount: 0},
                {purpose: 'duty', taskCount: 0, startedTaskCount: 0},
                {purpose: 'driving training', taskCount: 0, startedTaskCount: 0},
                {purpose: 'maintenance', taskCount: 0, startedTaskCount: 0},
                {purpose: 'others', taskCount: 0, startedTaskCount: 0},
                {purpose: 'familiarisation', taskCount: 0, startedTaskCount: 0},
                {purpose: 'wpt', taskCount: 0, startedTaskCount: 0},
                {purpose: 'mpt', taskCount: 0, startedTaskCount: 0},
                {purpose: 'avi', taskCount: 0, startedTaskCount: 0},
                {purpose: 'pm', taskCount: 0, startedTaskCount: 0}
            ]
            let newDataList = null
            if(modulePage.vehicleShow && startedTaskListStartedDataArray.length > 0){
                newDataList = startedTaskListStartedDataArray.reduce((acc, temp) => {   
                    for(let pur of vehiclePurposeDataList){
                        if (temp.purpose.toLowerCase().startsWith(pur.purpose)) {  
                            pur.startedTaskCount += temp.taskNum
                            break; 
                        }  
                    }  
                    return acc;  
                }, vehiclePurposeDataList);
            }
            hubData.vehiclePurposeData = newDataList ?? vehiclePurposeDataList;
            result.push(hubData);
        }

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-06-15 Customer user,Add two purpose(WPT and MPT)
module.exports.getVehicleByPurposeByNode = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let unitHub = req.body.unit;
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHub(unitHub, userUnit)

        // data by unit
        let taskVehicle = await TaskUtils.getTaskVehicle(true);
        let loanOutVehicle = await TaskUtils.getLoanOutVehicle();
        let taskVehicleTotal = await sequelizeObj.query(`
            select vv.currentUnit,vv.currentSubUnit,vv.currentStatus, vv.purpose, count(*) as taskNum from (
                SELECT veh.vehicleNo, t.purpose,
                IF(hh.toHub is NULL, un.unit, hh.toHub) as currentUnit, 
                IF(hh.toHub is NULL, un.subUnit, hh.toNode) as currentSubUnit,
                IF(FIND_IN_SET(veh.vehicleNo, ?), 'LOAN OUT', 
                    IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                    )
                )as currentStatus
                FROM vehicle veh
                left join unit un on un.id = veh.unitId
                Left join task t on t.vehicleNumber = veh.vehicleNo
                left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = veh.vehicleNo
                left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status = 1 and (NOW() BETWEEN vl.startTime AND vl.endTime)) ll ON ll.vehicleNo = veh.vehicleNo
                GROUP BY veh.vehicleNo
            ) vv where vv.currentStatus = 'Deployed'  and vv.purpose is not null
            group by vv.currentUnit, vv.currentSubUnit, vv.currentStatus, vv.purpose
        `, { type: QueryTypes.SELECT, replacements: [loanOutVehicle.join(","), taskVehicle.join(",")] })  
        
        // data by group
        const initTaskVehicleTotalByGroup = async function(){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let taskVehicleByGroup = await TaskUtils.getTaskVehicle(false);
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId);
            vehicleData = vehicleData.map(item => item.vehicleNo)
            let sql = `
            select vv.currentStatus, vv.purpose, count(*) as taskNum from (
                SELECT veh.vehicleNo, t.purpose,
                IF(hh.toHub is NULL, un.unit, hh.toHub) as currentUnit, 
                IF(hh.toHub is NULL, un.subUnit, hh.toNode) as currentSubUnit,
                IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                    IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                )as currentStatus                        
                FROM vehicle veh
                left join unit un on un.id = veh.unitId
                Left join (SELECT tt.purpose, tt.vehicleNumber FROM task tt WHERE tt.vehicleStatus NOT IN ('Cancelled', 'completed') AND (NOW() BETWEEN tt.indentStartTime AND tt.indentEndTime)) t on t.vehicleNumber = veh.vehicleNo
                left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = veh.vehicleNo
                left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status = 1 and (NOW() BETWEEN vl.startTime AND vl.endTime)) ll ON ll.vehicleNo = veh.vehicleNo
            `
            let replacements = [taskVehicleByGroup.join(",")]
            if(vehicleData){
                if(vehicleData.length > 0){
                    sql += ` where veh.vehicleNo in (?)`
                    replacements.push(vehicleData)
                } else {
                    sql += ' and 1=2'
                }
            }
            sql += `
                GROUP BY veh.vehicleNo
            ) vv where vv.currentStatus = 'Deployed' and vv.purpose is not null
            group by vv.currentStatus, vv.purpose
            `
            let taskVehicleTotalByGroup = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements }) 
            return taskVehicleTotalByGroup
        }

        // purpose data
        const initStartedTaskListStartedDataArray = async function (item){
            let startedTaskListStartedDataArray = []
            if(item.unit){
                startedTaskListStartedDataArray = taskVehicleTotal.filter(task => task.currentUnit == item.unit && task.currentSubUnit == item.subUnit);
            } else {
                startedTaskListStartedDataArray = await initTaskVehicleTotalByGroup();
            }
            return startedTaskListStartedDataArray
        }
        let result = []
        for(let item of unitList){
            let hubData = { unit: item.unit ? item.unit : 'Other', subunit: item.subUnit ? item.subUnit : 'Other', vehiclePurposeData: [], vehicleStatusData: [] }        
            let startedTaskListStartedDataArray = await initStartedTaskListStartedDataArray(item)

            let vehiclePurposeDataList = [
                {purpose: 'ops', taskCount: 0, startedTaskCount: 0},
                {purpose: 'training', taskCount: 0, startedTaskCount: 0},
                {purpose: 'admin', taskCount: 0, startedTaskCount: 0},
                {purpose: 'exercise', taskCount: 0, startedTaskCount: 0},
                {purpose: 'duty', taskCount: 0, startedTaskCount: 0},
                {purpose: 'driving training', taskCount: 0, startedTaskCount: 0},
                {purpose: 'maintenance', taskCount: 0, startedTaskCount: 0},
                {purpose: 'others', taskCount: 0, startedTaskCount: 0},
                {purpose: 'familiarisation', taskCount: 0, startedTaskCount: 0},
                {purpose: 'wpt', taskCount: 0, startedTaskCount: 0},
                {purpose: 'mpt', taskCount: 0, startedTaskCount: 0},
                {purpose: 'avi', taskCount: 0, startedTaskCount: 0},
                {purpose: 'pm', taskCount: 0, startedTaskCount: 0}
            ]
            let newDataList = null
            if(modulePage.vehicleShow && startedTaskListStartedDataArray.length > 0){
                newDataList = startedTaskListStartedDataArray.reduce((acc, temp) => {   
                    for(let pur of vehiclePurposeDataList){
                        if (temp.purpose.toLowerCase().startsWith(pur.purpose)) {  
                            pur.startedTaskCount += temp.taskNum
                            break; 
                        }  
                    }  
                    return acc;  
                }, vehiclePurposeDataList);
            }
            hubData.vehiclePurposeData = newDataList ?? vehiclePurposeDataList;
            result.push(hubData);
        }
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverAndVehicleDeployableTotalByHub = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = userUnit.unitList

        // data by unit
        let driverTotalByUnit = await sequelizeObj.query(`
            select COUNT(dd.driverId) as total, dd.unit from (
                select d.driverId, us.role,
                IF(hh.toHub is NULL, u.unit, hh.toHub) as unit,
                IF(hh.toHub is NULL, u.id, hh.unitId) as unitId
                from driver as d
                LEFT JOIN user us on us.driverId = d.driverId
                LEFT JOIN unit u on d.unitId = u.id
                LEFT JOIN (select ho.driverId,ho.toHub, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                where (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate is null) 
                AND us.role != 'TL'
                group by d.driverId
            ) dd GROUP BY dd.unit
        `, { type: QueryTypes.SELECT }) //and d.permitStatus != 'invalid'
        let vehicleTotalByUnit = await sequelizeObj.query(`
            select COUNT(vv.vehicleNo) as total, vv.unit from (
                select v.vehicleNo,
                IF(hh.toHub is NULL, u.unit, hh.toHub) as unit,
                IF(hh.toHub is NULL, u.id, hh.unitId) as unitId
                from vehicle as v
                LEFT JOIN unit u on v.unitId = u.id
                LEFT JOIN (select ho.vehicleNo,ho.toHub, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = v.vehicleNo
                group by v.vehicleNo
            ) vv GROUP BY vv.unit
        `, { type: QueryTypes.SELECT }) //where v.onhold = 0
        let mtRacByRiskLevel = await TaskUtils.getMtRacByRiskLevel();
        log.info(`mtRac high ${ JSON.stringify(mtRacByRiskLevel) }`)
        let driverByState = await TaskUtils.getDriverByState()
        log.info(`driver sos ${ JSON.stringify(driverByState) }`)
        let driverByDeployable = await TaskUtils.getDriverDeployable(null, '-1')
        let vehicleByDeployable = await TaskUtils.getVehicleDeployable(null, null, req.cookies.userType)
        const initDataByUnit = async function (item){
            let mtRacByRiskLevelArray
            let driverByStateArray
            let obj
            let driverTotalObj = driverTotalByUnit.filter(driverObj => driverObj.unit == item)
            let driverTotal = driverTotalObj.length > 0 ? driverTotalObj[0].total : 0;
            let vehicleTotalObj = vehicleTotalByUnit.filter(vehicleObj => vehicleObj.unit == item)
            let vehicleTotal = vehicleTotalObj.length > 0 ? vehicleTotalObj[0].total : 0;
            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            driverByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.toUpperCase() == obj.currentUnit.toUpperCase()){
                        assignedDriverNumber = obj.statusSum
                    }
                }
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.toUpperCase() == obj.currentUnit.toUpperCase()){
                        assignedVehicleNumber = obj.statusSum
                    }
                }
            })
            mtRacByRiskLevelArray = mtRacByRiskLevel.filter(obj => obj.unit == item);
            driverByStateArray = driverByState.filter(obj => obj.unit == item);
            if(!modulePage.driverShow){
                driverTotal = 0;
                assignedDriverNumber = 0;
                driverByStateArray = [];
            }
            if(!modulePage.vehicleShow){
                vehicleTotal = 0;
                assignedVehicleNumber = 0;
                mtRacByRiskLevelArray = [];
            }
            obj = {
                unit: item,
                driverListNumber: driverTotal || 0,
                vehicleListNumber: vehicleTotal || 0,
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
                driverByState: driverByStateArray.length > 0,
                mtRacByRiskLevel: mtRacByRiskLevelArray.length > 0
            }
            return obj
        }
        
        // data by group
        const initDataByGroup = async function (item){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let mtRacByRiskLevelArray
            let driverByStateArray
            let obj
            let driverData = await TaskUtils.getDriverByGroup(groupId);
            let driverData2 = await TaskUtils.getDriverByGroup(groupId, null, '1');
            let driverTotal = driverData2.length
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId);
            let vehicleData2 = await TaskUtils.getVehicleByGroup(groupId, null, '1');
            let vehicleTotal = vehicleData2.length;
            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            vehicleData = vehicleData.map(itemdata => itemdata.vehicleNo)
            driverData = driverData.map(itemdata => itemdata.driverId)
            let vehicleByDeployableByCu = await TaskUtils.getVehicleDeployable(null, vehicleData, req.cookies.userType)
            let driverByDeployableByCu = await TaskUtils.getDriverDeployable(null, groupId ?? '0', driverData)

            let mtRacByRiskLevel = await TaskUtils.getMtRacByRiskLevelByCustomer(groupId);
            log.info(`mtRac high ${ JSON.stringify(mtRacByRiskLevel) }`)
            let driverByState = await TaskUtils.getDriverByStateByCustomer(groupId)
            log.info(`driver sos ${ JSON.stringify( driverByState) }`)
            driverByDeployableByCu.forEach((obj)=>{
                assignedDriverNumber = obj.statusSum
            })
    
            vehicleByDeployableByCu.forEach((obj)=>{
                assignedVehicleNumber = obj.statusSum
            })
            mtRacByRiskLevelArray = mtRacByRiskLevel;
            driverByStateArray = driverByState;
            if(!modulePage.driverShow){
                driverTotal = 0;
                assignedDriverNumber = 0;
                driverByStateArray = [];
            }
            if(!modulePage.vehicleShow){
                vehicleTotal = 0;
                assignedVehicleNumber = 0;
                mtRacByRiskLevelArray = [];
            }
            obj = {
                unit: item ?? 'DV_LOA',
                driverListNumber: driverTotal,
                vehicleListNumber:  vehicleTotal,
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
                driverByState: driverByStateArray.length > 0,
                mtRacByRiskLevel: mtRacByRiskLevelArray.length > 0
            }
            return obj
        }
        let data = []
        //2023-07-05 CUSTOMER UNIT
        for(let item of unitList){
            let obj
            if(item){
                obj = await initDataByUnit(item);
            } else {
                obj = await initDataByGroup(item);
            }
            let hubConf = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
            for (let item2 of hubConf) {
                if (item?.toLowerCase() == item2.hub?.toLowerCase()) {
                    obj.circleColor = item2.color
                }  
            }
            if (!obj.circleColor) {
                obj.circleColor = hubNodeConf.defaultColor
            }
            data.push(obj)
        }
        return res.json(utils.response(1, data));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverAndVehicleDeployableTotalByNode = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let unitHub = req.body.unit;
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHub(unitHub, userUnit)

        //data by unit
        let driverTotalByUnit = await sequelizeObj.query(`
            select COUNT(dd.driverId) as total, dd.unit, dd.subUnit from (
                select d.driverId, us.role,
                IF(hh.toHub is NULL, u.unit, hh.toHub) as unit, IF(hh.toHub is NULL, u.subUnit, hh.toNode) as subUnit,
                IF(hh.toHub is NULL, u.id, hh.unitId) as unitId
                from driver as d
                LEFT JOIN user us on us.driverId = d.driverId
                LEFT JOIN unit u on d.unitId = u.id
                LEFT JOIN (select ho.driverId,ho.toHub,ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                where (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate is null) 
                and us.role != 'TL'
                group by d.driverId
            ) dd GROUP BY dd.unit, dd.subUnit
        `, { type: QueryTypes.SELECT }) //and d.permitStatus != 'invalid'
        let vehicleTotalByUnit = await sequelizeObj.query(`
            select COUNT(vv.vehicleNo) as total, vv.unit, vv.subUnit from (
                select v.vehicleNo,
                IF(hh.toHub is NULL, u.unit, hh.toHub) as unit, IF(hh.toHub is NULL, u.subUnit, hh.toNode) as subUnit,
                IF(hh.toHub is NULL, v.unitId, hh.unitId) as unitId
                from vehicle as v
                LEFT JOIN unit u on v.unitId = u.id
                LEFT JOIN (select ho.vehicleNo,ho.toHub,ho.toNode, ho.unitId from hoto ho where 
                    (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = v.vehicleNo
                group by v.vehicleNo
            ) vv GROUP BY vv.unit, vv.subUnit
        `, { type: QueryTypes.SELECT }) //where v.onhold = 0
        let mtRacByRiskLevel = await TaskUtils.getMtRacByRiskLevel();
        log.info(`mtRac high ${ JSON.stringify(mtRacByRiskLevel) }`)
        let driverByState = await TaskUtils.getDriverByState()
        log.info(`driver sos ${ JSON.stringify(driverByState) }`)
        let driverByDeployable = await TaskUtils.getDriverDeployable('', '-1')
        let vehicleByDeployable = await TaskUtils.getVehicleDeployable('', null, req.cookies.userType)
        const initDataByUnit = async function (item){
            let mtRacByRiskLevelArray
            let driverByStateArray
            let obj
            let driverTotalObj = driverTotalByUnit.filter(driverObj => driverObj.unit == item.unit && driverObj.subUnit == item.subUnit)
            let driverTotal = driverTotalObj.length > 0 ? driverTotalObj[0].total : 0;
            let vehicleTotalObj = vehicleTotalByUnit.filter(vehicleObj => vehicleObj.unit == item.unit && vehicleObj.subUnit == item.subUnit)
            let vehicleTotal = vehicleTotalObj.length > 0 ? vehicleTotalObj[0].total : 0;

            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            driverByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase() && (item.subUnit == obj.currentSubUnit)){
                        assignedDriverNumber = obj.statusSum
                    }
                }
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase() && (item.subUnit == obj.currentSubUnit)){
                        assignedVehicleNumber = obj.statusSum
                    }
                }
            })
            mtRacByRiskLevelArray = mtRacByRiskLevel.filter(obj => obj.unitId == item.id);
            driverByStateArray = driverByState.filter(obj => obj.unitId == item.id);
            if(!modulePage.driverShow){
                driverTotal = 0;
                assignedDriverNumber = 0;
                driverByStateArray = []
            }
            if(!modulePage.vehicleShow){
                vehicleTotal = 0;
                assignedVehicleNumber = 0;
                mtRacByRiskLevelArray = []
            }
            obj = {
                unit: item.unit,
                subunit: item.subUnit ? item.subUnit : 'Other',
                driverListNumber: driverTotal || 0,
                vehicleListNumber: vehicleTotal || 0,
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
                driverByState: driverByStateArray.length > 0,
                mtRacByRiskLevel: mtRacByRiskLevelArray.length > 0
            }
            return obj
        }

        // data by group 
        const initDataByGroup = async function (){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let mtRacByRiskLevelArray
            let driverByStateArray
            let obj
            let driverData = await TaskUtils.getDriverByGroup(groupId);
            let driverData2 = await TaskUtils.getDriverByGroup(groupId, null, '1');
            let driverTotal = driverData2.length
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId);
            let vehicleData2 = await TaskUtils.getVehicleByGroup(groupId, null, '1');
            let vehicleTotal = vehicleData2.length;
        
            vehicleData = vehicleData.map(item => item.vehicleNo)
            driverData = driverData.map(item => item.driverId)
            let vehicleByDeployableByCu = await TaskUtils.getVehicleDeployable(null, vehicleData, req.cookies.userType)
            let driverByDeployableByCu = await TaskUtils.getDriverDeployable(null, groupId ?? '0', driverData)

            let mtRacByRiskLevelByCu = await TaskUtils.getMtRacByRiskLevelByCustomer(groupId);
            log.info(`mtRac high ${ JSON.stringify(mtRacByRiskLevelByCu) }`)
            let driverByStateByCu = await TaskUtils.getDriverByStateByCustomer(groupId)
            log.info(`driver sos ${ JSON.stringify(driverByStateByCu) }`)
            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            driverByDeployableByCu.forEach((obj)=>{
                assignedDriverNumber = obj.statusSum
            })
    
            vehicleByDeployableByCu.forEach((obj)=>{
                assignedVehicleNumber = obj.statusSum
            })
            mtRacByRiskLevelArray = mtRacByRiskLevelByCu.filter(obj => obj);
            driverByStateArray = driverByStateByCu.filter(obj => obj);
            if(!modulePage.driverShow){
                driverTotal = 0;
                assignedDriverNumber = 0;
                driverByStateArray = []
            }
            if(!modulePage.vehicleShow){
                vehicleTotal = 0;
                assignedVehicleNumber = 0;
                mtRacByRiskLevelArray = []
            }
            obj = {
                unit: 'DV_LOA',
                subunit: 'DV_LOA',
                driverListNumber: driverTotal,
                vehicleListNumber: vehicleTotal,
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
                driverByState: driverByStateArray.length > 0,
                mtRacByRiskLevel: mtRacByRiskLevelArray.length > 0
            }
            return obj
        }
        let data = []
        for(let item of unitList){
            let obj
            if(item.unit){
                obj = await initDataByUnit(item);
            } else {
                obj = await initDataByGroup()
            }
            data.push(obj)
        }
        return res.json(utils.response(1, data));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }  
}

module.exports.getDriverAndVehicleDeployedTotalByNode = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let unitHub = req.body.unit;
        let unitNode = req.body.subUnit;
        let currentDate = req.body.currentDate;
        currentDate = currentDate.split(' ~ ')
        let dateStart = moment(currentDate[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let dateEnd = moment(currentDate[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let weekDate = await TaskUtils.getDayAll(dateStart, dateEnd);
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHubNode(unitHub, unitNode, userUnit)

        // data by unit
        const initDeployedListByUnit = async function (item, date){
            let driverByDeployable = null
            let vehicleByDeployable = null
            let obj
            driverByDeployable = await TaskUtils.getDriverStatusOrDeployed('', date, '', '-1')
            vehicleByDeployable = await TaskUtils.getVehicleStatusOrDeployed('', date, '', req.cookies.userType)
            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            driverByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase() && (item.subUnit == obj.currentSubUnit)){
                        assignedDriverNumber = obj.statusSum
                    }
                }
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase() && (item.subUnit == obj.currentSubUnit)){
                        assignedVehicleNumber = obj.statusSum
                    }
                }
            })
            if(!modulePage.driverShow){
                assignedDriverNumber = 0;
            }
            if(!modulePage.vehicleShow){
                assignedVehicleNumber = 0;
            }
            obj = {
                weekDate: date,
                unit: item.unit,
                subunit: item.subUnit ? item.subUnit : 'Other',
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
            }
            return obj
        }

        // data by group 
        const initDeployedListByGroup = async function (date){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let driverByDeployable = null
            let vehicleByDeployable = null
            let obj
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId, date);
            vehicleData = vehicleData.map(item => item.vehicleNo)
            let driverData = await TaskUtils.getDriverByGroup(groupId, date)
            driverData = driverData.map(item => item.driverId)
            driverByDeployable = await TaskUtils.getDriverStatusOrDeployed(null, date, '', groupId ?? '0', driverData)
            vehicleByDeployable = await TaskUtils.getVehicleStatusOrDeployed('', date, '', req.cookies.userType, vehicleData)
            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            driverByDeployable.forEach((obj)=>{
                assignedDriverNumber = obj.statusSum
            })
    
            vehicleByDeployable.forEach((obj)=>{
                assignedVehicleNumber = obj.statusSum
            })
            if(!modulePage.driverShow){
                assignedDriverNumber = 0;
            }
            if(!modulePage.vehicleShow){
                assignedVehicleNumber = 0;
            }
            obj = {
                weekDate: date,
                unit: 'DV_LOA',
                subunit: 'DV_LOA',
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
            }
            return obj
        }
        let data = []

        for(let date of weekDate){
            for(let item of unitList){
                let obj
                if(item.unit){
                    obj = await initDeployedListByUnit(item, date);
                } else {
                    obj = await initDeployedListByGroup(date)
                }
                data.push(obj)
            }
        }
       
        return res.json(utils.response(1, data));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }  
}

module.exports.getDriverAndVehicleDeployedTotalByHub = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let unitHub = req.body.unit;
        let currentDate = req.body.currentDate;
        currentDate = currentDate.split(' ~ ')
        let dateStart = moment(currentDate[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let dateEnd = moment(currentDate[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let weekDate = await TaskUtils.getDayAll(dateStart, dateEnd);
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHub(unitHub, userUnit)
        
        // data by unit
        const initDataByUnit = async function (item, date, driverByDeployable, vehicleByDeployable){
            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            driverByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase()){
                        assignedDriverNumber = obj.statusSum
                    }
                }
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase()){
                        assignedVehicleNumber = obj.statusSum
                    }
                }
            })
            if(!modulePage.driverShow){
                assignedDriverNumber = 0;
            }
            if(!modulePage.vehicleShow){
                assignedVehicleNumber = 0;
            }
            let obj = {
                weekDate: date,
                unit: item.unit,
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
            }
            return obj
        }
        
        let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
        // data by group
        const initDataByGroup = async function (date, driverByDeployableByGroup, vehicleByDeployableByGroup){
            let assignedDriverNumber = 0
            let assignedVehicleNumber = 0
            driverByDeployableByGroup.forEach((obj)=>{
                assignedDriverNumber = obj.statusSum
            })
    
            vehicleByDeployableByGroup.forEach((obj)=>{
                assignedVehicleNumber = obj.statusSum
            })
            if(!modulePage.driverShow){
                assignedDriverNumber = 0;
            }
            if(!modulePage.vehicleShow){
                assignedVehicleNumber = 0;
            }
            let obj = {
                weekDate: date,
                unit: 'DV_LOA',
                subunit: 'DV_LOA',
                assignedDriverNumber: assignedDriverNumber,
                assignedVehicleNumber: assignedVehicleNumber,
            }
            return obj
        }
        let data = []
        for(let date of weekDate){
            // unit by data
            let driverByDeployable = await TaskUtils.getDriverStatusOrDeployed(null, date, '', '-1')
            let vehicleByDeployable = await TaskUtils.getVehicleStatusOrDeployed(null, date, '', req.cookies.userType)

            // group by data
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId, date);
            vehicleData = vehicleData.map(item => item.vehicleNo)
            let driverData = await TaskUtils.getDriverByGroup(groupId, date)
            driverData = driverData.map(item => item.driverId)
            let driverByDeployableByGroup = await TaskUtils.getDriverStatusOrDeployed(null, date, '', groupId ?? '0', driverData)
            let vehicleByDeployableByGroup = await TaskUtils.getVehicleStatusOrDeployed(null, date, '', req.cookies.userType, vehicleData)

            for(let item of unitList){
                let obj
                if(item.unit){
                    obj = await initDataByUnit(item, date, driverByDeployable, vehicleByDeployable)
                } else {
                    obj = await initDataByGroup(date, driverByDeployableByGroup, vehicleByDeployableByGroup)
                }
                data.push(obj)
            }
        }
       
        return res.json(utils.response(1, data));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }  
}

module.exports.getDriverAndVehicleAvailabilityByNode = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let unitHub = req.body.unit;
        let unitNode = req.body.subUnit;
        let currentDate = req.body.currentDate;
        currentDate = currentDate.split(' ~ ')
        let dateStart = moment(currentDate[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let dateEnd = moment(currentDate[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let weekDate = await TaskUtils.getDayAll(dateStart, dateEnd);
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHubNode(unitHub, unitNode, userUnit)

        let driverTotalByUnit = await sequelizeObj.query(`
        select COUNT(dd.driverId) as total, dd.unit, dd.subUnit from (
            select d.driverId, us.role,
            IF(hh.toHub is NULL, u.unit, hh.toHub) as unit, IF(hh.toHub is NULL, u.subUnit, hh.toNode) as subUnit,
            IF(hh.toHub is NULL, u.id, hh.unitId) as unitId
            from driver as d
            LEFT JOIN user us on us.driverId = d.driverId
            LEFT JOIN unit u on d.unitId = u.id
            LEFT JOIN (select ho.driverId,ho.toHub,ho.toNode, ho.unitId from hoto ho where 
                (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) hh ON hh.driverId = d.driverId
            where (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate is null) 
            and us.role != 'TL'
            group by d.driverId
        ) dd group by  dd.unit, dd.subUnit
        `, { type: QueryTypes.SELECT }) //and d.permitStatus != 'invalid'
        let vehicleTotalByUnit = await sequelizeObj.query(`
        select COUNT(vv.vehicleNo) as total, vv.unit, vv.subUnit from (
            select v.vehicleNo,
            IF(hh.toHub is NULL, u.unit, hh.toHub) as unit, IF(hh.toHub is NULL, u.subUnit, hh.toNode) as subUnit,
            IF(hh.toHub is NULL, v.unitId, hh.unitId) as unitId
            from vehicle as v
            LEFT JOIN unit u on v.unitId = u.id
            LEFT JOIN (select ho.vehicleNo,ho.toHub,ho.toNode, ho.unitId from hoto ho where 
                (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) hh ON hh.vehicleNo = v.vehicleNo
            group by v.vehicleNo
            ) vv group by vv.unit, vv.subUnit 
        `, { type: QueryTypes.SELECT }) // where v.onhold = 0

        // data by unit 
        const initDataByUnit = async function (item, date){
            let driverByDeployable = null
            let vehicleByDeployable = null
            let obj
            let driverTotalObj = driverTotalByUnit.filter(driverObj => driverObj.unit == item.unit && driverObj.subUnit == item.subUnit)
            let driverTotal = driverTotalObj.length > 0 ? driverTotalObj[0].total : 0;
            let vehicleTotalObj = vehicleTotalByUnit.filter(vehicleObj => vehicleObj.unit == item.unit && vehicleObj.subUnit == item.subUnit)
            let vehicleTotal = vehicleTotalObj.length > 0 ? vehicleTotalObj[0].total : 0;
            driverByDeployable = await TaskUtils.getDriverStatusOrDeployed('', date, null, '-1')
            vehicleByDeployable = await TaskUtils.getVehicleStatusOrDeployed('', date, null, req.cookies.userType)
            let todayVehicleDeployed = 0;
            let todayVehicleDeployable = 0;
            let todayVehicleOnleave = 0;
            let tsOperatorOwnedDeployed = 0;
            let tsOperatorOwnedDeployable = 0;
            let tsOperatorOnleave = 0;
            driverByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase() && (item.subUnit == obj.currentSubUnit)){
                        if (obj.currentStatus == 'Deployable') {
                            tsOperatorOwnedDeployable = obj.statusSum
                        } else if (obj.currentStatus == 'On Leave') {
                            tsOperatorOnleave = obj.statusSum
                        } else if(obj.currentStatus == 'Deployed'){
                            tsOperatorOwnedDeployed = obj.statusSum
                        }
                    }
                }
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase() && (item.subUnit == obj.currentSubUnit)){
                        if (obj.currentStatus == 'Deployable') {
                            todayVehicleDeployable = todayVehicleDeployable + obj.statusSum
                        } else if (obj.currentStatus == 'Out Of Service') {
                            todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                        } else if (obj.currentStatus == 'Under Maintenance') {
                            todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                        } else if(obj.currentStatus == 'Deployed'){
                            todayVehicleDeployed = todayVehicleDeployed + obj.statusSum
                        }
                    }
                }
            })
            if(!modulePage.driverShow){
                tsOperatorOwnedDeployed = 0;
                tsOperatorOwnedDeployable = 0;
                tsOperatorOnleave = 0;
                driverTotal = 0;
            }
            if(!modulePage.vehicleShow){
                todayVehicleDeployed = 0;
                todayVehicleDeployable = 0;
                todayVehicleOnleave = 0;
                vehicleTotal = 0;
            }
            
            obj = {
                weekDate: date,
                unit: item.unit,
                subunit: item.subUnit ? item.subUnit : 'Other',
                todayVehicleDeployed: todayVehicleDeployed,
                todayVehicleDeployable: todayVehicleDeployable,
                todayVehicleOnleave: todayVehicleOnleave,
                tsOperatorOwnedDeployed: tsOperatorOwnedDeployed,
                tsOperatorOwnedDeployable: tsOperatorOwnedDeployable,
                tsOperatorOnleave: tsOperatorOnleave,
                driverTotal: driverTotal,
                vehicleTotal: vehicleTotal
            }
            return obj
        }
        // data by group
        const initDataByGroup = async function (date){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let driverByDeployable = null
            let vehicleByDeployable = null
            let obj
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId, date);
            let vehicleData2 = await TaskUtils.getVehicleByGroup(groupId, date, '1');
            vehicleData = vehicleData.map(item => item.vehicleNo)
            let driverData = await TaskUtils.getDriverByGroup(groupId, date)
            let driverData2 = await TaskUtils.getDriverByGroup(groupId, date, '1')
            driverData = driverData.map(item => item.driverId)
            driverByDeployable = await TaskUtils.getDriverStatusOrDeployed(null, date, null, groupId ?? '0', driverData)
            vehicleByDeployable = await TaskUtils.getVehicleStatusOrDeployed('', date, null, req.cookies.userType, vehicleData)
            let todayVehicleDeployed = 0;
            let todayVehicleDeployable = 0;
            let todayVehicleOnleave = 0;
            let tsOperatorOwnedDeployed = 0;
            let tsOperatorOwnedDeployable = 0;
            let tsOperatorOnleave = 0;
            driverByDeployable.forEach((obj)=>{
                if (obj.currentStatus == 'Deployable') {
                    tsOperatorOwnedDeployable = obj.statusSum
                } else if (obj.currentStatus == 'On Leave') {
                    tsOperatorOnleave = obj.statusSum
                } else if(obj.currentStatus == 'Deployed'){
                    tsOperatorOwnedDeployed = obj.statusSum
                }
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if (obj.currentStatus == 'Deployable') {
                    todayVehicleDeployable = todayVehicleDeployable + obj.statusSum
                } else if (obj.currentStatus == 'Out Of Service') {
                    todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                } else if (obj.currentStatus == 'Under Maintenance') {
                    todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                } else if(obj.currentStatus == 'Deployed'){
                    todayVehicleDeployed = todayVehicleDeployed + obj.statusSum
                }
            })
            if(!modulePage.driverShow){
                tsOperatorOwnedDeployed = 0;
                tsOperatorOwnedDeployable = 0;
                tsOperatorOnleave = 0;
                driverData2 = []
            }
            if(!modulePage.vehicleShow){
                todayVehicleDeployed = 0;
                todayVehicleDeployable = 0;
                todayVehicleOnleave = 0;
                vehicleData2 = [];
            }

            obj = {
                weekDate: date,
                unit: 'DV_LOA',
                subunit: 'DV_LOA',
                todayVehicleDeployed: todayVehicleDeployed,
                todayVehicleDeployable: todayVehicleDeployable,
                todayVehicleOnleave: todayVehicleOnleave,
                tsOperatorOwnedDeployed: tsOperatorOwnedDeployed,
                tsOperatorOwnedDeployable: tsOperatorOwnedDeployable,
                tsOperatorOnleave: tsOperatorOnleave,
                driverTotal: driverData2.length,
                vehicleTotal: vehicleData2.length
            }
            return obj
        }
        let data = []
        for(let date of weekDate){
            for(let item of unitList){
                let obj
                if(item.unit){
                    obj = await initDataByUnit(item, date)
                } else {
                    obj = await initDataByGroup(date)
                }
                data.push(obj)
            }
        }
       
        return res.json(utils.response(1, data));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }  
}

module.exports.getDriverAndVehicleAvailabilityByHub = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let unitHub = req.body.unit;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let currentDate = req.body.currentDate;
        currentDate = currentDate.split(' ~ ')
        let dateStart = moment(currentDate[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let dateEnd = moment(currentDate[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let weekDate = await TaskUtils.getDayAll(dateStart, dateEnd);
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        const initHubListByHub = function (unitHub){
            let unitList = []
            if(unitHub){
                if((unitHub.toLowerCase() == 'dv_loa' || unitHub.toLowerCase() == 'other')) {
                    unitList = []
                    unitList.push({ id: 0, unit: null, subUnit: null }) 
                 } else {
                    unitList = [{ unit: unitHub }]
                 }
                
            } else {
                unitList = userUnit.subUnitList
            }
            return unitList
        }
        let unitList = initHubListByHub(unitHub)

        // data by unit
        const initDataByUnit = async function (item, date){
            let driverByDeployable = null
            let vehicleByDeployable = null
            let obj
            driverByDeployable = await TaskUtils.getDriverStatusOrDeployed(null, date, null, '-1')
            vehicleByDeployable = await TaskUtils.getVehicleStatusOrDeployed(null, date, null, req.cookies.userType)
            let todayVehicleDeployed = 0;
            let todayVehicleDeployable = 0;
            let todayVehicleOnleave = 0;
            let tsOperatorOwnedDeployed = 0;
            let tsOperatorOwnedDeployable = 0;
            let tsOperatorOnleave = 0;
            let tsOperatorInvalid = 0;
            driverByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase()){
                        if (obj.currentStatus == 'Deployable') {
                            tsOperatorOwnedDeployable = tsOperatorOwnedDeployable + obj.statusSum
                        } else if (obj.currentStatus == 'On Leave') {
                            tsOperatorOnleave = tsOperatorOnleave + obj.statusSum
                        } else if(obj.currentStatus == 'Deployed'){
                            tsOperatorOwnedDeployed = tsOperatorOwnedDeployed + obj.statusSum
                        } else if(obj.currentStatus == 'permitInvalid') {
                            tsOperatorInvalid = tsOperatorInvalid + obj.statusSum
                        }
                    }
                }
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if(obj.currentUnit){
                    if(item.unit.toUpperCase() == obj.currentUnit.toUpperCase()){
                        if (obj.currentStatus == 'Deployable') {
                            todayVehicleDeployable = todayVehicleDeployable + obj.statusSum
                        } else if (obj.currentStatus == 'Out Of Service') {
                            todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                        } else if (obj.currentStatus == 'Under Maintenance') {
                            todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                        } else if(obj.currentStatus == 'Deployed'){
                            todayVehicleDeployed = todayVehicleDeployed + obj.statusSum
                        }
                    }
                }
            })
            if(!modulePage.driverShow){
                tsOperatorOwnedDeployed = 0;
                tsOperatorOwnedDeployable = 0;
                tsOperatorOnleave = 0;
                tsOperatorInvalid = 0;
            }
            if(!modulePage.vehicleShow){
                todayVehicleDeployed = 0;
                todayVehicleDeployable = 0;
                todayVehicleOnleave = 0;
            }
            obj = {
                weekDate: date,
                unit: item.unit,
                todayVehicleDeployed: todayVehicleDeployed,
                todayVehicleDeployable: todayVehicleDeployable,
                todayVehicleOnleave: todayVehicleOnleave,
                tsOperatorOwnedDeployed: tsOperatorOwnedDeployed,
                tsOperatorOwnedDeployable: tsOperatorOwnedDeployable,
                tsOperatorOnleave: tsOperatorOnleave,
                driverTotal: (tsOperatorOwnedDeployed +tsOperatorOwnedDeployable + tsOperatorOnleave),
                vehicleTotal: (todayVehicleDeployed + todayVehicleDeployable + todayVehicleOnleave)
            }
            return obj
        }

        // data by group
        const initDataByGroup = async function (date){
            let driverByDeployable = null
            let vehicleByDeployable = null
            let obj 
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let vehicleData = await TaskUtils.getVehicleByGroup(groupId, date);
            let vehicleData2 = await TaskUtils.getVehicleByGroup(groupId, date, '1');
            vehicleData = vehicleData.map(item => item.vehicleNo)
            let driverData = await TaskUtils.getDriverByGroup(groupId, date)
            let driverData2 = await TaskUtils.getDriverByGroup(groupId, date, '1')
            driverData = driverData.map(item => item.driverId)
            driverByDeployable = await TaskUtils.getDriverStatusOrDeployed(null, date, null, groupId ?? '0', driverData)
            vehicleByDeployable = await TaskUtils.getVehicleStatusOrDeployed(null, date, null, req.cookies.userType, vehicleData)
            let todayVehicleDeployed = 0;
            let todayVehicleDeployable = 0;
            let todayVehicleOnleave = 0;
            let tsOperatorOwnedDeployed = 0;
            let tsOperatorOwnedDeployable = 0;
            let tsOperatorOnleave = 0;
            driverByDeployable.forEach((obj)=>{
                if (obj.currentStatus == 'Deployable') {
                    tsOperatorOwnedDeployable = tsOperatorOwnedDeployable + obj.statusSum
                } else if (obj.currentStatus == 'On Leave') {
                    tsOperatorOnleave = tsOperatorOnleave + obj.statusSum
                } else if(obj.currentStatus == 'Deployed'){
                    tsOperatorOwnedDeployed = tsOperatorOwnedDeployed + obj.statusSum
                } 
            })
    
            vehicleByDeployable.forEach((obj)=>{
                if (obj.currentStatus == 'Deployable') {
                    todayVehicleDeployable = todayVehicleDeployable + obj.statusSum
                } else if (obj.currentStatus == 'Out Of Service') {
                    todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                } else if (obj.currentStatus == 'Under Maintenance') {
                    todayVehicleOnleave = todayVehicleOnleave + obj.statusSum
                } else if(obj.currentStatus == 'Deployed'){
                    todayVehicleDeployed = todayVehicleDeployed + obj.statusSum
                }
            })
            if(!modulePage.driverShow){
                tsOperatorOwnedDeployed = 0;
                tsOperatorOwnedDeployable = 0;
                tsOperatorOnleave = 0;
                driverData2 = []
            }
            if(!modulePage.vehicleShow){
                todayVehicleDeployed = 0;
                todayVehicleDeployable = 0;
                todayVehicleOnleave = 0;
            }

            obj = {
                weekDate: date,
                unit: 'DV_LOA',
                subunit: 'DV_LOA',
                todayVehicleDeployed: todayVehicleDeployed,
                todayVehicleDeployable: todayVehicleDeployable,
                todayVehicleOnleave: todayVehicleOnleave,
                tsOperatorOwnedDeployed: tsOperatorOwnedDeployed,
                tsOperatorOwnedDeployable: tsOperatorOwnedDeployable,
                tsOperatorOnleave: tsOperatorOnleave,
                driverTotal: driverData2.length,
                vehicleTotal: vehicleData2.length
            }
            return obj
        }
        let data = []
        for(let date of weekDate){
            for(let item of unitList){
                let obj
                if(item.unit){
                    obj = await initDataByUnit(item, date)
                } else {
                    obj = await initDataByGroup(date)
                }
                data.push(obj)
            }
        }
       
        return res.json(utils.response(1, data));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }  
}

module.exports.getDriverTotalByRoleByHub = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let timeNeeded = req.body.timeNeeded ? req.body.timeNeeded : moment().format('YYYY-MM-DD')
        let unitHub = req.body.hub
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = TaskUtils.getUnitHubList(unitHub, userUnit)
        
        let result = []
        let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;

        let driverTotalByRole = await sequelizeObj.query(`
            select count(*) as taskNum, dd.role, dd.unit  from (
                select d.driverId,
                IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.unit, IF(hh.toHub IS NULL, hr.toHub, hh.toHub)) AS unit,
                us.role
                from driver as d
                LEFT JOIN user us on us.driverId = d.driverId
                LEFT JOIN unit u on d.unitId = u.id
                LEFT JOIN (select ho.driverId,ho.toHub, ho.unitId from hoto ho where 
                    (? BETWEEN DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(ho.endDateTime, '%Y-%m-%d'))
                    and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                LEFT JOIN (select hr.driverId, hr.toHub, hr.toNode FROM hoto_record hr 
                    WHERE (? BETWEEN DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(hr.returnDateTime , '%Y-%m-%d'))
                    and hr.status = 'Approved'
                ) hr ON hr.driverId = d.driverId
                where (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null) 
                GROUP BY d.driverId
            ) dd GROUP BY dd.unit, dd.role
        `, { type: QueryTypes.SELECT, replacements: [timeNeeded, timeNeeded, timeNeeded] }) // and d.permitStatus != 'invalid'
        
        const initDriverRoleListByGroup = async function (){
            let sql = `
                select count(*) as taskNum, dd.role, dd.groupId from (
                    select d.driverId, us.role, 
                    if(l.groupId is null, if(lr.groupId is null, d.groupId, lr.groupId), l.groupId) groupId
                    from driver as d
                    LEFT JOIN user us on us.driverId = d.driverId
                    LEFT JOIN (SELECT lo.driverId, lo.groupId FROM loan lo WHERE 
                        ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                    ) l ON l.driverId = d.driverId
                    LEFT JOIN (SELECT lr.driverId, lr.groupId FROM loan_record lr WHERE 
                        ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                    ) lr ON lr.driverId = d.driverId
                    WHERE (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)		
                    GROUP BY d.driverId
                ) dd where 1=1
            `
            let replacements = [moment(timeNeeded).format('YYYY-MM-DD'), moment(timeNeeded).format('YYYY-MM-DD'), moment(timeNeeded).format('YYYY-MM-DD')]
            if(groupId){
                if(groupId.length > 0){
                    sql += ` and dd.groupId in (?)`
                    replacements.push(groupId)
                } else {
                    sql += ` and dd.groupId = ?`
                    replacements.push(groupId)
                }
            } else {
                sql += ` and dd.groupId is not null`
            }
            sql += ` GROUP BY dd.role`
            let driverTotalByRoleByGroup = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
            return driverTotalByRoleByGroup
        }

        const initDriverTotalByRoleArray = async function (item){
            let driverTotalByRoleArray = []
            if(item){
                driverTotalByRoleArray = driverTotalByRole.filter(task => task.unit == item && task.role);
            } else {
                let driverTotalByRoleByGroup = await initDriverRoleListByGroup();
                log.info(`GROUP ${ groupId } driverRoleList ==> ${ JSON.stringify(driverTotalByRoleByGroup) }`)
                driverTotalByRoleArray = driverTotalByRoleByGroup.filter(task => task && task.role);
            }
            return driverTotalByRoleArray
        }

        for(let item of unitList){
            let hubData = { unit: item || 'Other',  driverRoleData: [] }
            let driverTotalByRoleArray = await initDriverTotalByRoleArray(item)

            let toStartedCount = 0;
            let tlStartedCount = 0;
            let dvStartedCount = 0;
            let loaStartedCount = 0;
            if(modulePage.driverShow && driverTotalByRoleArray.length > 0){
                for (let temp of driverTotalByRoleArray) {
                    switch (temp.role.toLowerCase()){
                        case 'to': 
                            toStartedCount += temp.taskNum
                            break;
                        case 'tl':
                            tlStartedCount += temp.taskNum
                            break;
                        case 'dv':
                            dvStartedCount += temp.taskNum
                            break;
                        case 'loa':
                            loaStartedCount += temp.taskNum
                            break;
                    }
                }
            }
            hubData.driverRoleData.push({role: 'TO', roleTotal: toStartedCount})
            hubData.driverRoleData.push({role: 'TL', roleTotal: tlStartedCount})
            hubData.driverRoleData.push({role: 'DV', roleTotal: dvStartedCount})
            hubData.driverRoleData.push({role: 'LOA', roleTotal: loaStartedCount})
            result.push(hubData);
        }

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDriverTotalByRoleByNode = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        let timeNeeded = req.body.timeNeeded ? req.body.timeNeeded : moment().format('YYYY-MM-DD')
        let unitHub = req.body.hub;
        let unitNode = req.body.node;
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHubNode(unitHub, unitNode, userUnit)

        let driverTotalByRole = await sequelizeObj.query(`
            select count(*) as taskNum, dd.role, dd.unit, dd.subUnit  from (
                select d.driverId,
                IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.unit, IF(hh.toHub IS NULL, hr.toHub, hh.toHub)) AS unit, 
                IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.subUnit, IF(hh.toHub IS NULL, hr.toNode, hh.toNode)) AS subUnit,
                us.role
                from driver as d
                LEFT JOIN user us on us.driverId = d.driverId
                LEFT JOIN unit u on d.unitId = u.id
                LEFT JOIN (select ho.driverId,ho.toHub, ho.toNode, ho.unitId from hoto ho where 
                    (? BETWEEN DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(ho.endDateTime, '%Y-%m-%d'))
                    and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
                LEFT JOIN (select hr.driverId, hr.toHub, hr.toNode FROM hoto_record hr 
                    WHERE (? BETWEEN DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(hr.returnDateTime , '%Y-%m-%d'))
                    and hr.status = 'Approved'
                ) hr ON hr.driverId = d.driverId
                where (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null) 
                GROUP BY d.driverId
            ) dd GROUP BY dd.unit, dd.subUnit, dd.role
        `, { type: QueryTypes.SELECT, replacements: [timeNeeded, timeNeeded, timeNeeded] }) // and d.permitStatus != 'invalid'

        const initDriverRoleByGroup = async function (){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let sql = `
                select count(*) as taskNum, dd.role, dd.groupId from (
                    select d.driverId, us.role, 
                    if(l.groupId is null, if(lr.groupId is null, d.groupId, lr.groupId), l.groupId) groupId
                    from driver as d
                    LEFT JOIN user us on us.driverId = d.driverId
                    LEFT JOIN (SELECT lo.driverId, lo.groupId FROM loan lo WHERE 
                        ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND DATE_FORMAT(lo.endDate, '%Y-%m-%d')
                    ) l ON l.driverId = d.driverId
                    LEFT JOIN (SELECT lr.driverId, lr.groupId FROM loan_record lr WHERE 
                        ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                    ) lr ON lr.driverId = d.driverId
                    WHERE (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)		
                    GROUP BY d.driverId
                ) dd where 1=1
            `
            let replacements = [moment(timeNeeded).format('YYYY-MM-DD'), moment(timeNeeded).format('YYYY-MM-DD'), moment(timeNeeded).format('YYYY-MM-DD')]
            if(groupId){
                if(groupId.length > 0){
                    sql += ` and dd.groupId in (?)`
                    replacements.push(groupId)
                } else {
                    sql += ` and dd.groupId = ?`
                    replacements.push(groupId)
                }
            } else {
                sql += ` and dd.groupId is not null`
            }
            sql += ` GROUP BY dd.role`
            let driverTotalByRoleByGroup = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements }) // and d.permitStatus != 'invalid'
            return driverTotalByRoleByGroup;
        }

        const initDriverTotalByRoleArray = async function (item){
            let driverTotalByRoleArray = []
            if(item.unit){
                driverTotalByRoleArray = driverTotalByRole.filter(task => (task.unit == item.unit && task.subUnit == item.subUnit) && task.role);
            } else {
                let driverTotalByRoleByGroup = await initDriverRoleByGroup()
                log.info(`GROUP ${ groupId } driverRoleList ==> ${ JSON.stringify(driverTotalByRoleByGroup) }`)
                driverTotalByRoleArray = driverTotalByRoleByGroup.filter(task => task && task.role);
            } 
            return driverTotalByRoleArray
        }
        let result = []
        for(let item of unitList){
            let hubData = { unit: item.unit ?? 'Other', subunit: item.subUnit ?? 'Other', driverRoleData: [] }
            let driverTotalByRoleArray = await initDriverTotalByRoleArray(item)

            let toStartedCount = 0;
            let tlStartedCount = 0;
            let dvStartedCount = 0;
            let loaStartedCount = 0;
            if(modulePage.driverShow && driverTotalByRoleArray.length > 0){
                for (let temp of driverTotalByRoleArray) {
                    switch (temp.role.toLowerCase()){
                        case 'to':
                            toStartedCount += temp.taskNum
                            break;
                        case 'tl': 
                            tlStartedCount += temp.taskNum
                            break;
                        case 'dv':
                            dvStartedCount += temp.taskNum
                            break;
                        case 'loa':
                            loaStartedCount += temp.taskNum
                            break;
                    }
                }
            }
        
            hubData.driverRoleData.push({role: 'TO', roleTotal: toStartedCount})
            hubData.driverRoleData.push({role: 'TL', roleTotal: tlStartedCount})
            hubData.driverRoleData.push({role: 'DV', roleTotal: dvStartedCount})
            hubData.driverRoleData.push({role: 'LOA', roleTotal: loaStartedCount})
            result.push(hubData);
        }

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-06-15 Customer user,Add two purpose(WPT and MPT)
module.exports.getTaskTotalByPurposeByHub = async function (req, res) {
    try {
        let timeNeeded = req.body.timeNeeded ? req.body.timeNeeded : moment().format('YYYY-MM-DD')
        let unitHub = req.body.hub
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = TaskUtils.getUnitHubList(unitHub, userUnit)
        let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
        
        let taskTotalByPurpose = await sequelizeObj.query(`
        SELECT count( DISTINCT tt.taskId) as taskNum, tt.hub, tt.purpose
        FROM task tt
        LEFT JOIN user us ON us.driverId = tt.driverId
        WHERE tt.vehicleStatus not in ('Cancelled')  
        and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
        OR tt.vehicleStatus = 'started')
        and us.role != 'TL'
        GROUP BY tt.hub, tt.purpose
        `, { type: QueryTypes.SELECT, replacements: [timeNeeded] })

        const initTaskTotalByPurposeByGroup = async function (){
            let driverByGroup = await TaskUtils.getDriverByGroup(groupId, timeNeeded)
            driverByGroup = driverByGroup.map(item => item.driverId)
            let sql = `
            SELECT count( DISTINCT tt.taskId) as taskNum, us.unitId, tt.purpose
            FROM task tt
            LEFT JOIN user us ON us.driverId = tt.driverId
            WHERE tt.vehicleStatus not in ('Cancelled')  
            and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
            OR tt.vehicleStatus = 'started')
            `
            let replacements = [timeNeeded]
            if(driverByGroup){
                if(driverByGroup.length > 0){
                    sql += ` and tt.driverId in (?)`
                    replacements.push(driverByGroup)
                } else {
                    sql += ' and 1=2'
                }
            }
            sql += ` GROUP BY tt.purpose`
            let taskTotalByPurpose2 = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
            return taskTotalByPurpose2
        }

        const initTaskTotalByPurposeArray = async function (item){
            let taskTotalByPurposeArray = []
            if(item) {
                taskTotalByPurposeArray = taskTotalByPurpose.filter(task => task.hub == item && task.purpose);
            } else {
                let taskTotalByPurpose2 = await initTaskTotalByPurposeByGroup();
                taskTotalByPurposeArray = taskTotalByPurpose2.filter(task => task && task.purpose);
            }
            return taskTotalByPurposeArray
        }
        let result = []
        for(let item of unitList){
            let hubData = { unit: item || 'Other', taskPurposeData: [] }
            let taskTotalByPurposeArray = await initTaskTotalByPurposeArray(item)
            
            let taskPurposeData = [
                {purpose: 'ops', startedTaskCount: 0},
                {purpose: 'training', startedTaskCount: 0},
                {purpose: 'admin', startedTaskCount: 0},
                {purpose: 'exercise', startedTaskCount: 0},
                {purpose: 'duty', startedTaskCount: 0},
                {purpose: 'driving training', startedTaskCount: 0},
                {purpose: 'maintenance', startedTaskCount: 0},
                {purpose: 'others', startedTaskCount: 0},
                {purpose: 'familiarisation', startedTaskCount: 0},
                {purpose: 'wpt', startedTaskCount: 0},
                {purpose: 'mpt', startedTaskCount: 0},
                {purpose: 'avi', startedTaskCount: 0},
                {purpose: 'pm', startedTaskCount: 0}
            ]
            let newDataList = null
            if(taskTotalByPurposeArray.length > 0){
                newDataList = taskTotalByPurposeArray.reduce((acc, temp) => {   
                    for(let pur of taskPurposeData){
                        if (temp.purpose.toLowerCase().startsWith(pur.purpose)) {  
                            pur.startedTaskCount += temp.taskNum
                            break; 
                        }  
                    }  
                    return acc;  
                }, taskPurposeData);
            }
            hubData.taskPurposeData = newDataList ?? taskPurposeData;

            result.push(hubData);
        }

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-06-15 Customer user,Add two purpose(WPT and MPT)
module.exports.getTaskTotalByPurposeByNode = async function (req, res) {
    try {
        let timeNeeded = req.body.timeNeeded ? req.body.timeNeeded : moment().format('YYYY-MM-DD')
        let unitHub = req.body.hub;
        let unitNode = req.body.node;
        let userUnit = await TaskUtils.getUnitAndUnitIdByUserId(req.body.userId);
        let unitList = await TaskUtils.initUnitListByUnitHubNode(unitHub, unitNode, userUnit)
        
        let result = []
        let taskTotalByPurpose = await sequelizeObj.query(`
        SELECT count(DISTINCT tt.taskId) as taskNum, tt.hub, tt.node, tt.purpose
        FROM task tt
        LEFT JOIN user us ON us.driverId = tt.driverId
        WHERE tt.vehicleStatus not in ('Cancelled')  
        and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
        OR tt.vehicleStatus = 'started')
        and us.role != 'TL'
        GROUP BY tt.hub, tt.node, tt.purpose
        `, { type: QueryTypes.SELECT, replacements: [timeNeeded] })

        const initTaskTotalByPurpose = async function (){
            let groupId = userUnit.groupIdList?.length > 0 ? userUnit.groupIdList : null;
            let driverByGroup = await TaskUtils.getDriverByGroup(groupId, timeNeeded)
            driverByGroup = driverByGroup.map(item => item.driverId)
            let sql = `
            SELECT count(DISTINCT tt.taskId) as taskNum, us.unitId, tt.purpose
            FROM task tt
            LEFT JOIN user us ON us.driverId = tt.driverId
            WHERE tt.vehicleStatus not in ('Cancelled')  
            and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
            OR tt.vehicleStatus = 'started')
            `
            let replacements = [timeNeeded]
            if(driverByGroup){
                if(driverByGroup.length > 0){
                    sql += ` and tt.driverId in (?)`
                    replacements.push(driverByGroup)
                } else {
                    sql += ' and 1=2'
                }
            }
            sql +=  ` GROUP BY tt.purpose`
            let taskTotalByPurpose2 = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
            return taskTotalByPurpose2
        }

        const initTaskTotalByPurposeArray = async function (item){
            let taskTotalByPurposeArray
            if(item.unit){
                taskTotalByPurposeArray = taskTotalByPurpose.filter(task => (task.hub == item.unit && task.node == item.subUnit) && task.purpose);
            } else {
                let taskTotalByPurpose2 = await initTaskTotalByPurpose()
                taskTotalByPurposeArray = taskTotalByPurpose2.filter(task => task && task.purpose);
            }
            return taskTotalByPurposeArray
        }
        for(let item of unitList){
            let hubData = { unit: item.unit || 'Other', subunit: item.subUnit || 'Other', taskPurposeData: [] }        
            let taskTotalByPurposeArray = await initTaskTotalByPurposeArray(item)

            let taskPurposeData = [
                {purpose: 'ops', startedTaskCount: 0},
                {purpose: 'training', startedTaskCount: 0},
                {purpose: 'admin', startedTaskCount: 0},
                {purpose: 'exercise', startedTaskCount: 0},
                {purpose: 'duty', startedTaskCount: 0},
                {purpose: 'driving training', startedTaskCount: 0},
                {purpose: 'maintenance', startedTaskCount: 0},
                {purpose: 'others', startedTaskCount: 0},
                {purpose: 'familiarisation', startedTaskCount: 0},
                {purpose: 'wpt', startedTaskCount: 0},
                {purpose: 'mpt', startedTaskCount: 0},
                {purpose: 'avi', startedTaskCount: 0},
                {purpose: 'pm', startedTaskCount: 0}
            ]
            let newDataList = null
            if(taskTotalByPurposeArray.length > 0){
                newDataList = taskTotalByPurposeArray.reduce((acc, temp) => {   
                    for(let pur of taskPurposeData){
                        if (temp.purpose.toLowerCase().startsWith(pur.purpose)) {  
                            pur.startedTaskCount += temp.taskNum
                            break; 
                        }  
                    }  
                    return acc;  
                }, taskPurposeData);
            }
            hubData.taskPurposeData = newDataList ?? taskPurposeData;
            result.push(hubData);
        }

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-07-05 hub loanOut vehicle
module.exports.getVehicleTotalByLoanOut = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        if(!modulePage.vehicleShow){
            return res.json(utils.response(1, []));
        }
        let hub = req.body.hub;
        let node = req.body.node;
        let sql = `
        select COUNT(vv.vehicleNo) total, vv.unit, vv.subUnit from (
            SELECT v.vehicleNo,
            IF(hh.toHub is NULL, u.unit, hh.toHub) as unit, 
            IF(hh.toHub is NULL, u.subUnit, hh.toNode) as subUnit
            FROM loan l
            left join vehicle v ON v.vehicleNo = l.vehicleNo
            LEFT JOIN unit u ON u.id = v.unitId
            LEFT JOIN (SELECT ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId FROM hoto ho 
                WHERE (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.vehicleNo = v.vehicleNo
            WHERE now() BETWEEN l.startDate AND l.endDate
            GROUP BY v.vehicleNo
        ) vv where 1=1
        `//v.onhold = 0
        let replacements = []
        if(hub) {
            sql += ` AND vv.unit = ?`
            replacements.push(hub)
        }
        if(node) {
            if(node.toLowerCase() == 'other') {
                sql += ` AND vv.subUnit  is null`
            } else {
                sql += ` AND vv.subUnit  = ?`
                replacements.push(node)
            }
        }
        let loanOutTotal = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        return res.json(utils.response(1, loanOutTotal));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-07-14 hub loanOut driver
module.exports.getDriverTotalByLoanOut = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        if(!modulePage.driverShow){
            return res.json(utils.response(1, []));
        }
        let hub = req.body.hub;
        let node = req.body.node;
        let sql = `
        SELECT COUNT(dd.driverId) total, dd.unit, dd.subUnit FROM (
            SELECT d.driverId, 
            IF(hh.toHub is NULL, u.unit, hh.toHub) as unit, 
            IF(hh.toHub is NULL, u.subUnit, hh.toNode) as subUnit
            FROM loan l
            LEFT JOIN driver d ON d.driverId = l.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN (SELECT ho.driverId, ho.toHub, ho.toNode, ho.unitId FROM hoto ho 
                WHERE (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
                ) hh ON hh.driverId = d.driverId
            WHERE l.driverId IS NOT NULL
            and now() BETWEEN l.startDate AND l.endDate and d.permitStatus != 'invalid' 
            AND (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate IS NULL)
            GROUP BY d.driverId
        ) dd WHERE 1=1
        `
        let replacements = []
        if(hub) {
            sql += ` AND dd.unit = ?`
            replacements.push(hub)
        }
        if(node) {
            sql += ` AND dd.subUnit = ?`
            replacements.push(node)
        }
        let loanOutTotal = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        return res.json(utils.response(1, loanOutTotal));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-07-20 hub Status Invalid driver
module.exports.getDriverTotalByStatusInvalid = async function (req, res) {
    try {
        let pageType = req.body.pageType;
        let modulePage = await TaskUtils.getModulePageByUserId(req.cookies.userId, pageType);
        if(!modulePage.driverShow){
            return res.json(utils.response(1, []));
        }
        let hub = req.body.hub;
        let node = req.body.node;
        let sql = `
        SELECT COUNT(dd.driverId) total, dd.unit, dd.subUnit, dd.groupId, dd.taskId, dd.role, dd.unitId FROM (
            SELECT d.driverId,
            IF(h.toHub IS NULL, u.unit, h.toHub) AS unit, 
            IF(h.toHub IS NULL, u.subUnit, h.toNode) AS subUnit, l.groupId, l.taskId, us.role, us.unitId
            FROM  driver d
            LEFT JOIN (
                SELECT ho.driverId, ho.toHub, ho.toNode FROM hoto ho WHERE 
                (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) h ON h.driverId = d.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN user us ON us.driverId = d.driverId
            LEFT JOIN (SELECT lo.driverId, lo.groupId, lo.taskId FROM loan lo WHERE NOW() BETWEEN lo.startDate AND lo.endDate) l ON l.driverId = d.driverId
            WHERE d.permitStatus = 'invalid' 
            AND (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate IS NULL)
            GROUP BY d.driverId
        ) dd WHERE 1=1
        `
        let replacements = []
        if(hub) {
            if(hub.toLowerCase() == 'dv_loa' || (node ? node.toLowerCase() == 'dv_loa' : false)) {
                const initGroupSql = async function (){
                    let groupId = null
                    if(req.cookies.userType == 'CUSTOMER') {
                        let user = await User.findOne({ where: { userId: req.cookies.userId } })
                        groupId = user.unitId
                    }
                   if(groupId) {
                    sql += ` and ((dd.role in('DV', 'LOA') and dd.unitId = ?) or dd.groupId = ?) `
                    replacements.push(groupId)
                    replacements.push(groupId)
                   } else {
                    sql += ` and ((dd.role in('DV', 'LOA') and dd.unitId is not null) or dd.taskId is not null) `
                   }
                }
                await initGroupSql()
            } else {
                const initUnitSql = function (){
                    sql += ` AND dd.unit = ?`
                    replacements.push(hub)
                    if(node) {
                        if(node.toLowerCase() == 'other') {
                            sql += ` AND dd.subUnit is null`
                        } else {
                            sql += ` AND dd.subUnit = ?`
                            replacements.push(node)
                        }
                    }
                }
                initUnitSql()
            }
        }
        let invalidTotal = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        return res.json(utils.response(1, invalidTotal));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}