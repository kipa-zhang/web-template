const log = require('../log/winston').logger('HQ Dashboard Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')
const unitService = require('../services/unitService');
const mtAdminService = require('../services/mtAdminService');
const { Unit } = require('../model/unit');
const { Vehicle } = require('../model/vehicle');
const { User } = require('../model/user');

const Task_Completed_Status = ['completed', 'no show', 'late trip']

// const PurposeTypes = ['Ops', 'Exercise', 'Admin', 'Training']
const PurposeTypes = ['Ops', 'Training', 'Admin', 'Exercise', 'Duty', 'Driving Training', 'Maintenance', 'Others', 'Familiarisation', 'WPT', 'MPT', 'AVI', 'PM']

let TaskUtils = {
    getUnitAndUnitId: async function (userId) {
        let  { unitList, subUnitList, unitIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
        return { unitList, subUnitList, unitIdList };
    },
    getUnitIdListByHubNode: async function (hub, node) {
        return await unitService.UnitUtils.getUnitIdByUnitAndSubUnit(hub, node);
    },
    getVehicleByGroup: async function(groupId, vehicleType, startDate, endDate) {
        let vehicleList = await mtAdminService.TaskUtils.getVehicleByGroup(null, false, groupId, vehicleType, startDate, endDate)
        vehicleList = vehicleList.map(item => item.vehicleNo)
        return vehicleList
    },
    getDriverByGroup: async function(unitId, vehicleType, dateData) {
        let driverList = await mtAdminService.TaskUtils.getDriverByGroup(false, unitId, vehicleType, null, null, dateData)
        driverList = driverList.map(item => item.driverId)
        return driverList
    }
}

module.exports.GetAllUnits = async function (req, res) {
    try {
        let userId = req.body.userId;
        let unitListObj = await TaskUtils.getUnitAndUnitId(userId)
        let unitList = unitListObj.unitList;
        return res.json(utils.response(1, unitList));    
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.GetMilitaryVehicleAndIndents = async function (req, res) {
    try {
        let { hub } = req.body
        //2023-07-07 
        let userType = req.cookies.userType;
        let userId = req.cookies.userId;
        let unitIds = []
        
        if(hub){
            let rows = await Unit.findAll({ where: { unit: hub } })
            unitIds = rows.map(item => item.id)
        } else {
            let userUnit = await unitService.UnitUtils.getPermitUnitList(userId);
            unitIds = userUnit.unitIdList
        }

        let militaryVehicleDatas = await GetMilitaryIndentReadiness(unitIds, req.cookies.userId, req.cookies.userType)
        let todayMilitaryTaskDatas = await GetTodayMilitaryTasks(unitIds, userId, userType)
        let indentCountDatas = await GetIndentsCount(unitIds, userId, userType)
        
        let result = {
            militaryVehicles: militaryVehicleDatas,
            todayMilitaryTasks: todayMilitaryTaskDatas,
            indentCount: indentCountDatas,
        }
        return res.json(utils.response(1, result))    
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-07-11  (vehicle Demand and Deployable number)
const GetMilitaryIndentReadiness = async function (unitIds, userId, userType) {
    let vehicleTotal = null
    let vehicleData = null;
    if(userType.toUpperCase() == 'CUSTOMER') {
        let user = await User.findOne({ where: { userId: userId } })
        vehicleData = await TaskUtils.getVehicleByGroup(user.unitId);
        vehicleTotal = vehicleData.length;
        if(vehicleData.length < 1) {
            return {
                total: 0,
                available: 0
            }
        }
    } else {
        let sql = `
        SELECT COUNT(dd.vehicleNo) AS total, dd.unitId FROM (
            SELECT v.vehicleNo,
            IF(hh.toHub IS NULL, u.unit, hh.toHub) AS unit,
            IF(hh.toHub IS NULL, u.id, hh.unitId) AS unitId
            FROM vehicle AS v
            LEFT JOIN unit u ON v.unitId = u.id
            LEFT JOIN (SELECT ho.vehicleNo,ho.toHub, ho.unitId FROM hoto ho WHERE 
                (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
            ) hh ON hh.vehicleNo = v.vehicleNo
        ) dd`
        let replacementsByVehicleTotal = []
        if(unitIds.length > 0){
            sql += ` where dd.unitId in (?)`
            replacementsByVehicleTotal.push(`'${ unitIds.join("','") }'`)
        }
        vehicleTotal = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacementsByVehicleTotal }) 
        vehicleTotal = vehicleTotal[0].total
    }

    let taskVehicle = await sequelizeObj.query(`
    SELECT tt.vehicleNumber FROM task tt 
    WHERE tt.vehicleStatus not in ('Cancelled', 'completed')  
    and ((NOW() BETWEEN tt.indentStartTime and tt.indentEndTime)
    OR tt.vehicleStatus = 'started')
    ${ userType.toUpperCase() == 'CUSTOMER' ? ` and tt.taskId like 'CU-%'` : ` and tt.dataFrom = 'SYSTEM' and tt.driverId is not null` }
    group by tt.vehicleNumber
    `, { type: QueryTypes.SELECT })
    taskVehicle = taskVehicle.map(item => item.vehicleNumber)

    let loanOutVehicle = await sequelizeObj.query(` 
        SELECT l.vehicleNo FROM loan l
        LEFT JOIN vehicle v ON v.vehicleNo = l.vehicleNo
        WHERE NOW() BETWEEN l.startDate AND l.endDate
        AND l.vehicleNo IS NOT NULL
        GROUP BY l.vehicleNo
    `, { type: QueryTypes.SELECT })
    loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)

    let vehicleDeployableTotalSql = ` 
        select vv.currentStatus, count(*) as total, vv.currentUnitId from (
            SELECT veh.vehicleNo,
            IF(hh.unitId is NULL, un.id, hh.unitId) as currentUnitId, 
    `
    let replacementsByVehicleDeployableTotal = []
    if(userType.toUpperCase() == 'CUSTOMER'){
        vehicleDeployableTotalSql += `
            IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
            ) as currentStatus
        `
        replacementsByVehicleDeployableTotal.push(taskVehicle.join(","))
    } else {
        vehicleDeployableTotalSql += `
            IF(FIND_IN_SET(veh.vehicleNo, ?), 'LOAN OUT', 
                IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                    IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                )
            ) as currentStatus
        `
        replacementsByVehicleDeployableTotal.push(loanOutVehicle.join(","))
        replacementsByVehicleDeployableTotal.push(taskVehicle.join(","))
    }
    vehicleDeployableTotalSql += `
        FROM vehicle veh
        left join unit un on un.id = veh.unitId
        left join (
            select ho.vehicleNo, ho.unitId from hoto ho where 
            (NOW() BETWEEN ho.startDateTime AND ho.endDateTime) and ho.status = 'Approved'
        ) hh ON hh.vehicleNo = veh.vehicleNo
        left join (
            select vl.vehicleNo, vl.reason from vehicle_leave_record vl 
            where vl.status = 1 and (NOW() BETWEEN vl.startTime AND vl.endTime)
        ) ll ON ll.vehicleNo = veh.vehicleNo
        where 1=1
    `
    if(vehicleData?.length > 0){
        vehicleDeployableTotalSql += `
            and veh.vehicleNo in (?)
        `
        replacementsByVehicleDeployableTotal.push(`'${ vehicleData.join("','") }'`)
    }
    vehicleDeployableTotalSql += `
            GROUP BY veh.vehicleNo
        ) vv where vv.currentStatus = 'Deployable' 
    `
    if(unitIds.length > 0){
        vehicleDeployableTotalSql += `
            and vv.currentUnitId in (?)
        `
        replacementsByVehicleDeployableTotal.push(`'${ unitIds.join("','") }'`)
    }
    vehicleDeployableTotalSql += `  group by vv.currentStatus`
    let vehicleDeployableTotal = await sequelizeObj.query(vehicleDeployableTotalSql, { type: QueryTypes.SELECT, replacements: replacementsByVehicleDeployableTotal });

    return {
        total: vehicleTotal,
        available: vehicleDeployableTotal[0] ? vehicleDeployableTotal[0].total : 0
    }
}

// 2023-07-07 ( system task status total )
const GetTodayMilitaryTasks = async function (unitIds, userId, userType) {
    let user = await User.findOne({ where: { userId } })
    let today = moment().format("YYYY-MM-DD")
    let filter = ""
    let replacements = [today]
    if (userType.toUpperCase() == 'CUSTOMER') {
        filter += ` and r.groupId = ?`
        replacements.push(user.unitId)
    } else if (unitIds.length > 0) {
        filter += ` and a.mobiusUnit in (?)`
        replacements.push(unitIds)
    }
    let sql = `SELECT
                    a.taskStatus,
                    a.mobileStartTime 
                FROM
                    job_task a
                    LEFT JOIN job b ON a.tripId = b.id
                    LEFT JOIN service_type c ON b.serviceTypeId = c.id 
                    LEFT JOIN request r ON r.id = a.requestId
                WHERE
                    LOWER( c.category ) = 'mv' 
                    AND b.driver = 1 AND b.vehicleType != '-'
                    AND a.taskStatus != 'cancelled' 
                    AND a.executionDate = ?
                    ${filter}`;
    let rows = await sequelizeSystemObj.query(sql, {
        replacements: replacements,
        type: QueryTypes.SELECT
    })

    let total = rows.length
    let result = {
        pending: 0,
        ongoing: 0,
        completed: 0,
        total: total,
    }
    for (let row of rows) {
        let taskStatus = row.taskStatus
        let mobileStartTime = row.mobileStartTime
        if (taskStatus && Task_Completed_Status.indexOf(taskStatus.toLowerCase()) != -1) {
            result.completed = result.completed + 1
        } else if (Task_Completed_Status.indexOf(taskStatus.toLowerCase()) == -1 && mobileStartTime) {
            result.ongoing = result.ongoing + 1
        } else {
            result.pending = result.pending + 1
        }
    }

    let rows1 = []
    if (userType.toUpperCase() == 'CUSTOMER') {
        let sql1 = `SELECT
                    driverStatus
                FROM
                    task
                WHERE
                    taskId LIKE 'CU-%'
                AND driverStatus != 'Cancelled'
                AND driverId IN (
                    SELECT
                        driverId
                    FROM
                        USER
                    WHERE
                        unitId = ?
                    AND driverId IS NOT NULL
                ) AND DATE_FORMAT(indentStartTime, '%Y-%m-%d') = ?`;
        rows1 = await sequelizeObj.query(sql1, {
            replacements: [user.unitId, today],
            type: QueryTypes.SELECT
        })
    } else if (userType.toUpperCase() == 'HQ' || userType.toUpperCase() == 'ADMINISTRATOR') {
        if (unitIds.length > 0) {
            let sql1 = `SELECT
                        driverStatus
                    FROM
                        task
                    WHERE
                        taskId LIKE 'CU-%'
                    AND driverStatus != 'Cancelled'
                    AND driverId IN (
                        SELECT
                            driverId
                        FROM
                            USER
                        WHERE
                            unitId in (?)
                        AND driverId IS NOT NULL
                    )
                    AND DATE_FORMAT(indentStartTime, '%Y-%m-%d') = ?`;

            rows1 = await sequelizeObj.query(sql1, {
                replacements: [unitIds, today],
                type: QueryTypes.SELECT
            })
        } else {
            let sql1 = `SELECT
                        driverStatus
                    FROM
                        task
                    WHERE
                        taskId LIKE 'CU-%'
                    AND driverStatus != 'Cancelled'
                    AND DATE_FORMAT(indentStartTime, '%Y-%m-%d') = ?`;

            rows1 = await sequelizeObj.query(sql1, {
                replacements: [today],
                type: QueryTypes.SELECT
            })
        }
    }

    const initStatusNumber = function (){
        for (let row of rows1) {
            let taskStatus = row.driverStatus.toLowerCase()
            if (taskStatus == 'completed') {
                result.completed = result.completed + 1
            } else if (taskStatus == 'started') {
                result.ongoing = result.ongoing + 1
            } else if (taskStatus == 'waitcheck') {
                result.pending = result.pending + 1
            }
        }
    }
    initStatusNumber()
    result.total = result.total + rows1.length
    return result
}

// 2023-07-07 ( system Indents total )
const GetIndentsCount = async function (unitIds, userId, userType) {
    let user =  await User.findOne({ where: { userId } })
    let filter = ""
    let replacements = []
    if (userType.toUpperCase() == 'CUSTOMER') {
        filter += ` and r.groupId = ?`
        replacements.push(user.unitId)
    } else if (unitIds.length > 0) {
        filter += ` and a.mobiusUnit in (?)`
        replacements.push(unitIds)
    }

    let sql = `SELECT
                    b.\`status\`, a.taskStatus, d.vehicleNumber
                FROM
                    job_task a
                    LEFT JOIN job b ON a.tripId = b.id
                    LEFT JOIN service_type c ON b.serviceTypeId = c.id 
                    LEFT JOIN vehicle d ON a.id = d.taskId
                    LEFT JOIN request r ON r.id = a.requestId
                WHERE
                    LOWER( c.category ) = 'mv' 
                    AND a.taskStatus != 'cancelled'
                    AND b.driver = 1 AND b.vehicleType != '-'
                    ${filter}`;
    let rows = await sequelizeSystemObj.query(sql, {
        replacements: replacements,
        type: QueryTypes.SELECT
    })

    let total = rows.length
    let result = {
        assigned: 0,
        pendingAssignment: 0,
        pendingApproval: 0,
        total: total,
    }
    for (let row of rows) {
        let { vehicleNumber, status } = row
        if (vehicleNumber) {
            result.assigned += 1
        } else if (status.toLowerCase() == "approved") {
            result.pendingAssignment += 1
        } else if (status.toLowerCase().startsWith('pending')){
            result.pendingApproval += 1
        }
    }

    let rows1 = []
    if (userType.toUpperCase() == 'CUSTOMER') {
        let sql1 = `SELECT
                        count(*) as count
                    FROM
                        task
                    WHERE
                        taskId LIKE 'CU-%'
                    AND driverStatus != 'Cancelled'
                    AND driverId IN (
                        SELECT
                            driverId
                        FROM
                            USER
                        WHERE
                            unitId = ?
                        AND driverId IS NOT NULL
                    )`;
        rows1 = await sequelizeObj.query(sql1, {
            replacements: [user.unitId],
            type: QueryTypes.SELECT
        })
    } else if (userType.toUpperCase() == 'HQ' || userType.toUpperCase() == 'ADMINISTRATOR') {
        if (unitIds.length > 0) {
            let sql1 = `SELECT
                        count(*) as count
                    FROM
                        task
                    WHERE
                        taskId LIKE 'CU-%'
                    AND driverStatus != 'Cancelled'
                    AND driverId IN (
                        SELECT
                            driverId
                        FROM
                            USER
                        WHERE
                            unitId in (?)
                        AND driverId IS NOT NULL
                    )`;
            rows1 = await sequelizeObj.query(sql1, {
                replacements: [unitIds],
                type: QueryTypes.SELECT
            })
        } else {
            let sql1 = `SELECT
                            count(*) as count
                        FROM
                            task
                        WHERE
                            taskId LIKE 'CU-%'
                        AND driverStatus != 'Cancelled'`;
            rows1 = await sequelizeObj.query(sql1, {
                type: QueryTypes.SELECT
            })
        }
    }
    if (rows1.length > 0) {
        let CUCount = rows1[0].count
        result.assigned = result.assigned + CUCount
        result.total = result.total + CUCount
    }
    return result
}

module.exports.GetActivityTypeAndVehicleType = async function (req, res) {
    let result = {
        activityType: PurposeTypes,
        vehicelTypeSelect: [],
    }
    try {
        let { hub } = req.body
        let replacements = []
        let filter = ""
        let unitIds = []
        if (hub && hub != "") {
            let rows = await Unit.findAll({ where: { unit: hub } })
            unitIds = rows.map(item => item.id)
        } else {
            let userUnit = await unitService.UnitUtils.getPermitUnitList(req.cookies.userId)
            unitIds = userUnit.unitIdList
        }

        if(unitIds.length > 0){
            filter += ` and unitId in (?)`
            replacements.push(unitIds)
        }
        let sql = `select DISTINCT vehicleType from vehicle where 1=1 ${filter} ORDER BY vehicleType`
        let vehicleTypeList = await sequelizeObj.query(sql, {
            replacements: replacements,
            type: QueryTypes.SELECT
        })
        
        result.vehicelTypeSelect = vehicleTypeList.map(item => item.vehicleType)
        return res.json(utils.response(1, result))
    } catch (ex) {
        log.error(ex)
        return res.json(utils.response(0, result))
    }
}

const GetDateArr = function (dateStart, dateEnd) {
    let diffDays = moment(dateEnd).diff(moment(dateStart), 'd')
    let dateArr = []
    for (let i = 0; i <= diffDays; i++) {
        let day = moment(dateStart).add(i, 'd')
        dateArr.push(moment(day).format('YYYY-MM-DD'))
    }
    return dateArr
}

module.exports.getIndentAllocationGraph = async function (req, res) {
    try {
        let { hub, type, vehicle, dateRange } = req.body
        let replacements = []
        let filter = ""

        let userType = req.cookies.userType;
        let userId = req.cookies.userId;
        let typeArr = PurposeTypes
        if (utils.stringNotEmpty(type)) {
            typeArr = [type]
        }
        let queryTypeCount = []
        typeArr.forEach((val, index) => {
            queryTypeCount.push(`sum( IF ( c.purposeType like ?, 1, 0 ) ) as count${index}`)
            replacements.push(val+'%')
        });
        let unitIds = []
        
        if (utils.stringNotEmpty(hub)) {
            let rows = await Unit.findAll({ where: { unit: hub } })
            unitIds = rows.map(item => item.id)
        } else {
            let userUnit = await unitService.UnitUtils.getPermitUnitList(userId);
            unitIds = userUnit.unitIdList
        }

        let user =  await User.findOne({ where: { userId } })
        if (userType.toUpperCase() == 'CUSTOMER') {
            filter += ` and c.groupId = ?`
            replacements.push(user.unitId)
        } else if (unitIds.length > 0) {
            filter += ` and a.mobiusUnit in (?)`
            replacements.push(unitIds)
        }

        if (utils.stringNotEmpty(vehicle)) {
            filter += ` and b.vehicleType = ?`
            replacements.push(vehicle)
        }
        let dateRangeArr = dateRange.split(' - ')
        let dateStart = moment(dateRangeArr[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let dateEnd = moment(dateRangeArr[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
        filter += ` and (a.executionDate between ? and ?)`
        replacements.push(dateStart)
        replacements.push(dateEnd)


        let sql = `SELECT
                        a.executionDate,
                        ${queryTypeCount.join(',')}
                    FROM
                        job_task a
                        LEFT JOIN job b on a.tripId = b.id
                        LEFT JOIN request c ON a.requestId = c.id 
                        LEFT JOIN service_type s ON s.id = b.serviceTypeId
                    where s.category = 'MV' AND a.taskStatus != 'cancelled' 
                    AND b.driver = 1 AND b.vehicleType != '-' 
                    ${filter}
                    GROUP BY
                        a.executionDate 
                    ORDER BY
                        a.executionDate ASC`;
        let rows = await sequelizeSystemObj.query(sql, {
            replacements: replacements,
            type: QueryTypes.SELECT
        })

        let replacements1 = []
        let queryTypeCount1 = []
        typeArr.forEach((val, index) => {
            queryTypeCount1.push(`sum( IF ( purpose like ?, 1, 0 ) ) as count${index}`)
            replacements1.push(val+'%')
        });
        
        let sql1 = `
            SELECT DATE_FORMAT(t.indentStartTime, '%Y-%m-%d') as executionDate,
                ${queryTypeCount1.join(',')}
            FROM task t
            LEFT JOIN unit un on un.unit = t.hub and un.subUnit <=> t.node
            WHERE t.driverStatus != 'Cancelled'
        `
        if(userType.toUpperCase() != 'CUSTOMER'){
            if(unitIds.length > 0){
                sql1 += ` and t.taskId LIKE 'MT-%' and un.id in(?)`
                replacements1.push(unitIds)
            }
        } else if(user.unitId) {
            sql1 += ` and t.taskId LIKE 'CU-%' and t.groupId = ?`
            replacements1.push(user.unitId)
        }
        if(vehicle){
            sql1 += ` and t.vehicleNumber in (select vehicleNo from vehicle where vehicleType = ?)`
            replacements1.push(vehicle)
        }
        if(dateStart && dateEnd){
            sql1 += ` and DATE_FORMAT(t.indentStartTime, '%Y-%m-%d') BETWEEN ? and ?`
            replacements1.push(dateStart)
            replacements1.push(dateEnd)
        }
        sql1 += ` GROUP BY executionDate ORDER BY executionDate ASC`
        let rows1 = await sequelizeObj.query(sql1, {
            replacements: replacements1,
            type: QueryTypes.SELECT
        })
    
        let dateArr = GetDateArr(dateStart, dateEnd)
        let series = []
        typeArr.forEach((type, index) => {
            let data = []
            dateArr.forEach((val, i) => {
                let count = 0
                if(rows.length > 0) {
                    let row = rows.find(item => item.executionDate == val)
                    if (row) {
                        count += Number(row[`count${index}`])
                    }
                }
                
                if (rows1.length > 0) {
                    let row1 = rows1.find(item => item.executionDate == val)
                    if (row1) {
                        count += Number(row1[`count${index}`])
                    }
                }
                data.push(count)
            })
            series.push({ name: type, data: data })
        });
        let result = {
            xAxis: dateArr,
            series: series
        }
        return res.json(utils.response(1, result))    
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    } 
}

/**
 * Vehicle Availability Graph
 * @param {*} req 
 * @param {*} res 
 * @returns 
 */
module.exports.GetVehicleAvailabilityGraph = async function (req, res) {
    try {
        let { hub, type, vehicle, dateRange } = req.body

        let replacements = []
        let filter = ""
        let unitIds = []
        let userType = req.cookies.userType ;

        let queryDemand = `sum( IF ( b.vehicleType = ?, 1, 0 ) ) as demandCount`
        replacements.push(vehicle)
        if (type && type != "") {
            queryDemand = `sum( IF ( b.vehicleType = ? and c.purposeType like ?, 1, 0 ) ) as demandCount`
            replacements.push(vehicle)
            replacements.push(type+'%')
        }
        
        if(userType.toUpperCase() == 'CUSTOMER'){
            let user = await User.findOne({ where: { userId: req.cookies.userId } })
            filter += ` and r.groupId = ?`
            replacements.push(user.unitId)
        } else if (utils.stringNotEmpty(hub)) {
            let rows = await Unit.findAll({ where: { unit: hub } })
            unitIds = rows.map(item => item.id)
            if(unitIds.length > 0){
                filter += ` and a.mobiusUnit in (?)`
                replacements.push(unitIds)
            }
        } else {
            let userUnit = await unitService.UnitUtils.getPermitUnitList(req.cookies.userId);
            unitIds = userUnit.unitIdList
            if(unitIds.length > 0){
                filter += ` and a.mobiusUnit in (?)`
                replacements.push(unitIds)
            }
        }
        
        
        if (utils.stringNotEmpty(vehicle)) {
            filter += ` and b.vehicleType = ?`
            replacements.push(vehicle)
        }
    
        let dateRangeArr = dateRange.split(' - ')
        let dateStart = moment(dateRangeArr[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
        let dateEnd = moment(dateRangeArr[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
        filter += ` and (a.executionDate between ? and ?)`
        replacements.push(dateStart)
        replacements.push(dateEnd)
    
        
        let sql = `SELECT
                        a.executionDate,
                        ${queryDemand}
                    FROM
                        job_task a
                        LEFT JOIN job b on a.tripId = b.id
                        LEFT JOIN request c ON a.requestId = c.id 
                        LEFT JOIN service_type s ON s.id = b.serviceTypeId
                        LEFT JOIN request r ON r.id = a.requestId
                    where s.category = 'MV' AND a.taskStatus != 'cancelled'  ${filter}
                    GROUP BY
                        a.executionDate 
                    ORDER BY
                        a.executionDate ASC`;
        let rows = await sequelizeSystemObj.query(sql, {
            replacements: replacements,
            type: QueryTypes.SELECT
        })
       
        let dateArr = GetDateArr(dateStart, dateEnd)
        let demandCountArr = []
        let availabilityCountArr = []
        for(let date of dateArr){
            let row = rows.find(item=> item.executionDate == date)
            let demandCount = 0
            if(row){
                demandCount = Number(row.demandCount)
            }
            demandCountArr.push(demandCount)
            let vehicleDeployableNumber = await getVehicleDeployable(req.cookies.userType, date, unitIds, type, vehicle, req.cookies.userId)
            
            availabilityCountArr.push(vehicleDeployableNumber)
        }
    
        let result = {
            xAxis: dateArr,
            series: [
                { name: "Vehicle Demand", data: demandCountArr },
                { name: "Vehicle Availability", data: availabilityCountArr }
            ]
        }
        return res.json(utils.response(1, result))  
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

//2023-07-11 vehicle deployable number
const getVehicleDeployable = async function (userType, date, unitIdList, type, vehicle, userId) {
    let vehicleData = null
    if(userType.toUpperCase() == 'CUSTOMER') {
        let user = await User.findOne({ where: { userId: userId } })
        vehicleData = await TaskUtils.getVehicleByGroup(user.unitId);
        if(vehicleData.length < 1) return 0
    }
    let sqlTaskVehicle = `
        SELECT tt.vehicleNumber FROM task tt 
        WHERE tt.vehicleStatus not in ('Cancelled', 'completed')  
        and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
        or tt.vehicleStatus = 'started'
        )
        ${ userType.toUpperCase() == 'CUSTOMER' ? ` and tt.taskId like 'CU-%'` : ` and tt.dataFrom = 'SYSTEM' and tt.driverId is not null` }
    `
    let taskVehicleBYReplacements = [moment(date).format('YYYY-MM-DD')]
    if(type) {
        sqlTaskVehicle += ` and tt.purpose = ?`
        taskVehicleBYReplacements.push(type)
    }
    sqlTaskVehicle += ` group by tt.vehicleNumber`
    let taskVehicle = await sequelizeObj.query(sqlTaskVehicle, { type: QueryTypes.SELECT, replacements: taskVehicleBYReplacements })
    taskVehicle = taskVehicle.map(item => item.vehicleNumber)

    let loanOutVehicle = await sequelizeObj.query(` 
            SELECT ll.vehicleNo, ll.groupId FROM (
                SELECT IF(l.vehicleNo IS NULL, lr.vehicleNo, l.vehicleNo) AS vehicleNo,
                IF(l.groupId IS NULL, lr.groupId, l.groupId) AS groupId
                FROM vehicle v
                LEFT JOIN (SELECT lo.vehicleNo, lo.groupId FROM loan lo WHERE ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND DATE_FORMAT(lo.endDate, '%Y-%m-%d')) l ON l.vehicleNo = v.vehicleNo
                LEFT JOIN (SELECT lr.vehicleNo, lr.groupId FROM loan_record lr 
                    WHERE ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
                ) lr ON lr.vehicleNo = v.vehicleNo
            )ll WHERE ll.vehicleNo IS NOT NULL
            GROUP BY ll.vehicleNo
    `, { type: QueryTypes.SELECT, replacements: [moment(date).format('YYYY-MM-DD'), moment(date).format('YYYY-MM-DD')] })
    loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)
    
    let sqlVehicleStatusOrDeployed = `
        select vv.currentStatus, count(*) as statusSum, vv.currentUnitId, vv.currentUnit, vv.currentSubUnit from (
            SELECT veh.vehicleNo,
            IF(hh.unitId IS NULL AND  hr.unitId IS NULL, un.id, IF(hh.unitId IS NULL, hr.unitId, hh.unitId)) AS currentUnitId, 
            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, un.unit, IF(hh.toHub IS NULL, hr.toHub, hh.toHub)) AS currentUnit, 
            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, un.subUnit, IF(hh.toHub IS NULL, hr.toNode, hh.toNode)) AS currentSubUnit,
    `
    let vehicleStatusOrDeployedreplacements = [];
    if(userType.toUpperCase() == 'CUSTOMER'){
        sqlVehicleStatusOrDeployed += `
            IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
            ) as currentStatus
        `
        vehicleStatusOrDeployedreplacements.push(taskVehicle.join(","))
    } else {
        sqlVehicleStatusOrDeployed += ` 
            IF(FIND_IN_SET(veh.vehicleNo, ?), 'LOAN OUT', 
                IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(FIND_IN_SET(veh.vehicleNo, ?), 'Deployed', 'Deployable')
                )
            ) as currentStatus
        `
        vehicleStatusOrDeployedreplacements.push(loanOutVehicle.join(","))
        vehicleStatusOrDeployedreplacements.push(taskVehicle.join(","))
    }
    sqlVehicleStatusOrDeployed += ` 
        FROM vehicle veh
        left join unit un on un.id = veh.unitId
        left join (
            select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
            (? BETWEEN DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(ho.endDateTime, '%Y-%m-%d'))
            and ho.status = 'Approved'
        ) hh ON hh.vehicleNo = veh.vehicleNo
        LEFT JOIN (
            SELECT hr.vehicleNo, hr.toHub, hr.toNode, uni.id as unitId FROM hoto_record hr 
            LEFT JOIN unit uni on uni.unit = hr.toHub and uni.subUnit <=> hr.toNode
            WHERE (? BETWEEN DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(hr.returnDateTime, '%Y-%m-%d'))
            and hr.status = 'Approved'
        ) hr ON hr.vehicleNo = veh.vehicleNo
        left join (
            select vl.vehicleNo, vl.reason from vehicle_leave_record vl 
            where vl.status = 1 
            and (? BETWEEN DATE_FORMAT(vl.startTime, '%Y-%m-%d') AND DATE_FORMAT(vl.endTime, '%Y-%m-%d'))
        ) ll ON ll.vehicleNo = veh.vehicleNo
        where 1=1
    `
    vehicleStatusOrDeployedreplacements.push(moment(date).format('YYYY-MM-DD'))
    vehicleStatusOrDeployedreplacements.push(moment(date).format('YYYY-MM-DD'))
    vehicleStatusOrDeployedreplacements.push(moment(date).format('YYYY-MM-DD'))
    if(vehicleData?.length > 0) {
        sqlVehicleStatusOrDeployed += ` 
            and veh.vehicleNo in (?)
        `
        vehicleStatusOrDeployedreplacements.push(`'${ vehicleData.join("','") }'`)
    }
    if(vehicle){
        sqlVehicleStatusOrDeployed += `
            and veh.vehicleType = ?
        `
        vehicleStatusOrDeployedreplacements.push(vehicle)
    }
    sqlVehicleStatusOrDeployed += `
            GROUP BY veh.vehicleNo
        ) vv where vv.currentStatus = 'Deployable'
    `
    if(unitIdList.length > 0){
        sqlVehicleStatusOrDeployed += `
            and vv.currentUnitId in(?)
        `
        vehicleStatusOrDeployedreplacements.push(unitIdList.join(","))
    }
    sqlVehicleStatusOrDeployed += ` group by vv.currentStatus`
    let vehicleStatusOrDeployed = await sequelizeObj.query(sqlVehicleStatusOrDeployed, { type: QueryTypes.SELECT, replacements: vehicleStatusOrDeployedreplacements });

    return vehicleStatusOrDeployed[0] ? vehicleStatusOrDeployed[0].statusSum : 0
}

/**
 * TO Availability Graph
 * @param {*} req 
 * @param {*} res 
 * @returns 
 */
module.exports.GetTOAvailabilityGraph = async function (req, res) {
    try {
            let { hub, type, vehicle, dateRange } = req.body

            let replacements = []
            let filter = ""
            let unitIds = []
            
            
            let userType = req.cookies.userType ;
            if(userType.toUpperCase() == 'CUSTOMER'){
                let user = await User.findOne({ where: { userId: req.cookies.userId } })
                filter += ` and r.groupId = ?`
                replacements.push(user.unitId)
            } else if (utils.stringNotEmpty(hub)) {
                let rows = await Unit.findAll({ where: { unit: hub } })
                unitIds = rows.map(item => item.id)
                if(unitIds.length > 0) {
                    filter += ` and a.mobiusUnit in (?)`
                    replacements.push(unitIds)
                }
            } else {
                let userUnit = await unitService.UnitUtils.getPermitUnitList(req.cookies.userId);
                unitIds = userUnit.unitIdList
                if(unitIds.length > 0){
                    filter += ` and a.mobiusUnit in (?)`
                    replacements.push(unitIds)
                }
            }


            if (utils.stringNotEmpty(vehicle)) {
                filter += ` and b.vehicleType = ?`
                replacements.push(vehicle)
            }

            let dateRangeArr = dateRange.split(' - ')
            let dateStart = moment(dateRangeArr[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
            let dateEnd = moment(dateRangeArr[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
            filter += ` and (a.executionDate between ? and ?)`
            replacements.push(dateStart)
            replacements.push(dateEnd)

            let queryDemand = `IFNULL( b.noOfDriver, 0 ) AS noOfDriver `
            if (type && type != "") {
                queryDemand = `IF(c.purposeType like '${type}%',IFNULL( b.noOfDriver, 0 ),0) AS noOfDriver`
            }
            let sql = `SELECT
                            t.executionDate,
                            SUM( t.noOfDriver ) AS demandCount 
                        FROM
                            (
                            SELECT
                                a.executionDate,
                                a.tripId,
                                ${queryDemand}
                            FROM
                                job_task a
                                LEFT JOIN job b ON a.tripId = b.id
                                LEFT JOIN request c ON a.requestId = c.id 
                                LEFT JOIN service_type s ON s.id = b.serviceTypeId
                                LEFT JOIN request r ON r.id = a.requestId
                            WHERE s.category = 'MV' AND a.taskStatus != 'cancelled' ${filter}
                            GROUP BY
                                a.executionDate,
                                a.tripId 
                            ORDER BY
                                a.executionDate ASC 
                            ) t 
                        GROUP BY
                            t.executionDate`;
            let rows = await sequelizeSystemObj.query(sql, {
                replacements: replacements,
                type: QueryTypes.SELECT
            })
          
            
            let dateArr = GetDateArr(dateStart, dateEnd)
            let demandCountArr = []
            let availabilityCountArr = []

            for(let date of dateArr){
                let row = rows.find(item=> item.executionDate == date)
                let demandCount = 0
                if(row){
                    demandCount = Number(row.demandCount)
                }
                demandCountArr.push(demandCount)

                let vehicleDeployableNumber = await getDriverDeployable(req.cookies.userType, date, unitIds, type, vehicle, req.cookies.userId)
                
                availabilityCountArr.push(vehicleDeployableNumber)
            }

            let result = {
                xAxis: dateArr,
                series: [
                    { name: "TO Demand", data: demandCountArr },
                    { name: "TO Availability", data: availabilityCountArr }
                ]
            }
            return res.json(utils.response(1, result))    
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }

}

//2023-07-11 driver deployable number
const getDriverDeployable = async function (userType, date, unitIdList, type, vehicle, userId) {
    let newUnitId = null
    if(userType.toUpperCase() == 'CUSTOMER') {
        let user = await User.findOne({ where: { userId: userId } })
        newUnitId = user.unitId
    }
    let taskDriverSql = `
        SELECT tt.driverId FROM task tt
        WHERE tt.driverStatus not in ('Cancelled', 'completed') 
        and ((? BETWEEN DATE_FORMAT(tt.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(tt.indentEndTime, '%Y-%m-%d'))
        OR tt.driverStatus = 'started')
        ${ userType.toUpperCase() == 'CUSTOMER' ? ` and tt.taskId like 'CU-%'` : ` and tt.dataFrom = 'SYSTEM' and tt.driverId is not null` }
    `
    let replacementsByTaskDriver = [moment(date).format('YYYY-MM-DD')]
    if(type){
        taskDriverSql += ` and tt.purpose = ?`
        replacementsByTaskDriver.push(type)
    }
    taskDriverSql += ` group by tt.driverId`
    let taskDriver = await sequelizeObj.query(taskDriverSql, { type: QueryTypes.SELECT, replacements: replacementsByTaskDriver })
    taskDriver = taskDriver.map(item => item.driverId)
    let loanOutDriver = await sequelizeObj.query(` 
    SELECT ll.driverId FROM (
        SELECT IF(l.driverId IS NULL, lr.driverId, l.driverId) AS driverId
        FROM driver d
        LEFT JOIN (SELECT lo.driverId FROM loan lo WHERE ? BETWEEN DATE_FORMAT(lo.startDate, '%Y-%m-%d') AND DATE_FORMAT(lo.endDate, '%Y-%m-%d')) l ON l.driverId = d.driverId
        LEFT JOIN (SELECT lr.driverId FROM loan_record lr 
            WHERE ? BETWEEN DATE_FORMAT(lr.startDate, '%Y-%m-%d') AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d')
        ) lr ON lr.driverId = d.driverId
        where d.permitStatus != 'invalid' 
    )ll WHERE ll.driverId IS NOT NULL
    group by ll.driverId
    `, { type: QueryTypes.SELECT, replacements: [moment(date).format('YYYY-MM-DD'), moment(date).format('YYYY-MM-DD')] })
    loanOutDriver = loanOutDriver.map(item => item.driverId)
    let driverByGroup = null
    if(userType.toUpperCase() == 'CUSTOMER') driverByGroup = await TaskUtils.getDriverByGroup(newUnitId, vehicle, date)
    let sqlDriverDeployable = `
        select dd.currentStatus, count(*) as statusSum, dd.currentUnitId from (
            SELECT d.driverId, d.operationallyReadyDate, us.role,
            IF(hh.unitId IS NULL AND  hr.unitId IS NULL, u.id, IF(hh.unitId IS NULL, hr.unitId, hh.unitId)) AS currentUnitId, 
            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.unit, IF(hh.toHub IS NULL, hr.toHub, hh.toHub)) AS currentUnit, 
            IF(hh.toHub IS NULL AND  hr.toHub IS NULL, u.subUnit, IF(hh.toHub IS NULL, hr.toNode, hh.toNode)) AS currentSubUnit,
    `
    let driverDeployablereplacements = []
    if(userType.toUpperCase() == 'CUSTOMER'){
        sqlDriverDeployable += `
            IF(d.permitStatus = 'invalid', 'permitInvalid',
                IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                        IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                ) 
            ) as currentStatus
        `
        driverDeployablereplacements.push(taskDriver.join(","))
    } else {
        sqlDriverDeployable += `
            IF(d.permitStatus = 'invalid', 'permitInvalid',
                IF(FIND_IN_SET(d.driverId, ?), 'LOAN OUT', 
                    IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                            IF(FIND_IN_SET(d.driverId, ?), 'Deployed', 'Deployable')
                    )
                )
            ) as currentStatus
        `
        driverDeployablereplacements.push(loanOutDriver.join(","))
        driverDeployablereplacements.push(taskDriver.join(","))
    }
    sqlDriverDeployable += `
        FROM driver d
        LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
        LEFT JOIN user us ON us.driverId = d.driverId
        LEFT JOIN unit u ON u.id = d.unitId
        left join (
            select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where 
            (? BETWEEN DATE_FORMAT(ho.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(ho.endDateTime, '%Y-%m-%d'))
            and ho.status = 'Approved'
        ) hh ON hh.driverId = d.driverId
        LEFT JOIN (
            SELECT hr.driverId, hr.toHub, hr.toNode, uni.id as unitId FROM hoto_record hr 
            LEFT JOIN unit uni on uni.unit = hr.toHub and uni.subUnit <=> hr.toNode
            WHERE (? BETWEEN DATE_FORMAT(hr.startDateTime, '%Y-%m-%d') AND DATE_FORMAT(hr.returnDateTime, '%Y-%m-%d'))
            and hr.status = 'Approved'
        ) hr ON hr.driverId = d.driverId
        left join (
            select dl.driverId, dl.reason from driver_leave_record dl where dl.status = 1 
            and (? BETWEEN DATE_FORMAT(dl.startTime, '%Y-%m-%d') AND DATE_FORMAT(dl.endTime, '%Y-%m-%d'))
        ) ll ON ll.driverId = d.driverId
        where (d.operationallyReadyDate is null OR d.operationallyReadyDate > ? )
    `
    driverDeployablereplacements.push(moment(date).format('YYYY-MM-DD'))
    driverDeployablereplacements.push(moment(date).format('YYYY-MM-DD'))
    driverDeployablereplacements.push(moment(date).format('YYYY-MM-DD'))
    driverDeployablereplacements.push(moment(date).format('YYYY-MM-DD'))
    if(driverByGroup?.length > 0) {
        sqlDriverDeployable += `
            and d.driverId in (?)
        `
        driverDeployablereplacements.push(driverByGroup.join(","))
    }
    if(vehicle){
        sqlDriverDeployable += ` and dc.vehicleType = ?`
        driverDeployablereplacements.push(vehicle)
    }
    sqlDriverDeployable += `
            group by d.driverId
        ) dd where 1=1 and dd.currentStatus = 'Deployable'
    `
    if(unitIdList.length > 0){
        sqlDriverDeployable += `
            and dd.currentUnitId in(?)
        `
        driverDeployablereplacements.push(unitIdList.join(","))
    }
    sqlDriverDeployable += ` group by dd.currentStatus`
    let driverDeployable = await sequelizeObj.query(sqlDriverDeployable, { type: QueryTypes.SELECT, replacements: driverDeployablereplacements });
    // ${ newHub ? ` and dd.currentUnit = '${ newHub }'` : '' }
    // ${ newNode ? ` and dd.currentSubUnit = '${ newNode }'` : '' }
    return driverDeployable[0] ? driverDeployable[0].statusSum : 0
}

module.exports.GetWPTDueThisWeek = async function (req, res) {
    try {
    let userId = req.body.userId;
    let hub = req.body.hub;
    let user = await User.findOne({ where: { userId } })
    if (!user) {
        return res.json(utils.response(0, `User ${userId} does not exist.`));
    }

    let sqlloanOutVehicleNoList = `
        select l.vehicleNo from loan l 
        where l.vehicleNo IS NOT NULL and now() BETWEEN l.startDate AND l.endDate
    `
    let replacementsByloanOutVehicleNoList = []
    if((user.userType).toUpperCase() == 'CUSTOMER') {
        sqlloanOutVehicleNoList += ` 
            and l.groupId = ?
        `
        replacementsByloanOutVehicleNoList.push(user.unitId)
    }
    let loanOutVehicleNoList = await sequelizeObj.query(sqlloanOutVehicleNoList, { type: QueryTypes.SELECT, replacements: replacementsByloanOutVehicleNoList });
    let currentLoanOutVehiclenos = loanOutVehicleNoList.map(item => item.vehicleNo)

    let unitIdList = null;
    if(!hub){
        let userUnit = await unitService.UnitUtils.getPermitUnitList(userId);
        unitIdList = userUnit.unitIdList
    }
    
    let date2 = moment().weekday(1).add(6, 'day').format('YYYY-MM-DD')
    let sql = `
        SELECT
            b.unit,
            SUM( IF ( a.nextWpt1Time = ?, 1, 0 )) wpt,
            SUM( IF ( a.nextMptTime = ?, 1, 0 )) mpt 
        FROM vehicle a
        LEFT JOIN unit b ON a.unitId = b.id 
        WHERE b.id IS NOT NULL
        
    `;
    let replacements = [date2, date2];
    if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
        if (!currentLoanOutVehiclenos || currentLoanOutVehiclenos.length == 0) {
            return res.json(utils.response(1, [{ "unit": "Total", "wpt": "0", "mpt": "0", "total": "0" }]))  
        }
        unitIdList = null;
        hub = null;
        sql += ` and a.vehicleNo in (?) `;
        replacements.push(`'${ currentLoanOutVehiclenos.join("','") }'`)
    } else if (currentLoanOutVehiclenos?.length > 0) {
        sql += ` and a.vehicleNo not in (?) `;
        replacements.push(`'${ currentLoanOutVehiclenos.join("','") }'`)
    }
    if (hub) {
        sql += ` and b.unit = ? `;
        replacements.push(hub)
    } else if (unitIdList && unitIdList.length > 0) {
        sql += ` and b.id in (?) `;
        replacements.push(unitIdList)
    }

    sql += ` GROUP BY b.unit ORDER BY b.unit `;
    let rows = await sequelizeObj.query(sql, {
        replacements: replacements,
        type: QueryTypes.SELECT
    })
    let wptTotal = 0
    let mptTotal = 0
    for (let row of rows) {
        let wpt = Number(row.wpt)
        let mpt = Number(row.mpt)
        wptTotal += wpt
        mptTotal += mpt
        row.total = wpt + mpt
    }
    if (rows.length > 1) {
        rows.push({ "unit": "Total", "wpt": wptTotal.toString(), "mpt": mptTotal.toString(), "total": (wptTotal + mptTotal).toString() })
    } else {
        rows = [{ "unit": "Total", "wpt": wptTotal.toString(), "mpt": mptTotal.toString(), "total": (wptTotal + mptTotal).toString() }]
    }
    return res.json(utils.response(1, rows))  
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.GetVehicleServicingGraph = async function (req, res) {
    try {
        let { hub } = req.body
        const fmt = "YYYY-MM"
        let dateArr = []
        for (let i = 0; i < 6; i++) {
            dateArr.push(moment().add(i, 'M').format(fmt))
        }

        let userId = req.cookies.userId;
        let user = await User.findOne({ where: { userId: userId } })
        
        let sqlloanOutVehicleNoList = `
            select l.vehicleNo from loan l 
            where l.vehicleNo IS NOT NULL and now() BETWEEN l.startDate AND l.endDate
        `
        let replacementsByloanOutVehicleNoList = []
        if((user.userType).toUpperCase() == 'CUSTOMER'){
            sqlloanOutVehicleNoList += `
                and l.groupId = ?
            `
            replacementsByloanOutVehicleNoList.push(user.unitId)
        }
        let loanOutVehicleNoList = await sequelizeObj.query(sqlloanOutVehicleNoList, { type: QueryTypes.SELECT, replacements: replacementsByloanOutVehicleNoList });
        let currentLoanOutVehiclenos = loanOutVehicleNoList.map(item => item.vehicleNo)

        let unitIdList = null;
        if(!hub){
            let userUnit = await unitService.UnitUtils.getPermitUnitList(userId);
            unitIdList = userUnit.unitIdList;
        }
        let sql = `SELECT
                    a.nextAviTime,
                    a.nextPmTime,
                    a.nextWpt1Time,
                    a.nextMptTime,
                    b.unit 
                FROM
                    vehicle a
                    LEFT JOIN unit b ON a.unitId = b.id where 1=1 `;
        let replacements = []
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            unitIdList = null;
            hub = null;
            sql += ` and a.vehicleNo in (?) `;
            replacements.push(`'${ currentLoanOutVehiclenos.join("','") }'`)
        } else if (currentLoanOutVehiclenos?.length > 0) {
            sql += ` and a.vehicleNo not in (?) `;
            replacements.push(`'${ currentLoanOutVehiclenos.join("','") }'`)
        }
        if (hub) {
            sql += ` and b.unit = ? `;
            replacements.push(hub)
        } else if (unitIdList && unitIdList.length > 0) {
            sql += ` and b.id in (?) `;
            replacements.push(unitIdList)
        }
        let rows = await sequelizeObj.query(sql, {
            type: QueryTypes.SELECT
            , replacements: replacements
        })

        let aviCountArr = []
        let pmCountArr = []
        let wptCountArr = []
        let mptCountArr = []
        dateArr.forEach((val, index) => {
            let aviCount = rows.filter(item => moment(item.nextAviTime).format(fmt) == val).length
            let pmCount = rows.filter(item => moment(item.nextPmTime).format(fmt) == val).length
            let wptCount = rows.filter(item => moment(item.nextWpt1Time).format(fmt) == val).length
            let mptCount = rows.filter(item => moment(item.nextMptTime).format(fmt) == val).length
            aviCountArr.push(aviCount)
            pmCountArr.push(pmCount)
            wptCountArr.push(wptCount)
            mptCountArr.push(mptCount);
        })

        let result = {
            xAxis: dateArr,
            series: [
                { name: "AVI", data: aviCountArr },
                { name: "PM", data: pmCountArr },
                { name: "WPT", data: wptCountArr },
                { name: "MPT", data: mptCountArr },
            ]
        }
        return res.json(utils.response(1, result)) 
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}