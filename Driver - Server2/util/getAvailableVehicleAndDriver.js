const { sequelizeObj } = require('../db/dbConf')
const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const log = require('../log/winston').logger('getVehicleAndDriver');
const purposeList = ['avi', 'wpt', 'mpt', 'pm']
/*
 Note: Indent is TO Only/Vehicle Only, and taskType is passed in as loan. For others, taskType is passed in as both！
*/
const getVehicle = async function (purpose, startDate, endDate, vehicleType, hub, node, taskType) {
    startDate = moment(startDate).format('YYYY-MM-DD HH:mm:ss');
    endDate = moment(endDate).format('YYYY-MM-DD HH:mm:ss');
    let vehicle_leave = await sequelizeObj.query(`
        SELECT vl.vehicleNo FROM vehicle_leave_record vl WHERE vl.status = 1 
        AND ( ('${ startDate }' >= vl.startTime AND '${ startDate }' <= vl.endTime) 
        OR ('${ endDate }' >= vl.startTime AND '${ endDate }' <= vl.endTime) 
        OR ('${ startDate }' < vl.startTime AND '${ endDate }' > vl.endTime))
        AND vl.vehicleNo IS NOT NULL GROUP BY vl.vehicleNo
    `, { type: QueryTypes.SELECT })
    vehicle_leave = vehicle_leave.map(item => item.vehicleNo);
    // log.warn(`leave vehicleList ${ JSON.stringify(vehicle_leave) }`)
    let loanOutVehicle = await sequelizeObj.query(`
        SELECT vehicleNo FROM loan 
        WHERE (('${ startDate }' >= startDate AND '${ startDate }' <= endDate) 
        OR ('${ endDate }' >= startDate AND '${ endDate }' <= endDate) 
        OR ('${ startDate }' < startDate AND '${ endDate }' > endDate))
        and vehicleNo is not null
        group by vehicleNo
    `, { type: QueryTypes.SELECT })
    loanOutVehicle = loanOutVehicle.map(item => item.vehicleNo)
    // log.warn(`loan out vehicleList ${ JSON.stringify(loanOutVehicle) }`)
    let hotoVehicleListByNotScope = await sequelizeObj.query(
        `select vehicleNo, startDateTime, endDateTime
        from hoto 
        where (('${ startDate }' >= startDateTime AND '${ startDate }' <= endDateTime) 
        OR ('${ endDate }' >= startDateTime AND '${ endDate }' <= endDateTime) 
        OR ('${ startDate }' < startDateTime AND '${ endDate }' > endDateTime))
        and vehicleNo not in (select vehicleNo from hoto 
            where '${ startDate }' >= startDateTime AND '${ endDate }' <= endDateTime 
            and vehicleNo is not null  and status = 'Approved'
        ) and status = 'Approved'
        and vehicleNo is not null
        group by vehicleNo
        `,
    {
        type: QueryTypes.SELECT
    })
    hotoVehicleListByNotScope = hotoVehicleListByNotScope.map(item => item.vehicleNo)
    let task_vehicle = []
    if(taskType.toLowerCase() == 'loan'){
        task_vehicle = await sequelizeObj.query(`
            SELECT t.vehicleNumber FROM task t
            LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
            WHERE v.vehicleType = '${ vehicleType }'
            and t.vehicleStatus not in ('completed', 'cancelled')
            AND ((('${ startDate }' >= t.indentStartTime AND '${ startDate }' <= t.indentEndTime) 
            OR ('${ endDate }' >= t.indentStartTime AND '${ endDate }' <= t.indentEndTime) 
            OR ('${ startDate }' < t.indentStartTime AND '${ endDate }' > t.indentEndTime))
            OR t.vehicleStatus = 'started'
            )
        `, { type: QueryTypes.SELECT })
        task_vehicle = task_vehicle.map(item => item.vehicleNumber)
        // log.warn(`task vehicleList ${ JSON.stringify(task_vehicle) }`)
    }
    // log.warn(`hoto Not within the specified range vehicleList ${ JSON.stringify(hotoVehicleListByNotScope) }`)
    let excludeVehicle = vehicle_leave.concat(loanOutVehicle).concat(hotoVehicleListByNotScope).concat(task_vehicle)    
    excludeVehicle = excludeVehicle.map(item => item);
    excludeVehicle = Array.from(new Set(excludeVehicle))  
    // log.warn(`Need to exclude the vehicle ${ JSON.stringify(excludeVehicle) }`)
    let vehicleList = await sequelizeObj.query(
        `select vv.vehicleNo, vv.hub, vv.node  from (
            SELECT a.vehicleNo, 
            IF(h.toHub IS NULL, u.unit, h.toHub) AS hub, 
	        IF(h.toNode IS NULL, u.subUnit, h.toNode) AS node, a.groupId FROM vehicle a
            LEFT JOIN unit u ON u.id = a.unitId
            left join (
                select ho.vehicleNo, ho.toHub, ho.toNode from hoto ho where (('${ startDate }' >= ho.startDateTime AND '${ endDate }' <= ho.endDateTime)) and ho.status = 'Approved'
            ) h ON h.vehicleNo = a.vehicleNo 
            where a.vehicleType = '${ vehicleType }' and a.groupId is null
            ${ purpose ? purposeList.indexOf(purpose.toLowerCase()) == -1 ? `
            AND (a.nextAviTime > '${ moment(endDate).format('YYYY-MM-DD') }' OR a.nextAviTime IS NULL)
            ` : `
            ${ purpose.toLowerCase() == 'mpt' ? ` AND (a.nextAviTime > '${ moment(endDate).format('YYYY-MM-DD') }' OR a.nextAviTime IS NULL)` : '' }
            ` : '' }
            ) vv where vv.hub = '${ hub }' 
            ${ node ? ` AND vv.node = '${ node }'` : `` }
            ${ excludeVehicle.length > 0 ? ` and vv.vehicleNo not in ('${ excludeVehicle.join("','") }')` : '' }
            GROUP BY vv.vehicleNo
            `,
            {
                type: QueryTypes.SELECT
            }
    );
    log.warn(`Available Vehicles ==> ${ JSON.stringify(vehicleList) }`)
}
getVehicle('duty', '2023-11-17 13:00', '2023-11-30 23:59', '8-Seater Bus', 'hub1', 'node1', 'loan')

const getDriver = async function (purpose, startDate, endDate, vehicleType, hub, node, taskType) {
    if(purpose.toLowerCase() == 'driving training'){
        vehicleType = null
    } 
    if(purpose.toLowerCase() == 'familiarisation') {
        vehicleType = null
        hub = null;
        node = null;
    }
    let indentStartTime = moment(startDate).format('YYYY-MM-DD HH:mm:ss');
    let indentEndTime = moment(endDate).format('YYYY-MM-DD HH:mm:ss');
    let driver_leave = await sequelizeObj.query(`
        SELECT ifnull(dl.driverId, -1) FROM driver_leave_record dl WHERE dl.status = 1 AND ( 
        ('${ indentStartTime }' >= dl.startTime AND '${ indentStartTime }' <= dl.endTime) 
        OR ('${ indentEndTime }' >= dl.startTime AND '${ indentEndTime }' <= dl.endTime) 
        OR ('${ indentStartTime }' < dl.startTime AND '${ indentEndTime }' > dl.endTime)
        ) GROUP BY dl.driverId
    `, { type: QueryTypes.SELECT })
    driver_leave = driver_leave.map(item => item.driverId)
    // log.warn(`leave driverList ${ JSON.stringify(driver_leave) }`)
    let loanOutDriver = await sequelizeObj.query(`
    SELECT driverId FROM loan 
    WHERE (('${ indentStartTime }' >= startDate AND '${ indentStartTime }' <= endDate) 
    OR ('${ indentEndTime }' >= startDate AND '${ indentEndTime }' <= endDate) 
    OR ('${ indentStartTime }' < startDate AND '${ indentEndTime }' > endDate))
    and driverId is not null
    group by driverId
   `, { type: QueryTypes.SELECT })
    loanOutDriver = loanOutDriver.map(item => item.driverId)
    // log.warn(`loan out driverList ${ JSON.stringify(loanOutDriver) }`)
    let hotoDriverListByNotScope = await sequelizeObj.query(
        `select driverId, startDateTime, endDateTime
        from hoto 
        where (('${ indentStartTime }' >= startDateTime AND '${ indentStartTime }' <= endDateTime) 
        OR ('${ indentEndTime }' >= startDateTime AND '${ indentEndTime }' <= endDateTime) 
        OR ('${ indentStartTime }' < startDateTime AND '${ indentEndTime }' > endDateTime))
        and driverId not in (select driverId from hoto 
            where '${ indentStartTime }' >= startDateTime AND '${ indentEndTime }' <= endDateTime
            and driverId is not null and status = 'Approved'
        ) and status = 'Approved'
        and driverId is not null group by driverId`,
    {
        type: QueryTypes.SELECT
    })
    hotoDriverListByNotScope = hotoDriverListByNotScope.map(item => item.driverId)
    // log.warn(`hoto Not within the specified range driverList ${ JSON.stringify(hotoDriverListByNotScope) }`)
    let taskDriver = []
    if(taskType.toLowerCase() == 'loan') {
        // 2023-07-13 Tasks that do not require a driver need to exclude vehicles that are not completed within the task time frame
        taskDriver = await sequelizeObj.query(`
            SELECT t.driverId FROM task t
            WHERE t.driverStatus not in ('completed', 'cancelled')
            AND ((('${ indentStartTime }' >= t.indentStartTime AND '${ indentStartTime }' <= t.indentEndTime) 
            OR ('${ indentEndTime }' >= t.indentStartTime AND '${ indentEndTime }' <= t.indentEndTime) 
            OR ('${ indentStartTime }' < t.indentStartTime AND '${ indentEndTime }' > t.indentEndTime))
            OR t.driverStatus = 'started'
            ) and t.driverId is not null
        `, { type: QueryTypes.SELECT })
        taskDriver = taskDriver.map(item => item.driverId)
        // log.warn(`task driverList ${ JSON.stringify(taskDriver) }`)
    }
    let excludeDriver = driver_leave.concat(loanOutDriver).concat(hotoDriverListByNotScope).concat(taskDriver)   
    excludeDriver = excludeDriver.map(item => item);
    excludeDriver = Array.from(new Set(excludeDriver))
    // log.warn(`Need to exclude the driver ${ JSON.stringify(excludeDriver) }`)
    let driverList = await sequelizeObj.query(
        `select dd.driverId, dd.driverName, dd.vehicleType, dd.hub, dd.node from (
            select d.driverId, d.driverName,dc.vehicleType,d.contactNumber, IF(h.toHub IS NULL, u.unit, h.toHub) AS hub,
            IF(h.toHub IS NULL, u.subUnit, h.toNode) AS node, d.overrideStatus 
            from driver d
            LEFT JOIN unit u ON u.id = d.unitId
            LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            left join (
                select ho.driverId, ho.toHub, ho.toNode from hoto ho where (
                   ('${ indentStartTime }' >= ho.startDateTime AND '${ indentEndTime }' <= ho.endDateTime)
                    ) and ho.status = 'Approved'
            ) h ON h.driverId = d.driverId 
            where d.permitStatus != 'invalid'
            ${ vehicleType ? ` and FIND_IN_SET('${ vehicleType }', dc.vehicleType)` : '' } 
            and (d.operationallyReadyDate > '${ moment(indentEndTime).format('YYYY-MM-DD') }' OR d.operationallyReadyDate is null)
            ) dd where 1=1 
            ${ hub ? ` and dd.hub = '${ hub }'` : '' } 
            ${ node ? ` and dd.node = '${ node }'` : '' } 
            ${ excludeDriver.length > 0 ? ` and dd.driverId not in (${ excludeDriver.join(",") })` : '' }
            GROUP BY dd.driverId
            `,
            {
                type: QueryTypes.SELECT
            }
    );
    log.warn(`Available Drivers ==> ${ JSON.stringify(driverList) }`)
}
getDriver('duty', '2023-11-17 13:00', '2023-11-30 23:59', '8-Seater Bus', 'hub1', 'node', 'loan')
