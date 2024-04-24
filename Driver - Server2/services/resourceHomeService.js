const log = require('../log/winston').logger('Resource Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')
const userService = require('./userService');
const unitService = require('../services/unitService');

const { User } = require('../model/user.js');
const { Unit } = require('../model/unit.js');

module.exports.getResourcesStatData = async function (req, res) {
    try {
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ req.cookies.userId } does not exist.`);
            return res.json(utils.response(0, `User ${ req.cookies.userId } does not exist.`));
        }
        let unit = req.body.unit;
        let subunit = req.body.subunit;

        let result = {
            vehicleData: {},
            tsOperatorData: {}
        };
        let vehicleData = await statVehicleData(unit, subunit, user.userType, user.unitId, user.hq);
        result.vehicleData = vehicleData;

        let tsOperatorData = await statDriverData(unit, subunit, user.userType, user.unitId, user.hq);
        result.tsOperatorData = tsOperatorData;

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error('(getResourcesStatData) : ', error);

        return res.json(utils.response(0, 'Query data fail!'));
    }
};

const statVehicleData = async function(unit, subUnit, userType, userUnitId, userHqUnit) {
    let vehicleData = {
        vehicleTodayAssigned: 0,
        vehicleTodayAssignedEffective: 0,
        todayVehicleDistinctTotal: 0,
        vehicleTodayYetToStart: 0,
        vehicleTodayStarted: 0,
        vehicleTodayCompleted: 0,
        vehicleTodayCancelled: 0,
        vehicleOwned: 0,
        vehicleOwnedAssigned: 0,
        vehicleOwnedUnassigned: 0,

        vehicleAvailable: 0,
        vehicleDeployed: 0,
        vehicleDeployable: 0,
        vehicleMaintenance: 0,
        vehicleOutOfService: 0
    };
    let customerVehicleNumsStr = '';
    if (userType && userType == CONTENT.USER_TYPE.CUSTOMER) {
        unit = null;
        subUnit = null;
        let vehicleNos = await sequelizeObj.query(`
            select l.vehicleNo from loan l where l.groupId=${userUnitId} and l.vehicleNo IS NOT NULL and now() BETWEEN l.startDate and l.endDate
        `, {
            type: QueryTypes.SELECT,
        });
        let groupVehicleNoList = await sequelizeObj.query(`
            select vv.vehicleNo from vehicle vv where vv.groupId=${userUnitId}
        `, { type: QueryTypes.SELECT });

        vehicleNos = vehicleNos.concat(groupVehicleNoList);

        for (let temp of vehicleNos) {
            customerVehicleNumsStr += temp.vehicleNo + ',';
        }
        if (!customerVehicleNumsStr) {
            customerVehicleNumsStr = 'nodata'
        }
    }

    let hqUserUnitNameList = [];
    if (userType == CONTENT.USER_TYPE.HQ) {
        let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(userHqUnit);
        hqUserUnitNameList = userUnitList.map(item => item.unit);
        hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
    }

    //query current assigned task
    async function buildVehicleTaskData() {
        let baseSql = `
            SELECT
                tt.vehicleStatus,
                count(*) as statusNum
            FROM task tt
            WHERE tt.driverId is not null and  (NOW() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.vehicleStatus = 'started')
        `;
        let replacements = [];
        if (unit) {
            baseSql += ` and tt.hub =? `;
            replacements.push(unit);
        } else if (userType == CONTENT.USER_TYPE.HQ) {
            if (hqUserUnitNameList.length > 0) {
                baseSql += ` and tt.hub in('${hqUserUnitNameList.join("','")}') `;
            } else {
                baseSql += ` and 1=2 `;
            }
        }
        if (subUnit) {
            if (subUnit == 'null') {
                baseSql += ` and tt.node is null `;
            } else {
                baseSql += ` and  tt.node =? `;
                replacements.push(subUnit);
            }
        }
        if (customerVehicleNumsStr) {
            baseSql += ` and FIND_IN_SET(tt.vehicleNumber, '${customerVehicleNumsStr}') `;
        }
        baseSql += ` GROUP BY tt.vehicleStatus`;
        
        let todayDataStat = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements })
        let todayTotal = 0;
        let todayTotalEffective = 0;
        let yetToStart = 0;
        function buildVehicleTaskNum(params) {
            for (let data of todayDataStat) {
                let num = Number(data.statusNum);
                todayTotal += num;
                if (data.vehicleStatus == 'completed') {
                    vehicleData.vehicleTodayCompleted = num;
                } else if (data.vehicleStatus == 'started') {
                    todayTotalEffective += num;
                    vehicleData.vehicleTodayStarted = num;
                } else if (data.vehicleStatus == 'Cancelled') {
                    vehicleData.vehicleTodayCancelled = num;
                } else {
                    todayTotalEffective += num;
                    yetToStart += num;
                }
            }
        }
        buildVehicleTaskNum();
        vehicleData.vehicleTodayYetToStart = yetToStart;
        vehicleData.vehicleTodayAssigned = todayTotal;
        vehicleData.vehicleTodayAssignedEffective = todayTotalEffective;
    }
    await buildVehicleTaskData();

    //query current assigned vehicleNo
    let currentAssignedVehicleNoBaseSql = `
        SELECT
            DISTINCT tt.vehicleNumber
        FROM task tt
        WHERE tt.vehicleStatus != 'Cancelled' and tt.vehicleStatus != 'completed' and tt.driverId IS NOT NULL and 
        (NOW() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.vehicleStatus = 'started')
    `;
    let currentVehicleNos = await sequelizeObj.query(currentAssignedVehicleNoBaseSql, { type: QueryTypes.SELECT })
    let currentVehicleNumStr = '';
    if (currentVehicleNos && currentVehicleNos.length > 0) {
        for (let temp of currentVehicleNos) {
            currentVehicleNumStr += temp.vehicleNumber + ',';
        }
    }

    //loan out vehicle
    let currentLoanOutVehicleList = await sequelizeObj.query(`
        select l.vehicleNo from loan l where l.vehicleNo IS NOT NULL and now() BETWEEN l.startDate and l.endDate
    `, { type: QueryTypes.SELECT , replacements: []})
    let currentLoanOutVehicleNos = '';
    if (currentLoanOutVehicleList && currentLoanOutVehicleList.length > 0) {
        for (let temp of currentLoanOutVehicleList) {
            currentLoanOutVehicleNos += temp.vehicleNo + ','
        }
    }

    //query all vehicle owned
    let baseSql = `
        select vv.currentStatus, count(*) as statusSum from (
            SELECT
                IF(hh.toHub is NULL, un.unit, hh.toHub) as currentUnit,
                IF(hh.toHub is NULL, un.subUnit, hh.toNode) as currentSubUnit,
    `;
    let replacements = [];
    function buildVehicleStatusSql() {
        if (userType == CONTENT.USER_TYPE.CUSTOMER) {
            baseSql += `
                IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(FIND_IN_SET(veh.vehicleNo, '${currentVehicleNumStr}'), 'Deployed', 'Deployable')
                ) as currentStatus
            `;
        } else {
            baseSql += `
                IF(FIND_IN_SET(veh.vehicleNo, '${currentLoanOutVehicleNos}'), 'Loan Out', 
                    IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                            IF(FIND_IN_SET(veh.vehicleNo, '${currentVehicleNumStr}'), 'Deployed', 'Deployable')
                    )
                ) as currentStatus
            `;
        }
        baseSql += `
                FROM
                    vehicle veh
                left join unit un on un.id = veh.unitId
                left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and NOW() BETWEEN ho.startDateTime AND ho.endDateTime) hh ON hh.vehicleNo = veh.vehicleNo
                left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status=1 and NOW() BETWEEN vl.startTime AND vl.endTime) ll ON ll.vehicleNo = veh.vehicleNo
        `;
        if (customerVehicleNumsStr) {
            baseSql += ` where FIND_IN_SET(veh.vehicleNo, '${customerVehicleNumsStr}') `
        }
        baseSql +=`
                GROUP BY veh.vehicleNo
            ) vv where 1=1
        `
        if (unit) {
            baseSql += ` and vv.currentUnit =? `;
            replacements.push(unit)
        } else if (userType == CONTENT.USER_TYPE.HQ) {
            if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                baseSql += ` and vv.currentUnit in('${hqUserUnitNameList.join("','")}') `;
            } else {
                baseSql += ` and 1=2 `;
            }
        }
        if (subUnit) {
            if (subUnit == 'null') {
                baseSql += ` and vv.currentSubUnit is null `;
            } else {
                baseSql += ` and vv.currentSubUnit =? `;
                replacements.push(subUnit)
            }
        }
        baseSql +=` group by vv.currentStatus`;
    }
    buildVehicleStatusSql();
    let vehicleQueryResult = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements })
    let vehicleTotal = 0;
    let todayVehicleDeployable = 0;
    let todayVehicleDeployed = 0;
    let todayVehicleOutOfService = 0;
    let todayVehicleMaintenance = 0;
    let todayVehicleLoanout = 0;
    function buildVehicleStatusData() {
        for (let data of vehicleQueryResult) {
            vehicleTotal += data.statusSum
            if (data.currentStatus == 'Deployed') {
                todayVehicleDeployed = data.statusSum
            } else if (data.currentStatus == 'Out Of Service') {
                todayVehicleOutOfService = data.statusSum
            } else if (data.currentStatus == 'Under Maintenance') {
                todayVehicleMaintenance = data.statusSum
            } else if (data.currentStatus == 'Loan Out') {
                todayVehicleLoanout = data.statusSum
            } else {
                todayVehicleDeployable = data.statusSum
            }
        }
    }
    buildVehicleStatusData();

    vehicleData.vehicleDeployable = todayVehicleDeployable
    vehicleData.vehicleDeployed = todayVehicleDeployed
    vehicleData.vehicleMaintenance = todayVehicleMaintenance
    vehicleData.vehicleOutOfService = todayVehicleOutOfService
    vehicleData.vehicleLoanout = todayVehicleLoanout
    vehicleData.vehicleOwned = vehicleTotal;

    return vehicleData;
}

const statDriverData = async function(unit, subUnit, userType, userUnitId, userHqUnit) {
    let tsOperatorData = {
        tsOperatorTodayAssigned: 0,
        tsOperatorTodayAssignedEffective: 0,
        todayDriverDistinctTotal: 0,
        tsOperatorTodayYetToStart: 0,
        tsOperatorTodayStarted: 0,
        tsOperatorTodayCompleted: 0,
        tsOperatorTodayCancelled: 0,
        tsOperatorOwned: 0,
        tsOperatorOwnedAssigned: 0,
        tsOperatorOwnedUnassigned: 0,

        tsOperatorAvailable: 0,
        tsOperatoreDeployed: 0,
        tsOperatorDeployable: 0,
        tsOperatorOnleave: 0
    };

    let customerUnitId = '';
    if (userType && userType == CONTENT.USER_TYPE.CUSTOMER) {
        unit = null;
        subUnit = null;
        customerUnitId = userUnitId;
    }

    let hqUserUnitNameList = [];
    if (userType == CONTENT.USER_TYPE.HQ) {
        let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(userHqUnit);
        hqUserUnitNameList = userUnitList.map(item => item.unit);
        hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
    }

    //query current assigned tsOperator
    let baseSql = `
        SELECT
            tt.driverStatus, lo.groupId,
            count(*) as statusNum
        FROM
            task tt
        LEFT JOIN user us ON tt.driverId=us.driverId
        LEFT JOIN driver dd ON tt.driverId = dd.driverId
        left join (select l.driverId, l.groupId, l.indentId from loan l where now() BETWEEN l.startDate and l.endDate) lo ON lo.driverId = tt.driverId
        WHERE tt.driverId is not null and tt.driverId != '' and (NOW() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
    `;
    let replacements = [];
    function buildTOTaskDataSql() {
        if (customerUnitId) {
            baseSql += ` and ((dd.groupId is not null and dd.groupId = ?) or lo.groupId = ?)`;
            replacements.push(customerUnitId);
            replacements.push(customerUnitId);
        } else {
            baseSql += ` and us.role='TO' `;
        }
        if (unit) {
            baseSql += ` and tt.hub =?`;
            replacements.push(unit);
        } else if (userType == CONTENT.USER_TYPE.HQ) {
            if (hqUserUnitNameList.length > 0) {
                baseSql += ` and tt.hub in('${hqUserUnitNameList.join("','")}') `;
            } else {
                baseSql += ` and 1=2 `;
            }
        }
        if (subUnit) {
            if (subUnit == 'null') {
                baseSql += ` and tt.node is null `;
            } else {
                baseSql += ` and tt.node =? `;
                replacements.push(subUnit);
            }
        }
        baseSql += ` GROUP BY tt.driverStatus`;
    }
    buildTOTaskDataSql();

    let todayDataStat = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements })
    let todayTotal = 0;
    let todayTotalEffective = 0;
    let yetToStart = 0;
    function buildTOTaskDataNum() {
        for (let data of todayDataStat) {
            let num = Number(data.statusNum);
            todayTotal += num;
            if (data.driverStatus == 'completed') {
                tsOperatorData.tsOperatorTodayCompleted = num
            } else if (data.driverStatus == 'started') {
                todayTotalEffective += num;
                tsOperatorData.tsOperatorTodayStarted = num;
            } else if (data.driverStatus == 'Cancelled') {
                tsOperatorData.tsOperatorTodayCancelled = num;
            }  else {
                todayTotalEffective += num;
                yetToStart += num;
            }
        }
    }
    buildTOTaskDataNum();
    tsOperatorData.tsOperatorTodayYetToStart = yetToStart;
    tsOperatorData.tsOperatorTodayAssigned = todayTotal;
    tsOperatorData.tsOperatorTodayAssignedEffective = todayTotalEffective;

    //query current assigned driver
    let currentAssignedDriverIdBaseSql = `
        SELECT tt.driverId as driverId
        FROM task tt
        WHERE tt.driverId is not null and tt.driverId != '' and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed'
        and (NOW() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
        group by tt.driverId
    `;
    let currentDriverStat = await sequelizeObj.query(currentAssignedDriverIdBaseSql, { type: QueryTypes.SELECT })
    let currentTaskDriverIdsStr = '';
    if (currentDriverStat && currentDriverStat.length > 0) {
        for (let temp of currentDriverStat) {
            currentTaskDriverIdsStr += temp.driverId + ',';
        }
    }

    //loan out driver
    let currentLoanOutDriverList = await sequelizeObj.query(`
        select l.driverId from loan l where l.driverId IS NOT NULL and now() BETWEEN l.startDate and l.endDate
    `, { type: QueryTypes.SELECT , replacements: []})
    let currentLoanOutDriverIds = '';
    for (let temp of currentLoanOutDriverList) {
        currentLoanOutDriverIds += temp.driverId + ','
    }

    //query all driver owned
    baseSql = `
        select dd.currentStatus, count(*) as statusSum from (
            SELECT 
                IF(hh.toHub is NULL, u.unit, hh.toHub) as currentUnit, IF(hh.toHub is NULL, u.subUnit, hh.toNode) as currentSubUnit,
                lo.groupId,
    `
                if (userType != CONTENT.USER_TYPE.CUSTOMER) {
                    baseSql += `
                        IF(d.permitStatus = 'invalid', 'Invalid',
                            IF(FIND_IN_SET(d.driverId, '${currentLoanOutDriverIds}'), 'Loan Out', 
                                IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                    IF(FIND_IN_SET(d.driverId, '${currentTaskDriverIdsStr}'), 'Deployed', 'Deployable')
                                )
                            ) 
                        ) as currentStatus
                    `;
                } else {
                    baseSql += `
                        IF(d.permitStatus = 'invalid', 'Invalid',
                            IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
                                IF(FIND_IN_SET(d.driverId, '${currentTaskDriverIdsStr}'), 'Deployed', 'Deployable')
                            ) 
                        ) as currentStatus
                    `;
                }
            
    baseSql += `        
            FROM driver d 
            LEFT JOIN user us ON d.driverId=us.driverId
            LEFT JOIN unit u ON u.id = d.unitId
            left join (select ho.driverId, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and NOW() BETWEEN ho.startDateTime AND ho.endDateTime) hh ON hh.driverId = d.driverId
            left join (select dl.driverId, dl.reason from driver_leave_record dl where dl.status=1 and NOW() BETWEEN dl.startTime AND dl.endTime) ll ON ll.driverId = d.driverId
            left join (select l.driverId, l.groupId, l.indentId from loan l where NOW() >= l.startDate) lo ON lo.driverId = d.driverId
            where (d.operationallyReadyDate is null OR d.operationallyReadyDate > DATE_FORMAT(NOW(), '%Y-%m-%d'))
    `;
    replacements = [];
    function buildTOStatusSql() {
        if (customerUnitId) {
            baseSql += ` and ((d.groupId is not null and d.groupId = ?) or lo.groupId = ?)`;
            replacements.push(customerUnitId)
            replacements.push(customerUnitId)
        } else {
            baseSql += ` and us.role='TO' `;
        }
        baseSql += `
                group by d.driverId
            ) dd where 1=1 
        `
        if (unit) {
            baseSql += ` and dd.currentUnit =? `;
            replacements.push(unit)
        } else if (userType == CONTENT.USER_TYPE.HQ) {
            if (hqUserUnitNameList.length > 0) {
                baseSql += ` and dd.currentUnit in('${hqUserUnitNameList.join("','")}') `;
            } else {
                baseSql += ` and 1=2 `;
            }
        }
        if (subUnit) {
            if (subUnit == 'null') {
                baseSql += ` and dd.currentSubUnit is null `;
            } else {
                baseSql += ` and dd.currentSubUnit =? `;
                replacements.push(subUnit)
            }
            
        }
        baseSql += ` group by dd.currentStatus `;
    }
    buildTOStatusSql();
    let driverQueryResult = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements })
    let driverTotal = 0;
    let tsOperatorOwnedAssigned = 0;
    let tsOperatorOwnedUnassigned = 0;
    let tsOperatorOnleave = 0;
    let tsOperatorLoanout = 0;
    let tsOperatorInvalid = 0;
    function buildTOStatusNum() {
        for (let data of driverQueryResult) {
            driverTotal +=  data.statusSum
            if (data.currentStatus == 'Deployable') {
                tsOperatorOwnedUnassigned = data.statusSum
            } else if (data.currentStatus == 'On Leave') {
                tsOperatorOnleave = data.statusSum
            } else if (data.currentStatus == 'Loan Out') {
                tsOperatorLoanout = data.statusSum
            } else if (data.currentStatus == 'Invalid') {
                tsOperatorInvalid = data.statusSum
            } else {
                tsOperatorOwnedAssigned = data.statusSum
            }
        }
    }
    buildTOStatusNum();
    tsOperatorData.tsOperatorOwned = driverTotal;
    tsOperatorData.tsOperatorOnleave = tsOperatorOnleave;
    tsOperatorData.tsOperatorDeployable = tsOperatorOwnedUnassigned;
    tsOperatorData.tsOperatoreDeployed = tsOperatorOwnedAssigned;
    tsOperatorData.tsOperatoreLoanOut = tsOperatorLoanout;
    tsOperatorData.tsOperatorInvalid = tsOperatorInvalid;

    return tsOperatorData;
}