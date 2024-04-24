const log = require('../log/winston').logger('Report Service');
const utils = require('../util/utils');
const moment = require('moment');
const CONTENT = require('../util/content');
const conf = require('../conf/conf');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')
const userService = require('../services/userService.js');
const unitService = require('../services/unitService');

const _ = require('lodash');
const jsonfile = require('jsonfile');

const checkNumber = function (number) {
    return !(number == Infinity 
        || number == -Infinity 
        || number == null 
        || number == '' 
        || isNaN(number)
        || typeof number == 'undefined')
}
const getDateLength = function (date1, date2) {
    let tempDate1 = moment(date1).format('YYYY-MM-DD HH:mm:ss')
    let tempDate2 = moment(date2).format('YYYY-MM-DD HH:mm')
    let dateLength = moment(tempDate2).diff(moment(tempDate1), 'd') + 1;
    return Math.abs(dateLength)
}
const getQuarterMonthList = function (month) {
    const quarterList = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
        [10, 11, 12]
    ]

    if (!month) {
        throw Error(`(getQuarterMonthList): param ${ month } is not correct`)
    }
    // need -1 to find array index
    let quarter = quarterList[ moment(month).quarter() - 1 ];
    // need -1 to find month index
    return quarter.map(item => moment().month(item - 1))
}

const getServiceTypeIdList = async function (category) {
    let serviceTypeIdList = await sequelizeSystemObj.query(` SELECT * FROM service_type WHERE category = ? `, { type: QueryTypes.SELECT, replacements: [ category ] })
    return serviceTypeIdList.map(item => item.id)
}
const getIndentsData = async function (month, groupId) {
    try {
        // find out serviceType Id where category is 'MV'
        let serviceTypeIdList = await getServiceTypeIdList('MV');
        if (serviceTypeIdList.length) {
            let systemSql = `
                SELECT t.executionDate, t.executionTime, t.createdAt, t.updatedAt, t.taskStatus, 
                t.id, t.tripId, t.requestId, t.cancellationTime, r.purposeType, j.tripNo
                FROM job_task t
                LEFT JOIN job j ON j.id = t.tripId
                LEFT JOIN request r ON r.id = t.requestId
                WHERE t.executionDate LIKE ?
                AND r.groupId = ?
                AND j.serviceTypeId in ( ? )
            `
            return await sequelizeSystemObj.query(systemSql, { type: QueryTypes.SELECT, replacements: [ month.format('YYYY-MM') + '%', groupId, serviceTypeIdList ] })
        } else {
            log.warn(`(getIncidentsData) there is no service type category named 'MV'`)
            return []
        }        
    } catch (error) {
        log.error(`(getIndentsData)`, error)
        return []
    }
}
const getIndentsDataGroupByTripNo = async function (month, groupId) {
    try {
        // find out serviceType Id where category is 'MV'
        let serviceTypeIdList = await getServiceTypeIdList('MV');
        if (serviceTypeIdList.length) {
            let systemSql = `
                SELECT t.executionDate, t.executionTime, t.createdAt, t.updatedAt, t.taskStatus, 
                t.id, t.tripId, t.requestId, t.cancellationTime, r.purposeType, j.tripNo
                FROM job_task t
                LEFT JOIN job j ON j.id = t.tripId
                LEFT JOIN request r ON r.id = t.requestId
                WHERE t.executionDate LIKE ?
                AND r.groupId = ?
                AND j.serviceTypeId in ( ? )
                GROUP BY j.tripNo
            `
            return await sequelizeSystemObj.query(systemSql, { type: QueryTypes.SELECT, replacements: [ month.format('YYYY-MM') + '%', groupId, serviceTypeIdList ] })
        } else {
            log.warn(`(getIncidentsData) there is no service type category named 'MV'`)
            return []
        }        
    } catch (error) {
        log.error(`(getIndentsData)`, error)
        return []
    }
}
const getSafetyCount = async function (tripNoList) {
    try {
        let sosResult = await sequelizeObj.query(`
            SELECT COUNT(*) AS count 
            FROM sos s
            LEFT JOIN task t ON t.taskId = s.taskId 
            WHERE t.driverId IS NOT NULL
            AND t.indentId IN (?)
        `, { type: QueryTypes.SELECT, replacements: [ tripNoList ] })
        return sosResult[0].count
    } catch (error) {
        log.error(`(getIncidentsData)`, error)
        return 0
    }
}
const getOffenceCount = async function (tripNoList) {
    let result = {
        RapidAccQty: 0,
        HardBrakingQty: 0,
        SpeedingQty: 0,
    }
    try {
        let taskResult = await sequelizeObj.query(`
            SELECT t.taskId, t.driverId, t.vehicleNumber, t.indentId,
            t.mobileStartTime, t.mobileEndTime
            FROM task t  
            WHERE t.driverId IS NOT NULL
            AND t.mobileStartTime IS NOT NULL
            AND t.indentId IN (?)
        `, { type: QueryTypes.SELECT, replacements: [ tripNoList ] })
        for (let task of taskResult) {
            if (!task.mobileStartTime) {
                log.warn(`TaskID ${ task.taskId } do not started yet, will continue.`)
                continue;
            }
            let offenceSql = `
                SELECT COUNT(*) AS \`count\`, th.violationType
                FROM track_history th
                WHERE th.dataFrom = 'mobile'
                AND th.deviceId = '${ task.driverId }'
                AND th.vehicleNo = '${ task.vehicleNumber }'
                AND (
                    th.occTime >= '${ moment(task.mobileStartTime).format('YYYY-MM-DD HH:mm:ss') }' 
                    ${ task.mobileEndTime ? ` AND th.occTime <= '${ moment(task.mobileEndTime).format('YYYY-MM-DD HH:mm:ss') }' ` : '' }
                )
                GROUP BY th.violationType
            `
            let offenceResult = await sequelizeObj.query(offenceSql, { type: QueryTypes.SELECT })
            for (let offence of offenceResult) {
                if (offence.violationType == 'Rapid Acc') {
                    result.RapidAccQty = offence.count
                } else if (offence.violationType == 'Speeding') {
                    result.SpeedingQty = offence.count
                } else if (offence.violationType == 'Hard Braking') {
                    result.HardBrakingQty = offence.count
                }
            }
        }
        return result
    } catch (error) {
        log.error(`(getOffenceCount)`, error)
        return result
    }
}

const getIndentsReport = async function (month, groupId) {
    let result = {
        total: {
            averageQty: 0,
            preMonthQty: 0,
            currentMonthQty: 0,
            change: 0,
        },
        late: {
            averageQty: 0,
            preMonthQty: 0,
            currentMonthQty: 0,
            change: 0,
        },
        amendment: {
            averageQty: 0,
            preMonthQty: 0,
            currentMonthQty: 0,
            change: 0,
        },
        cancel: {
            averageQty: 0,
            preMonthQty: 0,
            currentMonthQty: 0,
            change: 0,
        }
    }
    const resultHandler = function () {
        // Calculate averageQty
        result.total.averageQty = (result.total.preMonthQty + result.total.currentMonthQty) / 2
        result.late.averageQty = (result.late.preMonthQty + result.late.currentMonthQty) / 2
        result.amendment.averageQty = (result.amendment.preMonthQty + result.amendment.currentMonthQty) / 2
        result.cancel.averageQty = (result.cancel.preMonthQty + result.cancel.currentMonthQty) / 2

        // Calculate change
        result.total.change = (result.total.currentMonthQty - result.total.preMonthQty) / result.total.preMonthQty
        result.late.change = (result.late.currentMonthQty - result.late.preMonthQty) / result.late.preMonthQty
        result.amendment.change = (result.amendment.currentMonthQty - result.amendment.preMonthQty) / result.amendment.preMonthQty
        result.cancel.change = (result.cancel.currentMonthQty - result.cancel.preMonthQty) / result.cancel.preMonthQty

        // If only preMonthQty = 0, update change to 1
        // If preMonthQty = 0 and currentMonthQty = 0, update change to 0
        const checkResult1 = function () {
            if (result.total.preMonthQty == 0) {
                if (result.total.currentMonthQty == 0) result.total.change = 0
                else result.total.change = 1
            }
            if (result.late.preMonthQty == 0) {
                if (result.late.currentMonthQty == 0) result.late.change = 0
                else result.late.change = 1
            }
        }
        const checkResult2 = function () {
            if (result.amendment.preMonthQty == 0) {
                if (result.amendment.currentMonthQty == 0) result.amendment.change = 0
                else result.amendment.change = 1
            }
            if (result.cancel.preMonthQty == 0) {
                if (result.cancel.currentMonthQty == 0) result.cancel.change = 0
                else result.cancel.change = 1
            }
        }
        checkResult1()
        checkResult2()

        result.total.change = checkNumber(result.total.change) ? result.total.change : 0
        result.late.change = checkNumber(result.late.change) ? result.late.change : 0
        result.amendment.change = checkNumber(result.amendment.change) ? result.amendment.change : 0
        result.cancel.change = checkNumber(result.cancel.change) ? result.cancel.change : 0

        result.total.change = Math.floor(result.total.change * 10000) / 100 + '%'
        result.late.change = Math.floor(result.late.change * 10000) / 100 + '%'
        result.amendment.change = Math.floor(result.amendment.change * 10000) / 100 + '%'
        result.cancel.change = Math.floor(result.cancel.change * 10000) / 100 + '%'

        return result;
    }
    const getIndentsResult = async function (target, month, groupId) { 
        let indents = await getIndentsData(month, groupId)
        for (let indent of indents) {
            result.total[target]++

            if (indent.taskStatus?.toLowerCase() == 'cancelled' && getDateLength(indent.cancellationTime, indent.executionDate + ' ' + indent.executionTime) < 7) {
                result.cancel[target]++;
            } else if (indent.taskStatus?.toLowerCase() !== 'cancelled') {
                // If both lat and amendment, select late, so judge late first here
                if (getDateLength(indent.createdAt, indent.executionDate + ' ' + indent.executionTime) < 7) {
                    result.late[target]++;
                } else if (getDateLength(indent.updatedAt, indent.executionDate + ' ' + indent.executionTime) < 7) {
                    result.amendment[target]++;
                }
            }
        }
    }

    try {
        let currentMonth = month
        let preMonth = moment(currentMonth.format('YYYY-MM')).subtract(1, 'month')
        await getIndentsResult('currentMonthQty', currentMonth, groupId)  
        await getIndentsResult('preMonthQty', preMonth, groupId)
        return resultHandler()
    } catch (error) {
        log.error(`(getIndentReport): `, error)
        return result
    }
}
const getIndentTypesReport = async function (month, groupId) {
    let result = {}
    try {
        let currentMonth = month
        let quarterMonthList = getQuarterMonthList(currentMonth)
        for (let quarterMonth of quarterMonthList) {
            let monthStr = quarterMonth.format('MMM');
            result[ monthStr ] = {
                Ops: 0,
                Operation: 0,
                Training: 0,
                Admin: 0,
            }
            let indents = await getIndentsData(quarterMonth, groupId)
            for (let indent of indents) {
                if (!indent.purposeType) {
                    log.warn(`(getIndentTypesReport): indent of job_task ${ indent.id } has no purpose type`)
                    continue;
                }
                for (let key in result[ monthStr ]) {
                    if (indent.purposeType.toLowerCase().startsWith(key.toLowerCase())) {
                        result[ monthStr ][ key ]++
                    }
                }
            }
        }
        return result
    } catch (error) {
        log.error(`(getIndentReport): `, error)
        return result
    }
}
const getIndentSafetyReport = async function (month, groupId) {
    let result = {}
    try {
        let currentMonth = month
        let quarterMonthList = getQuarterMonthList(currentMonth)
        for (let quarterMonth of quarterMonthList) {
            let monthStr = quarterMonth.format('MMM');
            result[ monthStr ] = {
                IncidentQty: 0,
                RapidAccQty: 0,
                HardBrakingQty: 0,
                SpeedingQty: 0,
            }
            // get tripNo for checkout task data
            let systemIndentsData = await getIndentsDataGroupByTripNo(quarterMonth, groupId)
            let tripNoList = systemIndentsData.map(item => item.tripNo)
            if (tripNoList.length) {
                // cover RapidAccQty, HardBrakingQty, SpeedingQty
                result[ monthStr ] = await getOffenceCount(tripNoList)
                // add key IncidentQty
                result[ monthStr ].IncidentQty = await getSafetyCount(tripNoList)
            } else {
                log.warn(`(getIndentSafetyReport): tripNoList is empty, return 0`)
            }
        }
        return result
    } catch (error) {
        log.error(`(getIndentSafetyReport): `, error)
        return result
    }
}
const getTOReport = async function (month, groupId) {
    let result = {
        toAndTLQty: 0,
        dvQty: 0,
        loaQty: 0,
    }
    // get current driver where assign to group task
    try {
        // search by system task
        let serviceTypeIdList = await getServiceTypeIdList('MV');
        if (serviceTypeIdList.length) {
            let toResult = await sequelizeSystemObj.query(`
                SELECT t.driverId
                FROM job_task t
                LEFT JOIN job j ON j.id = t.tripId
                LEFT JOIN request r ON r.id = t.requestId
                WHERE t.executionDate LIKE ?
                AND r.groupId = ?
                AND j.serviceTypeId IN ( ? )
                AND t.driverId IS NOT NULL
                GROUP BY t.driverId
            `, { type: QueryTypes.SELECT, replacements: [ month.format('YYYY-MM') + '%', groupId, serviceTypeIdList ] })
            result.toAndTLQty = toResult.length
        } else {
            log.warn(`(getTOReport) there is no service type category named 'MV'`)
        }

        let dvLoaResult = await sequelizeObj.query(`
            SELECT COUNT(*) AS \`count\`, u.role 
            FROM \`user\` u
            LEFT JOIN driver d ON u.driverId = d.driverId
            WHERE u.userType = 'Mobile' 
            AND u.role IN ('DV', 'LOA') 
            AND d.groupId = ?
            GROUP BY u.role
        `, { type: QueryTypes.SELECT, replacements: [ groupId ] })

        if (dvLoaResult.length) {
            for (let r of dvLoaResult) {
                if (r.role.toLowerCase() == 'dv') {
                    result.dvQty += r.count
                } else if (r.role.toLowerCase() == 'loa') {
                    result.loaQty += r.count
                }
            }
        }

        return result
    } catch (error) {
        log.error(`(getTOReport)`, error)
        return result
    }
}
const getVehicleReport = async function (month, groupId) {
    let result = {
        Ops: 0,
        Operation: 0,
        Training: 0,
        Admin: 0,
    }
    // get current vehicle where assign to group task
    try {
        // search by system task
        let serviceTypeIdList = await getServiceTypeIdList('MV');
        if (serviceTypeIdList.length) {
            let vehicleResult = await sequelizeSystemObj.query(`
                SELECT COUNT(*) AS \`count\`, r.purposeType
                FROM job_task t
                LEFT JOIN job j ON j.id = t.tripId
                LEFT JOIN request r ON r.id = t.requestId
                LEFT JOIN vehicle v ON v.taskId = t.id
                WHERE t.executionDate LIKE ?
                AND r.groupId = ?
                AND j.serviceTypeId IN ( ? )
                AND v.vehicleNumber IS NOT NULL
                GROUP BY r.purposeType
            `, { type: QueryTypes.SELECT, replacements: [ month.format('YYYY-MM') + '%', groupId, serviceTypeIdList ] })
            for (let r of vehicleResult) {
                for (let key in result) {
                    if (r.purposeType.toLowerCase().startsWith(key.toLowerCase())) {
                        result[ key ] += r.count
                    }
                }
            }
        } else {
            log.warn(`(getVehicleReport) there is no service type category named 'MV'`)
        }
        
        return result
    } catch (error) {
        log.error(`(getVehicleReport)`, error)
        return result
    }
}

const getUnitFeedback = async function (month, groupId) {
    let commentResult = []
    try {
        // find out serviceType Id where category is 'MV'
        let serviceTypeIdList = await getServiceTypeIdList('MV');
        if (serviceTypeIdList.length) {
            let sql = `
                SELECT c.*, u.username, u.nric 
                FROM \`comment\` c
                LEFT JOIN \`user\` u ON u.id = c.createdBy
                LEFT JOIN job_task t ON t.id = c.taskId
                LEFT JOIN job j ON j.id = t.tripId
                LEFT JOIN request r ON r.id = t.requestId
                WHERE t.executionDate LIKE ?
                AND r.groupId = ?
                AND j.serviceTypeId IN ( ? )
                AND c.dataFrom = 'SYSTEM'
            `
            commentResult = await sequelizeSystemObj.query(sql, { type: QueryTypes.SELECT, replacements: [ month.format('YYYY-MM') + '%', groupId, serviceTypeIdList ] })
        }
        return commentResult
    } catch (error) {
        log.error(`(getUnitFeedback)`, error)
        return commentResult
    }
}
const getTOSFeedback = async function (tripNoList) {
    let commentResult = []
    try {
        if (tripNoList.length) {
            let sql = `
                SELECT c.*, t.indentId, d.driverName, d.nric
                FROM \`comment\` c
                LEFT JOIN task t ON t.taskId = c.taskId
                LEFT JOIN driver d ON d.driverId = t.driverId
                WHERE t.driverId IS NOT NULL 
                AND t.indentId IN (?)
            `
            commentResult = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: [ tripNoList ] })
        } else {
            log.warn(`(getTOSFeedback) tripNoList is empty.`)
        }
        return commentResult
    } catch (error) {
        log.error(`(getTOSFeedback)`, error)
        return commentResult
    }
}

const getUserUnitInfos = async function(user, selectedHub, selectedNode) {
    //user unit info
    let unitInfoSql = `
        select un.id, un.unit, un.subUnit from unit un where 1=1 
    `;
    let replacements = [];
    if (selectedHub) {
        unitInfoSql += ` and un.unit = ? `;
        replacements.push(selectedHub);
    } else if (user.userType == CONTENT.USER_TYPE.HQ) {
        let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
        let hqUserUnitNameList = userUnitList.map(item => item.unit);
        if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
            hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
        }
        if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
            unitInfoSql += ` and un.unit in('${hqUserUnitNameList.join("','")}') `;
        } else {
            return resultDataList;
        }
    }
    if (selectedNode) {
        if (selectedNode == '-') {
            unitInfoSql += ` and un.subUnit is null `;
        } else {
            unitInfoSql += ` and un.subUnit = ? `;
            replacements.push(selectedNode);
        }
    }
    unitInfoSql += ` ORDER BY un.unit, un.subUnit `;
    return await sequelizeObj.query(unitInfoSql, { type: QueryTypes.SELECT, replacements })
}

const getUnitTOList = async function(selectedHub, selectedNode, dbForamt, dateStr) {
    let unitToInfoSql = `
        SELECT
            d.driverId,
            d.unitId
        FROM driver d
        LEFT JOIN USER u ON d.driverId = u.driverId
        LEFT JOIN unit un ON d.unitId = un.id
        WHERE u.role = 'TO' AND d.unitId IS NOT NULL AND (d.operationallyReadyDate is NULL OR DATE_FORMAT(d.operationallyReadyDate, ?) >= ?) 
    `;
    let replacements = [dbForamt, dateStr];
    if (selectedHub) {
        unitToInfoSql += ` and un.unit = ? `;
        replacements.push(selectedHub);
    }
    if (selectedNode) {
        if (selectedNode == '-') {
            unitToInfoSql += ` and un.subUnit is null `;
        } else {
            unitToInfoSql += ` and un.subUnit = ? `;
            replacements.push(selectedNode);
        }
    }
    return await sequelizeObj.query(unitToInfoSql, { type: QueryTypes.SELECT, replacements })
}

const getUnitVehicleList = async function(selectedHub, selectedNode, vehicleType) {
    let unitVehicleInfoSql = `
        select
            vv.unitId,
            vv.vehicleType,
            vv.vehicleNo
        from vehicle vv
        LEFT JOIN unit un ON vv.unitId = un.id
        WHERE vv.unitId is NOT NULL
    `;
    let replacements = [];
    if (selectedHub) {
        unitVehicleInfoSql += ` and un.unit = ? `;
        replacements.push(selectedHub);
    }
    if (selectedNode) {
        if (selectedNode == '-') {
            unitVehicleInfoSql += ` and un.subUnit is null `;
        } else {
            unitVehicleInfoSql += ` and un.subUnit = ? `;
            replacements.push(selectedNode);
        }
    }
    if (vehicleType) {
        unitVehicleInfoSql += ` and vv.vehicleType = ? `;
        replacements.push(vehicleType);
    }

    return await sequelizeObj.query(unitVehicleInfoSql, { type: QueryTypes.SELECT, replacements })
}

const getTOUtilisationReport = async function (user, selectedYear, selectedMonth, selectedHub, selectedNode) {
    let resultDataList = {};
    try {
        let allUnitToNumber = 0;
        let allUnitTaskNumber = 0;
        let allUnitPlanWorkDaysSelf = 0;
        let allUnitPlanWorkDaysOthers = 0;
        let allUnitActualWorkDaysSelf = 0;
        let allUnitActualWorkDaysOthers = 0;
        let allUnitToLeaveDays = 0;
        let allUnitToHotoOutDays = 0;
        let allUnitToHotoInDays = 0;

        let dbForamt = '%Y-%m';
        let dateStr = '';
        let monthWorkDayNum = 0;
        async function initDateInfo() {
            if (selectedMonth) {
                dateStr = selectedYear + '-' + selectedMonth;
            } else {
                dbForamt = '%Y';
                dateStr = selectedYear;
            }
    
    
            let monthWorkDayList = [] ;
            if (selectedMonth) {
                monthWorkDayList = await utils.getMonthWeekdays(dateStr);
            } else {
                monthWorkDayList = await utils.getYearWeekdays(dateStr);
            }
            monthWorkDayNum = monthWorkDayList.length;
        }
        await initDateInfo();
        resultDataList = { totalData: {
            unitId: 'All',
            unitFullName: 'All',
            hub: '',
            node: '',
            toNumber: allUnitToNumber,
            taskNumber: allUnitTaskNumber,
            planWorkDaysSelf: allUnitPlanWorkDaysSelf,
            planWorkDaysOthers: allUnitPlanWorkDaysOthers,
            actualWorkDaysSelf: allUnitActualWorkDaysSelf,
            actualWorkDaysOthers: allUnitActualWorkDaysOthers,
            toLeaveDays: allUnitToLeaveDays,
            toHotoOutDays: allUnitToHotoOutDays,
            toHotoInDays: allUnitToHotoInDays,
            monthWorkDays: monthWorkDayNum
        }, unitDataList: []};

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER || user.userType == CONTENT.USER_TYPE.LICENSING_OFFICER) {
            return resultDataList;
        }
        let result = initVehicleUnitInfo(user);
        selectedHub = result.selectedHub ? result.selectedHub : selectedHub
        selectedNode = result.selectedNode ? result.selectedNode : selectedNode

        //user unit info
        let unitInfoResult = await getUserUnitInfos(user, selectedHub, selectedNode);

        let resultUnitDataList = [];
        for(let unit of unitInfoResult) {
            resultUnitDataList.push({
                unitId: unit.id,
                unitFullName: unit.unit + '/' + (unit.subUnit ? unit.subUnit : '-'),
                hub: unit.unit,
                node: unit.subUnit ? unit.subUnit : '-',
                toNumber: 0,
                toIdList: [],
                toDatas: [],
                taskNumber: 0,
                planWorkDaysSelf: 0,
                planWorkDaysOthers: 0,
                actualWorkDaysSelf: 0,
                actualWorkDaysOthers: 0,
                toLeaveDays: 0,
                toHotoOutDays: 0,
                toHotoInDays: 0,
                monthWorkDays: monthWorkDayNum
            });
        }
        if (resultUnitDataList.length == 0) {
            return resultDataList;
        }

        //stat TO driver numbers
        let unitToInfoResult = await getUnitTOList(selectedHub, selectedNode, dbForamt, dateStr);

        const updateUnitInfo = function () {
            for (let unitInfo of resultUnitDataList) {
                let unitToInfoList = unitToInfoResult.filter(item => item.unitId == unitInfo.unitId);
                if (unitToInfoList.length) {
                    allUnitToNumber += unitToInfoList.length;
                    unitInfo.toNumber = unitToInfoList.length;
                    unitInfo.toIdList = unitToInfoList.map(toInfo => toInfo.driverId);
                }
            }
        }
        updateUnitInfo()

        // month to workdays data
        let toMonthWorkdaysSql = `
            SELECT ds.*
            FROM driver_month_workdays_stat ds
            LEFT JOIN driver d ON d.driverId = ds.driverId
            LEFT JOIN unit un ON d.unitId = un.id
            WHERE ds.month LIKE ?
        `;
        let toMonthWorkdaysData = await sequelizeObj.query(toMonthWorkdaysSql, { type: QueryTypes.SELECT, replacements: [dateStr+"%"] });

        for (let unitInfo of resultUnitDataList) {
            let unitToIdList = unitInfo.toIdList;
            let unitToDataList = [];
            if (unitToIdList.length) {
                let planWorkDaysSelf = 0;
                let planWorkDaysOthers = 0;
                let actualWorkDaysSelf = 0;
                let actualWorkDaysOthers = 0;
                let toLeaveDays = 0;
                let toHotoOutDays = 0;
                let unitTaskNumber = 0;
                function buildTOStatInfo() {
                    for (let toId of unitToIdList) {
                        let toWorkTimeInfoList = toMonthWorkdaysData.filter(item => item.driverId == toId);
                        for (let toWorkTimeInfo of toWorkTimeInfoList) {
                            unitToDataList.push(toWorkTimeInfo);
    
                            unitTaskNumber += toWorkTimeInfo.taskNum;
                            toLeaveDays += toWorkTimeInfo.leaveDays;
                            toHotoOutDays += toWorkTimeInfo.hotoOutDays;
                            if (toWorkTimeInfo.driverUnitId == toWorkTimeInfo.workUnitId) {
                                planWorkDaysSelf += toWorkTimeInfo.planWorkDays;
                                actualWorkDaysSelf += toWorkTimeInfo.actualWorkDays;
                            } else {
                                planWorkDaysOthers += toWorkTimeInfo.planWorkDays;
                                actualWorkDaysOthers += toWorkTimeInfo.actualWorkDays;
                            }
                        }
                    }
                }
                buildTOStatInfo();
                unitInfo.toDatas = unitToDataList;
                unitInfo.taskNumber = unitTaskNumber;
                unitInfo.planWorkDaysSelf = planWorkDaysSelf;
                unitInfo.planWorkDaysOthers = planWorkDaysOthers;
                unitInfo.actualWorkDaysSelf = actualWorkDaysSelf;
                unitInfo.actualWorkDaysOthers = actualWorkDaysOthers;
                unitInfo.toLeaveDays = toLeaveDays;
                unitInfo.toHotoOutDays = toHotoOutDays;

                allUnitTaskNumber += unitTaskNumber;
                allUnitToLeaveDays += toLeaveDays;
                allUnitToHotoOutDays += toHotoOutDays;
                
                allUnitPlanWorkDaysSelf += planWorkDaysSelf;
                allUnitPlanWorkDaysOthers += planWorkDaysOthers;
                allUnitActualWorkDaysSelf += actualWorkDaysSelf;
                allUnitActualWorkDaysOthers += actualWorkDaysOthers;
            }
        }
        
        let allUnitInfoData = {
            unitId: 'All',
            unitFullName: 'All',
            hub: '',
            node: '',
            toNumber: allUnitToNumber,
            taskNumber: allUnitTaskNumber,
            planWorkDaysSelf: allUnitPlanWorkDaysSelf,
            planWorkDaysOthers: allUnitPlanWorkDaysOthers,
            actualWorkDaysSelf: allUnitActualWorkDaysSelf,
            actualWorkDaysOthers: allUnitActualWorkDaysOthers,
            toLeaveDays: allUnitToLeaveDays,
            toHotoOutDays: allUnitToHotoOutDays,
            toHotoInDays: allUnitToHotoInDays,
            monthWorkDays: monthWorkDayNum
        }

        resultDataList = { totalData: allUnitInfoData, unitDataList: resultUnitDataList};

        return resultDataList;
    } catch (error) {
        log.error(`getTOUtilisationReport`, error)
        return resultDataList;
    }
}

function initVehicleUnitInfo(user) {
    let selectedHub = null, selectedNode = null
    if (user.userType == CONTENT.USER_TYPE.UNIT) {
        if (user.unit) {
            selectedHub = user.unit;
        }
        if (user.subUnit) {
            selectedNode = user.subUnit;
        }
    }
    return { selectedHub, selectedNode }
}

const getVehicleUtilisationReport = async function (user, selectedYear, selectedMonth, selectedHub, selectedNode, vehicleType) {
    let resultDataList = {};
    try {
        let allUnitVehicleNumber = 0;
        let allUnitTaskNumber = 0;
        let allUnitPlanWorkDaysSelf = 0;
        let allUnitPlanWorkDaysOthers = 0;
        let allUnitActualWorkDaysSelf = 0;
        let allUnitActualWorkDaysOthers = 0;
        let allUnitVehicleLeaveDays = 0;
        let allUnitVehicleHotoOutDays = 0;
        let allUnitVehicleHotoInDays = 0;

        let dateStr = '';
        let monthWorkDayNum = 0;
        async function initDateInfo() {
            if (selectedMonth) {
                dateStr = selectedYear + '-' + selectedMonth;
            } else {
                dateStr = selectedYear;
            }
    
            let monthWorkDayList = [] ;
            if (selectedMonth) {
                monthWorkDayList = await utils.getMonthWeekdays(dateStr);
            } else {
                monthWorkDayList = await utils.getYearWeekdays(dateStr);
            }
            monthWorkDayNum = monthWorkDayList.length;
        }
        await initDateInfo();

        resultDataList = { totalData: {
            unitId: 'All',
            unitFullName: 'All',
            vehicleType: 'All',
            hub: '',
            node: '',
            vehicleNumber: allUnitVehicleNumber,
            taskNumber: allUnitTaskNumber,
            planWorkDaysSelf: allUnitPlanWorkDaysSelf,
            planWorkDaysOthers: allUnitPlanWorkDaysOthers,
            actualWorkDaysSelf: allUnitActualWorkDaysSelf,
            actualWorkDaysOthers: allUnitActualWorkDaysOthers,
            vehicleLeaveDays: allUnitVehicleLeaveDays,
            vehicleHotoOutDays: allUnitVehicleHotoOutDays,
            monthWorkDays: monthWorkDayNum
        }, unitDataList: []};

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER || user.userType == CONTENT.USER_TYPE.LICENSING_OFFICER) {
            return resultDataList;
        }
        let result = initVehicleUnitInfo(user);
        selectedHub = result.selectedHub ? result.selectedHub : selectedHub
        selectedNode = result.selectedNode ? result.selectedNode : selectedNode

        //user unit info
        let unitInfoResult = await getUserUnitInfos(user, selectedHub, selectedNode)

        let vehicleTypeSql = `select vehicleName from vehicle_category where 1=1`;
        let replacements = [];
        if (vehicleType) {
            vehicleTypeSql += ` and vehicleName = ? `
            replacements.push(vehicleType);
        }
        let vehicleTypeList = await sequelizeObj.query(vehicleTypeSql, { type: QueryTypes.SELECT, replacements })

        let resultUnitDataList = [];
        const getResultUnitDataList = function () {
            for(let unit of unitInfoResult) {
                for (let vehicleType of vehicleTypeList) {
                    resultUnitDataList.push({
                        unitId: unit.id,
                        unitFullName: unit.unit + '/' + (unit.subUnit ? unit.subUnit : '-'),
                        hub: unit.unit,
                        node: unit.subUnit ? unit.subUnit : '-',
                        vehicleType: vehicleType.vehicleName,
                        vehicleNumber: 0,
                        vehicleNoList: [],
                        vehicleDatas: [],
                        taskNumber: 0,
                        planWorkDaysSelf: 0,
                        planWorkDaysOthers: 0,
                        actualWorkDaysSelf: 0,
                        actualWorkDaysOthers: 0,
                        vehicleLeaveDays: 0,
                        vehicleHotoOutDays: 0,
                        monthWorkDays: monthWorkDayNum
                    });
                }
            }
        }
        getResultUnitDataList();
        
        if (resultUnitDataList.length) {
            return resultDataList;
        }

        // filter vehicle number > 0 data.
        let newResultUnitDataList = [];
        async function buildNewUnitData() {
            let unitVehicleInfoResult = await getUnitVehicleList(selectedHub, selectedNode, vehicleType);
            for (let unitInfo of resultUnitDataList) {
                let unitVehicleInfoList = unitVehicleInfoResult.filter(item => item.unitId == unitInfo.unitId && item.vehicleType == unitInfo.vehicleType);
                if (unitVehicleInfoList?.length > 0) {
                    allUnitVehicleNumber += unitVehicleInfoList.length;
                    unitInfo.vehicleNumber = unitVehicleInfoList.length;
                    unitInfo.vehicleNoList = unitVehicleInfoList.map(vehicleInfo => vehicleInfo.vehicleNo);
    
                    newResultUnitDataList.push(unitInfo);
                }
            }
        }
        await buildNewUnitData();
        resultUnitDataList = newResultUnitDataList;

        // month to workdays data
        let vehicleMonthWorkdaysSql = `
            SELECT vs.*
            FROM vehicle_month_workdays_stat vs
            LEFT JOIN vehicle v ON vs.vehicleNo = v.vehicleNo
            LEFT JOIN unit un ON v.unitId = un.id
            WHERE v.unitId IS NOT NULL AND vs.month LIKE ?
        `;
        let vehicleMonthWorkdaysData = await sequelizeObj.query(vehicleMonthWorkdaysSql, { type: QueryTypes.SELECT, replacements: [dateStr+"%"] });
        for (let unitInfo of resultUnitDataList) {
            let unitVehicleNoList = unitInfo.vehicleNoList;
            let unitVehicleDataList = [];
            function buildVehicleStatData() {
                if (unitVehicleNoList?.length > 0) {
                    let planWorkDaysSelf = 0;
                    let planWorkDaysOthers = 0;
                    let actualWorkDaysSelf = 0;
                    let actualWorkDaysOthers = 0;
                    let vehicleLeaveDays = 0;
                    let vehicleHotoOutDays = 0;
                    let unitTaskNumber = 0;
                    
                    for (let vehicleNo of unitVehicleNoList) {
                        let vehicleWorkTimeInfoList = vehicleMonthWorkdaysData.filter(item => item.vehicleNo == vehicleNo);
                        for (let vehicleWorkTimeInfo of vehicleWorkTimeInfoList) {
                            unitVehicleDataList.push(vehicleWorkTimeInfo);

                            unitTaskNumber += vehicleWorkTimeInfo.taskNum;
                            vehicleLeaveDays += vehicleWorkTimeInfo.eventDays;
                            vehicleHotoOutDays += vehicleWorkTimeInfo.hotoOutDays;

                            if (vehicleWorkTimeInfo.vehicleUnitId == vehicleWorkTimeInfo.workUnitId) {
                                planWorkDaysSelf += vehicleWorkTimeInfo.planWorkDays;
                                actualWorkDaysSelf += vehicleWorkTimeInfo.actualWorkDays;
                            } else {
                                planWorkDaysOthers += vehicleWorkTimeInfo.planWorkDays;
                                actualWorkDaysOthers += vehicleWorkTimeInfo.actualWorkDays;
                            }
                        }
                    }
                    unitInfo.vehicleDatas = unitVehicleDataList;
                    unitInfo.taskNumber = unitTaskNumber;
                    unitInfo.vehicleLeaveDays = vehicleLeaveDays;
                    unitInfo.vehicleHotoOutDays = vehicleHotoOutDays;
                    unitInfo.planWorkDaysSelf = planWorkDaysSelf;
                    unitInfo.planWorkDaysOthers = planWorkDaysOthers;
                    unitInfo.actualWorkDaysSelf = actualWorkDaysSelf;
                    unitInfo.actualWorkDaysOthers = actualWorkDaysOthers;

                    allUnitTaskNumber += unitTaskNumber;
                    allUnitVehicleLeaveDays += vehicleLeaveDays;
                    allUnitVehicleHotoOutDays += vehicleHotoOutDays;
                    allUnitPlanWorkDaysSelf += planWorkDaysSelf;
                    allUnitPlanWorkDaysOthers += planWorkDaysOthers;
                    allUnitActualWorkDaysSelf += actualWorkDaysSelf;
                    allUnitActualWorkDaysOthers += actualWorkDaysOthers;
                }
            }
            buildVehicleStatData();
        }

        let allUnitInfoData = {
            unitId: 'All',
            unitFullName: 'All',
            vehicleType: 'All',
            hub: '',
            node: '',
            vehicleNumber: allUnitVehicleNumber,
            taskNumber: allUnitTaskNumber,
            planWorkDaysSelf: allUnitPlanWorkDaysSelf,
            planWorkDaysOthers: allUnitPlanWorkDaysOthers,
            actualWorkDaysSelf: allUnitActualWorkDaysSelf,
            actualWorkDaysOthers: allUnitActualWorkDaysOthers,
            vehicleLeaveDays: allUnitVehicleLeaveDays,
            vehicleHotoOutDays: allUnitVehicleHotoOutDays,
            vehicleHotoInDays: allUnitVehicleHotoInDays,
            monthWorkDays: monthWorkDayNum
        }

        resultDataList = { totalData: allUnitInfoData, unitDataList: resultUnitDataList};

        return resultDataList;
    } catch (error) {
        log.error(`getVehicleUtilisationReport`, error)
        return resultDataList;
    }
}

const calcLicensingDataLastStatus = function(driverLicensingAppry, selectedYear, selectedMonth) {
    let dateFormat = 'YYYY';
    let selectDateStr = selectedYear;
    if (selectedMonth) {
        dateFormat = 'YYYY-MM';
        selectDateStr = selectedYear + '-' + selectedMonth;
    }
    let status = null;
    function calcStatus1() {
        if (driverLicensingAppry.failDate && moment(driverLicensingAppry.failDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Failed';
        }
        if (driverLicensingAppry.successDate && moment(driverLicensingAppry.successDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Success';
        }
        if (driverLicensingAppry.rejectDate && moment(driverLicensingAppry.rejectDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Rejected';
        }
        if (driverLicensingAppry.pendingDate && moment(driverLicensingAppry.pendingDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Pending';
        }
        return null;
    }
    status = calcStatus1();
    if (status) {
        return status;
    }
    function calcStatus2() {
        if (driverLicensingAppry.recommendDate && moment(driverLicensingAppry.recommendDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Recommended';
        }
        if (driverLicensingAppry.verifyDate && moment(driverLicensingAppry.verifyDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Verified';
        }
        if (driverLicensingAppry.endorseDate && moment(driverLicensingAppry.endorseDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Endorsed';
        }
        if (driverLicensingAppry.submitDate && moment(driverLicensingAppry.submitDate, dateFormat).format(dateFormat) == selectDateStr) {
            return 'Submitted';
        }
        return null;
    }
    status = calcStatus2();
    if (status) {
        return status;
    }

    return 'Pending Approval';
}

const getTOLicecsingReport1 = async function (user, selectedYear, selectedMonth, selectedHub, selectedNode, pageNum, pageLength) {
    try {
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER || user.userType == CONTENT.USER_TYPE.LICENSING_OFFICER) {
            return {data: [], totalCount: 0};
        }
        let selectedMonthStr = selectedMonth
        let dbForamt = '%Y-%m';
        let dateStr = '';

        function initParams1() {
            if (user.userType == CONTENT.USER_TYPE.UNIT) {
                if (user.unit) {
                    selectedHub = user.unit;
                }
                if (user.subUnit) {
                    selectedNode = user.subUnit;
                }
            }
            if (!selectedMonthStr) {
                selectedMonthStr = 'All';
            }
    
            if (selectedMonth) {
                dateStr = selectedYear + '-' + selectedMonth;
            } else {
                dbForamt = '%Y';
                dateStr = selectedYear;
            }
        }
        initParams1();

        //user unit info
        let unitInfoSql = `
            select un.id, un.unit, un.subUnit from unit un where 1=1 
        `;
        let replacements = [];
        async function initUnitSql() {
            if (selectedHub) {
                unitInfoSql += ` and un.unit = ? `;
                replacements.push(selectedHub);
            } else if (user.userType == CONTENT.USER_TYPE.HQ) {
                let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
                let hqUserUnitNameList = userUnitList.map(item => item.unit);
                if (hqUserUnitNameList.length > 0) {
                    hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
                    unitInfoSql += ` and un.unit in('${hqUserUnitNameList.join("','")}') `;
                } else {
                    return false;
                }
            }
            if (selectedNode) {
                if (selectedNode == '-') {
                    unitInfoSql += ` and un.subUnit is null `;
                } else {
                    unitInfoSql += ` and un.subUnit = ? `;
                    replacements.push(selectedNode);
                }
            }
            return true;
        }
        let result = await initUnitSql();
        if (!result) {
            return {data: [], totalCount: 0};
        }
        let unitInfoTotalSql = unitInfoSql;

        let unitInfoTotalResult = await sequelizeObj.query(unitInfoTotalSql, { type: QueryTypes.SELECT, replacements });
        let totalCount = unitInfoTotalResult.length || 0;

        replacements.push(pageNum);
        replacements.push(pageLength);
        unitInfoSql += ` ORDER BY un.unit, un.subUnit limit ?, ? `;
        let unitInfoResult = await sequelizeObj.query(unitInfoSql, { type: QueryTypes.SELECT, replacements });
        let resultUnitDataList = [];
        for(let unit of unitInfoResult) {
            resultUnitDataList.push({
                unitId: unit.id,
                unitFullName: unit.unit + '/' + (unit.subUnit ? unit.subUnit : '-'),
                hub: unit.unit,
                node: unit.subUnit ? unit.subUnit : '-',
                year: selectedYear,
                month: selectedMonthStr,
                appliedNo: 0,
                submittedNo: 0,
                endorsedNo: 0,
                verifiedNo: 0,
                recommendedNo: 0,
                rejectedNo: 0,
                pendingNo: 0,
                successedNo: 0,
                failedNo: 0,
                totalNo: 0,
                rejectedReasonSummary: '',
                rejectedReasonDatas: [],
                pendingReasonSummary: '',
                pendingReasonDatas: [],
                failedReasonSummary: '',
                failedReasonDatas: []
            });
        }
        if (resultUnitDataList.length == 0) {
            return {data: [], totalCount: 0};
        }

        //to licensing data
        let toLicensingInfoSql = `
            SELECT
                d.unitId, 
                un.unit,
                un.subUnit,
                d.driverName,
                ru.fullname as rejectUserName,
                pu.fullname as pendingUserName,
                fu.fullname as failUserName,
                dl.*
            FROM driver_license_exchange_apply dl
            LEFT JOIN driver d on dl.driverId = d.driverId
            LEFT JOIN unit un on un.id = d.unitId
            LEFT JOIN user ru ON ru.userId = dl.rejectBy
            LEFT JOIN user pu ON pu.userId = dl.pendingBy
            LEFT JOIN user fu ON fu.userId = dl.failBy
            where d.unitId is NOT NULL and (
                DATE_FORMAT(dl.applyDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.submitDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.endorseDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.verifyDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.recommendDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.rejectDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.successDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.failDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
                OR DATE_FORMAT(dl.pendingDate, '${dbForamt}') = `+sequelizeObj.escape(dateStr)+`
            )
        `;
        replacements = [];
        function buildSqlParams1() {
            if (selectedHub) {
                toLicensingInfoSql += ` and un.unit = ? `;
                replacements.push(selectedHub);
            }
            if (selectedNode) {
                if (selectedNode == '-') {
                    toLicensingInfoSql += ` and un.subUnit is null `;
                } else {
                    toLicensingInfoSql += ` and un.subUnit = ? `;
                    replacements.push(selectedNode);
                }
            }
        }
        buildSqlParams1();
        let toLicensingInfoResult = await sequelizeObj.query(toLicensingInfoSql + ` ORDER BY un.unit ASC, un.subUnit asc `, { type: QueryTypes.SELECT, replacements })

        for (let unitInfo of resultUnitDataList) {
            let appliedNo = 0;
            let submittedNo = 0;
            let endorsedNo = 0;
            let verifiedNo = 0;
            let recommendedNo = 0;
            let rejectedNo = 0;
            let pendingNo = 0;
            let successedNo = 0;
            let failedNo = 0;
            let totalNo = 0;

            let rejectedReasonDatas = [];
            let pendingReasonDatas = [];
            let failedReasonDatas = [];
            let rejectedReasonSummary = '';
            let pendingReasonSummary = '';
            let failedReasonSummary = '';

            let unitDriverLicensingList = toLicensingInfoResult.filter(item => item.unitId == unitInfo.unitId);
            function buildUnitStatInfo() {
                for (let driverLicensing of unitDriverLicensingList) {
                    //recalc licensing data select month last status
                    let selectedDateLastStatus = calcLicensingDataLastStatus(driverLicensing, selectedYear, selectedMonth);
    
                    totalNo++;
                    switch (selectedDateLastStatus) {
                        case 'Pending Approval':
                            appliedNo++;
                            break;
                        case 'Submitted':
                            submittedNo++;
                            break;
                        case 'Endorsed':
                            endorsedNo++;
                            break;
                        case 'Verified':
                            verifiedNo++;
                            break;
                        case 'Recommended':
                            recommendedNo++;
                            break;
                        case 'Success':
                            successedNo++;
                            break;
                        case 'Rejected':
                            rejectedNo++;
                            if (!rejectedReasonSummary && driverLicensing.rejectReason) {
                                rejectedReasonSummary += `${driverLicensing.applyId}:${driverLicensing.rejectReason}; `;
                            }
                            rejectedReasonDatas.push({
                                applyId: driverLicensing.applyId,
                                driverId: driverLicensing.driverId, 
                                driverName: driverLicensing.driverName,
                                permitType: driverLicensing.permitType,
                                optDate: driverLicensing.rejectDate,
                                optUserName: driverLicensing.rejectUserName,
                                reason: driverLicensing.rejectReason
                            });
                            break;
                        case 'Pending':
                            pendingNo++;
                            if (!pendingReasonSummary && driverLicensing.pendingReason) {
                                pendingReasonSummary += `${driverLicensing.applyId}:${driverLicensing.pendingReason}; `;
                            }
                            
                            pendingReasonDatas.push({
                                applyId: driverLicensing.applyId,
                                driverId: driverLicensing.driverId, 
                                driverName: driverLicensing.driverName,
                                permitType: driverLicensing.permitType,
                                optDate: driverLicensing.pendingDate,
                                optUserName: driverLicensing.pendingUserName,
                                reason: driverLicensing.pendingReason
                            });
                            break;
                        case 'Failed':
                            failedNo++;
                            if (!failedReasonSummary && driverLicensing.failReason) {
                                failedReasonSummary += `${driverLicensing.applyId}:${driverLicensing.failReason}; `;
                            }
                            
                            failedReasonDatas.push({
                                applyId: driverLicensing.applyId,
                                driverId: driverLicensing.driverId, 
                                driverName: driverLicensing.driverName,
                                permitType: driverLicensing.permitType,
                                optDate: driverLicensing.failDate,
                                optUserName: driverLicensing.failUserName,
                                reason: driverLicensing.failReason
                            });
                            break;
                    }
                }
            }
            buildUnitStatInfo();

            unitInfo.appliedNo = appliedNo;
            unitInfo.submittedNo = submittedNo;
            unitInfo.endorsedNo = endorsedNo;
            unitInfo.verifiedNo = verifiedNo;
            unitInfo.recommendedNo = recommendedNo;
            unitInfo.rejectedNo = rejectedNo;
            unitInfo.pendingNo = pendingNo;
            unitInfo.successedNo = successedNo;
            unitInfo.failedNo = failedNo;
            unitInfo.totalNo = totalNo;
            unitInfo.rejectedReasonDatas = rejectedReasonDatas;
            unitInfo.pendingReasonDatas = pendingReasonDatas;
            unitInfo.failedReasonDatas = failedReasonDatas;
            unitInfo.rejectedReasonSummary = rejectedReasonSummary;
            unitInfo.pendingReasonSummary = pendingReasonSummary;
            unitInfo.failedReasonSummary = failedReasonSummary;
        }

        return {data: resultUnitDataList, totalCount: totalCount};
    } catch (error) {
        log.error(`getTOLicecsingReport1`, error)
        return resultDataList;
    }
}

const getTOLicecsingReport2 = async function (params) {
    try {
        let {user, selectedYear, selectedMonth, permitType, selectedHub, selectedNode, pageNum, pageLength} = params;
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER || user.userType == CONTENT.USER_TYPE.LICENSING_OFFICER) {
            return {data: [], totalCount: 0};
        }
        let selectedMonthStr = selectedMonth
        let dbForamt = '%Y-%m';
        let dateStr = '';

        function initParams() {
            if (user.userType == CONTENT.USER_TYPE.UNIT) {
                if (user.unit) {
                    selectedHub = user.unit;
                }
                if (user.subUnit) {
                    selectedNode = user.subUnit;
                }
            }
            if (!selectedMonthStr) {
                selectedMonthStr = 'All';
            }
            if (selectedMonth) {
                dateStr = selectedYear + '-' + selectedMonth;
            } else {
                dbForamt = '%Y';
                dateStr = selectedYear;
            }
            log.info('')
        }
        initParams();

        let permitTypeSql = `select * from permittype where 1=1 and permitType like 'CL%' `;
        let permitTypeCountSql = `select count(*) as permitTypeNo from permittype where 1=1 and permitType like 'CL%' `;
        let replacements = [];
        if (permitType) {
            permitTypeSql += ` and permitType = ? `;
            permitTypeCountSql += ` and permitType = ? `;
            replacements.push(permitType);
        }
        let permitTypeCountData = await sequelizeObj.query(permitTypeCountSql + ' ORDER BY permitType ', { type: QueryTypes.SELECT, replacements });
        let totalCount = permitTypeCountData ? permitTypeCountData[0].permitTypeNo : 0;

        permitTypeSql += ` ORDER BY permitType limit ?, ? `;
        replacements.push(pageNum);
        replacements.push(pageLength);
        let resultDataList = [];
        let permitTypeList = await sequelizeObj.query(permitTypeSql, { type: QueryTypes.SELECT, replacements })
        for (let permitTypeInfo of permitTypeList) {
            resultDataList.push({
                year: selectedYear,
                month: selectedMonthStr,
                permitType: permitTypeInfo.permitType,
                issuedNo: 0,
                toNo: 0,
                tlNo: 0,
                dvNo: 0,
                loaNo: 0
            });
        }
        if (resultDataList.length == 0) {
            return {data: [], totalCount: 0};
        }

        //to licensing data
        let toLicensingInfoSql = `
            SELECT
                dp.permitType,
                us.role,
                count(*) as dataNum
            FROM driver_permittype_detail dp
            LEFT JOIN driver d on dp.driverId = d.driverId
            LEFT JOIN unit un on un.id = d.unitId
            LEFT JOIN user us on d.driverId = us.driverId
            where us.role in('TO', 'TL', 'DV', 'LOA') AND dp.approveStatus='Approved' and dp.permitType like 'CL%' and DATE_FORMAT(dp.passDate, '${dbForamt}') = ?
        `;
        replacements = [dateStr];
        async function buildSqlParams() {
            if (selectedHub) {
                toLicensingInfoSql += ` and un.unit = ? `;
                replacements.push(selectedHub);
            } else if (user.userType == CONTENT.USER_TYPE.HQ) {
                let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
                let hqUserUnitNameList = userUnitList.map(item => item.unit);
                if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                    hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
                    toLicensingInfoSql += ` and un.unit in('${hqUserUnitNameList.join("','")}') `;
                } else {
                    return false;
                }
            }
            if (selectedNode) {
                if (selectedNode == '-') {
                    toLicensingInfoSql += ` and un.subUnit is null `;
                } else {
                    toLicensingInfoSql += ` and un.subUnit = ? `;
                    replacements.push(selectedNode);
                }
            }
            return true;
        }
        let result = await buildSqlParams();
        if (!result) {
            return {data: [], totalCount: 0};
        }
       
        let toLicensingInfoResult = await sequelizeObj.query(toLicensingInfoSql + ` GROUP BY dp.permitType, us.role ORDER BY dp.permitType ASC `, { type: QueryTypes.SELECT, replacements })

        for (let permitTypeInfo of resultDataList) {
            let issuedNo = 0;

            let permitTypeLicensingList = toLicensingInfoResult.filter(item => item.permitType == permitTypeInfo.permitType);
            for (let permitTypeLicensing of permitTypeLicensingList) {
                switch (permitTypeLicensing.role) {
                    case 'TO':
                        issuedNo += permitTypeLicensing.dataNum;
                        permitTypeInfo.toNo = permitTypeLicensing.dataNum;
                        break;
                    case 'TL':
                        issuedNo += permitTypeLicensing.dataNum;
                        permitTypeInfo.tlNo = permitTypeLicensing.dataNum;
                        break;
                    case 'DV':
                        issuedNo += permitTypeLicensing.dataNum;
                        permitTypeInfo.dvNo = permitTypeLicensing.dataNum;
                        break;
                    case 'LOA':
                        issuedNo += permitTypeLicensing.dataNum;
                        permitTypeInfo.loaNo = permitTypeLicensing.dataNum;
                        break;
                }
            }
            permitTypeInfo.issuedNo = issuedNo;
        }

        return {data: resultDataList, totalCount: totalCount};
    } catch (error) {
        log.error(`getTOLicecsingReport1`, error)
        return resultDataList;
    }
}

const calcOpsSummary = async function(opsSummaryResult, startDate, endDate, allNodeList, supportHubList) {
    if (allNodeList.length > 0) {
        // all node task(system, mobile)
        let allHubTaskList = await sequelizeObj.query(`
            SELECT
                t.taskId, t.driverId, t.vehicleNumber as vehicleNo, t.hub, t.node, t.purpose,
                t.mobileStartTime,
                t.mobileEndTime
            FROM task t
            LEFT JOIN driver d on t.driverId = d.driverId
            LEFT JOIN user us on d.driverId = us.driverId
            where us.role='TO' and t.dataFrom != 'MT-ADMIN' and t.node is not null and t.mobileStartTime is NOT null and t.mobileEndTime is not NULL and t.hub in(?)
            AND DATE_FORMAT(t.mobileStartTime, '%Y-%m-%d') <= '${endDate}'
            AND DATE_FORMAT(t.mobileEndTime, '%Y-%m-%d') >= '${startDate}'
            AND (UPPER(t.purpose) LIKE 'OPS%' OR UPPER(t.purpose) LIKE 'TRAINING%' OR UPPER(t.purpose) LIKE 'ADMIN%')
        `, { type: QueryTypes.SELECT, replacements: [supportHubList] });

        let allHubAtmsTaskList = await sequelizeObj.query(`
            SELECT
                t.taskId, t.driverId, t.vehicleNumber as vehicleNo, t.hub, t.node, 'TRAINING' as purpose,
                t.mobileStartTime,
                t.mobileEndTime
            FROM task t
            LEFT JOIN mt_admin m on t.dataFrom='MT-ADMIN' and t.indentId = m.id
            LEFT JOIN driver d on t.driverId = d.driverId
            LEFT JOIN user us on d.driverId = us.driverId
            where us.role='TO' and m.id is not null and m.dataType='mb' and t.node is not null and t.mobileStartTime is NOT null and t.mobileEndTime is not NULL and t.hub in(?)
            AND DATE_FORMAT(t.mobileStartTime, '%Y-%m-%d') <= '${endDate}'
            AND DATE_FORMAT(t.mobileEndTime, '%Y-%m-%d') >= '${startDate}'
        `, { type: QueryTypes.SELECT, replacements: [supportHubList] });

        allHubTaskList = allHubTaskList.concat(allHubAtmsTaskList);

        let allHubToList = await sequelizeObj.query(`
            select 
                d.driverId, d.driverName, d.unitId, un.unit, IFNULL(un.subUnit, '-') as subUnit
            from driver d LEFT JOIN unit un on d.unitId = un.id
            LEFT JOIN user us on d.driverId = us.driverId
            where us.role='TO' and d.unitId is not NULL AND un.unit in(?)
            and (d.operationallyReadyDate is null or d.operationallyReadyDate >= '${startDate}') 
        `, { type: QueryTypes.SELECT, replacements: [supportHubList] });

        let allHubToLeaveList = await sequelizeObj.query(`
            SELECT
                dl.driverId,
                dl.startTime,
                dl.endTime,
                IF (dayType = 'all', 1, 0.5) AS leaveDays
            FROM driver_leave_record dl
            LEFT JOIN driver d on dl.driverId = d.driverId
            LEFT JOIN user us on d.driverId = us.driverId
            LEFT JOIN unit un on d.unitId = un.id
            WHERE dl.STATUS = 1 and d.unitId is not null and us.role='TO'
            AND DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN '${startDate}' and '${endDate}'
            AND un.unit in(?)
            GROUP BY dl.driverId, DATE_FORMAT(startTime, '%Y-%m-%d')
        `, { type: QueryTypes.SELECT, replacements: [supportHubList] });

        let allHubVehicleList = await sequelizeObj.query(`
            select 
                veh.vehicleNo, veh.unitId, un.unit, IFNULL(un.subUnit, '-') as subUnit
            from vehicle veh LEFT JOIN unit un on veh.unitId = un.id
            where veh.unitId is not NULL and un.unit in(?)
        `, { type: QueryTypes.SELECT, replacements: [supportHubList] });

        let allHubVehicleEventList = await sequelizeObj.query(`
            SELECT
                vl.vehicleNo,
                vl.startTime,
                vl.endTime,
                IF (dayType = 'all', 1, 0.5) AS leaveDays
            FROM vehicle_leave_record vl
            LEFT JOIN vehicle veh on vl.vehicleNo = veh.vehicleNo
            LEFT JOIN unit un on veh.unitId = un.id
            WHERE vl.STATUS = 1 and veh.unitId is not null
            AND DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN '${startDate}' and '${endDate}'
            AND un.unit in(?)
            GROUP BY vl.vehicleNo, DATE_FORMAT(startTime, '%Y-%m-%d')
        `, { type: QueryTypes.SELECT, replacements: [supportHubList] });

        let allHubHotoList = await sequelizeObj.query(`
            select 
                h.driverId, h.vehicleNo, h.fromHub, h.fromNode, h.toHub, h.toNode, h.startDateTime, h.endDateTime, us.role
            from hoto h
            LEFT JOIN driver d on h.driverId = d.driverId
            LEFT JOIN user us on d.driverId = us.driverId
            where h.status='Approved'
            AND DATE_FORMAT(startDateTime, '%Y-%m-%d') <= '${endDate}'
            AND DATE_FORMAT(endDateTime, '%Y-%m-%d') >= '${startDate}'
            AND (fromHub in(?) or toHub in(?))
            UNION ALL
            select 
                h.driverId, h.vehicleNo, h.fromHub, h.fromNode, h.toHub, h.toNode, h.startDateTime, h.returnDateTime as endDateTime, us.role
            from hoto_record h
            LEFT JOIN driver d on h.driverId = d.driverId
            LEFT JOIN user us on d.driverId = us.driverId
            where h.status='Approved'
            AND DATE_FORMAT(startDateTime, '%Y-%m-%d') <= '${endDate}'
            AND DATE_FORMAT(returnDateTime, '%Y-%m-%d') >= '${startDate}'
            AND (fromHub in(?) or toHub in(?))
        `, { type: QueryTypes.SELECT, replacements: [supportHubList, supportHubList, supportHubList, supportHubList] });
        let allHubLoanList = await sequelizeObj.query(`
            select t.driverId, t.vehicleNo, t.unitId, t.startDate as startDateTime, t.endDate as endDateTime, t.role, un.unit, un.subUnit from (
                select lo.driverId, lo.vehicleNo, IFNULL(d.unitId, veh.unitId) as unitId, startDate, endDate, us.role
                from loan lo
                LEFT JOIN driver d on d.driverId = lo.driverId
                LEFT JOIN user us on d.driverId = us.driverId
                LEFT JOIN vehicle veh on veh.vehicleNo = lo.vehicleNo
                where DATE_FORMAT(lo.startDate, '%Y-%m-%d') <= '${endDate}'
                AND DATE_FORMAT(lo.endDate, '%Y-%m-%d') >= '${startDate}'
                UNION ALL
                select lr.driverId, lr.vehicleNo, IFNULL(d.unitId, veh.unitId) as unitId, startDate, returnDate as endDate, us.role
                from loan_record lr
                LEFT JOIN driver d on d.driverId = lr.driverId
                LEFT JOIN user us on d.driverId = us.driverId
                LEFT JOIN vehicle veh on veh.vehicleNo = lr.vehicleNo
                where DATE_FORMAT(lr.startDate, '%Y-%m-%d') <= '${endDate}'
                AND DATE_FORMAT(lr.returnDate, '%Y-%m-%d') >= '${startDate}'
            ) t 
            LEFT JOIN unit un on t.unitId = un.id
            where t.unitId is not null AND un.unit in(?)
        `, { type: QueryTypes.SELECT, replacements: [supportHubList] });

        let restDates = await utils.getDateRangeRestdays(startDate, endDate);
        for (let supportNodeObj of allNodeList) {
            let nodeTaskList = allHubTaskList.filter(item => item.hub == supportNodeObj.unit && item.node == supportNodeObj.subUnit);

            let nodeToList = allHubToList.filter(item => item.unitId == supportNodeObj.unitId);
            let nodeToIdList = nodeToList.map(item => item.driverId);
            let nodeToLeaveList = allHubToLeaveList.filter(item => nodeToIdList.indexOf(item.driverId) > -1);

            let nodeVehicleList = allHubVehicleList.filter(item => item.unitId == supportNodeObj.unitId);
            let nodeVehicleNoList = nodeVehicleList.map(item => item.vehicleNo);
            let nodeVehicleLeaveList = allHubVehicleEventList.filter(item => nodeVehicleNoList.indexOf(item.vehicleNo) > -1);

            let nodeHotoList = allHubHotoList.filter(item => ((item.fromHub == supportNodeObj.unit && item.fromNode == supportNodeObj.subUnit) 
                || (item.toHub == supportNodeObj.unit && item.toNode == supportNodeObj.subUnit)));
            let nodeLoanList = allHubLoanList.filter(item => item.unit == supportNodeObj.unit && item.subUnit == supportNodeObj.subUnit);

            let nodeOpsSummaryResult = await calcNodeOpsSummary({supportNodeObj, startDate, endDate, nodeTaskList, 
                nodeToList, nodeToLeaveList, nodeVehicleList, nodeVehicleLeaveList, nodeHotoList, nodeLoanList, restDates});

            let supportHubData = opsSummaryResult.hubData.find(item => item.name.toUpperCase() == supportNodeObj.unit.toUpperCase());
            if (supportHubData && nodeOpsSummaryResult) {
                supportHubData.taskWorkingDays += nodeOpsSummaryResult.taskWorkingDays;
                supportHubData.opsTaskWorkingDays += nodeOpsSummaryResult.opsTaskWorkingDays;
                supportHubData.trgTaskWorkingDays += nodeOpsSummaryResult.trgTaskWorkingDays;
                supportHubData.admTaskWorkingDays += nodeOpsSummaryResult.admTaskWorkingDays;

                supportHubData.toActualWorkingDays += nodeOpsSummaryResult.toActualWorkingDays;
                supportHubData.vehiclePlanWorkingDays += nodeOpsSummaryResult.vehiclePlanWorkingDays;
                supportHubData.vehicleActualWorkingDays += nodeOpsSummaryResult.vehicleActualWorkingDays;

                let newToWorkingRate = supportHubData.toActualWorkingDays <= 0 ? 0 : (supportHubData.taskWorkingDays / supportHubData.toActualWorkingDays * 100);
                supportHubData.toWorkingRate = Number(newToWorkingRate.toFixed(1));

                supportHubData.nodeData.push(nodeOpsSummaryResult);
            }
        }
        for (let supportHubData of opsSummaryResult.hubData) {
            opsSummaryResult.taskWorkingDays += supportHubData.taskWorkingDays;
            opsSummaryResult.opsTaskWorkingDays += supportHubData.opsTaskWorkingDays;
            opsSummaryResult.trgTaskWorkingDays += supportHubData.trgTaskWorkingDays;
            opsSummaryResult.admTaskWorkingDays += supportHubData.admTaskWorkingDays;

            opsSummaryResult.toActualWorkingDays += supportHubData.toActualWorkingDays;
            opsSummaryResult.vehiclePlanWorkingDays += supportHubData.vehiclePlanWorkingDays;
            opsSummaryResult.vehicleActualWorkingDays += supportHubData.vehicleActualWorkingDays;
        }

        let totalToWordingRate = opsSummaryResult.toActualWorkingDays <= 0 ? 0 : (opsSummaryResult.taskWorkingDays / opsSummaryResult.toActualWorkingDays * 100);
        opsSummaryResult.toWorkingRate = Number(totalToWordingRate.toFixed(1));
    }

    return opsSummaryResult;
}
const calcNodeOpsSummary = async function(params) {
    let {supportNodeObj, startDate, endDate, nodeTaskList, 
        nodeToList, nodeToLeaveList, nodeVehicleList, nodeVehicleLeaveList, nodeHotoList, nodeLoanList, restDates} = params;
    let supportNodeData = supportNodeObj;
    let nodeHotoDataList = nodeHotoList;
    let nodeLoanDataList = nodeLoanList;

    let nodeOpsSummaryResult = {
        name: supportNodeData.subUnit,
        hubName: supportNodeData.unit,
        unitId: supportNodeData.unitId,
        workingDays: 0,
        taskWorkingDays: 0,
        opsTaskWorkingDays: 0,
        trgTaskWorkingDays: 0,
        admTaskWorkingDays: 0,
        toWorkingRate: 0,
        toNumber: 0,
        toActualWorkingDays: 0,
        toLoanInDays: 0,
        toLoanOutDays: 0,
        toLeaveDays: 0,
        vehicleNumber: 0,
        vehiclePlanWorkingDays: 0,
        vehicleActualWorkingDays: 0,
        vehicleLoanInDays: 0,
        vehicleLoanOutDays: 0,
        vehicleLeaveDays: 0,
    };
    let dateRangeDiffDays = moment(endDate).diff(moment(startDate), 'day') + 1;
    nodeOpsSummaryResult.workingDays = dateRangeDiffDays - restDates.length;

    let startDateTimeLong = Number(moment(startDate + " 00:00:00").format('YYYYMMDDHHmmss'));
    let endDateTimeLong = Number(moment(endDate + " 23:59:59").format('YYYYMMDDHHmmss'));
    // calc node task working days
    let nodeTaskWorkingDays = 0;
    let nodeOpsWorkingDays = 0;
    let nodeTrgWorkingDays = 0;
    let nodeAdmWorkingDays = 0;
    function calcTaskWorkingDays() {
        for (let task of nodeTaskList) {
            let taskStartDate = moment(moment(task.mobileStartTime).format('YYYY-MM-DD'));
            let taskEndDate = moment(moment(task.mobileEndTime).format('YYYY-MM-DD'));
            let selectedStartDate = moment(startDate, 'YYYY-MM-DD');
            let selectedEndDate = moment(endDate, 'YYYY-MM-DD');
            if (taskStartDate.isBefore(selectedStartDate)) {
                taskStartDate = selectedStartDate;
            }
            if (taskEndDate.isAfter(selectedEndDate)) {
                taskEndDate = selectedEndDate;
            }
            let taskWorkingDays = taskEndDate.diff(taskStartDate, 'day') + 1;
    
            nodeTaskWorkingDays += taskWorkingDays;
            if (task.purpose.toUpperCase().startsWith('OPS')) {
                nodeOpsWorkingDays += taskWorkingDays;
            } else if (task.purpose.toUpperCase().startsWith('TRAINING')) {
                nodeTrgWorkingDays += taskWorkingDays;
            } else if (task.purpose.toUpperCase().startsWith('ADMIN')) {
                nodeAdmWorkingDays += taskWorkingDays;
            }
        }
    }
    calcTaskWorkingDays();
    nodeOpsSummaryResult.taskWorkingDays = nodeTaskWorkingDays;
    nodeOpsSummaryResult.opsTaskWorkingDays = nodeOpsWorkingDays;
    nodeOpsSummaryResult.trgTaskWorkingDays = nodeTrgWorkingDays;
    nodeOpsSummaryResult.admTaskWorkingDays = nodeAdmWorkingDays;

    //calc node driver working days
    if (nodeToList.length > 0) {
        nodeOpsSummaryResult.toNumber = nodeToList.length;
        let nodeAllToPlanWorkingDays = nodeOpsSummaryResult.workingDays * nodeOpsSummaryResult.toNumber;
        let nodeToLeaveDays = 0;
        let nodeToLoanInDays = 0;  // hoto in
        let nodeToLoanOutDays = 0; // hoto out + loan out

        let nodeToIdList = nodeToList.map(item => item.driverId);
        //calc node to leave days
        function calcToLeaveDatas() {
            nodeToLeaveList = nodeToLeaveList.filter(item => nodeToIdList.indexOf(item.driverId) > -1);
            if (nodeToLeaveList && nodeToLeaveList.length > 0) {
                nodeToLeaveDays = calcResourceLeaveDays(nodeToLeaveList, restDates);
                nodeOpsSummaryResult.toLeaveDays = nodeToLeaveDays;
            }
        }
        calcToLeaveDatas();
        //calc node to hoto in days
        function calcToLoanInDatas() {
            let nodeToHotoInList = nodeHotoDataList.filter(item => {
                return item.driverId && item.role == 'TO' && item.toHub == supportNodeData.unit && item.toNode == supportNodeData.subUnit;
            });
            if (nodeToHotoInList && nodeToHotoInList.length > 0) {
                let nodeHotoInDriverIds = nodeToHotoInList.map(item => item.driverId);
                nodeHotoInDriverIds = Array.from(new Set(nodeHotoInDriverIds));
                for (let driverId of nodeHotoInDriverIds) {
                    let currentDriverHotoInList = nodeToHotoInList.filter(item => item.driverId == driverId);
                    if (currentDriverHotoInList.length > 0) {
                        nodeToLoanInDays += calcResourceUseDays(startDateTimeLong, endDateTimeLong, currentDriverHotoInList, restDates);
                    }
                }
            }
            nodeOpsSummaryResult.toLoanInDays = nodeToLoanInDays;
        }
        calcToLoanInDatas();

        //calc node to hoto out days
        let nodeToHotoOutDays = 0;
        function calcToHotoOutDatas() {
            let nodeToHotoOutList = nodeHotoDataList.filter(item => {
                return item.driverId && item.role == 'TO' && item.fromHub == supportNodeData.unit && item.fromNode == supportNodeData.subUnit;
            });
            if (nodeToHotoOutList && nodeToHotoOutList.length > 0) {
                let nodeHotoOutDriverIds = nodeToHotoOutList.map(item => item.driverId);
                nodeHotoOutDriverIds = Array.from(new Set(nodeHotoOutDriverIds));
                for (let driverId of nodeHotoOutDriverIds) {
                    let currentDriverHotoOutList = nodeToHotoOutList.filter(item => item.driverId == driverId);
                    if (currentDriverHotoOutList.length > 0) {
                        nodeToHotoOutDays += calcResourceUseDays(startDateTimeLong, endDateTimeLong, currentDriverHotoOutList, restDates);
                    }
                }
            }
        }
        calcToHotoOutDatas();

        //calc node to loan out days
        function calcToLoanOutDatas() {
            let tempNodeToLoanOutDays = 0;
            let nodeToLoanOutList = nodeLoanDataList.filter(item => {
                return item.driverId && item.role == 'TO' && item.unit == supportNodeData.unit && item.subUnit == supportNodeData.subUnit;
            });
            if (nodeToLoanOutList && nodeToLoanOutList.length > 0) {
                let nodeLoanOutDriverIds = nodeToLoanOutList.map(item => item.driverId);
                nodeLoanOutDriverIds = Array.from(new Set(nodeLoanOutDriverIds));
                for (let driverId of nodeLoanOutDriverIds) {
                    let currentDriverLoanOutList = nodeToLoanOutList.filter(item => item.driverId == driverId);
                    if (currentDriverLoanOutList.length > 0) {
                        tempNodeToLoanOutDays += calcResourceUseDays(startDateTimeLong, endDateTimeLong, currentDriverLoanOutList, restDates);
                    }
                }
            }
            nodeToLoanOutDays = nodeToHotoOutDays + tempNodeToLoanOutDays;
            nodeOpsSummaryResult.toLoanOutDays = nodeToLoanOutDays;
        }
        calcToLoanOutDatas();

        nodeOpsSummaryResult.toActualWorkingDays = nodeAllToPlanWorkingDays + nodeToLoanInDays - nodeToLoanOutDays - nodeToLeaveDays;
        if (nodeOpsSummaryResult.toActualWorkingDays < 0) {
            nodeOpsSummaryResult.toActualWorkingDays = 0;
        }
        let nodeToWorkingRate = nodeOpsSummaryResult.toActualWorkingDays == 0 ? 
            0 : (nodeOpsSummaryResult.taskWorkingDays / nodeOpsSummaryResult.toActualWorkingDays * 100);
        nodeOpsSummaryResult.toWorkingRate = Number(nodeToWorkingRate.toFixed(1));
    }

    if (nodeVehicleList.length > 0) {
        nodeOpsSummaryResult.vehicleNumber = nodeVehicleList.length;
        let nodeAllVehiclePlanWorkingDays = nodeOpsSummaryResult.workingDays * nodeOpsSummaryResult.vehicleNumber;
        let nodeVehicleLeaveDays = 0;
        let nodeVehicleLoanInDays = 0;  // hoto in
        let nodeVehicleLoanOutDays = 0; // hoto out + loan out

        let nodeVehicleNoList = nodeVehicleList.map(item => item.vehicleNo);
        //calc node vehicle leave days
        function calcVehicleLeaveDatas() {
            nodeVehicleLeaveList = nodeVehicleLeaveList.filter(item => nodeVehicleNoList.indexOf(item.vehicleNo) > -1);
            if (nodeVehicleLeaveList && nodeVehicleLeaveList.length > 0) {
                nodeVehicleLeaveDays = calcResourceLeaveDays(nodeVehicleLeaveList, restDates);
                nodeOpsSummaryResult.vehicleLeaveDays = nodeVehicleLeaveDays;
            }
        }
        calcVehicleLeaveDatas();
        //calc node vehicle hoto in days
        function calcVehicleHotoInDatas() {
            let nodeVehicleHotoInList = nodeHotoDataList.filter(item => {
                return item.vehicleNo && item.toHub == supportNodeData.unit && item.toNode == supportNodeData.subUnit;
            });
            if (nodeVehicleHotoInList && nodeVehicleHotoInList.length > 0) {
                let nodeHotoInVehicleNos = nodeVehicleHotoInList.map(item => item.vehicleNo);
                nodeHotoInVehicleNos = Array.from(new Set(nodeHotoInVehicleNos));
                for (let vehicleNo of nodeHotoInVehicleNos) {
                    let currentVehicleHotoInList = nodeVehicleHotoInList.filter(item => item.vehicleNo == vehicleNo);
                    if (currentVehicleHotoInList.length > 0) {
                        nodeVehicleLoanInDays += calcResourceUseDays(startDateTimeLong, endDateTimeLong, currentVehicleHotoInList, restDates);
                    }
                }
            }
            nodeOpsSummaryResult.vehicleLoanInDays = nodeVehicleLoanInDays;
        }
        calcVehicleHotoInDatas();

        //calc node vehicle hoto out days
        let nodeVehicleHotoOutDays = 0;
        function calcVehicleHotoOutDatas() {
            let nodeVehicleHotoOutList = nodeHotoDataList.filter(item => {
                return item.vehicleNo && item.fromHub == supportNodeData.unit && item.fromNode == supportNodeData.subUnit;
            });
            if (nodeVehicleHotoOutList && nodeVehicleHotoOutList.length > 0) {
                let nodeHotoOutVehicleNos = nodeVehicleHotoOutList.map(item => item.vehicleNo);
                nodeHotoOutVehicleNos = Array.from(new Set(nodeHotoOutVehicleNos));
                for (let vehicleNo of nodeHotoOutVehicleNos) {
                    let currentVehicleHotoOutList = nodeVehicleHotoOutList.filter(item => item.driverId == vehicleNo);
                    if (currentVehicleHotoOutList.length > 0) {
                        nodeVehicleHotoOutDays += calcResourceUseDays(startDateTimeLong, endDateTimeLong, currentVehicleHotoOutList, restDates);
                    }
                }
            }
        }
        calcVehicleHotoOutDatas();

        //calc node vehicle loan out days
        function calcVehicleLoanOutDatas() {
            let tempNodeVehicleLoanOutDays = 0;
            let nodeVehicleLoanOutList = nodeLoanDataList.filter(item => {
                return item.vehicleNo && item.unit == supportNodeData.unit && item.subUnit == supportNodeData.subUnit;
            });
            if (nodeVehicleLoanOutList && nodeVehicleLoanOutList.length > 0) {
                let nodeLoanOutVehicleNos = nodeVehicleLoanOutList.map(item => item.vehicleNo);
                nodeLoanOutVehicleNos = Array.from(new Set(nodeLoanOutVehicleNos));
                for (let vehicleNo of nodeLoanOutVehicleNos) {
                    let currentVehicleLoanOutList = nodeVehicleLoanOutList.filter(item => item.vehicleNo == vehicleNo);
                    if (currentVehicleLoanOutList.length > 0) {
                        tempNodeVehicleLoanOutDays += calcResourceUseDays(startDateTimeLong, endDateTimeLong, currentVehicleLoanOutList, restDates);
                    }
                }
            }
            nodeVehicleLoanOutDays = nodeVehicleHotoOutDays + tempNodeVehicleLoanOutDays;
            nodeOpsSummaryResult.vehicleLoanOutDays = nodeVehicleLoanOutDays;
        }
        calcVehicleLoanOutDatas();

        nodeOpsSummaryResult.vehicleActualWorkingDays = (nodeAllVehiclePlanWorkingDays + nodeVehicleLoanInDays - nodeVehicleLoanOutDays - nodeVehicleLeaveDays) / nodeOpsSummaryResult.workingDays;
        if (nodeOpsSummaryResult.vehicleActualWorkingDays < 0) {
            nodeOpsSummaryResult.vehicleActualWorkingDays = 0;
        }
        nodeOpsSummaryResult.vehicleActualWorkingDays = Math.round(nodeOpsSummaryResult.vehicleActualWorkingDays);
        nodeOpsSummaryResult.vehiclePlanWorkingDays = (nodeAllVehiclePlanWorkingDays + nodeVehicleLoanInDays - nodeVehicleLoanOutDays) / nodeOpsSummaryResult.workingDays;
        nodeOpsSummaryResult.vehiclePlanWorkingDays = Math.round(nodeOpsSummaryResult.vehiclePlanWorkingDays);
    }

    return nodeOpsSummaryResult;
}

const calcResourceLeaveDays = function(nodeResourceLeaveList, restDates) {
    let nodeToLeaveDays = 0;
    for (let temp of nodeResourceLeaveList) {
        if (temp.startTime) {
            let leaveDate = moment(temp.startTime).format('YYYY-MM-DD');
            if (restDates.indexOf(leaveDate) == -1) {
                nodeToLeaveDays += temp.leaveDays ? Number(temp.leaveDays) : 0;
            }
        }
    }

    return nodeToLeaveDays;
}

const calcResourceUseDays = function(startDateTimeLong, endDateTimeLong, nodeResourceHotoList, restDates) {
    let nodeHotoDays = 0;
    function buildDateInfo() {
        for (let temp of nodeResourceHotoList) {
            if (Number(moment(temp.startDateTime).format('H')) < 12) {
                temp.startDateTime = moment(temp.startDateTime).format('YYYY-MM-DD') + ' 00:00:00';
            } else {
                temp.startDateTime = moment(temp.startDateTime).format('YYYY-MM-DD') + ' 12:00:00';
            }
            if (Number(moment(temp.endDateTime).format('H')) < 12) {
                temp.endDateTime = moment(temp.endDateTime).format('YYYY-MM-DD') + ' 11:59:59';
            } else {
                temp.endDateTime = moment(temp.endDateTime).format('YYYY-MM-DD') + ' 23:59:59';
            }
            let hotoStartTimeLong = moment(temp.startDateTime).format('YYYYMMDDHHmmss');
            if (startDateTimeLong > hotoStartTimeLong) {
                temp.startDateTime = moment(startDateTimeLong, 'YYYYMMDDHHmmss').format('YYYY-MM-DD HH:mm:ss');
            }
            let hotoEndTimeLong = moment(temp.endDateTime).format('YYYYMMDDHHmmss');
            if (endDateTimeLong < hotoEndTimeLong) {
                temp.endDateTime = moment(endDateTimeLong, 'YYYYMMDDHHmmss').format('YYYY-MM-DD HH:mm:ss');
            }
        }
    }
    buildDateInfo();

    //split hoto days to every day list then exclude month rest days(weekend and holidays)
    let newHotoDaysList = [];
    function buildHotoDateInfo() {
        for (let temp of nodeResourceHotoList) {
            let diffDays = moment(temp.endDateTime, 'YYYY-MM-DD').diff(moment(temp.startDateTime, 'YYYY-MM-DD'), 'day');
            let index = 0;
            while (index <= diffDays) {
                let hotoDayStr = moment(temp.startDateTime).add(index, 'days').format('YYYY-MM-DD');
                if (restDates.indexOf(hotoDayStr) != -1) {
                    index++;
                    continue;
                }
    
                let hotoDay = {startDateTime: '', endDateTime: ''};
                if (index == 0) {
                    hotoDay.startDateTime = temp.startDateTime;
                } else {
                    hotoDay.startDateTime = hotoDayStr + ' 00:00:00';
                }
                if (index == diffDays) {
                    hotoDay.endDateTime = temp.endDateTime;
                } else {
                    hotoDay.endDateTime = hotoDayStr + ' 23:59:59';
                }
                newHotoDaysList.push(hotoDay);
                index++;
            }
        }
    }
    buildHotoDateInfo();

    //hoto data order by startDateTime asc
    newHotoDaysList = newHotoDaysList.sort(function(item1, item2) {
        if (moment(item1.startDateTime).isBefore(moment(item2.startDateTime))) {
            return -1;
        } 
        return 1;
    });

    //Merge Intersections days
    let hotoIntervalList = [];
    function mergeHotoDate() {
        for (let temp of newHotoDaysList) {
            let tempStartTimeLong = Number(moment(temp.startDateTime).format('YYYYMMDDHHmmss'));
            let tempEndTimeLong = Number(moment(temp.endDateTime).format('YYYYMMDDHHmmss'));
    
            if (hotoIntervalList.length > 0) {
                let preHoto = hotoIntervalList[hotoIntervalList.length - 1];
                if (tempStartTimeLong < preHoto.endTime) {
                    if (tempEndTimeLong > preHoto.endTime) {
                        preHoto.endTime = tempEndTimeLong;
                    }
                } else {
                    hotoIntervalList.push({
                        startTime: tempStartTimeLong,
                        endTime: tempEndTimeLong
                    });
                }
            } else {
                hotoIntervalList.push({
                    startTime: tempStartTimeLong,
                    endTime: tempEndTimeLong
                });
            }
        }
    }
    mergeHotoDate();
    for (let timeInterval of hotoIntervalList) {
        let startTime = timeInterval.startTime;
        let endTime = timeInterval.endTime;
        if (startTime < startDateTimeLong) {
            startTime = startDateTimeLong;
        }
        if (endTime > endDateTimeLong) {
            endTime = endDateTimeLong;
        }
        let tempHotoOutDays = calcWorkingTime(startTime, endTime);
        nodeHotoDays += tempHotoOutDays;
    }
    return nodeHotoDays;
}

const calcWorkingTime = function(startTime, endTime) {
    if (endTime < startTime) {
        return 0;
    }
    startTime = moment('' + startTime, 'YYYYMMDDHHmmss');
    endTime = moment('' + endTime, 'YYYYMMDDHHmmss');

    let startAm = Number(startTime.format('H')) >= 12 ? "pm" : 'am';
    let endAm = Number(endTime.format('H')) >= 12 ? "pm" : 'am';

    let diffDays = moment(endTime.format("YYYY-MM-DD")).diff(moment(startTime.format("YYYY-MM-DD")), 'days');
    let workDays = 0.5;

    let startDays = (startAm == 'am' ? 1 : 0.5);
    let endDays = (endAm == 'am' ? 0.5 : 1);
    if (diffDays == 0) {
        if (startAm != endAm) {
            workDays = 1;
        }
    } else if (diffDays == 1) {
        let startTimeWorkDays = startDays;
        let endTimeWorkDays = endDays;

        workDays = startTimeWorkDays + endTimeWorkDays;
    } else {
        let startTimeWorkDays = startDays;
        let endTimeWorkDays = endDays;

        workDays = startTimeWorkDays + endTimeWorkDays + (diffDays - 1);
    }

    return workDays;
}

module.exports = {
    report: async function (req, res) {
        try {
            let { month, groupId } = req.body
            if (!month || !groupId) {
                throw Error(`Month or unit is needed.`)
            } else {
                month = moment(month, 'YYYY-MM')
            }

            let result = { }
            result.indentsReport = await getIndentsReport(month, groupId)
            result.indentTypesReport = await getIndentTypesReport(month, groupId)
            result.safetyReport = await getIndentSafetyReport(month, groupId)
            result.toReport = await getTOReport(month, groupId)
            result.vehicleReport = await getVehicleReport(month, groupId)
            
            // Not ready
            result.unitFeedback = await getUnitFeedback(month, groupId)

            // get tripNo for checkout task data
            let systemIndentsData = await getIndentsDataGroupByTripNo(month, groupId)
            let tripNoList = systemIndentsData.map(item => item.tripNo)
            result.tosFeedback = await getTOSFeedback(tripNoList)

            return res.json(utils.response(1, result))
        } catch (error) {
            log.error(`(report): `, error)
            return res.json(utils.response(0, error))
        }
    },

    resourceMonthUtilisationReport: async function(req, res) {
        let userId = req.body.userId;
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let resourceType = req.body.resourceType;
        let vehicleType = req.body.vehicleType;
        let selectedYear = req.body.selectedYear;
        let selectedMonth = req.body.selectedMonth;
        let selectedHub = req.body.selectedHub;
        let selectedNode = req.body.selectedNode;

        let pageNum = Number(req.body.pageNum).valueOf();
        let pageLength = Number(req.body.pageLength).valueOf();

        let startIndex = pageNum;
        let endIndex = startIndex + pageLength;

        if (resourceType == 'to') {
            let toReportData = await getTOUtilisationReport(user, selectedYear, selectedMonth, selectedHub, selectedNode);
            let totalData = toReportData.totalData;
            let unitDataList = toReportData.unitDataList;

            let totalCount = 0;
            let resultData = [];
            function buildToResult() {
                if(unitDataList) totalCount = unitDataList.length;
                if (totalCount > 0) {
                    if (endIndex > totalCount) {
                        endIndex = totalCount;
                    }
                    resultData = unitDataList.slice(startIndex, endIndex);
                    if (resultData && resultData.length > 0) {
                        let index = startIndex;
                        for (let data of resultData) {
                            index++;
                            data.index = index;
                        }
                    }
                    if (totalCount > 1) {
                        resultData = [totalData].concat(resultData);
                    }
                }
            }
            buildToResult();

            return res.json({ respMessage: resultData, recordsFiltered: totalCount, recordsTotal: totalCount });
        } else {
            let vehicleReportData = await getVehicleUtilisationReport(user, selectedYear, selectedMonth, selectedHub, selectedNode, vehicleType);
            let totalData = vehicleReportData.totalData;
            let unitDataList = vehicleReportData.unitDataList;

            let totalCount = unitDataList.length;
            let resultData = [];
            function buildVehicleResult() {
                if (totalCount > 0) {
                    if (endIndex > totalCount) {
                        endIndex = totalCount;
                    }
                    resultData = unitDataList.slice(startIndex, endIndex);
                    if (resultData && resultData.length > 0) {
                        let index = startIndex;
                        for (let data of resultData) {
                            index++;
                            data.index = index;
                        }
                    }
                    if (totalCount > 1) {
                        resultData = [totalData].concat(resultData);
                    }
                }
            }
            buildVehicleResult();

            return res.json({ respMessage: resultData, recordsFiltered: totalCount, recordsTotal: totalCount });
        }
    },

    licensingMonthReport: async function(req, res) {
        try {
            let userId = req.body.userId;
            let user = await userService.getUserDetailInfo(userId)
            if (!user) {
                log.warn(`User ${ userId } does not exist.`);
                return res.json(utils.response(0, `User ${ userId } does not exist.`));
            }

            let resourceType = req.body.resourceType;
            let permitType = req.body.permitType;
            let selectedYear = req.body.selectedYear;
            let selectedMonth = req.body.selectedMonth;
            let selectedHub = req.body.selectedHub;
            let selectedNode = req.body.selectedNode;
            let pageNum = Number(req.body.pageNum).valueOf();
            let pageLength = Number(req.body.pageLength).valueOf();

            if (resourceType ==  '2') {
                let report2Data = await getTOLicecsingReport2({user, selectedYear, selectedMonth, permitType, selectedHub, selectedNode, pageNum, pageLength});

                return res.json({ respMessage: report2Data.data, recordsFiltered: report2Data.totalCount, recordsTotal: report2Data.totalCount });
            } else {
                let report1Data = await getTOLicecsingReport1(user, selectedYear, selectedMonth, selectedHub, selectedNode, pageNum, pageLength);

                return res.json({ respMessage: report1Data.data, recordsFiltered: report1Data.totalCount, recordsTotal: report1Data.totalCount });
            }
        } catch (error) {
            log.error(`(report): `, error)
            return res.json(utils.response(0, error))
        }
    },

    arbReportByHubNode: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let { hub, node, dateTimeZone, pageNum, pageLength, sortBy, sort } = req.body;

            pageNum = Number.parseInt(pageNum)
            pageLength = Number.parseInt(pageLength)

            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) throw Error(`User ${ userId } does not exist.`);
            if (![ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.UNIT ].includes(user.userType)) {
                log.warn(`Only hq, unit, admin user can view arb report`)
                return []
            }

            // init hub/node list
            let hubNodeSql = `
                SELECT id AS unitId, u.unit AS hub, IFNULL(u.subUnit, '-') AS node 
                FROM unit u 
            `
            let limitHubNodeList = [], limitHubNodeReplacements = []

            const getLimitSql = async function () {
                let { unitIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId)
                    if (hub) {
                        limitHubNodeList.push(` u.unit = ? `)
                        limitHubNodeReplacements.push(hub)
                    }
                    if (node && node != '-') {
                        limitHubNodeList.push(` u.subUnit = ? `)
                        limitHubNodeReplacements.push(node)
                    } else if (node == '-') {
                        limitHubNodeList.push(` u.subUnit IS NULL `)
                    }
    
                    if (unitIdList.length) {
                        limitHubNodeList.push(` u.id in (?) `)
                        limitHubNodeReplacements.push(unitIdList)
                    }
            }
            const getLimitSql2 = async function () {
                if (hub) {
                    limitHubNodeList.push(` u.unit = ? `)
                    limitHubNodeReplacements.push(hub)
                }
                if (node && node != '-') {
                    limitHubNodeList.push(` u.subUnit = ? `)
                    limitHubNodeReplacements.push(node)
                } else if (node == '-') {
                    limitHubNodeList.push(` u.subUnit IS NULL `)
                }
            }
            if (CONTENT.USER_TYPE.HQ == user.userType) {
                await getLimitSql()
            } else {
                await getLimitSql2()
            }

            if (limitHubNodeList.length) {
                hubNodeSql += ` WHERE ` + limitHubNodeList.join(' AND ')
            }

            let hubNodeList = await sequelizeObj.query(hubNodeSql, {
                type: QueryTypes.SELECT,
                replacements: limitHubNodeReplacements,
            })

            // init offence list
            let offenceSql = `
                SELECT us.*, th.violationType, th.occTime, COUNT(*) AS \`count\`
                FROM track_history th
                LEFT JOIN (
                    SELECT driverId, unitId, fullName, nric, role 
                    FROM \`user\` 
                    WHERE userType = 'MOBILE'
                ) us ON us.driverId = th.deviceId 
                WHERE th.dataFrom = 'mobile'
            `
            if (dateTimeZone && dateTimeZone.length == 2) {
                offenceSql += ` AND DATE(th.occTime) BETWEEN DATE(?) AND DATE(?) `
            }
            offenceSql += ` AND us.\`role\` = 'TO' `
            offenceSql += ` GROUP BY unitId, violationType `
            let offenceList = await sequelizeObj.query(offenceSql, {
                type: QueryTypes.SELECT,
                replacements: dateTimeZone
            })

            // generate data for ui
            let all = {
                fullName: 'ALL',
                hub: 'ALL',
                node: '',
                total: 0,
                speeding: 0,
                hardBraking: 0,
                rapidAcc: 0,
                missing: 0,
                nogoAlert: 0,
            }

            const initHubNodeList = function () {
                for (let hubNode of hubNodeList) {
                    hubNode.speeding = 0
                    hubNode.hardBraking = 0
                    hubNode.rapidAcc = 0
                    hubNode.missing = 0
                    hubNode.nogoAlert = 0
                    for (let offence of offenceList) {
                        if (offence.unitId == hubNode.unitId) {
                            if (offence.violationType == CONTENT.ViolationType.Speeding) {
                                hubNode.speeding = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.HardBraking) {
                                hubNode.hardBraking = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.RapidAcc) {
                                hubNode.rapidAcc = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.Missing) {
                                hubNode.missing = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.NoGoZoneAlert) {
                                hubNode.nogoAlert = offence.count
                            } 
                        }
                    }
                    
                    hubNode.total = hubNode.speeding + hubNode.hardBraking + hubNode.rapidAcc + hubNode.nogoAlert
    
                    all.total += hubNode.total
                    all.speeding += hubNode.speeding
                    all.hardBraking += hubNode.hardBraking
                    all.rapidAcc += hubNode.rapidAcc
                    all.nogoAlert += hubNode.nogoAlert
                }
            }
            initHubNodeList()

            // filter 0
            hubNodeList = hubNodeList.filter(item => item.total != 0)

            // sort
            if (!sortBy) {
                sortBy = 'total'
                sort = 'desc'
            }
            if (sortBy == 'total') {
                // asc
                hubNodeList = _.sortBy(hubNodeList, ['total', 'speeding', 'hardBraking', 'rapidAcc', 'nogoAlert'])
            } else {
                // asc
                hubNodeList = _.sortBy(hubNodeList, [ sortBy ])
            }
            // desc
            if (sort && sort.toLowerCase() == 'desc') {
                hubNodeList.reverse();
            }

            // page
            let data = []
            data = hubNodeList.slice(pageNum, pageNum + pageLength)

            data = [all].concat(data)
            return res.json({respMessage: data, recordsFiltered: hubNodeList.length, recordsTotal: hubNodeList.length })
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error))
        }
    },
    arbReportByTO: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let { hub, node, driverId, dateTimeZone, pageNum, pageLength, sortBy, sort } = req.body;

            pageNum = Number.parseInt(pageNum)
            pageLength = Number.parseInt(pageLength)

            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) throw Error(`User ${ userId } does not exist.`);
            if (![ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.UNIT ].includes(user.userType)) {
                log.warn(`Only hq, unit, admin user can view arb report`)
                return []
            }

            // init driver list
            let driverSql = `
                SELECT us.driverId, us.unitId, us.fullName, us.nric, u.unit AS hub, IFNULL(u.subUnit, '-') AS node 
                FROM \`user\` us
                LEFT JOIN unit u ON us.unitId = u.id
                WHERE us.userType = 'MOBILE'
                AND us.\`role\` = 'TO'
            `
            let limitHubNodeList = [], limitHubNodeReplacements = []

            const getPermitSql1 = async function () { 
                let { unitIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId)
                if (hub) {
                    limitHubNodeList.push(` u.unit = ? `)
                    limitHubNodeReplacements.push(hub)
                }
                if (node && node != '-') {
                    limitHubNodeList.push(` u.subUnit = ? `)
                    limitHubNodeReplacements.push(node)
                } else if (node == '-') {
                    limitHubNodeList.push(` u.subUnit IS NULL `)
                }
            
                if (unitIdList.length) {
                    limitHubNodeList.push(` u.id in (?) `)
                    limitHubNodeReplacements.push(unitIdList)
                }
                log.info('')
            }
            const getPermitSql2 = async function () {
                if (hub) {
                    limitHubNodeList.push(` u.unit = ? `)
                    limitHubNodeReplacements.push(hub)
                }
                if (node && node != '-') {
                    limitHubNodeList.push(` u.subUnit = ? `)
                    limitHubNodeReplacements.push(node)
                } else if (node == '-') {
                    limitHubNodeList.push(` u.subUnit IS NULL `)
                }
            
                if (driverId) {
                    limitHubNodeList.push(` us.driverId = ? `)
                    limitHubNodeReplacements.push(driverId)
                }
            }

            if (CONTENT.USER_TYPE.HQ == user.userType) {
                await getPermitSql1()
            } else {
                await getPermitSql2()
            }

            if (limitHubNodeList.length) {
                driverSql += ' AND ' + limitHubNodeList.join(' AND ')
            }
            let driverList = await sequelizeObj.query(driverSql, {
                type: QueryTypes.SELECT,
                replacements: limitHubNodeReplacements
            })

            // init offence list
            let offenceSql = `
                SELECT us.*, th.violationType, th.occTime, COUNT(*) AS \`count\`
                FROM track_history th
                LEFT JOIN (
                    SELECT us.driverId, us.unitId, us.fullName, us.nric, us.role, u.unit AS hub, u.subUnit AS node  
                    FROM \`user\` us
                    LEFT JOIN unit u ON us.unitId = u.id
                    WHERE us.userType = 'MOBILE'
                ) us ON us.driverId = th.deviceId 
                WHERE th.dataFrom = 'mobile'
            `
            if (dateTimeZone?.length == 2) {
                offenceSql += ` AND DATE(th.occTime) BETWEEN DATE(?) AND DATE(?) `
            }
            let limitOffenceHubNodeList = [], limitOffenceHubNodeReplacements = []
            const getLimitSql = function () {
                if (hub) {
                    limitOffenceHubNodeList.push(` us.hub = ? `)
                    limitOffenceHubNodeReplacements.push(hub)
                }
                if (node && node != '-') {
                    limitOffenceHubNodeList.push(` us.node = ? `)
                    limitOffenceHubNodeReplacements.push(node)
                } else if (node == '-') {
                    limitHubNodeList.push(` us.node IS NULL `)
                }
    
                if (driverId) {
                    limitOffenceHubNodeList.push(` us.driverId = ? `)
                    limitOffenceHubNodeReplacements.push(driverId)
                }
            }
            getLimitSql()

            if (limitOffenceHubNodeList.length) {
                offenceSql += ' AND ' + limitOffenceHubNodeList.join(' AND ')
            }
            offenceSql += ` AND us.\`role\` = 'TO' `
            offenceSql += ` GROUP BY driverId, unitId, violationType `
            log.info(offenceSql)
            let offenceList = await sequelizeObj.query(offenceSql, {
                type: QueryTypes.SELECT,
                replacements: dateTimeZone.concat(limitOffenceHubNodeReplacements)
            })

            // init data for ui
            let all = {
                fullName: 'ALL',
                hub: 'ALL',
                node: '',
                total: 0,
                speeding: 0,
                hardBraking: 0,
                rapidAcc: 0,
                missing: 0,
                nogoAlert: 0,
            }
            const initDriverList = function () {
                for (let driver of driverList) {
                    driver.total = 0
                    driver.speeding = 0
                    driver.hardBraking = 0
                    driver.rapidAcc = 0
                    driver.missing = 0
                    driver.nogoAlert = 0
                    for (let offence of offenceList) {
                        if (offence.driverId == driver.driverId) {
                            if (offence.violationType == CONTENT.ViolationType.Speeding) {
                                driver.speeding = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.HardBraking) {
                                driver.hardBraking = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.RapidAcc) {
                                driver.rapidAcc = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.Missing) {
                                driver.missing = offence.count
                            } else if (offence.violationType == CONTENT.ViolationType.NoGoZoneAlert) {
                                driver.nogoAlert = offence.count
                            } 
                        }
                    }
    
                    driver.total = driver.speeding + driver.hardBraking + driver.rapidAcc + driver.nogoAlert
    
                    all.total += driver.total
                    all.speeding += driver.speeding
                    all.hardBraking += driver.hardBraking
                    all.rapidAcc += driver.rapidAcc
                    all.nogoAlert += driver.nogoAlert
                }
            }
            initDriverList()

            // filter 0
            driverList = driverList.filter(item => item.total != 0)

            // sort
            if (!sortBy) {
                sortBy = 'total'
                sort = 'desc'
            }
            if (sortBy == 'total') {
                // asc
                driverList = _.sortBy(driverList, ['total', 'speeding', 'hardBraking', 'rapidAcc', 'nogoAlert'])
            } else {
                // asc
                driverList = _.sortBy(driverList, [ sortBy ])
            }
            // desc
            if (sort && sort.toLowerCase() == 'desc') {
                driverList.reverse();
            }

            // page
            let data = []
            data = driverList.slice(pageNum, pageNum + pageLength)

            data = [all].concat(data)
            return res.json({respMessage: data, recordsFiltered: driverList.length, recordsTotal: driverList.length })
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error))
        }
    },

    opsSummaryReport: async function(req, res) {
        let userId = req.cookies.userId;
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let selectedCycleResult = {
            name: 'TOTAL',
            taskWorkingDays: 0,
            opsTaskWorkingDays: 0,
            trgTaskWorkingDays: 0,
            admTaskWorkingDays: 0,
            toWorkingRate: 0,
            toActualWorkingDays: 0,
            vehiclePlanWorkingDays: 0,
            vehicleActualWorkingDays: 0,
            hubData: []
        }

        let preCycleResult = {
            name: 'TOTAL',
            taskWorkingDays: 0,
            opsTaskWorkingDays: 0,
            trgTaskWorkingDays: 0,
            admTaskWorkingDays: 0,
            toWorkingRate: 0,
            toActualWorkingDays: 0,
            vehiclePlanWorkingDays: 0,
            vehicleActualWorkingDays: 0,
            hubData: []
        }

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            return res.json(utils.response(1, {selectedCycleResult, preCycleResult}));
        }
        let startDate = req.body.startDate;
        let endDate = req.body.endDate;
        function initDate() {
            if (!startDate || startDate == 'Invalid date') {
                startDate = moment().add(-7, 'day').format('YYYY-MM-DD');
            }
            if (!endDate || endDate == 'Invalid date') {
                endDate = moment().format('YYYY-MM-DD');
            }
        }
        initDate();
        let dateRangeDiffDays = moment(endDate).diff(moment(startDate), 'day') + 1;
        let preCycleStartDate = moment(startDate).add((0 - dateRangeDiffDays), 'day').format('YYYY-MM-DD');
        let preCycleEndDate = moment(endDate).add((0 - dateRangeDiffDays), 'day').format('YYYY-MM-DD');

        selectedCycleResult.startDate = startDate;
        selectedCycleResult.endDate = endDate;
        preCycleResult.startDate = preCycleStartDate;
        preCycleResult.endDate = preCycleEndDate;

        let opsSummaryConfHub = conf.OPS_SUMMARY_HUB_LIST;
        let userHubList = [];
        let userNode = null;
        async function initUnitInfo() {
            if (user.userType == CONTENT.USER_TYPE.UNIT) {
                if (user.unit) {
                    userHubList.push(user.unit);
                }
                userNode = user.subUnit;
            } else if ([ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.LICENSING_OFFICER ].includes(user.userType)) {
                let unitList = await sequelizeObj.query(` 
                    SELECT unit FROM unit GROUP BY unit;
                `, { type: QueryTypes.SELECT });
    
                unitList.forEach(item => {
                    if (item.unit) {
                        userHubList.push(item.unit);
                    }
                });
             } else if (CONTENT.USER_TYPE.HQ == user.userType) {
                let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
                userHubList = userUnitList.map(item => item.unit);
             }
        }
        await initUnitInfo();
        
         let supportHubList = opsSummaryConfHub.filter(item => userHubList.indexOf(item) > -1);
         if (supportHubList && supportHubList.length > 0) {
            let hubColorConfList = jsonfile.readFileSync(`./conf/hubNodeConf.json`);
            for (let supportHub of supportHubList) {
                let supportHubColorObj = hubColorConfList.find(item => item.hub.toUpperCase() == supportHub.toUpperCase());
                let supportHubColor = "#8da0a2";
                if (supportHubColorObj) {
                    supportHubColor = supportHubColorObj.color;
                }
                selectedCycleResult.hubData.push({
                    name: supportHub,
                    color: supportHubColor,
                    taskWorkingDays: 0,
                    opsTaskWorkingDays: 0,
                    trgTaskWorkingDays: 0,
                    admTaskWorkingDays: 0,
                    toWorkingRate: 0,
                    toActualWorkingDays: 0,
                    vehiclePlanWorkingDays: 0,
                    vehicleActualWorkingDays: 0,
                    nodeData: []
                });

                preCycleResult.hubData.push({
                    name: supportHub,
                    color: supportHubColor,
                    taskWorkingDays: 0,
                    opsTaskWorkingDays: 0,
                    trgTaskWorkingDays: 0,
                    admTaskWorkingDays: 0,
                    toWorkingRate: 0,
                    toActualWorkingDays: 0,
                    vehiclePlanWorkingDays: 0,
                    vehicleActualWorkingDays: 0,
                    nodeData: []
                });
            }
            let supportNodeSql = ` 
                select id as unitId, unit, subUnit, lat, lng 
                from unit where subUnit is not null and unit in(?) 
            `;
            if (userNode) {
                supportNodeSql += ` and subUnit='${userNode}' `;
            }
            let supportNodeList = await sequelizeObj.query(supportNodeSql, { type: QueryTypes.SELECT, replacements: [supportHubList] });
            if (supportNodeList.length > 0) {
                selectedCycleResult = await calcOpsSummary(selectedCycleResult, startDate, endDate, supportNodeList, supportHubList);

                preCycleResult = await calcOpsSummary(preCycleResult, preCycleStartDate, preCycleEndDate, supportNodeList, supportHubList);
                return res.json(utils.response(1, {selectedCycleResult, preCycleResult}));
            } else {
                return res.json(utils.response(1, {selectedCycleResult, preCycleResult}));
            }
         } else {
            return res.json(utils.response(1, {selectedCycleResult, preCycleResult}));
         }
    }
}