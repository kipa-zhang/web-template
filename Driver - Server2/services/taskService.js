const log = require('../log/winston').logger('Task Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const hubNodeConf = require('../conf/hubNodeConf');
const CONTENT = require('../util/content');
const jsonfile = require('jsonfile')

const moment = require('moment');
let axios = require('axios');

const { Sequelize, Op, QueryTypes, NUMBER } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { Unit } = require('../model/unit.js');
const { Vehicle } = require('../model/vehicle.js');
const { Task } = require('../model/task.js');
const { MT_RAC } = require('../model/mtRac.js');
const { loan } = require('../model/loan.js');
const { SOS } = require('../model/sos.js');
const { PurposeMode: SystemPurposeMode } = require('../model/system/purposeMode.js');

const { UnitUtils } = require('./unitService');
const driverService = require('./driverService');
const userService = require('./userService')
const unitService = require('./unitService');


const { cacheData, cacheCommonData } = require('../cache_tool/dashboard.js');

const { User } = require('../model/user');
const { OperationRecord } = require('../model/operationRecord');
const header = {
    "AccountKey": "3c4imVWJT6u+yh3WphD2OQ==",
    "accept": "application/json",
}

const TaskUtils = {
    getPurpose: async () => {
        let purposeList = []
        let systemPurpose = await SystemPurposeMode.findAll();
        purposeList = purposeList.concat(systemPurpose.map(purpose => purpose.name));
        purposeList = Array.from(new Set(purposeList))
        return purposeList;
    },           
    delSameObjValue: async (arr, resultNum, keyName, keyValue) => {
        const warp = new Map();
        arr.forEach(i => {
            let str = keyName.map(v => i[v]).join('_');
            i[resultNum] = keyValue.reduce((p, c) => p += i[c], 0);
            warp.has(str) ? warp.get(str)[resultNum] += i[resultNum] : warp.set(str, i);
        });
        return Array.from(warp).map(([, v]) => v);
    },
    parseData: async function (property) {
        return function(a, b) {
            let value1 = a[property];
            let value2 = b[property];
            return Date.parse(value2) - Date.parse(value1)
        }
    },
    getGroupById: async function (id) {
        let group = await sequelizeSystemObj.query(
            `select id, groupName from \`group\` where id = ? `, {
                type: QueryTypes.SELECT,
                replacements: [ id ]
            }
        );
        return group[0]
    },
    getGroupVehicleNoList: async function (groupId) {
        try {
            let vehicleNoList = await sequelizeSystemObj.query(`
                select v.vehicleNumber from vehicle v
                LEFT JOIN job_task jt ON jt.id = v.taskId
                LEFT JOIN job j ON j.requestId = jt.requestId
                LEFT JOIN service_type st ON j.serviceTypeId = st.id
                LEFT JOIN request r ON r.id = jt.requestId
                where r.groupId = ? and j.driver = 0 
                and jt.taskStatus = 'Assigned'
                and st.category = 'MV'
                and (now() >= DATE_FORMAT(jt.startDate, '%Y-%m-%d %H:%i:%s') and now() <= DATE_FORMAT(jt.endDate, '%Y-%m-%d %H:%i:%s'))
                GROUP BY v.vehicleNumber
            `, {
                type: QueryTypes.SELECT,
                replacements: [ groupId ]
            });
            vehicleNoList = vehicleNoList.map(item => item.vehicleNumber)
            return vehicleNoList;
        } catch (error) {
            log.error(`getGroupVehicleNoList:`, error)
            return []
        }
    },
    checkoutAlertEvent: async function (locationList, hubNodeGroup) {
        try {
            let result = [], alertZoneList = []
            if (hubNodeGroup.hub && hubNodeGroup.hub != '-') {
                alertZoneList = await this.getNoGoZoneListByHubNode(hubNodeGroup.hub, hubNodeGroup.node)
            } else if (hubNodeGroup.groupId) {
                alertZoneList = await this.getNoGoZoneListByGroup(hubNodeGroup.groupId)
            }
            for (let alertZone of alertZoneList) {
                log.info(`checkoutAlertEvent => ${ alertZone.zoneName }`)
                for (let location of locationList) {
                    location.createdAt = moment(location.createdAt).format('YYYY-MM-DD HH:mm:ss')
                    if (this.checkAlertDate(alertZone, location.createdAt) 
                        && this.checkAlertTime(alertZone, location.createdAt)
                        && this.checkPointInPolygon([location.lat, location.lng], JSON.parse(alertZone.polygon))) {
                            result.push({
                                driverName: location.driverName,
                                vehicleNo: location.vehicleNo,
                                createdAt: location.createdAt,
                                zoneName: alertZone.zoneName
                            })
                    }
                }
            }

            return result;
        } catch (error) {
            log.error(error)
            return []
        }
    },
    checkAlertDate: function (noGoZone, dateTime) {
        let currentDate = moment(dateTime).format('YYYY-MM-DD')
        return moment(currentDate, 'YYYY-MM-DD').isBetween(moment(noGoZone.startDate, 'YYYY-MM-DD'), moment(noGoZone.endDate, 'YYYY-MM-DD'), null, []);
    },
    checkAlertTime: function (noGoZone, dateTime) {
        // DATA => 'YYYY-MM-DD HH:mm:ss'
        const checkWeek = function (selectedWeeks, date) {
            let week = moment(date).day()
            let weeks = selectedWeeks.split(',').map(item => Number.parseInt(item))
            return weeks.indexOf(week) > -1;
        }
    
        // DATA => 'YYYY-MM-DD HH:mm:ss'
        const checkTime = function (selectedTimes, date) {
            let timezones = selectedTimes.split(',')
            for (let timezone of timezones) {
                let timeList = timezone.split('-').map(item => item.trim())
                // Compare 'HH:mm:ss'
                if (moment(moment(date, 'YYYY-MM-DD HH:mm:ss').format('HH:mm:ss'), 'HH:mm:ss').isBetween(moment(timeList[0] + ':00', 'HH:mm:ss'), moment(timeList[1] + ':59', 'HH:mm:ss'))) {
                    return true;
                }
            }
            return false
        }
    
        let selectedTimes = noGoZone.selectedTimes
        let selectedWeeks = noGoZone.selectedWeeks
        if (!selectedTimes || !selectedWeeks) return false
    
        if (checkWeek(selectedWeeks, dateTime) && checkTime(selectedTimes, dateTime)) {
            log.warn(`********************************`)
            log.warn(selectedWeeks)
            log.warn(selectedTimes)
            log.warn(dateTime)
            log.warn(`********************************`)
            return true
        }
    
        return false
    },
    checkPointInPolygon: function (point, polygon) {
        let x = point[0], y = point[1];
    
        let inside = false;
        for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
            let xi = polygon[i][0], yi = polygon[i][1];
            let xj = polygon[j][0], yj = polygon[j][1];
    
            let intersect = (( yi > y ) != ( yj > y )) &&
                (x < ( xj - xi ) * ( y - yi ) / ( yj - yi ) + xi);
            if (intersect) inside = !inside;
        }
    
        return inside;
    },
    getNoGoZoneList: async function (user) {
        try {
            let sql = `
                SELECT nz.*, u.unitId, u.userType,
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                LEFT JOIN user u on nz.owner = u.userId
                WHERE nz.deleted = 0 and nz.alertType = 1 and nz.enable = 1
            `
            
            if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                sql += ` AND (u.unitId = ${ user.unitId } AND u.userType = '${ CONTENT.USER_TYPE.CUSTOMER }') `
            } else if ([CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.HQ].includes(user.userType)) {
                sql += ` AND 1=1 `
            } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
                let permitUnitIdList = await unitService.UnitUtils.getUnitIdByUnitAndSubUnit(user.unit, user.subUnit);
                sql += ` AND (u.unitId IN (${ permitUnitIdList }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') `
            } else {
                sql += ` AND 1=2 `
            }

            sql += ` GROUP BY nz.id `

            let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
            return noGoZoneList
        } catch (error) {
            return []
        }
    },
    getNoGoZoneListByHubNode: async function (hub, node) {
        try {
            let sql = `
                SELECT nz.*, u.unitId, u.userType,
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                LEFT JOIN user u on nz.owner = u.userId
                WHERE nz.deleted = 0 and nz.alertType = 1 and nz.enable = 1
            `
            // node
            let permitUnitIdList = await unitService.UnitUtils.getUnitIdByUnitAndSubUnit(hub, node);
            // hub
            let permitUnitIdList2 = await unitService.UnitUtils.getUnitIdByUnitAndSubUnit(hub);
            sql += ` AND (
                (u.unitId IN (${ permitUnitIdList }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') 
                OR
                (u.unitId IN (${ permitUnitIdList2 }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') 
                OR
                (u.userType IN ('${ CONTENT.USER_TYPE.HQ }', '${ CONTENT.USER_TYPE.ADMINISTRATOR }'))
            )`

            sql += ` GROUP BY nz.id `
            let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
            return noGoZoneList
        } catch (error) {
            log.error(error)
            return []
        }
    },
    getNoGoZoneListByGroup: async function (groupId) {
        try {
            let sql = `
                SELECT nz.*, u.unitId, u.userType,
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                LEFT JOIN user u on u.userId = nz.owner
                WHERE nz.deleted = 0 and nz.alertType = 1 and nz.enable = 1                
            `

            sql += ` AND (
                (u.unitId = ? and u.userType = ?) 
                OR
                (u.userType IN (?, ?))
            )`

            let replacements = [ groupId, CONTENT.USER_TYPE.CUSTOMER, CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.ADMINISTRATOR ]
            sql += ` GROUP BY nz.id `
            let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })
            return noGoZoneList
        } catch (error) {
            return []
        }
    }
}

const generateTotalTask = function (hubNodeDataArray) {
    let opsCount = 0;
    let trainingCount = 0;
    let adminCount = 0;
    let exerciseCount = 0;
    let dutyCount = 0;
    let drivingTrainingCount = 0;
    let maintenanceCount = 0;
    let othersCount = 0;
    let familiarisationCount = 0;
    let wptCount = 0;
    let mptCount = 0;
    let aviCount = 0;
    let pmCount = 0;
    let totalTaskCount = 0;

    hubNodeDataArray.forEach(item => totalTaskCount += item.taskNum)

    let hasPurposeList = hubNodeDataArray.filter(item => item.purpose)
    for (let temp of hasPurposeList) {
        if (temp.purpose.toLowerCase().startsWith('ops')) {
            opsCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('training')) {
            trainingCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('admin')) {
            adminCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('exercise')) {
            exerciseCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('duty')) {
            dutyCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('driving training')) {
            drivingTrainingCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('maintenance')) {
            maintenanceCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('others')) {
            othersCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('familiarisation')) {
            familiarisationCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('wpt')) {
            wptCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('mpt')) {
            mptCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('avi')) {
            aviCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('pm')) {
            pmCount += temp.taskNum
        } 
    }
    return {
        totalTaskCount,
        opsCount,
        trainingCount,
        adminCount,
        exerciseCount,
        dutyCount,
        drivingTrainingCount,
        maintenanceCount,
        othersCount,
        familiarisationCount,
        wptCount,
        mptCount,
        aviCount,
        pmCount,
    }
}

const generateStartedTask = function (hubNodeStartedDataArray) {
    let opsStartedCount = 0;
    let trainingStartedCount = 0;
    let adminStartedCount = 0;
    let exerciseStartedCount = 0;
    let dutyStartedCount = 0;
    let drivingTrainingStartedCount = 0;
    let maintenanceStartedCount = 0;
    let othersStartedCount = 0;
    let familiarisationStartedCount = 0;
    let wptStartedCount = 0;
    let mptStartedCount = 0;
    let aviStartedCount = 0;
    let pmStartedCount = 0;

    let hasPurposeList = hubNodeStartedDataArray.filter(item => item.purpose)
    for (let temp of hasPurposeList) {
        if (temp.purpose.toLowerCase().startsWith('ops')) {
            opsStartedCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('training')) {
            trainingStartedCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('admin')) {
            adminStartedCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('exercise')) {
            exerciseStartedCount += temp.taskNum
        } else if (temp.purpose.toLowerCase().startsWith('duty')) {
            dutyStartedCount += temp.taskNum
         } else if (temp.purpose.toLowerCase().startsWith('driving training')) {
            drivingTrainingStartedCount += temp.taskNum
         } else if (temp.purpose.toLowerCase().startsWith('maintenance')) {
            maintenanceStartedCount += temp.taskNum
         } else if (temp.purpose.toLowerCase().startsWith('others')) {
            othersStartedCount += temp.taskNum
         }  else if (temp.purpose.toLowerCase().startsWith('familiarisation')) {
            familiarisationStartedCount += temp.taskNum
         }  else if (temp.purpose.toLowerCase().startsWith('wpt')) {
            wptStartedCount += temp.taskNum
         }  else if (temp.purpose.toLowerCase().startsWith('mpt')) {
            mptStartedCount += temp.taskNum
         }  else if (temp.purpose.toLowerCase().startsWith('avi')) {
            aviStartedCount += temp.taskNum
         }  else if (temp.purpose.toLowerCase().startsWith('pm')) {
            pmStartedCount += temp.taskNum
         }
    }

    return {
        opsStartedCount,
        trainingStartedCount,
        adminStartedCount,
        exerciseStartedCount,
        dutyStartedCount,
        drivingTrainingStartedCount,
        maintenanceStartedCount,
        othersStartedCount,
        familiarisationStartedCount,
        wptStartedCount,
        mptStartedCount,
        aviStartedCount,
        pmStartedCount
    }
}

const getHubNodeList = async function (hub, user) {
    let hubNodeList = []
    if (hub) {
        let unitData = await Unit.findAll({ where: { unit: hub } })
        hubNodeList = unitData.map(item => {
            return {
                hub: item.unit,
                node: item.subUnit ? item.subUnit : 'Other',
                lat: item.lat,
                lng: item.lng
            }
        })
    } else if (user.unitId) {
        let unitList = await Unit.findOne({ where: { id: user.unitId } })
        if(unitList.subUnit){
            hubNodeList.push({
                hub: unitList.unit,
                node: unitList.subUnit ? unitList.subUnit : 'Other',
                lat: unitList.lat,
                lng: unitList.lng
            })
        } else {
            let unitData = await Unit.findAll({ where: { unit: unitList.unit } })
            hubNodeList = unitData.map(item => {
                return {
                    hub: item.unit,
                    node: item.subUnit ? item.subUnit : 'Other',
                    lat: item.lat,
                    lng: item.lng
                }
            })
        }
    } else {
        let unitData = await Unit.findAll()
        hubNodeList = unitData.map(item => {
            return {
                hub: item.unit,
                node: item.subUnit ? item.subUnit : 'Other',
                lat: item.lat,
                lng: item.lng
            }
        })
    }

    return hubNodeList
}

const getPermitData = async function (hub, user) {
    let hubNodeIdList = [], groupId = null;

    if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
        groupId = user.unitId;
        log.info(`getTodayOffenceDashboard => (groupId) : ${ groupId }`)
    } else {
        if (!hub) {
            let unitList = await unitService.UnitUtils.getPermitUnitList2(user.userId);
            hubNodeIdList = unitList.hubNodeIdList;
        } else {
            hubNodeIdList = await unitService.UnitUtils.getUnitIdByUnitAndSubUnit(hub);
        }
        log.info(`getTodayOffenceDashboard => (hubNodeIdList) : ${ JSON.stringify(hubNodeIdList) }`)
    }

    return {
        hubNodeIdList,
        groupId
    }
}

const generateTempSql = function (unitIdList, groupIdList) {
    let tempSqlList = [], tempReplacements0 = []
    if (unitIdList.length) {
        tempSqlList.push(` u.id IN ( ? ) `) 
        tempReplacements0.push(unitIdList)
    }
    if (groupIdList.length) {
        tempSqlList.push(` tt.groupId in ( ? ) `) 
        tempReplacements0.push(groupIdList)
    }

    return {
        tempSqlList,
        tempReplacements0
    }
}
const generateUrgentTempSql = function (unitIdList, groupIdList) {
    let tempSqlList = [], tempReplacements0 = []
    if (unitIdList.length) {
        tempSqlList.push(` u.id IN ( ? ) `) 
        tempReplacements0.push(unitIdList)
    }
    if (groupIdList.length) {
        tempSqlList.push(` ui.groupId in ( ? ) `) 
        tempReplacements0.push(groupIdList)
    }

    return {
        tempSqlList,
        tempReplacements0
    }
}
const  generateLimitSQL = async function (hub, user, hubNodeIdList) {
    let tempSql = ``, tempReplacements = []
    if (CONTENT.USER_TYPE.CUSTOMER == user.userType) {
        tempSql += ` AND tt.groupId = ? `
        tempReplacements.push(user.unitId)
    } else if ([ CONTENT.USER_TYPE.HQ ].includes(user.userType)) {
        let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)

        if (hub) {
            tempSql += ` AND tt.hub = ? `
            tempReplacements.push(hub)
        } else if (hub == null) {
            tempSql += ` AND tt.groupId IS NOT NULL `
        }

        let { tempSqlList, tempReplacements0 } = generateTempSql(unitIdList, groupIdList)
        tempReplacements = tempReplacements.concat(tempReplacements0)

        if (tempSqlList.length) {
            tempSql += ` and (${ tempSqlList.join(' OR ') }) ` 
        } else {
            tempSql += ` and 1=2 ` 
        }
    } else if ([ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.LICENSING_OFFICER ].includes(user.userType)) {
        if (hub) {
            tempSql += ` AND tt.hub = ? `
            tempReplacements.push(hub)
        } else if (hub == null) {
            tempSql += ` AND tt.groupId IS NOT NULL `
        }
    } else if (CONTENT.USER_TYPE.UNIT == user.userType) {
        tempSql += ` AND ( u.id IN ( ? ) AND tt.groupId IS NULL ) `
        tempReplacements.push(hubNodeIdList)
    }

    return {
        tempSql,
        tempReplacements
    }
}
const  generateUrgentLimitSQL = async function (hub, user, hubNodeIdList) {
    let tempSql = ``, tempReplacements = []
    if (CONTENT.USER_TYPE.CUSTOMER == user.userType) {
        tempSql += ` AND ui.groupId = ? `
        tempReplacements.push(user.unitId)
    } else if ([ CONTENT.USER_TYPE.HQ ].includes(user.userType)) {
        let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)

        if (hub) {
            tempSql += ` AND ui.hub = ? `
            tempReplacements.push(hub)
        } else if (hub == null) {
            tempSql += ` AND ui.groupId IS NOT NULL `
        }

        let { tempSqlList, tempReplacements0 } = generateUrgentTempSql(unitIdList, groupIdList)
        tempReplacements = tempReplacements.concat(tempReplacements0)

        if (tempSqlList.length) {
            tempSql += ` and (${ tempSqlList.join(' OR ') }) ` 
        } else {
            tempSql += ` and 1=2 ` 
        }
    } else if ([ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.LICENSING_OFFICER ].includes(user.userType)) {
        if (hub) {
            tempSql += ` AND ui.hub = ? `
            tempReplacements.push(hub)
        } else if (hub == null) {
            tempSql += ` AND ui.groupId IS NOT NULL `
        }
    } else if (CONTENT.USER_TYPE.UNIT == user.userType) {
        tempSql += ` AND ( u.id IN ( ? ) AND ui.groupId IS NULL ) `
        tempReplacements.push(hubNodeIdList)
    }

    return {
        tempSql,
        tempReplacements
    }
}

const generateLimitCondition = async function (group, user) {
    const getLimitConditionByGroup = function (group) {
        let limitCondition = [], replacements = []
        if (group) {
            limitCondition.push(` t.groupId = ? `)
            replacements.push(group)
        } else if (group == 0) {
            limitCondition.push(` t.groupId IS NOT NULL `)
        } else {
            limitCondition.push(` t.groupId IS NULL `)
        }

        return {
            limitCondition,
            replacements
        }
    }
    const getLimitConditionByGroup2 = async function (group, user) {
        let limitCondition = [], replacements = []
        
        let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)
        let tempSqlList = []
        if (unitIdList.length) {
            tempSqlList.push(` u.id IN (?) `)
            replacements.push(unitIdList);
        }
        if (groupIdList.length) {
            tempSqlList.push(` t.groupId in (?) `)
            replacements.push(groupIdList);
        }
        
        if (tempSqlList.length) {
            limitCondition.push(` (${ tempSqlList.join(' OR ') }) `)
        } else {
            limitCondition.push(` 1=2 `)
        }

        // HQ user has group permission
        if (user.group?.length) {
            if (group) {
                limitCondition.push(` t.groupId = ? `)
                replacements.push(group)
            } else if (group == 0) {
                limitCondition.push(` t.groupId in (?) `)
                replacements.push(groupIdList)
            } else {
                limitCondition.push(` t.groupId IS NULL `)
            }
        }

        return {
            limitCondition,
            replacements
        }
    }

    let limitCondition = [], replacements = []

    if (user.userType.toLowerCase() == 'customer') {
        limitCondition.push(` t.groupId = ? `)
        replacements.push(user.unitId)
    } else if (user.userType.toLowerCase() == 'administrator') {
        let result = getLimitConditionByGroup(group)
        limitCondition = limitCondition.concat(result.limitCondition)
        replacements = replacements.concat(result.replacements)
    } else if (user.userType.toLowerCase() == 'hq') {
        let result = await getLimitConditionByGroup2(group, user)
        limitCondition = limitCondition.concat(result.limitCondition)
        replacements = replacements.concat(result.replacements)
    } else if (user.userType.toLowerCase() == 'unit') {
        let hubNodeIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.hub, user.node);
        if (hubNodeIdList.length) {
            // Maybe not more than 1000 node in one hub
            limitCondition.push(` ( u.id IN (?) AND t.groupId IS NULL ) `)
            replacements.push(hubNodeIdList);
        }
    }

    return {
        limitCondition,
        replacements
    }
}


module.exports = {
    TaskUtils,
    getPurposeList: async (req, res) => {
        let purposeList = await TaskUtils.getPurpose();
        return res.json(utils.response(1, purposeList));
    },
    //2023-06-15 Customer user,Add two purpose(WPT and MPT), task null and - (DV/LOA)
    getHubByPurpose2: async (req, res) => {
        try {
            const generateHubData = function (hub, hubConf) {
                let hubData = { hub: (hub && hub != '-') ? hub : 'DV_LOA', purposeData: [] }

                hubData.circleColor = hubNodeConf.defaultColor
                hubConf.forEach(item => {
                    if (hubData.hub?.toLowerCase() == item.hub?.toLowerCase()) {
                        hubData.circleColor = item.color
                    } 
                })

                return hubData
            }

            let userId = req.body.userId;
            let timeSelected = req.body.timeSelected;
            if (!timeSelected) timeSelected = moment().format('YYYY-MM-DD');
            let user = await User.findOne({ where: { userId: userId } })

            let userUnit = await UnitUtils.getPermitUnitList(userId);
            let unitList = userUnit.unitList
            if(userUnit.groupIdList || (!user.hq && !user.unitId)) unitList.push(null)

            let taskList = await sequelizeObj.query(`
                select count(t.taskId) as taskNum, t.hub, t.purpose from task t
                LEFT JOIN driver d ON d.driverId = t.driverId
                where t.driverId is not null and t.driverStatus != 'Cancelled' and d.permitStatus != 'invalid' and
                ((? between (DATE_FORMAT(t.indentStartTime,'%Y-%m-%d')) and DATE_FORMAT(t.indentEndTime,'%Y-%m-%d'))
                OR t.driverStatus = 'started') ${ req.cookies.userType != 'ADMINISTRATOR' ? ` and t.hub != '-' ` : '' }
                GROUP BY t.hub, t.purpose 
            `, { 
                type: QueryTypes.SELECT,
                replacements: [ moment(timeSelected).format('YYYY-MM-DD') ] 
            })

            let taskListByGroup = null
            if(userUnit?.groupIdList.length){
                taskListByGroup = await sequelizeObj.query(`
                    select count(t.taskId) as taskNum, t.hub, t.purpose from task t
                    LEFT JOIN driver d ON d.driverId = t.driverId
                    where t.driverId is not null and t.driverStatus != 'Cancelled' and d.permitStatus != 'invalid' and
                    ((? between (DATE_FORMAT(t.indentStartTime,'%Y-%m-%d')) and DATE_FORMAT(t.indentEndTime,'%Y-%m-%d'))
                    OR t.driverStatus = 'started')
                    and t.groupId in ( ? )
                    GROUP BY t.hub, t.purpose 
                `, { 
                    type: QueryTypes.SELECT,
                    replacements: [ moment(timeSelected).format('YYYY-MM-DD'), userUnit.groupIdList ] 
                })
            }
    
             let result = []
             let hubConf = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
             for (let hub of unitList) {
                let hubData = generateHubData(hub, hubConf)
                
                // total task
                let taskListDataArray
                if (hub) {
                    taskListDataArray = taskList.filter(item => item.hub == hub);
                } else if (taskListByGroup) {
                    taskListDataArray = taskListByGroup.filter(item => item);
                } else {
                    taskListDataArray = taskList.filter(item => (!item.hub || item.hub == '-'));
                }
                let {
                    opsCount,
                    trainingCount,
                    adminCount,
                    exerciseCount,
                    dutyCount,
                    drivingTrainingCount,
                    maintenanceCount,
                    othersCount,
                    familiarisationCount,
                    wptCount,
                    mptCount,
                    aviCount,
                    pmCount,
                } = generateTotalTask(taskListDataArray)
                   
                 
                if((hubData.hub).toUpperCase() == 'DV_LOA') {
                    hubData.purposeData.push({purpose: 'Ops', taskCount: opsCount})
                    hubData.purposeData.push({purpose: 'Training', taskCount: trainingCount})
                    hubData.purposeData.push({purpose: 'Admin', taskCount: adminCount})   
                    hubData.purposeData.push({purpose: 'WPT', taskCount: wptCount})
                    hubData.purposeData.push({purpose: 'MPT', taskCount: mptCount})   
                } else {
                    hubData.purposeData.push({purpose: 'Ops', taskCount: opsCount})
                    hubData.purposeData.push({purpose: 'Training', taskCount: trainingCount})
                    hubData.purposeData.push({purpose: 'Admin', taskCount: adminCount})   
                    hubData.purposeData.push({purpose: 'Exercise', taskCount: exerciseCount})
                    hubData.purposeData.push({purpose: 'Duty', taskCount: dutyCount})
                    hubData.purposeData.push({purpose: 'Driving Training', taskCount: drivingTrainingCount})
                    hubData.purposeData.push({purpose: 'Maintenance', taskCount: maintenanceCount})
                    hubData.purposeData.push({purpose: 'Others', taskCount: othersCount})
                    hubData.purposeData.push({purpose: 'Familiarisation', taskCount: familiarisationCount})
                    hubData.purposeData.push({purpose: 'WPT', taskCount: wptCount})
                    hubData.purposeData.push({purpose: 'MPT', taskCount: mptCount})   
                    hubData.purposeData.push({purpose: 'AVI', taskCount: aviCount})
                    hubData.purposeData.push({purpose: 'PM', taskCount: pmCount})   

                }
            
                result.push(hubData);
            }
                     
            return res.json(utils.response(1, result));
        } catch(error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    //2023-06-15 Customer user,Add two purpose(WPT and MPT) , task null and - (DV/LOA)
    getNodeByPurpose2: async (req, res) => {
        try {
            let userId = req.body.userId;
            if (!userId) {
                log.error(`UserId ${ userId } is not correct! current login user is ${ req.cookies.userId }`)
                return res.json(utils.response(0, `UserId ${ userId } is not correct!`));
            }
            let timeSelected = req.body.timeSelected;
            let hub = req.body.hub;
            if (!timeSelected) timeSelected = moment().format('YYYY-MM-DD');
            let user = await User.findOne({ where: { userId: userId } })
            let hubNodeList = await getHubNodeList(hub, user)
            
            let nodeTaskData = await sequelizeObj.query(`
                SELECT
                    t.hubNode,
                    t.purpose,
                    count(*) AS taskNum
                FROM (
                        SELECT tt.taskId, CONCAT(tt.hub, '-', IFNULL(tt.node, 'Other')) AS hubNode, tt.purpose
                        FROM task tt
                        LEFT JOIN driver d ON d.driverId = tt.driverId
                        WHERE tt.driverId is not null and tt.driverStatus != 'Cancelled' and d.permitStatus != 'invalid' and
                        ((? between (DATE_FORMAT(tt.indentStartTime,'%Y-%m-%d')) and DATE_FORMAT(tt.indentEndTime,'%Y-%m-%d'))
                        or tt.driverStatus = 'started')
                ) t GROUP BY t.hubNode,t.purpose
            `, { 
                type: QueryTypes.SELECT,
                replacements: [ moment(timeSelected).format('YYYY-MM-DD') ]
            })
    
            let nodeStartedTaskData = await sequelizeObj.query(`
                SELECT
                    t.hubNode,
                    t.purpose,
                    count(*) AS taskNum
                FROM (
                        SELECT tt.taskId, CONCAT(tt.hub, '-', IFNULL(tt.node, 'Other')) AS hubNode, tt.purpose
                        FROM task tt
                        LEFT JOIN driver d ON d.driverId = tt.driverId
                        WHERE tt.mobileStartTime is not null and tt.driverId is not null and d.permitStatus != 'invalid' and
                        ? between (DATE_FORMAT(tt.indentStartTime,'%Y-%m-%d')) and DATE_FORMAT(tt.indentEndTime,'%Y-%m-%d')
                ) t GROUP BY t.hubNode,t.purpose
            `, { 
                type: QueryTypes.SELECT,
                replacements: [ moment(timeSelected).format('YYYY-MM-DD') ]
            })
    
            let result = []
            log.info(`getNodeByPurpose => (hubNodeList) : ${ JSON.stringify(hubNodeList) }`)
            for (let hubNode of hubNodeList) {
                let hubData = { hub: hubNode.hub, node: hubNode.node, totalTaskCount: 0, location: { lat: hubNode.lat, lng: hubNode.lng }, purposeData: [] }
                // total task
                let hubNodeDataArray = nodeTaskData.filter(item => item.hubNode == hubNode.hub + '-' +  hubNode.node);
                let {
                    totalTaskCount,
                    opsCount,
                    trainingCount,
                    adminCount,
                    exerciseCount,
                    dutyCount,
                    drivingTrainingCount,
                    maintenanceCount,
                    othersCount,
                    familiarisationCount,
                    wptCount,
                    mptCount,
                    aviCount,
                    pmCount,
                } = generateTotalTask(hubNodeDataArray)

                hubData.totalTaskCount = totalTaskCount

                // started task
                let hubNodeStartedDataArray = nodeStartedTaskData.filter(item => item.hubNode == hubNode.hub + '-' +  hubNode.node);
                let {
                    opsStartedCount,
                    trainingStartedCount,
                    adminStartedCount,
                    exerciseStartedCount,
                    dutyStartedCount,
                    drivingTrainingStartedCount,
                    maintenanceStartedCount,
                    othersStartedCount,
                    familiarisationStartedCount,
                    wptStartedCount,
                    mptStartedCount,
                    aviStartedCount,
                    pmStartedCount
                } = generateStartedTask(hubNodeStartedDataArray)

                if((hubData.hub).toUpperCase() == 'DV_LOA') {
                    hubData.purposeData.push({purpose: 'Ops', taskCount: opsCount, startedTaskCount: opsStartedCount})
                    hubData.purposeData.push({purpose: 'Training', taskCount: trainingCount, startedTaskCount: trainingStartedCount})
                    hubData.purposeData.push({purpose: 'Admin', taskCount: adminCount, startedTaskCount: adminStartedCount})
                    hubData.purposeData.push({purpose: 'WPT', taskCount: wptCount, startedTaskCount: wptStartedCount})
                    hubData.purposeData.push({purpose: 'MPT', taskCount: mptCount, startedTaskCount: mptStartedCount})
                } else {
                    hubData.purposeData.push({purpose: 'Ops', taskCount: opsCount, startedTaskCount: opsStartedCount})
                    hubData.purposeData.push({purpose: 'Training', taskCount: trainingCount, startedTaskCount: trainingStartedCount})
                    hubData.purposeData.push({purpose: 'Admin', taskCount: adminCount, startedTaskCount: adminStartedCount})
                    hubData.purposeData.push({purpose: 'Exercise', taskCount: exerciseCount, startedTaskCount: exerciseStartedCount})
                    hubData.purposeData.push({purpose: 'Duty', taskCount: dutyCount, startedTaskCount: dutyStartedCount})
                    hubData.purposeData.push({purpose: 'Driving Training', taskCount: drivingTrainingCount, startedTaskCount: drivingTrainingStartedCount})
                    hubData.purposeData.push({purpose: 'Maintenance', taskCount: maintenanceCount, startedTaskCount: maintenanceStartedCount})
                    hubData.purposeData.push({purpose: 'Others', taskCount: othersCount, startedTaskCount: othersStartedCount})
                    hubData.purposeData.push({purpose: 'Familiarisation', taskCount: familiarisationCount, startedTaskCount: familiarisationStartedCount})
                    hubData.purposeData.push({purpose: 'WPT', taskCount: wptCount, startedTaskCount: wptStartedCount})
                    hubData.purposeData.push({purpose: 'MPT', taskCount: mptCount, startedTaskCount: mptStartedCount})
                    hubData.purposeData.push({purpose: 'AVI', taskCount: aviCount, startedTaskCount: aviStartedCount})
                    hubData.purposeData.push({purpose: 'PM', taskCount: pmCount, startedTaskCount: pmStartedCount})
                }
                result.push(hubData);
            }
            return res.json(utils.response(1, result));
        } catch(error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },

    getTodayOffenceDashboard: async (req, res) => {
        try {
            
            let timeSelected = req.body.timeSelected
            if (!timeSelected) {
                timeSelected = moment().format('YYYY-MM-DD');
            }

            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let hub = req.body.hub
            let {
                hubNodeIdList
            } = await getPermitData(hub, user)

            // Driver
            let driverSql = `
                SELECT SUBSTRING_INDEX(GROUP_CONCAT(tt.occTime ORDER BY tt.occTime DESC), ',', 1) AS lastOccTime, COUNT(*) AS total, tt.* FROM 
                (
                    SELECT th.deviceId, th.violationType, th.vehicleNo, v.vehicleType, th.occTime, th.dataFrom, d.driverName, tt.* 
                    FROM track_history th
                    LEFT JOIN (
                        select driverId, unitId, nric, driverName, contactNumber, state FROM driver 
                        UNION ALL 
                        select driverId, unitId, nric, driverName, contactNumber, state FROM driver_history
                    ) d ON d.driverId = th.deviceId
                    LEFT JOIN (
                        select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle 
                        UNION ALL 
                        select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle_history
                    ) v ON v.vehicleNo = th.vehicleNo
                    LEFT JOIN user us ON d.driverId = us.driverId
                    LEFT JOIN (
                        SELECT t.driverId, t.taskId, t.mobileStartTime, t.mobileEndTime, t.hub, t.node, t.vehicleNumber, t.groupId
                        FROM task t WHERE 
                        ( (Date( ? ) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                        OR t.driverStatus = 'started'

                        UNION

                        SELECT ui.driverId, CONCAT('DUTY-', ui.dutyId) as taskId, ui.mobileStartTime, ui.mobileEndTime, 
                        ui.hub, ui.node, ui.vehicleNo as vehicleNumber, ui.groupId
                        FROM urgent_indent ui 
                        WHERE ( (Date( ? ) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                        OR ui.status IN ('started', 'ready')

                    ) tt ON tt.driverId = th.deviceId 
                    left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                    WHERE th.occTime LIKE ?
                    AND (th.occTime >= tt.mobileStartTime AND (tt.mobileEndTime is null or th.occTime <= tt.mobileEndTime ) )
                    AND th.dataFrom = 'mobile' 
                    AND tt.taskId is not null 
            `

            let replacements = [ timeSelected, timeSelected, timeSelected + '%']
            let driverResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            driverSql += driverResultSQL.tempSql;
            replacements = replacements.concat(driverResultSQL.tempReplacements)
            driverSql += ` 
                ORDER BY th.occTime DESC
                ) tt GROUP BY tt.deviceId, tt.violationType, vehicleNo
            `
            log.info(driverSql)
            let driverOffenceList = await sequelizeObj.query(driverSql, { type: QueryTypes.SELECT, replacements });

            let deviceSql = `
                SELECT SUBSTRING_INDEX(GROUP_CONCAT(tt.occTime ORDER BY tt.occTime DESC), ',', 1) AS lastOccTime, COUNT(*) AS total, tt.* FROM 
                (
                    SELECT th.deviceId, th.violationType, v.vehicleNo, v.vehicleType, th.occTime, th.dataFrom, tt.* FROM track_history th
                    LEFT JOIN (
                        select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle 
                        UNION ALL 
                        select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle_history
                    ) v ON v.deviceId = th.deviceId
                    LEFT JOIN (
                        SELECT t.driverId, t.taskId, t.mobileStartTime, t.mobileEndTime, t.hub, t.node, t.vehicleNumber, t.groupId
                        FROM task t WHERE ( (Date(?) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                        OR t.driverStatus = 'started'

                        UNION

                        SELECT ui.driverId, CONCAT('DUTY-', ui.dutyId) as taskId, ui.mobileStartTime, ui.mobileEndTime, 
                        ui.hub, ui.node, ui.vehicleNo as vehicleNumber, ui.groupId
                        FROM urgent_indent ui 
                        WHERE ( (Date(?) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                        OR ui.status IN ('started', 'ready')
                        
                    ) tt ON tt.vehicleNumber = v.vehicleNo 
                    left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                    WHERE th.occTime LIKE ?
                    AND (th.occTime >= tt.mobileStartTime AND (tt.mobileEndTime is null or th.occTime <= tt.mobileEndTime))
                    AND th.dataFrom = 'obd'
                    AND tt.taskId is not null 
            `
            replacements = [ timeSelected, timeSelected, timeSelected + '%' ]

            let deviceResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            deviceSql += deviceResultSQL.tempSql;
            replacements = replacements.concat(deviceResultSQL.tempReplacements)

            deviceSql += ` 
                ORDER BY th.occTime DESC
                ) tt GROUP BY tt.deviceId, tt.violationType
            `
            log.info(deviceSql)
            let deviceOffenceList = await sequelizeObj.query(deviceSql, { type: QueryTypes.SELECT, replacements });

            let groupList = await sequelizeSystemObj.query(`select * from \`group\``, { type: QueryTypes.SELECT })

            // Calculate result
            let result = { list: [], hardBraking: 0, rapidAcc: 0, speeding: 0, missing: 0, idleTime: 0, noGoAlert: 0, outOfService: 0, parked: 0, onRoad: 0 };
            
            let deviceList = Array.from(new Set(driverOffenceList.map(driverOffence => driverOffence.deviceId + ',' + driverOffence.vehicleNo)));
            deviceList = deviceList.map(device => { 
                let list = device.split(',')
                return {
                    deviceId: list[0],
                    vehicleNo: list[1]
                }
            })
            const generateDriverResult = function () {
                for (let device of deviceList) {
                    let data = {
                        hardBraking: {
                            count: 0,
                            occTime: null
                        },
                        rapidAcc: { 
                            count: 0,
                            occTime: null
                        },
                        speeding: {
                            count: 0,
                            occTime: null
                        },
                        missing: {
                            count: 0,
                            occTime: null
                        },
                        noGoAlert: {
                            count: 0,
                            occTime: null
                        }
                    }
                    for (let driverOffence of driverOffenceList) {
                        if (driverOffence.deviceId == device.deviceId && driverOffence.vehicleNo == device.vehicleNo) {
                            data.deviceId = device.deviceId;
                            data.vehicleNo = device.vehicleNo;
                            data.dataFrom = driverOffence.dataFrom;
                            data.driver = driverOffence.driverName;
                            data.vehicleType = driverOffence.vehicleType
                            data.hub = driverOffence.hub
                            data.node = driverOffence.node
                            data.groupId = driverOffence.groupId
                            if (data.groupId) {
                                groupList.some(item => {
                                    if (item.id == data.groupId) {
                                        data.groupName = item.groupName
                                        return true;
                                        
                                    }
                                })
                            }
    
                            switch (driverOffence.violationType) {
                                case CONTENT.ViolationType.HardBraking: {
                                    data.hardBraking = {
                                        count: driverOffence.total,
                                        occTime: driverOffence.lastOccTime,
                                    }
                                    result.hardBraking += driverOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.RapidAcc: {
                                    data.rapidAcc = {
                                        count: driverOffence.total,
                                        occTime: driverOffence.lastOccTime,
                                    }
                                    result.rapidAcc += driverOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.Speeding: {
                                    data.speeding = {
                                        count: driverOffence.total,
                                        occTime: driverOffence.lastOccTime,
                                    }
                                    result.speeding += driverOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.Missing: {
                                    data.missing = {
                                        count: driverOffence.total,
                                        occTime: driverOffence.lastOccTime,
                                    }
                                    result.missing += driverOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.NoGoZoneAlert: {
                                    data.noGoAlert = {
                                        count: driverOffence.total,
                                        occTime: driverOffence.lastOccTime,
                                    }
                                    result.noGoAlert += driverOffence.total
                                    break;
                                }
                            }
                        }
                    }
                    result.list.push(data)
                }
            }
            generateDriverResult()

            let deviceList2 = Array.from(new Set(deviceOffenceList.map(deviceOffence => deviceOffence.deviceId)));
            const generateDeviceResult = function () {
                for (let device of deviceList2) {
                    let data = { 
                        hardBraking: {
                            count: 0,
                            occTime: null
                        },
                        rapidAcc: {
                            count: 0,
                            occTime: null
                        },
                        speeding: {
                            count: 0,
                            occTime: null
                        },
                        missing: {
                            count: 0,
                            occTime: null
                        },
                        noGoAlert: {
                            count: 0,
                            occTime: null
                        }
                    }
                    for (let deviceOffence of deviceOffenceList) {
                        if (deviceOffence.deviceId == device) {
                            data.deviceId = device.deviceId;
                            data.vehicleNo = device.vehicleNo;
                            data.dataFrom = deviceOffence.dataFrom;
                            data.driver = deviceOffence.driverName;
                            data.vehicleType = deviceOffence.vehicleType
                            data.hub = deviceOffence.hub
                            data.node = deviceOffence.node
                            data.groupId = deviceOffence.groupId
                            if (data.groupId) {
                                groupList.some(item => {
                                    if (item.id == data.groupId) {
                                        data.groupName = item.groupName
                                        return true;
                                    }
                                })
                            }

                            switch (deviceOffence.violationType) {
                                case CONTENT.ViolationType.HardBraking: {
                                    data.hardBraking = {
                                        count: deviceOffence.total,
                                        occTime: deviceOffence.lastOccTime,
                                    }
                                    result.hardBraking += deviceOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.RapidAcc: {
                                    data.rapidAcc = {
                                        count: deviceOffence.total,
                                        occTime: deviceOffence.lastOccTime,
                                    }
                                    result.rapidAcc += deviceOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.Speeding: {
                                    data.speeding = {
                                        count: deviceOffence.total,
                                        occTime: deviceOffence.lastOccTime,
                                    }
                                    result.speeding += deviceOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.Missing: {
                                    data.missing = {
                                        count: deviceOffence.total,
                                        occTime: deviceOffence.lastOccTime,
                                    }
                                    result.missing += deviceOffence.total
                                    break;
                                }
                                case CONTENT.ViolationType.NoGoZoneAlert: {
                                    data.noGoAlert = {
                                        count: deviceOffence.total,
                                        occTime: deviceOffence.lastOccTime,
                                    }
                                    result.noGoAlert += deviceOffence.total
                                    break;
                                }
                            }

                        }
                    }
                    result.list.push(data)
                }
            }
            generateDeviceResult()

            if (req.body.timeSelected == moment().format('YYYY-MM-DD')) {
                cacheData({ url: req.originalUrl, hub, user }, result)
            }

            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error);
            return res.json(utils.response(0, error));
        }
    },
    getTodayRealSpeeding: async (req, res) => {
        try {
            let timeSelected = req.body.timeSelected
            if(!timeSelected){
                timeSelected = moment().format('YYYY-MM-DD');
            }
            let userId = req.cookies.userId;
            let hub = req.body.hub;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let { hubNodeIdList } = await getPermitData(hub, user)

            // Driver
            let replacements = [ timeSelected, timeSelected, timeSelected ]
            let driverSql = `
                SELECT rs.driverId, rs.vehicleNo, rs.createdAt, d.driverName, rs.speed, rs.limitSpeed
                FROM realtime_speeding rs
                LEFT JOIN (
                    select t.taskId, t.hub, t.node, t.groupId, t.driverId, t.vehicleNumber
                    from task t
                    where ( (Date(?) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                    OR t.driverStatus = 'started'
                    group by t.driverId, t.vehicleNumber

                    union

                    select ui.id, ui.hub, ui.node, ui.groupId, ui.driverId, ui.vehicleNo
                    from urgent_indent ui
                    where ( (Date(?) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                    OR ui.status = 'started'
                    group by ui.driverId, ui.vehicleNo
                ) tt ON tt.driverId = rs.driverId AND tt.vehicleNumber = rs.vehicleNo
                LEFT JOIN unit u ON u.unit <=> tt.hub AND u.subUnit <=> tt.node
                LEFT JOIN (
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver
                    UNION ALL
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) d ON d.driverId = rs.driverId
                WHERE DATE(rs.createdAt) = ? AND tt.taskId IS NOT NULL 
            `

            let driverResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            driverSql += driverResultSQL.tempSql;
            replacements = replacements.concat(driverResultSQL.tempReplacements)
            
            driverSql += ' ORDER BY rs.createdAt DESC '
            let driverSpeeding = await sequelizeObj.query(driverSql, {
                type: QueryTypes.SELECT, 
                replacements: replacements 
            });

            // Vehicle
            let replacements2 = [ timeSelected, timeSelected, timeSelected ]
            let deviceSql = `
                SELECT rs.driverId, rs.vehicleNo, rs.createdAt, d.driverName, rs.speed, rs.limitSpeed
                FROM realtime_speeding rs
                LEFT JOIN (
                    SELECT t0.taskId, t0.hub, t0.node, t0.groupId, t0.driverId, t0.vehicleNumber, v.deviceId
                    FROM task t0
                    LEFT JOIN vehicle v ON t0.vehicleNumber = v.vehicleNo
                    WHERE ( (DATE(?) BETWEEN DATE(t0.mobileStartTime) AND DATE(t0.mobileEndTime)) AND t0.mobileStartTime IS NOT NULL )
                    OR t0.driverStatus = 'started'
                    group by t0.driverId, t0.vehicleNumber

                    union

                    select ui.id, ui.hub, ui.node, ui.groupId, ui.driverId, ui.vehicleNo, v.deviceId
                    from urgent_indent ui
                    LEFT JOIN vehicle v ON ui.vehicleNo = v.vehicleNo
                    where ( (Date(?) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                    OR ui.status = 'started'
                    group by ui.driverId, ui.vehicleNo
                ) tt ON tt.deviceId = rs.deviceId
                LEFT JOIN unit u ON u.unit <=> tt.hub AND u.subUnit <=> tt.node
                LEFT JOIN (
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver
                    UNION ALL
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) d ON d.driverId = rs.driverId
                WHERE DATE(rs.createdAt) = ? AND tt.taskId IS NOT NULL
            `
            
            let deviceResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            deviceSql += deviceResultSQL.tempSql;
            replacements2 = replacements2.concat(deviceResultSQL.tempReplacements)

            deviceSql += ' ORDER BY rs.createdAt DESC '
            let deviceSpeeding = await sequelizeObj.query(deviceSql, { 
                type: QueryTypes.SELECT, 
                replacements: replacements2
            });

            let result = [].concat(driverSpeeding, deviceSpeeding)

            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },
    getTodayRealAlert: async (req, res) => {
        try {
            let timeSelected = req.body.timeSelected
            if (!timeSelected) {
                timeSelected = moment().format('YYYY-MM-DD');
            }
            let userId = req.cookies.userId;
            let hub = req.body.hub
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let { hubNodeIdList } = await getPermitData(hub, user)

            let replacements = [ timeSelected, timeSelected, timeSelected ]
            let sql = `
                SELECT ra.driverId, ra.vehicleNo, ra.createdAt, d.driverName, nz.zoneName 
                FROM realtime_alert ra
                LEFT JOIN (
                    select t.taskId, t.hub, t.node, t.groupId, t.driverId, t.vehicleNumber
                    from task t
                    where ( (Date( ? ) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                    OR t.driverStatus = 'started'
                    group by t.driverId, t.vehicleNumber

                    union

                    select ui.id, ui.hub, ui.node, ui.groupId, ui.driverId, ui.vehicleNo
                    from urgent_indent ui
                    where ( (Date( ? ) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                    OR ui.status = 'started'
                    group by ui.driverId, ui.vehicleNo
                ) tt ON tt.driverId = ra.driverId AND tt.vehicleNumber = ra.vehicleNo
                LEFT JOIN unit u ON u.unit <=> tt.hub AND u.subUnit <=> tt.node
                LEFT JOIN nogo_zone nz ON nz.id = ra.zoneId
                LEFT JOIN (
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver
                    UNION ALL
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) d ON d.driverId = ra.driverId
                WHERE DATE(ra.createdAt) = ? AND tt.taskId IS NOT NULL
            `
            
            let driverResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            sql += driverResultSQL.tempSql;
            replacements = replacements.concat(driverResultSQL.tempReplacements)

            sql += ' ORDER BY ra.createdAt DESC '
            let driverAlert = await sequelizeObj.query(sql, { 
                type: QueryTypes.SELECT,
                replacements: replacements
            })

            let replacements2 = [ timeSelected, timeSelected, timeSelected ]
            let sql2 = `
                SELECT ra.driverId, ra.vehicleNo, ra.createdAt, d.driverName, nz.zoneName 
                FROM realtime_alert ra
                LEFT JOIN (
                    SELECT t0.taskId, t0.hub, t0.node, t0.groupId, t0.driverId, t0.vehicleNumber, v.deviceId
                    FROM task t0
                    LEFT JOIN vehicle v ON t0.vehicleNumber = v.vehicleNo
                    WHERE ( (DATE(?) BETWEEN DATE(t0.mobileStartTime) AND DATE(t0.mobileEndTime)) AND t0.mobileStartTime IS NOT NULL )
                    OR t0.driverStatus = 'started'
                    group by t0.driverId, t0.vehicleNumber

                    union

                    select ui.id, ui.hub, ui.node, ui.groupId, ui.driverId, ui.vehicleNo, v.deviceId
                    from urgent_indent ui
                    LEFT JOIN vehicle v ON ui.vehicleNo = v.vehicleNo
                    where ( (Date(?) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                    OR ui.status = 'started'
                    group by ui.driverId, ui.vehicleNo
                ) tt ON tt.deviceId = ra.deviceId
                LEFT JOIN unit u ON u.unit <=> tt.hub AND u.subUnit <=> tt.node
                LEFT JOIN nogo_zone nz ON nz.id = ra.zoneId
                LEFT JOIN (
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver
                    UNION ALL
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) d ON d.driverId = ra.driverId
                WHERE DATE(ra.createdAt) = ? AND tt.taskId IS NOT NULL
            `

            let deviceResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            sql2 += deviceResultSQL.tempSql;
            replacements2 = replacements2.concat(deviceResultSQL.tempReplacements)

            sql2 += ' ORDER BY ra.createdAt DESC '
            let deviceAlert = await sequelizeObj.query(sql2, { 
                type: QueryTypes.SELECT,
                replacements: replacements2
            })

            let result = [].concat(driverAlert, deviceAlert)
            
            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(`getTodayRealAlert: `, error)
            return res.json(utils.response(0, error));
        }
    },
    getTodayOffenceList: async (req, res) => {
        try {
            let timeSelected = req.body.timeSelected;
            if (!timeSelected) {
                timeSelected = moment().format('YYYY-MM-DD');
            }

            let userId = req.cookies.userId;

            let hub = req.body.hub
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let { hubNodeIdList } = await getPermitData(hub, user)

            // driver
            let replacements = []
            let driverSql = `
                SELECT th.deviceId, th.violationType, th.vehicleNo, th.occTime, th.dataFrom, th.lat, th.lng, d.driverName
                FROM track_history th
                LEFT JOIN (
                    select driverId, unitId, nric, driverName, contactNumber, state FROM driver 
                    UNION ALL 
                    select driverId, unitId, nric, driverName, contactNumber, state FROM driver_history
                ) d ON d.driverId = th.deviceId
                LEFT JOIN (
                    SELECT t.taskId, t.hub, t.node, t.groupId, t.driverId, t.vehicleNumber, 
                    t.mobileStartTime, t.mobileEndTime 
                    FROM task t 
                    WHERE ( (Date(?) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                    OR t.driverStatus = 'started'

                    union

                    select CONCAT('DUTY-', dutyId) AS taskId, ui.hub, ui.node, ui.groupId, ui.driverId, ui.vehicleNo as vehicleNumber, 
                    ui.mobileStartTime, ui.mobileEndTime 
                    from urgent_indent ui 
                    where 1=1
                    AND (
                        ( (Date(?) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                        OR 
                        ui.status = 'started'
                    )

                ) tt ON tt.driverId = th.deviceId
                left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                left join user us on us.driverId = tt.driverId
                WHERE th.occTime LIKE ?
                AND (th.occTime >= tt.mobileStartTime AND (tt.mobileEndTime is null or tt.mobileEndTime >= th.occTime))
                AND th.dataFrom = 'mobile' 
                AND tt.taskId is not null  
            `;

            replacements = [ timeSelected, timeSelected, timeSelected + '%' ]

            let driverResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            driverSql += driverResultSQL.tempSql;
            replacements = replacements.concat(driverResultSQL.tempReplacements)

            driverSql += ` ORDER BY th.occTime DESC`
            log.info(driverSql)
            let driverOffenceList = await sequelizeObj.query(driverSql, { 
                type: QueryTypes.SELECT, 
                replacements 
            });

            // vehicle
            let replacements2 = []
            let deviceSql = `
                SELECT th.deviceId, th.violationType, v.vehicleNo, th.occTime, th.dataFrom, th.lat, th.lng , d.driverName
                FROM track_history th
                LEFT JOIN (
                    select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle 
                    UNION ALL 
                    select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle_history
                ) v ON v.deviceId = th.deviceId
                LEFT JOIN (
                    SELECT t.taskId, t.hub, t.node, t.groupId, t.driverId, t.vehicleNumber, 
                    t.mobileStartTime, t.mobileEndTime 
                    FROM task t 
                    WHERE ( (Date( ? ) BETWEEN Date(t.mobileStartTime) AND Date(t.mobileEndTime)) and t.mobileStartTime is not null )
                    OR t.driverStatus = 'started'

                    union

                    select CONCAT('DUTY-', dutyId) AS taskId, ui.hub, ui.node, ui.groupId, ui.driverId, ui.vehicleNo as vehicleNumber, 
                    ui.mobileStartTime, ui.mobileEndTime 
                    from urgent_indent ui 
                    where 1=1
                    AND (
                        ( (Date( ? ) BETWEEN Date(ui.mobileStartTime) AND Date(ui.mobileEndTime)) and ui.mobileStartTime is not null )
                        OR 
                        ui.status = 'started'
                    )
                ) tt ON tt.vehicleNumber = v.vehicleNo 
                left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                left join driver d on d.driverId = tt.driverId
                WHERE th.occTime LIKE ?
                AND (th.occTime >= tt.mobileStartTime AND (tt.mobileEndTime is null or tt.mobileEndTime >= th.occTime))
                AND th.dataFrom = 'obd'
            `;

            replacements2 = [ timeSelected, timeSelected, timeSelected + '%' ]

            let deviceResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            deviceSql += deviceResultSQL.tempSql;
            replacements2 = replacements2.concat(deviceResultSQL.tempReplacements)

            deviceSql += ` ORDER BY th.occTime DESC `
            log.info(deviceSql)
            let deviceOffenceList = await sequelizeObj.query(deviceSql, { 
                type: QueryTypes.SELECT, 
                replacements: replacements2 
            });

            if (req.body.timeSelected == moment().format('YYYY-MM-DD')) {
                cacheData({ url: req.originalUrl, hub, user }, { driverOffenceList, deviceOffenceList })
            }

            return res.json(utils.response(1, { driverOffenceList, deviceOffenceList }));
        } catch (error) {
            log.error(error);
            return res.json(utils.response(0, error));
        }
    },
    getTodayInTaskVehicleList: async (req, res) => {
        try {
            let selectedDate = req.body.timeSelected;
            if (!selectedDate) selectedDate = moment().format('YYYY-MM-DD')

            let userId = req.cookies.userId;

            let hub = req.body.hub
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let { hubNodeIdList } = await getPermitData(hub, user)

            // Driver
            let replacements1 = []
            let driverSql = `
                SELECT tt.taskId, tt.hub, tt.node, tt.groupId, tt.vehicleNumber, d.lat, d.lng, dd.driverName, dd.state, d.updatedAt, 
                d.speed, d.missingType, v.limitSpeed, u.id as unitId, dd.groupId, us.role, d.realtimeAlert as alert, d.realtimeSpeeding as speeding
                FROM task tt
                left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                LEFT JOIN driver_position d ON d.driverId = tt.driverId AND d.vehicleNo = tt.vehicleNumber
                LEFT JOIN (
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver 
                    UNION ALL 
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) dd ON dd.driverId = tt.driverId
                LEFT JOIN (
                    select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle 
                    UNION ALL 
                    select vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle_history
                ) v ON v.vehicleNo = tt.vehicleNumber
                LEFT JOIN user us ON dd.driverId = us.driverId
                WHERE (d.lat IS NOT NULL AND d.lng IS NOT NULL)
            `;

            let replacements2 = []
            let driverSql2 = `
                SELECT CONCAT('DUTY-', dutyId) AS taskId, ui.hub, ui.node, ui.groupId, 
                d.lat, d.lng, dd.driverName, dd.state, d.updatedAt, ui.vehicleNo as vehicleNumber, ui.vehicleNo,
                d.speed, d.missingType, v.limitSpeed, u.id as unitId, us.role, d.realtimeAlert as alert, d.realtimeSpeeding as speeding
                FROM urgent_indent ui
                LEFT JOIN unit u ON u.unit = ui.hub AND u.subUnit <=> ui.node
                LEFT JOIN driver_position d ON d.driverId = ui.driverId AND d.vehicleNo = ui.vehicleNo
                LEFT JOIN (
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver 
                    UNION ALL 
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) dd ON dd.driverId = ui.driverId
                LEFT JOIN (
                    SELECT vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle 
                    UNION ALL 
                    SELECT vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle_history
                ) v ON v.vehicleNo = ui.vehicleNo
                LEFT JOIN USER us ON dd.driverId = us.driverId
                WHERE 1 = 1
            `

            let driverResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            driverSql += driverResultSQL.tempSql;
            replacements1 = replacements1.concat(driverResultSQL.tempReplacements)

            let driverUrgentResultSQL = await generateUrgentLimitSQL(hub, user, hubNodeIdList)
            driverSql2 += driverUrgentResultSQL.tempSql;
            replacements2 = replacements2.concat(driverUrgentResultSQL.tempReplacements)
            
            if (selectedDate) {
                driverSql += ` AND Date(d.updatedAt) = ? `
                replacements1.push(selectedDate)
                driverSql2 += ` AND Date(d.updatedAt) = ? `
                replacements2.push(selectedDate)
            }
            let driverPositionList = await sequelizeObj.query(driverSql, { type: QueryTypes.SELECT, replacements: replacements1 });
            let driverPositionList2 = await sequelizeObj.query(driverSql2, { type: QueryTypes.SELECT, replacements: replacements2 });
            driverPositionList = driverPositionList.concat(driverPositionList2)

            // Vehicle
            let replacements3 = []
            let deviceSql = `
                SELECT tt.*, dv.deviceId, dv.lat, dv.lng, dd.driverName, dv.updatedAt, dv.speed, 
                dv.limitSpeed, u.id as unitId, dd.groupId, us.role, dv.realtimeAlert as alert, dv.realtimeSpeeding as speeding
                FROM task tt
                LEFT JOIN (
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver 
                    UNION ALL 
                    select driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) dd ON dd.driverId = tt.driverId
                LEFT JOIN user us ON dd.driverId = us.driverId
                left join unit u on u.unit = tt.hub and u.subUnit <=> tt.node
                LEFT JOIN (
                    SELECT dd.lat, dd.lng, dd.deviceId, dd.updatedAt, v.vehicleNo, dd.speed, v.limitSpeed, v.unitId,
                    dd.realtimeAlert, dd.realtimeSpeeding 
                    FROM device dd
                    LEFT JOIN (
                        select vehicleNo, deviceId, unitId, vehicleType, limitSpeed FROM vehicle 
                        UNION ALL 
                        select vehicleNo, deviceId, unitId, vehicleType, limitSpeed FROM vehicle_history
                    ) v ON v.deviceId = dd.deviceId
                ) dv ON dv.vehicleNo = tt.vehicleNumber 
                WHERE (dv.lat IS NOT NULL AND dv.lng IS NOT NULL)
            `

            let replacements4 = []
            let deviceSql2 = `
                SELECT ui.*, ui.vehicleNo as vehicleNumber, dv.deviceId, dv.lat, dv.lng, 
                dd.driverName, dv.updatedAt, dv.speed, dv.limitSpeed, u.id as unitId, us.role, dv.realtimeAlert as alert, dv.realtimeSpeeding as speeding
                FROM urgent_indent ui
                LEFT JOIN (
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver 
                    UNION ALL 
                    SELECT driverId, unitId, groupId, nric, driverName, contactNumber, state FROM driver_history
                ) dd ON dd.driverId = ui.driverId
                LEFT JOIN USER us ON dd.driverId = us.driverId
                LEFT JOIN unit u ON u.unit = ui.hub AND u.subUnit <=> ui.node
                LEFT JOIN (
                    SELECT dd.lat, dd.lng, dd.deviceId, dd.updatedAt, v.vehicleNo, dd.speed, v.limitSpeed, v.unitId,
                    dd.realtimeAlert, dd.realtimeSpeeding 
                    FROM device dd
                    LEFT JOIN (
                    SELECT vehicleNo, deviceId, unitId, vehicleType, limitSpeed FROM vehicle 
                    UNION ALL 
                    SELECT vehicleNo, deviceId, unitId, vehicleType, limitSpeed FROM vehicle_history
                    ) v ON v.deviceId = dd.deviceId
                ) dv ON dv.vehicleNo = ui.vehicleNo
                WHERE 1 = 1
            `

            let deviceResultSQL = await generateLimitSQL(hub, user, hubNodeIdList)
            deviceSql += deviceResultSQL.tempSql;
            replacements3 = replacements3.concat(deviceResultSQL.tempReplacements)

            let deviceUrgentResultSQL = await generateUrgentLimitSQL(hub, user, hubNodeIdList)
            deviceSql2 += deviceUrgentResultSQL.tempSql;
            replacements4 = replacements4.concat(deviceUrgentResultSQL.tempReplacements)

            if (selectedDate) {
                deviceSql += ` AND Date(dv.updatedAt) = ? `
                replacements3.push(selectedDate)
                deviceSql2 += ` AND Date(dv.updatedAt) = ? `
                replacements4.push(selectedDate)
            }
            console.log(deviceSql)
            console.log(deviceSql2)
            let devicePositionList = await sequelizeObj.query(deviceSql, { type: QueryTypes.SELECT, replacements: replacements3 });
            let devicePositionList2 = await sequelizeObj.query(deviceSql2, { type: QueryTypes.SELECT, replacements: replacements4 });
            devicePositionList = devicePositionList.concat(devicePositionList2)

            // Calculate
            let hubConf = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
            const generateColor = async function (hubConf, list) {
                for (let data of list) {
                    data.circleColor = hubNodeConf.defaultColor
                    for (let hubNode of hubConf) {
                        if (data.hub?.toLowerCase() == hubNode.hub?.toLowerCase()) {
                            data.circleColor = hubNode.color
                        }
                    }
    
                    if (data.role && (data.role == 'DV' || data.role == 'LOA') && data.groupId) {
                        let group = await TaskUtils.getGroupById(data.groupId)
                        data.groupName = group.groupName 
                    }
                }
                return list
            }

            await generateColor(hubConf, driverPositionList)
            await generateColor(hubConf, devicePositionList)
            
            return res.json(utils.response(1, { driverPositionList, devicePositionList }));
        } catch (error) {
            log.error(error);
            return res.json(utils.response(0, error));
        }
    },


    getTaskList: async function (req, res) {
        try {
            const generateSearchSql = function (option) {
                let {
                    taskId,
                    activity,
                    purpose,
                    taskStatus,
                    selectedDate,
                    hub,
                    node,
                    driverName,
                    vehicleNo
                } = option
                let limitCondition = [], replacements = []

                if (taskId) {
                    limitCondition.push(` t.taskId LIKE ` + sequelizeObj.escape("%"+taskId+"%"))
                }
                if (activity) {
                    limitCondition.push(` t.activity LIKE ` + sequelizeObj.escape("%"+activity+"%"))
                }
                if (purpose) {
                    limitCondition.push(` t.purpose LIKE ` + sequelizeObj.escape("%"+purpose+"%"))
                }
                if (taskStatus) {
                    if (taskStatus.toLowerCase() == 'system expired') {
                        limitCondition.push(` (t.driverStatus LIKE '%waitcheck%' and now() > t.indentEndTime)`)
                    } else if (taskStatus.toLowerCase() == 'waitcheck') {
                        limitCondition.push(` (t.driverStatus LIKE '%waitcheck%' and now() < t.indentEndTime)`)
                    } else {
                        limitCondition.push(` t.driverStatus LIKE ` + sequelizeObj.escape("%"+taskStatus+"%"))
                    }
                }
                if (selectedDate) {
                    limitCondition.push(` (DATE(t.indentStartTime) <= ? AND DATE(t.indentEndTime) >= ? ) `)
                    replacements.push(selectedDate + '%');
                    replacements.push(selectedDate + '%');
                }
    
                if (hub) {
                    limitCondition.push(` t.hub=? `)
                    replacements.push(hub);
                }
                if (node) {
                    limitCondition.push(` t.node=? `)
                    replacements.push(node);
                }
                if (driverName) {
                    limitCondition.push(` d.driverName like ` + sequelizeObj.escape("%"+driverName+"%"))
                }
                if (vehicleNo) {
                    limitCondition.push(` t.vehicleNumber like ` + sequelizeObj.escape("%"+vehicleNo+"%"))
                }

                return {
                    limitCondition,
                    replacements
                }
            }

            const getSortSql = function (sortBy, sort) {
                let taskSql = ''
                if (sortBy == 'driverName') {
                    if (sort.toLowerCase() == 'asc') {
                        taskSql += ` ORDER BY d.driverName ASC `
                    } else {
                        taskSql += ` ORDER BY d.driverName DESC `
                    }
                } else {
                    taskSql += ` ORDER BY t.indentStartTime desc `
                }
            
                return taskSql
            }

            let { selectedDate, taskStatus, taskId, driverName, vehicleNo, hub, node, group, pageNum, pageLength, purpose, activity, sort, sortBy } = req.body;
            
            group = group ? Number.parseInt(group) : null

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let taskSql = `
                SELECT t.taskId, t.groupId, t.createdAt, t.dataFrom, d.driverId, d.driverName, du.role as driverRole, t.driverStatus, d.contactNumber, t.vehicleNumber, v.vehicleType,
                t.routeNo, t.indentStartTime, t.indentEndTime, t.purpose, t.activity, t.mobileStartTime, t.mobileEndTime, t.indentId,
                t.pickupDestination, t.dropoffDestination, m.cancelledCause, m.cancelledDateTime, m.amendedBy, us.fullName as amendedByUsername,
                t.hub, t.node
                
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
                LEFT JOIN user us ON us.userId = m.amendedBy
                LEFT JOIN user du ON du.driverId = t.driverId
            `;
            let taskSql2 = `
                SELECT count(*) as count
                
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
                LEFT JOIN user us ON us.userId = m.amendedBy
            `;

            let limitCondition = [], replacements = []
            let result = await generateLimitCondition(group, user)
            limitCondition = limitCondition.concat(result.limitCondition)
            replacements = replacements.concat(result.replacements)

            let searchResult = generateSearchSql({
                taskId,
                activity,
                purpose,
                taskStatus,
                selectedDate,
                hub,
                node,
                driverName,
                vehicleNo
            })
            limitCondition = limitCondition.concat(searchResult.limitCondition)
            replacements = replacements.concat(searchResult.replacements)

            if (limitCondition.length) {
                taskSql += ' WHERE ' + limitCondition.join(' AND ');
                taskSql2 += ' WHERE ' + limitCondition.join(' AND ');
            }
            
            let totalList = await sequelizeObj.query(taskSql2, { type: QueryTypes.SELECT, replacements });

            let sortSql = getSortSql(sortBy, sort)
            taskSql += sortSql

            taskSql += ` limit ?, ?`
            replacements.push(Number(pageNum));
            replacements.push(Number(pageLength));
            console.log(taskSql)
            let taskList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements });

            //task withdraw and return key time
            if (taskList.length) {
                let taskIdList = taskList.map(item => item.taskId);
                let taskKeyOptRecords = await sequelizeObj.query(`
                    select taskId, optType, optTime, createdAt from key_opt_record k 
                    where k.optType in('withdrawConfirm', 'returnConfirm') and k.taskId IS NOT NULL and taskId in(?) ORDER BY createdAt desc
                `, { type: QueryTypes.SELECT, replacements: [taskIdList] });

                const generateResult = function (taskList) {
                    for (let task of taskList) {
                        let taskWithdrawConfirm = taskKeyOptRecords.find(item => (item.taskId == task.taskId && item.optType == 'withdrawConfirm'))
                        if (taskWithdrawConfirm) {
                            task.withdrawKeyTime = taskWithdrawConfirm.createdAt;
                        }
                        let taskReturnConfirm = taskKeyOptRecords.find(item => (item.taskId == task.taskId && item.optType == 'returnConfirm'))
                        if (taskReturnConfirm) {
                            task.returnKeyTime = taskReturnConfirm.createdAt;
                        }
    
                        if (task.driverStatus == 'waitcheck' && moment().isAfter(task.indentEndTime)) {
                            task.driverStatus = 'System Expired'
                        }
                    }
                    return taskList
                }
                taskList = generateResult(taskList)
            }
            
            return res.json({ respMessage: taskList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },

    getWithoutVehicleTaskNum: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }
            //check user has task assign permission
            let pageList = await userService.getUserPageList(userId, 'Task Assign');
            let operationList = pageList.map(item => `${ item.action }`).join(',');
            let hasAssignPermission = false;
            if (operationList && operationList.indexOf('Assign') != -1) {
                hasAssignPermission = true;
            }

            let taskNum = 0;
            if (hasAssignPermission) {
                let taskSql = `
                    SELECT count(*) as count
                    FROM task t 
                    LEFT JOIN unit u ON u.unit = t.hub AND u.subUnit <=> t.node
                    where t.vehicleNumber is null and t.driverStatus ='waitcheck' and t.indentEndTime > now()
                `;

                let replacements = []
                if (user.userType.toLowerCase() == 'customer') {
                    taskSql += ` t.groupId = ? `
                    replacements.push(user.unitId)
                } else if (user.userType.toLowerCase() == 'unit') {
                    let hubNodeIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.hub, user.node);
                    if (hubNodeIdList.length) {
                        // Maybe not more than 1000 node in one hub
                        taskSql += ` ( u.id IN (?) AND t.groupId IS NULL ) `
                        replacements.push(hubNodeIdList);
                    }
                }
                let totalList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements });

                taskNum = totalList[0].count;
            } else {
                taskNum = 'noPermission'
            }
            
            return res.json({ respMessage: { taskNum } });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },

    getWithoutVehicleTaskList: async function (req, res) {
        try {
            let { driverName, hub, node, group, pageNum, pageLength, purpose, activity, sort, sortBy } = req.body;
            
            group = group ? Number.parseInt(group) : null

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let taskSql = `
                SELECT t.taskId, t.groupId, t.createdAt, t.dataFrom, d.driverId, d.driverName, du.role as driverRole, t.driverStatus, d.contactNumber, t.vehicleNumber,
                t.routeNo, t.indentStartTime, t.indentEndTime, t.purpose, t.activity, t.mobileStartTime, t.mobileEndTime, t.indentId,
                t.pickupDestination, t.dropoffDestination, t.hub, t.node
                FROM task t 
                LEFT JOIN unit u ON u.unit = t.hub AND u.subUnit <=> t.node
                LEFT JOIN (
                    SELECT * FROM (
                        SELECT driverId, unitId, nric, driverName, contactNumber FROM driver  
                        UNION ALL 
                        SELECT driverId, unitId, nric, driverName, contactNumber FROM driver_history  
                    ) dd GROUP BY dd.driverId
                ) d ON d.driverId = t.driverId
                LEFT JOIN user du ON du.driverId = t.driverId
                where t.vehicleNumber is null and t.driverStatus ='waitcheck' and t.indentEndTime > now()
            `;
            let taskSql2 = `
                SELECT count(*) as count
                FROM task t 
                LEFT JOIN unit u ON u.unit = t.hub AND u.subUnit <=> t.node
                LEFT JOIN (
                    SELECT * FROM (
                        SELECT driverId, unitId, nric, driverName, contactNumber FROM driver  
                        UNION ALL 
                        SELECT driverId, unitId, nric, driverName, contactNumber FROM driver_history  
                    ) dd GROUP BY dd.driverId
                ) d ON d.driverId = t.driverId
                where t.vehicleNumber is null and t.driverStatus ='waitcheck' and t.indentEndTime > now()
            `;

            let limitCondition = [], replacements = []
            if (user.userType.toLowerCase() == 'customer') {
                limitCondition.push(` t.groupId = ? `)
                replacements.push(user.unitId)
            } else if (user.userType.toLowerCase() == 'unit') {
                let hubNodeIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.hub, user.node);
                if (hubNodeIdList.length) {
                    // Maybe not more than 1000 node in one hub
                    limitCondition.push(` ( u.id IN (?) AND t.groupId IS NULL ) `)
                    replacements.push(hubNodeIdList);
                }
            }

            const generateLimitCondition = function (option) {
                let {
                    activity,
                    purpose,
                    group,
                    hub,
                    node,
                    driverName
                } = option
                let limitCondition = [], replacements = []
                if (activity) {
                    limitCondition.push(` t.activity LIKE ? `)
                    replacements.push(`%`+ activity +`%`);
                }
                if (purpose) {
                    limitCondition.push(` t.purpose LIKE ? `)
                    replacements.push(`%`+ purpose +`%`);
                }
                if (group) {
                    limitCondition.push(` t.groupId = ? `)
                    replacements.push(group);
                } 
    
                if (hub) {
                    limitCondition.push(` t.hub = ? `)
                    replacements.push(hub);
                }
                if (node) {
                    limitCondition.push(` t.node = ? `)
                    replacements.push(node);
                }
                if (driverName) {
                    limitCondition.push(` d.driverName like ? `)
                    replacements.push(`%` + driverName + `%`);
                }

                return { limitCondition, replacements }
            }
            let result = generateLimitCondition({
                activity,
                purpose,
                group,
                hub,
                node,
                driverName
            })
            limitCondition = limitCondition.concat(result.limitCondition)
            replacements = replacements.concat(result.replacements)

            if (limitCondition.length) {
                taskSql += ' AND ' + limitCondition.join(' AND ');
                taskSql2 += ' AND ' + limitCondition.join(' AND ');
            }
            
            if (sortBy == 'driverName' && sort) {
                if (sort.toLowerCase() == 'asc') {
                    taskSql += ` ORDER BY d.driverName ASC `
                } else {
                    taskSql += ` ORDER BY d.driverName DESC `
                }
            } else {
                taskSql += ` ORDER BY t.indentStartTime desc `
            }

            let totalList = await sequelizeObj.query(taskSql2, { type: QueryTypes.SELECT, replacements });

            taskSql += ` limit ?, ?`
            replacements.push(NUMBER(pageNum));
            replacements.push(NUMBER(pageLength));
            console.log(taskSql)
            let taskList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements });
            
            return res.json({ respMessage: taskList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },

    getDriverMobileTaskList: async function (req, res) {
        try {
            let { selectedDate, taskStatus, taskId, driverName, vehicleNo, group, pageNum, pageLength, purpose } = req.body;
            
            group = group ? Number.parseInt(group) : null

            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let taskSql = `
                SELECT
                    mt.id as tripId,
                    t.taskId,
                    mt.purpose,
                    mt.vehicleNumber,
                    veh.vehicleType,
                    mt.driverId,
                    d.driverName,
                    du.role as driverRole,
                    mt.indentStartTime,
                    mt.indentEndTime,
                    mt.pickupDestination,
                    mt.dropoffDestination,
                    mt.status as approveStatus,
                    t.driverStatus,
                    t.mobileStartTime,
                    t.mobileEndTime,
                    cu.username as cancelledBy,
                    mt.cancelledCause,
                    mt.cancelledDateTime
                FROM mobile_trip mt
                LEFT JOIN driver d on mt.driverId = d.driverId
                LEFT JOIN user du on d.driverId = du.driverId
                LEFT JOIN user cu on cu.userId = mt.cancelledBy
                LEFT JOIN vehicle veh on veh.vehicleNo = mt.vehicleNumber
                LEFT JOIN task t ON t.dataFrom = 'MOBILE' and CONCAT('CU-M-', mt.id) = t.taskId
            `;
            let taskSql2 = `
                SELECT count(*) as count
                FROM mobile_trip mt
                LEFT JOIN driver d on mt.driverId = d.driverId
                LEFT JOIN user du on d.driverId = du.driverId
                LEFT JOIN task t ON t.dataFrom = 'MOBILE' and CONCAT('CU-M-', mt.id) = t.taskId
            `;
            
            let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
            if (!unitIdList) {
                unitIdList = [''];
            }
            if (groupIdList?.length) {
                groupIdList = [ 0 ];
            }

            let limitCondition = [], replacements = []
            const getPermitData = function (group, user) {  
                let limitCondition = [], replacements = []              
                if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                    limitCondition.push(` du.unitId = ? `)
                    replacements.push(user.unitId)
                } else if (user.userType == CONTENT.USER_TYPE.ADMINISTRATOR) {
                    if (group) {
                        limitCondition.push(` du.unitId = ? `)
                        replacements.push(group)
                    }
                } else if (user.userType == CONTENT.USER_TYPE.HQ) {
                    if (user.group?.length) {
                        if (group) {
                            limitCondition.push(` du.unitId = ? `)
                            replacements.push(group)
                        } else {
                            limitCondition.push(` du.unitId in (?) `)
                            replacements.push(groupIdList)
                        } 
                    }
                } else {
                    limitCondition.push(` du.unitId IN (?) `)
                    replacements.push(unitIdList)
                }

                return { limitCondition, replacements }
            }
            let result = getPermitData(group, user)
            limitCondition = limitCondition.concat(result.limitCondition)
            replacements = replacements.concat(result.replacements)

            const getSearchLimitSql = function (option) {
                let {
                    taskId,
                    purpose,
                    taskStatus,
                    selectedDate,
                    driverName,
                    vehicleNo
                } = option
                let limitCondition = []
                if (taskId) {
                    limitCondition.push(` t.taskId LIKE ` + sequelizeObj.escape("%"+taskId+"%"))
                }
                if (purpose) {
                    limitCondition.push(` t.purpose LIKE ` + sequelizeObj.escape("%"+purpose+"%"))
                }
                if (taskStatus) {
                    limitCondition.push(` (mt.status LIKE `+sequelizeObj.escape("%"+taskStatus+"%")+` OR t.driverStatus LIKE `+sequelizeObj.escape("%"+taskStatus+"%")+`) `)
                }
                if (selectedDate) {
                    limitCondition.push(` (DATE(mt.indentStartTime) <= `+sequelizeObj.escape(selectedDate)+` AND DATE(mt.indentEndTime) >= `+sequelizeObj.escape(selectedDate)+` ) `)
                }
                if (driverName) {
                    limitCondition.push(` d.driverName like ` + sequelizeObj.escape("%"+driverName+"%"))
                }
                if (vehicleNo) {
                    limitCondition.push(` t.vehicleNumber like ` + sequelizeObj.escape("%"+vehicleNo+"%"))
                }

                return limitCondition
            }

            let limitResult = getSearchLimitSql({
                taskId,
                purpose,
                taskStatus,
                selectedDate,
                driverName,
                vehicleNo
            })
            limitCondition = limitCondition.concat(limitResult)

            if (limitCondition.length) {
                taskSql += ' WHERE ' + limitCondition.join(' AND ');
                taskSql2 += ' WHERE ' + limitCondition.join(' AND ');
            }
            
            taskSql += ` ORDER BY mt.indentStartTime desc `
            console.log(taskSql2)
            let totalList = await sequelizeObj.query(taskSql2, { type: QueryTypes.SELECT, replacements });

            taskSql += ` limit ?, ?`
            replacements.push(Number.parseInt(pageNum));
            replacements.push(Number.parseInt(pageLength));
            let taskList = await sequelizeObj.query(taskSql, { type: QueryTypes.SELECT, replacements });
            
            return res.json({ respMessage: taskList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },

    getMT_RACList: async function (req, res) {
        try {
            let { selectedDate, taskId, driverName, vehicleNo, riskLevel, hub, node, group, pageNum, pageLength } = req.body;

            group = group ? Number.parseInt(group) : null

            let baseSql = `
                FROM mt_rac mr

                LEFT JOIN task t ON mr.taskId = t.taskId
                LEFT JOIN driver d ON t.driverId = d.driverId 
                LEFT JOIN unit un ON un.unit = t.hub AND un.subUnit <=> t.node

                left join urgent_duty ud on ud.dutyId = mr.taskId
                left join driver d2 on d2.driverId = ud.driverId 
                left join urgent_config uc on uc.id = ud.configId
                left join unit un2 on un2.unit = uc.hub AND un2.subUnit <=> uc.node
            `;
            
            let limitCondition = [], replacements = [];

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);

            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            const generateSearchSql = function (option) {
                let {
                    taskId,
                    selectedDate,
                    hub,
                    node,
                    riskLevel,
                    driverName,
                    vehicleNo
                } = option

                let limitCondition = [], replacements = [];
                if (taskId) {
                    limitCondition.push(` (t.taskId LIKE `+sequelizeObj.escape(taskId + "%")+` or ud.dutyId LIKE `+sequelizeObj.escape(taskId + "%")+`) `);
                }
                if (selectedDate) {
                    limitCondition.push(` mr.createdAt LIKE `+sequelizeObj.escape(selectedDate + "%"));
                }
                if (hub) {
                    limitCondition.push(` (t.hub=? or uc.hub = ?) `);
                    replacements.push(hub)
                    replacements.push(hub)
                }
                if (node) {
                    limitCondition.push(` (t.node=? or uc.node = ?) `);
                    replacements.push(node)
                    replacements.push(node)
                }
                if (riskLevel) {
                    limitCondition.push(` mr.riskLevel=? `);
                    replacements.push(riskLevel)
                }
                if (driverName) {
                    limitCondition.push(` (d.driverName like `+sequelizeObj.escape("%" + driverName + "%")+` or d2.driverName like `+sequelizeObj.escape("%" + driverName + "%")+`) `);
                }
                if (vehicleNo) {
                    limitCondition.push(` t.vehicleNumber like `+sequelizeObj.escape("%" + vehicleNo + "%"));
                }

                return {
                    limitCondition,
                    replacements
                }
            }

            let searchSqlResult = generateSearchSql({
                taskId,
                selectedDate,
                hub,
                node,
                riskLevel,
                driverName,
                vehicleNo
            })
            limitCondition = limitCondition.concat(searchSqlResult.limitCondition)
            replacements = replacements.concat(searchSqlResult.replacements)

            
            const getPermitSql = async function (group, user) {
                let limitCondition = [], replacements = [];

                let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
                if (user.userType.toLowerCase() == 'customer') {
                    limitCondition.push(` (t.groupId = ${ user.unitId } or uc.groupId = ${ user.unitId }) `)
                } else if (user.userType.toLowerCase() == 'administrator') {
                    const getSQL = function (group) {
                        let limitCondition = [], replacements = [];
                        if (group) {
                            limitCondition.push(` (t.groupId = ? or uc.groupId = ?) `)
                            replacements.push(group)
                            replacements.push(group)
                        } else if (group == 0) {
                            limitCondition.push(` (t.groupId IS NOT NULL or uc.groupId is not null) `)
                        } else {
                            limitCondition.push(` (t.groupId IS NULL and uc.groupId is null) `)
                        }
                        return { limitCondition, replacements }
                    }
                    let result = getSQL(group)
                    limitCondition = limitCondition.concat(result.limitCondition)
                    replacements = replacements.concat(result.replacements)
                } else if (user.userType.toLowerCase() == 'hq') {
                    const getSql = function (group, user) {
                        let limitCondition = [], replacements = [];

                        if (user.group?.length) {
                            if (group) {
                                limitCondition.push(` (t.groupId = ? or uc.groupId = ?) `)
                                replacements.push(group)
                                replacements.push(group)
                            } else if (group == 0) {
                                limitCondition.push(` (t.groupId in (?) or uc.groupId in (?)) `)
                                replacements.push(groupIdList, groupIdList)
                            } else {
                                limitCondition.push(` (t.groupId IS NULL and uc.groupId is null) `)
                            }
                        } 
                        
                        let tempSqlList = []
                        if (unitIdList.length) {
                            tempSqlList.push(` (un.id IN ( ? ) or un2.id IN ( ? )) `)
                            replacements.push(unitIdList, unitIdList)
                        }
                        if (groupIdList.length) {
                            tempSqlList.push(` (t.groupId in (?) or uc.groupId in (?)) `)
                            replacements.push(groupIdList, groupIdList)
                        }
        
                        if (tempSqlList.length) {
                            limitCondition.push(` (${ tempSqlList.join(' OR ') }) `)
                        } else {
                            limitCondition.push(` 1=2 `)
                        }

                        return { limitCondition, replacements }
                    }
                    let result = getSql(group, user)
                    limitCondition = limitCondition.concat(result.limitCondition)
                    replacements = replacements.concat(result.replacements)
                } else if (user.userType.toLowerCase() == 'unit') {
                    limitCondition.push(` (un.id IN ( ? ) or un2.id IN ( ? )) `);
                    replacements.push(unitIdList, unitIdList)
                }

                return {
                    limitCondition,
                    replacements
                }
            }

            let permitResult = await getPermitSql(group, user)
            limitCondition = limitCondition.concat(permitResult.limitCondition)
            replacements = replacements.concat(permitResult.replacements)

            if (limitCondition.length) {
                baseSql += ' WHERE ' + limitCondition.join(' AND ');
            }

            let baseSql2 =  ` select count(*) as count ${ baseSql } `

            log.info(`getMT_RACList baseSql2 => start`)
            log.info(baseSql2)
            let totalList = await sequelizeObj.query(baseSql2, { type: QueryTypes.SELECT, replacements });
            log.info(`getMT_RACList baseSql2 => end`)
            
            log.info(`getMT_RACList baseSql1 => start`)
            let baseSql1 = `
                SELECT 
                mr.taskId, mr.riskLevel, mr.needCommander, mr.commander, mr.commanderContactNumber, 
                mr.commanderSignatureDateTime, mr.officer, mr.officerSignatureDateTime, mr.mitigation, mr.riskAssessment, 
                mr.driverDeclaration, mr.createdAt, 

                ifnull(t.dataFrom, 'SYSTEM') as dataFrom, 
                ifnull(d.driverName, d2.driverName) as submittedBy, 
                ifnull(d.driverName, d2.driverName) as driverName, 
                ifnull(t.vehicleNumber, uc.vehicleNo) as vehicleNumber, 
                
                ifnull(t.hub, uc.hub) as hub, 
                ifnull(t.node, uc.node) as node, 
                ifnull(un.id, un2.id) as unitId,
                ifnull(t.groupId, uc.groupId) as groupId,
                t.indentId
            ` + baseSql + ` ORDER BY mr.createdAt desc `
            baseSql1 += ` limit ?, ?`
            replacements.push(Number(pageNum))
            replacements.push(Number(pageLength))
            log.info(baseSql1)
            let mtRACList = await sequelizeObj.query(baseSql1, { type: QueryTypes.SELECT, replacements });
            log.info(`getMT_RACList baseSql1 => start`)
            
            log.info(`getMT_RACList attach => start`)
            let riskAssessmentList = await sequelizeObj.query(`
                SELECT id, riskType, assessment, \`level\` FROM risk_assessment
            `, { type: QueryTypes.SELECT })
            let driverDeclarationList = await sequelizeObj.query(`
                SELECT id, content FROM driver_declaration
            `, { type: QueryTypes.SELECT })
            let vehicleList = await Vehicle.findAll();
            for (let mtRac of mtRACList) {
                let riskAssessmentIdList = mtRac.riskAssessment.split(',').map(item => Number.parseInt(item))
                let declarationIdList = mtRac.driverDeclaration.split(',').map(item => Number.parseInt(item))
                mtRac.riskAssessmentList = riskAssessmentList.filter(item => {
                    return riskAssessmentIdList.includes(item.id)
                })
                mtRac.driverDeclarationList = driverDeclarationList.filter(item => {
                    return declarationIdList.includes(item.id)
                })
                mtRac.vehicleType = vehicleList.filter(item => item.vehicleNo == mtRac.vehicleNumber)[0]?.vehicleType
            }
            log.info(`getMT_RACList attach => end`)

            return res.json({ respMessage: mtRACList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },
    deleteMT_RAC: async function (req, res) {
        try {
            return res.json(utils.response(1, '')); 
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },
    getOddList: async function (req, res) {
        try { 
            let { selectedDate, driverName, vehicleNo, taskId, hub, node, group, pageNum, pageLength } = req.body;

            group = group ? Number.parseInt(group) : null

            let baseSql = `
                SELECT o.id as oddId, o.taskId, o.contentFrom, o.content, o.createdAt, o.rectifyBy,
                ifnull(t.vehicleNumber, uc.vehicleNo) as vehicleNumber, 
                ifnull(d.driverName, d2.driverName) as submittedBy, 
                ifnull(d.driverName, d2.driverName) as driverName, 
                ifnull(v.vehicleType, v2.vehicleType) as vehicleType, 
                ifnull(t.hub, uc.hub) as hub, 
                ifnull(t.node, uc.node) as node,
                ifnull(un.id, un2.id) as unitId,
                t.groupId
                FROM odd o
                LEFT JOIN task t ON t.taskId = o.taskId
                LEFT JOIN unit un ON un.unit = t.hub AND un.subUnit <=> t.node
                LEFT JOIN driver d ON d.driverId = t.driverId
                LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber

                left join urgent_duty ud on ud.dutyId = o.taskId
                left join urgent_config uc on uc.id = ud.configId
                left join unit un2 on un2.unit = uc.hub AND un2.subUnit <=> uc.node
                left join driver d2 on d2.driverId = uc.driverId
                left join vehicle v2 on v2.vehicleNo = uc.vehicleNo
            `
            
            let limitCondition = [], replacements = [];

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);

            const getPermitSql = async function (group, user) {

                let limitCondition = [], replacements = []

                let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
                if (user.userType.toLowerCase() == 'customer') {
                    limitCondition.push(`  tt.groupId = ? `)
                    replacements.push(user.unitId)
                } else if (user.userType.toLowerCase() == 'administrator') {
                    const getSql = function () {
                        if (group) {
                            limitCondition.push(` tt.groupId = ? `)
                            replacements.push(group);
                        } else if (group == 0) {
                            limitCondition.push(` tt.groupId IS NOT NULL `)
                        } else {
                            limitCondition.push(` tt.groupId IS NULL `)
                        }
                    }
                    getSql()
                } else if (user.userType.toLowerCase() == 'hq') {
                    const getSql = function () {
                        if (user.group?.length) {
                            if (group) {
                                limitCondition.push(` tt.groupId = ? `)
                                replacements.push(group);
                            } else if (group == 0) {
                                limitCondition.push(` tt.groupId IS NOT NULL `)
                            } else {
                                limitCondition.push(` tt.groupId IS NULL `)
                            }
                        } 
                        
                        let tempSqlList = []
                        if (unitIdList.length) {
                            tempSqlList.push(` tt.unitId IN ( ? ) `)
                            replacements.push(unitIdList)
                        }
                        if (groupIdList.length) {
                            tempSqlList.push(` tt.groupId IN ( ? ) `)
                            replacements.push(groupIdList)
                        }
        
                        if (tempSqlList.length) {
                            limitCondition.push(` (${ tempSqlList.join(' OR ') }) `);
                        } else {
                            limitCondition.push(` 1=2 `);
                        }
                    }
                    getSql()
                } else if (user.userType.toLowerCase() == 'unit') {
                    limitCondition.push(` tt.unitId IN ( ? ) `);
                    replacements.push(unitIdList)
                }

                return {
                    limitCondition,
                    replacements
                }
            }
            
            let permitResult = await getPermitSql(group, user) 
            limitCondition = limitCondition.concat(permitResult.limitCondition)
            replacements = replacements.concat(permitResult.replacements)

            limitCondition.push(` ( tt.rectifyBy is null or tt.rectifyBy = '' ) `);

            if (taskId) {
                limitCondition.push(` tt.taskId LIKE ` + sequelizeObj.escape(taskId + "%"));
            }
            if (selectedDate) {
                limitCondition.push(` tt.createdAt LIKE ` + sequelizeObj.escape(selectedDate + "%"));
            }
            if (hub) {
                limitCondition.push(` tt.hub=? `);
                replacements.push(hub)
            }
            if (node) {
                limitCondition.push(` tt.node=? `);
                replacements.push(node)
            }
            if (driverName) {
                limitCondition.push(` tt.driverName like ` + sequelizeObj.escape("%" +driverName + "%"));
            }
            if (vehicleNo) {
                limitCondition.push(` tt.vehicleNumber like ` + sequelizeObj.escape("%" +vehicleNo + "%"));
            }

            
            let baseSql2 =  `
                select count(*) as count from (
                    ${ baseSql }
                ) tt
            `
            baseSql = `
                select * from (
                    ${ baseSql }
                ) tt
            `

            if (limitCondition.length) {
                baseSql += ' WHERE ' + limitCondition.join(' AND ');
                baseSql2 += ' WHERE ' + limitCondition.join(' AND ');
            }

            baseSql += ` ORDER BY createdAt desc `

            let totalList = await sequelizeObj.query(baseSql2, { type: QueryTypes.SELECT, replacements });
            baseSql += ` limit ?, ?`
            replacements.push(Number(pageNum))
            replacements.push(Number(pageLength))
            console.log(baseSql)
            let oddList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements });
            return res.json({ respMessage: oddList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },
    getIncidentList: async function (req, res) {
        try {
            let { selectedDate, driverName, hub, node, pageNum, pageLength } = req.body;
            let baseSql = `
                SELECT i.*, d.driverName, un.unit AS hub, un.subUnit AS node  
                FROM incident i
                LEFT JOIN \`user\` u ON u.userId = i.creator
                LEFT JOIN unit un ON un.id = u.unitId
                LEFT JOIN driver d ON d.driverId = u.driverId
                WHERE u.userType = 'MOBILE'
            `
            
            let limitCondition = [], replacements = [];

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);

            let { unitIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
            if (user.userType.toLowerCase() == 'customer') {
                // Return null
                limitCondition.push(` 1 = 2 `)
            } else if (user.userType.toLowerCase() == 'administrator') {
                // Return all
                limitCondition.push(` 1 = 1 `)
            } else if (user.userType.toLowerCase() == 'hq') {
                let tempSqlList = []
                if (unitIdList.length) {
                    tempSqlList.push(` un.id IN ( ? ) `)
                    replacements.push(unitIdList)
                }

                if (tempSqlList.length) {
                    limitCondition.push(` (${ tempSqlList.join(' OR ') }) `);
                } else {
                    limitCondition.push(` 1=2 `);
                }

            } else if (user.userType.toLowerCase() == 'unit') {
                limitCondition.push(` un.id IN ( ? ) `);
                replacements.push(unitIdList)
            }
            
            if (selectedDate) {
                limitCondition.push(` i.updatedAt LIKE ` + sequelizeObj.escape(selectedDate + "%"));
            }
            if (hub) {
                limitCondition.push(` un.unit=? `);
                replacements.push(hub)
            }
            if (node) {
                limitCondition.push(` un.subUnit=? `);
                replacements.push(node)
            }
            if (driverName) {
                limitCondition.push(` d.driverName like ` + sequelizeObj.escape("%" + driverName + "%"));
            }

            if (limitCondition.length) {
                baseSql += ' AND ' + limitCondition.join(' AND ');
            }

            let totalList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements });
            baseSql += ` limit ?, ?`
            replacements.push(Number(pageNum))
            replacements.push(Number(pageLength))
            let incidentList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements });
            return res.json({ respMessage: incidentList, recordsFiltered: totalList.length, recordsTotal: totalList.length });

        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },
    getSurveyList: async function (req, res) {
        try {
            let { taskId, driverName, vehicleNo, hub, node, group, selectedDate, pageNum, pageLength } = req.body;

            group = group ? Number.parseInt(group) : null

            let baseSql = `
                SELECT c.*, 
                ifnull(t.vehicleNumber, ui.vehicleNo) as vehicleNumber, 
                ifnull(t.hub, ui.hub) as hub, 
                ifnull(t.node, ui.node) as node,
                ifnull(d.driverName, d2.driverName) as driverName, 
                ifnull(v.vehicleType, v2.vehicleType) as vehicleType, 
                ifnull(un.id, un2.id) as unitId,
                t.groupId
                FROM \`comment\` c
                LEFT JOIN task t ON c.taskId = t.taskId
                LEFT JOIN unit un ON un.unit = t.hub AND un.subUnit <=> t.node
                LEFT JOIN driver d ON d.driverId = t.driverId
                LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber

                left join urgent_indent ui on c.taskId = CONCAT('DUTY-', ui.dutyId, '-', ui.id)
                left join unit un2 on un2.unit = ui.hub AND un2.subUnit <=> ui.node
                left join driver d2 on d2.driverId = ui.driverId
                left join vehicle v2 on v2.vehicleNo = ui.vehicleNo
            `;

            let limitCondition = [], replacements = [];

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);

            const getPermitSql = async function () {
                let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
                if (user.userType.toLowerCase() == 'customer') {
                    limitCondition.push(` tt.groupId = ? `)
                    replacements.push(user.unitId)
                } else if (user.userType.toLowerCase() == 'administrator') {
                    const getSql = function () {
                        if (group) {
                            limitCondition.push(` tt.groupId = ? `)
                            replacements.push(group)
                        } else if (group == 0) {
                            limitCondition.push(` tt.groupId IS NOT NULL `)
                        } else {
                            limitCondition.push(` tt.groupId IS NULL `)
                        }
                    }
                    getSql()
                } else if (user.userType.toLowerCase() == 'hq') {
                    const getSql = function () {
                        if (user.group?.length) {
                            if (group) {
                                limitCondition.push(` tt.groupId = ? `)
                                replacements.push(group)
                            } else if (group == 0) {
                                limitCondition.push(` tt.groupId IS NOT NULL `)
                            } else {
                                limitCondition.push(` tt.groupId IS NULL `)
                            }
                        } 
                        let tempSqlList = []
                        if (unitIdList.length) {
                            tempSqlList.push(` tt.unitId IN ( ? ) `)
                            replacements.push(unitIdList)
                        }
                        if (groupIdList.length) {
                            tempSqlList.push(` tt.groupId IN ( ? ) `)
                            replacements.push(groupIdList)
                        }
        
                        if (tempSqlList.length) {
                            limitCondition.push(` (${ tempSqlList.join(' OR ') }) `);
                        } else {
                            limitCondition.push(` 1=2 `);
                        }
                    }
                    
                    getSql();
                } else if (user.userType.toLowerCase() == 'unit') {
                    limitCondition.push(` tt.unitId IN ( ? ) `);
                    replacements.push(unitIdList)
                }
            }
            await getPermitSql()

            const getLimitSql = function () {
                if (taskId) {
                    limitCondition.push(` tt.taskId LIKE ` + sequelizeObj.escape("%"+taskId+"%"));
                }
                if (selectedDate) {
                    limitCondition.push(` tt.createdAt LIKE ` + sequelizeObj.escape("%"+selectedDate+"%"));
                }
                if (hub) {
                    limitCondition.push(` tt.hub=? `);
                    replacements.push(hub)
                }
                if (node) {
                    limitCondition.push(` tt.node=? `);
                    replacements.push(node)
                }
                if (driverName) {
                    limitCondition.push(` tt.driverName like ` + sequelizeObj.escape("%"+driverName+"%"));
                }
                if (vehicleNo) {
                    limitCondition.push(` tt.vehicleNumber like ` + sequelizeObj.escape("%"+vehicleNo+"%"));
                }
            }

            getLimitSql()
            
            let baseSql2 =  `
                select count(*) as count from (
                    ${ baseSql }
                ) tt
            `
            baseSql = `
                select * from (
                    ${ baseSql }
                ) tt
            `
            if (limitCondition.length) {
                baseSql += ' WHERE ' + limitCondition.join(' AND ');
                baseSql2 += ' WHERE ' + limitCondition.join(' AND ');
            }

            baseSql += ` ORDER BY tt.createdAt desc `

            console.log(baseSql2)
            let totalList = await sequelizeObj.query(baseSql2, { type: QueryTypes.SELECT, replacements });
            baseSql += ` limit ?,? `
            replacements.push(Number(pageNum))
            replacements.push(Number(pageLength))
            let surveyList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements });
            
            for (let survey of surveyList) {
                let task = await sequelizeObj.query(`
                    SELECT d.driverName, v.vehicleType, t.vehicleNumber, t.hub, t.node FROM task t
                    LEFT JOIN driver d ON d.driverId = t.driverId
                    LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber 
                    WHERE t.taskId = ?
                `, { type: QueryTypes.SELECT, replacements: [ survey.taskId ] })
                if (task.length) {
                    survey.submittedBy = task[0].driverName;
                    survey.vehicleNumber = task[0].vehicleNumber;
                    survey.vehicleType = task[0].vehicleType;
                    survey.hub = task[0].hub;
                    survey.node = task[0].node;
                }
            }
            return res.json({ respMessage: surveyList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    },
    getTrafficList: async function (req, res) {
        try {
            let option = {
                headers: header
            }
            if (conf.openProxy) {
                option.proxy = conf.proxy
            }
            axios.get("http://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents", option)
            .then(async response =>  {
                let data = response.data.value
                let newData = []
                for(let item of data) {
                    let subscript = (item.Message).indexOf(':')
                    let date = (item.Message).substring(0, subscript+3)
                  
                    let month = date.substr(((date).lastIndexOf('/'))+1, 1)
                    let day = date.substr(1, (date).lastIndexOf('/')-1)
                    let time = date.substr(((date).lastIndexOf(')'))+1, date.length)
                    let dateTime = `2023-${ month }-${ day } ${ time }`
                    let obj = {
                        Type: item.Type,
                        Latitude: item.Latitude,
                        Longitude: item.Longitude,
                        Message: item.Message,
                        DateTime: dateTime
                    }
                    newData.push(obj)
                }
                newData = newData.sort(await TaskUtils.parseData("DateTime"));

                cacheCommonData({ url: req.originalUrl }, newData)

                return res.json(utils.response(1, newData));
            }).catch(error => {
                log.error('(getTrafficList) : ', error);
                return res.json(utils.response(0, 'Server error!'));
            });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }  
    },
    getDriverStateSos: async function (req, res) {
        try {
            let timeSelected = req.body.timeSelected;
            if (!timeSelected) timeSelected = moment().format('YYYY-MM-DD');
            let userId = req.cookies.userId;
            let hub = req.body.hub
            let user = await userService.UserUtils.getUserDetailInfo(userId);

            let baseSql = `
                SELECT d.driverName, d.driverId, d.contactNumber, d.lastSOSDateTime, 
                ifnull(t.vehicleNumber, ud.vehicleNo) as vehicleNumber, 
                ifnull(t.taskId, ud.dutyId) as taskId,
                ifnull(v.vehicleType, v2.vehicleType) as vehicleType,

                IF(t.taskId IS NOT NULL, u.id, IF(hh.id IS NOT NULL, hh.id, IF(hh2.id IS NOT NULL, hh2.id, u2.id))) AS unitId,
                IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.groupId, NULL))) AS groupId,
                IF(t.taskId IS NOT NULL, t.hub, IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, IF( 
                    IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.unitId, NULL)))
                    IS NOT NULL, '-', u2.unit)))) AS hub,
                IF(t.taskId IS NOT NULL, t.node, IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, IF(
                    IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.unitId, NULL)))
                    IS NOT NULL, '-', u2.subUnit)))) AS node
                                    
                FROM driver d
                LEFT JOIN task t ON t.driverId = d.driverId 
                    AND (d.lastSOSDateTime >= t.mobileStartTime AND (t.mobileEndTime IS NULL OR t.mobileEndTime >= d.lastSOSDateTime ))
                LEFT JOIN USER us ON us.driverId = d.driverId
                LEFT JOIN unit u ON u.unit <=> t.hub AND u.subUnit <=> t.node
                LEFT JOIN vehicle v on v.vehicleNo = t.vehicleNumber

                left join urgent_duty ud on ud.driverId = d.driverId 
                    and (d.lastSOSDateTime >= ud.mobileStartTime AND (ud.mobileEndTime IS NULL OR ud.mobileEndTime >= d.lastSOSDateTime ))
                left join vehicle v2 on v2.vehicleNo = ud.vehicleNo
                
                LEFT JOIN unit u2 ON u2.id = d.unitId
                LEFT JOIN hoto         hh ON hh.driverId = d.driverId and hh.status = 'Approved' AND d.lastSOSDateTime BETWEEN hh.startDateTime AND hh.endDateTime
                LEFT JOIN hoto_record hh2 ON hh2.driverId = d.driverId and hh2.status = 'Approved' AND d.lastSOSDateTime BETWEEN hh2.startDateTime AND hh2.returnDateTime
                LEFT JOIN loan         ll ON ll.driverId = d.driverId AND d.lastSOSDateTime BETWEEN ll.startDate AND ll.endDate
                LEFT JOIN loan_record ll2 ON ll2.driverId = d.driverId AND d.lastSOSDateTime BETWEEN ll2.startDate AND ll2.returnDate
                
                WHERE d.state LIKE '%sos%'
                AND (d.lastSOSDateTime IS NOT NULL AND ? = DATE_FORMAT(d.lastSOSDateTime, '%Y-%m-%d'))
            `
            let sosSql = ` SELECT * FROM ( ${ baseSql } ) ss WHERE 1=1 `
            let replacements = [timeSelected]

            const getPermit = async function () {
                if (user.userType.toLowerCase() == 'customer') {
                    sosSql += ` AND ss.groupId = ? `
                    replacements.push(user.unitId)
                } else if (user.userType.toLowerCase() == 'hq') {
                    const getSql = async function () {
                        let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)
    
                        // HQ user has group permission
                        if (hub && user.group?.length) {
                            if((hub).toLowerCase() == 'dv_loa') {
                                sosSql += ` AND ss.groupId is not null `
                            } else {
                                sosSql += ` AND ss.hub = ? `
                                replacements.push(hub);
                            }
                        }
                        
                        let tempSqlList = []
                        if (unitIdList.length) {
                            tempSqlList.push(` ss.unitId IN (?) `)
                            replacements.push(unitIdList);
                        }
                        if (groupIdList.length) {
                            tempSqlList.push(` ss.groupId in (?) `)
                            replacements.push(groupIdList);
                        }
                        
                        if (tempSqlList.length) {
                            sosSql += ` AND (${ tempSqlList.join(' OR ') }) `
                        } else {
                            sosSql += ` and 1=2 `
                        }
                    }
    
                    await getSql()
                } else if (user.userType.toLowerCase() == 'administrator' && hub) {
                    if((hub).toLowerCase() == 'dv_loa') {
                        sosSql += ` AND ss.groupId is not null `
                    } else {
                        sosSql += ` AND ss.hub = '? `
                        replacements.push(hub);
                    }
    
                } else if (user.userType.toLowerCase() == 'unit') {
                    let hubNodeIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.hub, user.node);
                    if (hubNodeIdList.length) {
                        // Maybe not more than 1000 node in one hub
                        sosSql += ` AND ( ss.unitId IN (?) AND ss.groupId IS NULL ) `
                        replacements.push(hubNodeIdList);
                    } else {
                        sosSql += ` AND 1=2 `
                    }
                }
            }
            getPermit()
            
            console.log(sosSql)
            let driverStateSos = await sequelizeObj.query(sosSql, { type: QueryTypes.SELECT, replacements });
            let data = []
            for (let item of driverStateSos) {
                const getObj = async function () {
                    let obj = {
                        type: '-',
                        driverId: item.driverId,
                        driverName: item.driverName,
                        vehicleNumber: item.vehicleNumber,
                        vehicleType: item.vehicleType,
                        contactNumber: item.contactNumber,
                        taskId: item.taskId,
                        lastSOSDateTime: moment(item.lastSOSDateTime).format('YYYY-MM-DD HH:mm:ss')
                    }
                    let mtRac = await MT_RAC.findOne({ where: { taskId: item.taskId } })
                    if (mtRac?.needCommander) {
                        obj.commander = mtRac.commander
                    }
    
                    if (item.groupId) {
                        let group = await TaskUtils.getGroupById(item.groupId)
                        obj.group = group ? group.groupName : item.groupId;
                    } else {
                        obj.hub = item.hub
                        obj.node = item.node
                    }
    
                    // get sos type
                    let sos = await SOS.findOne({ 
                        where: { driverId: item.driverId, taskId: item.taskId }, 
                        order: [ ['id', 'DESC']] 
                    })
                    if (sos) {
                        obj.type = sos.type;
                    }

                    return obj
                }
                
                let obj = await getObj()
                data.push(obj)
            }
            data = data.sort(await TaskUtils.parseData("lastSOSDateTime"));

            // get sos location
            for (let item of data) {
                let sosLocation = await sequelizeObj.query(`
                    SELECT lat, lng FROM driver_position WHERE driverId = ? AND vehicleNo = ?
                `, { type: QueryTypes.SELECT, replacements: [ item.driverId, item.vehicleNumber ] })
                if(sosLocation?.length) {
                    item.lat = sosLocation[0].lat
                    item.lng = sosLocation[0].lng
                }
            }

            cacheData({ url: req.originalUrl, hub, user }, data)

            return res.json(utils.response(1, data));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }  
    },

    getATMSLoanTaskList: async function (req, res) {
        try {
            let { selectedDate, taskStatus, driverName, vehicleNo, hub, node, group, pageNum, pageLength, purpose, activity, taskId, sort, sortBy } = req.body;
            
            group = group ? Number.parseInt(group) : null

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            // Only find out data that already assigned (vehicle/driver)
            let baseSql = `
                SELECT ll.taskId, mt.reportingLocation, mt.destination, NULL AS groupName,
                mt.cancelledDateTime, mt.cancelledCause,
                ll.indentId, ll.startDate, ll.endDate, ll.purpose, ll.activity as activityName,
                ll.actualStartTime, ll.actualEndTime, ll.vehicleNo, ll.vehicleType, ll.driverId, ll.driverName, ll.contactNumber, ll.loanId, ll.groupId,
                u.unit AS hub, u.subUnit AS node, u.id AS unitId, us.fullName AS amendedByUsername,
                NULL AS returnUserName, NULL AS returnRemark, NULL AS returnDate, ll.unprefixTaskId
                FROM (
                    SELECT l.actualStartTime, l.actualEndTime, l.id AS loanId, l.groupId, l.vehicleNo, v.vehicleType,
                    d.driverId, d.driverName, d.contactNumber, l.taskId, l.unitId, l.indentId, 
                    l.startDate, l.endDate, l.purpose, l.activity, REPLACE(l.taskId, 'AT-', '') AS unprefixTaskId
                    FROM loan l
                    LEFT JOIN driver d ON d.driverId = l.driverId
                    LEFT JOIN vehicle v ON v.vehicleNo = l.vehicleNo
                ) AS ll
                LEFT JOIN (select * from mt_admin where dataType = 'mb') mt ON mt.id = ll.unprefixTaskId
                LEFT JOIN unit u ON u.id = ll.unitId
                LEFT JOIN USER us ON us.userId = mt.amendedBy
                WHERE ll.taskId LIKE 'AT-%'
                
                UNION
                
                SELECT ll.taskId, mt.reportingLocation, mt.destination, NULL AS groupName,
                mt.cancelledDateTime, mt.cancelledCause,
                ll.indentId, ll.startDate, ll.endDate, ll.purpose, ll.activity as activityName,
                ll.actualStartTime, ll.actualEndTime, ll.vehicleNo, ll.vehicleType, ll.driverId, ll.driverName, ll.contactNumber, ll.loanId, ll.groupId,
                u.unit AS hub, u.subUnit AS node, u.id AS unitId, us.fullName AS amendedByUsername,
                us2.fullName AS returnUserName, ll.returnRemark, ll.returnDate, ll.unprefixTaskId
                FROM (
                    SELECT l.actualStartTime, l.actualEndTime, l.id AS loanId, l.groupId, l.vehicleNo, v.vehicleType, l.returnBy, l.returnRemark, l.returnDate,
                    d.driverId, d.driverName, d.contactNumber, l.taskId, l.unitId, l.indentId, 
                    l.startDate, l.endDate, l.purpose, l.activity, REPLACE(l.taskId, 'AT-', '') AS unprefixTaskId
                    FROM loan_record l
                    LEFT JOIN driver d ON d.driverId = l.driverId
                    LEFT JOIN vehicle v ON v.vehicleNo = l.vehicleNo
                ) AS ll
                LEFT JOIN (select * from mt_admin where dataType = 'mb') mt ON mt.id = ll.unprefixTaskId
                LEFT JOIN unit u ON u.id = ll.unitId
                LEFT JOIN USER us ON us.userId = mt.amendedBy
                LEFT JOIN USER us2 ON us2.userId = ll.returnBy
                where ll.taskId LIKE 'AT-%'
            `;

            let sql = ` 
                SELECT *
                FROM (
                    SELECT *, 
                    IF(ll.cancelledDateTime IS NOT NULL, 'Cancelled', 
                        IF(ll.returnDate IS NOT NULL, 'Returned', 
                            IF(ll.actualEndTime IS NOT NULL, 'Completed', 
                                IF(ll.actualStartTime IS NOT NULL, 'Started', 'Pending')
                            )
                        )
                    ) AS \`status\`
                    FROM ( ${ baseSql } ) ll order by ll.unprefixTaskId desc
                ) l `

            let sql2 = ` 
                SELECT count(*) as count
                FROM (
                    SELECT *, 
                    IF(ll.cancelledDateTime IS NOT NULL, 'Cancelled', 
                        IF(ll.returnDate IS NOT NULL, 'Returned', 
                            IF(ll.actualEndTime IS NOT NULL, 'Completed', 
                                IF(ll.actualStartTime IS NOT NULL, 'Started', 'Pending')
                            )
                        )
                    ) AS \`status\`
                    FROM ( ${ baseSql } ) ll 
                ) l`
            let limitCondition = [], replacements = []

            const getPermitSql = async function () {
                if (user.userType.toLowerCase() == 'customer') {
                    limitCondition.push(` 1=2 `)
                } else if (user.userType.toLowerCase() == 'hq') {
                    let { unitIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
                    
                    let tempSqlList = []
                    if (unitIdList.length) {
                        tempSqlList.push(` l.unitId IN ( ? ) `)
                        replacements.push(unitIdList)
                    }
    
                    if (tempSqlList.length) {
                        limitCondition.push(` (${ tempSqlList.join(' OR ') }) `);
                    } else {
                        limitCondition.push(` 1=2 `);
                    }
    
                } else if (user.userType.toLowerCase() == 'unit') {
                    let hubNodeIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.hub, user.node);
                    if (hubNodeIdList.length) {
                        // Maybe not more than 1000 node in one hub
                        limitCondition.push(` l.unitId IN (?) `)
                        replacements.push(hubNodeIdList);
                    }
                }
            }
            await getPermitSql()

            const getSearchSql = function () {
                if (activity) {
                    limitCondition.push(` l.activityName LIKE ` + sequelizeObj.escape("%"+activity+"%"))
                }
                if (purpose) {
                    limitCondition.push(` l.purpose LIKE ` + sequelizeObj.escape("%"+purpose+"%"))
                }
                if (taskStatus) {
                    if (taskStatus.toLowerCase() == 'waitcheck') taskStatus = 'pending'
                    else if (taskStatus.toLowerCase() == 'completed') {
                        limitCondition.push(` ( l.status = 'returned' or l.status = 'completed' ) `)
                    } else {
                        limitCondition.push(` l.status = ? `)
                        replacements.push(taskStatus);
                    }
                }
                if (selectedDate) {
                    limitCondition.push(` (DATE(l.startDate) <= ? AND DATE(l.endDate) >= ? ) `)
                    replacements.push(selectedDate)
                    replacements.push(selectedDate)
                }
                if (taskId) {
                    limitCondition.push(` l.taskId like ` + sequelizeObj.escape("%"+taskId+"%"))
                }
                if (group || group == 0) {
                    // ATMS task has no group
                    limitCondition.push(` 1=2 `)
                }
                if (hub) {
                    limitCondition.push(` l.hub=? `)
                    replacements.push(hub);
                }
                if (node) {
                    limitCondition.push(` l.node=? `)
                    replacements.push(node);
                }
                if (driverName) {
                    limitCondition.push(` l.driverName like ` + sequelizeObj.escape("%"+driverName+"%"))
                }
                if (vehicleNo) {
                    limitCondition.push(` l.vehicleNo like ` + sequelizeObj.escape("%"+vehicleNo+"%"))
                }
            }
            getSearchSql()

            if (limitCondition.length) {
                sql += ' WHERE ' + limitCondition.join(' AND ');
                sql2 += ' WHERE ' + limitCondition.join(' AND ');
            }

            if (sortBy == 'driverName' && sort) {
                if (sort.toLowerCase() == 'asc') {
                    sql += ` ORDER BY l.driverName asc `
                } else {
                    sql += ` ORDER BY l.driverName desc `
                }
            } else {
                sql += ` ORDER BY l.startDate desc `
            }

            let totalList = await sequelizeObj.query(sql2, { type: QueryTypes.SELECT, replacements });

            sql += ` limit ?, ?`
            replacements.push(Number(pageNum));
            replacements.push(Number(pageLength));
            let taskList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements });
            
            let pageList = await userService.getUserPageList(userId, 'Task Dashboard', 'ATMS')
            let operationList = pageList.map(item => `${ item.action }`).join(',')
            for (let task of taskList) {
                task.operation = operationList
            }
            // 2024-03-18 NEW ATMS get Location、group
            let atmsTaskId = taskList.filter(item => (item.taskId).includes('AT-'));
            let newAtmsTaskId = atmsTaskId.map(item => item.unprefixTaskId)
            if (newAtmsTaskId.length > 0) {
                let result = await sequelizeSystemObj.query(`
                    SELECT jt.id, jt.pickupDestination, jt.dropoffDestination, g.groupName
                    FROM job_task jt
                    LEFT JOIN request r ON r.id = jt.requestId
                    LEFT JOIN \`group\` g ON g.id = r.groupId
                    WHERE jt.id IN (?)
                `, { type: QueryTypes.SELECT, replacements: [ newAtmsTaskId ]})

                for (let task of taskList) {
                    result.some(item => {
                        if (item.id == task.unprefixTaskId) {
                            task.reportingLocation = item.pickupDestination
                            task.destination = item.dropoffDestination
                            task.groupName = item.groupName
                            return true
                        }
                    })
                }
            }
            return res.json({ respMessage: taskList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getCVLoanTaskList: async function (req, res) {
        try {
            let { selectedDate, taskStatus, driverName, vehicleNo, group, hub, node, pageNum, pageLength, purpose, activity, taskId, sort, sortBy } = req.body;
            
            group = group ? Number.parseInt(group) : null

            // check hub, node
            let userId = req.cookies.userId;
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let baseSql = `
                SELECT l.id, l.driverId, l.vehicleNo, l.startDate, l.endDate, l.groupId, l.actualStartTime, l.actualEndTime, 
                l.id AS loanId, d.driverName, d.contactNumber, v.vehicleType, l.taskId, l.indentId, l.unitId, l.purpose, l.activity as activityName,
                NULL AS returnUserName, NULL AS returnRemark, NULL AS returnDate, un.unit as hub, un.subUnit as node
                FROM loan l
                LEFT JOIN driver d ON d.driverId = l.driverId
                LEFT JOIN vehicle v ON v.vehicleNo = l.vehicleNo
                LEFT JOIN unit un ON un.id = l.unitId
                WHERE l.groupId > 0 AND ( l.driverId IS NOT NULL OR l.vehicleNo IS NOT NULL )
                
                UNION 
                
                SELECT l.id, l.driverId, l.vehicleNo, l.startDate, l.endDate, l.groupId, l.actualStartTime, l.actualEndTime,
                l.id AS loanId, d.driverName, d.contactNumber, v.vehicleType, l.taskId, l.indentId, l.unitId, l.purpose, l.activity as activityName,
                us2.fullName AS returnUserName, l.returnRemark, l.returnDate, un.unit as hub, un.subUnit as node
                FROM loan_record l
                LEFT JOIN driver d ON d.driverId = l.driverId
                LEFT JOIN vehicle v ON v.vehicleNo = l.vehicleNo
                LEFT JOIN unit un ON un.id = l.unitId
                LEFT JOIN USER us2 ON us2.userId = l.returnBy
                WHERE l.groupId > 0 AND ( l.driverId IS NOT NULL OR l.vehicleNo IS NOT NULL )
            `
            let sql = ` 
                SELECT * 
                FROM (
                    SELECT *,
                    IF(ll.returnDate IS NOT NULL, 'Returned', 
                        IF(ll.actualEndTime IS NOT NULL, 'Completed', 
                            IF(ll.actualStartTime IS NOT NULL, 'Started', 'Pending')
                        )
                    ) AS \`status\`
                    FROM ( ${ baseSql } ) ll order by ll.taskId
                ) l `
            let sql2 = ` 
                SELECT count(*) as count 
                FROM (
                    SELECT *,
                    IF(ll.returnDate IS NOT NULL, 'Returned', 
                        IF(ll.actualEndTime IS NOT NULL, 'Completed', 
                            IF(ll.actualStartTime IS NOT NULL, 'Started', 'Pending')
                        )
                    ) AS \`status\`
                    FROM ( ${ baseSql } ) ll 
                ) l `
            let limitCondition = [], replacements = []
            // 2024-03-18 new ATMS exclusion
            limitCondition.push(` l.taskId not like 'AT-%' `)

            const getPermitSql = async function () {
                if (user.userType.toLowerCase() == 'customer') {
                    limitCondition.push(` l.groupId = ? `)
                    replacements.push(user.unitId)
                } else if (user.userType.toLowerCase() == 'hq') {
                    const getSql = async function () {
                        let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(userId);
                        // HQ user has group permission
                        if (user.group?.length) {
                            if (group) {
                                limitCondition.push(` l.groupId = ? `)
                                replacements.push(group)
                            } else if (group == 0) {
                                limitCondition.push(` l.groupId in (?) `)
                                replacements.push(groupIdList)
                            }
                        }
                        
                        let tempSqlList = []
                        if (unitIdList.length) {
                            tempSqlList.push(` l.unitId IN (?) `)
                            replacements.push(unitIdList);
                        }
                        if (groupIdList.length) {
                            tempSqlList.push(` l.groupId in (?) `)
                            replacements.push(groupIdList);
                        }
                        
                        if (tempSqlList.length) {
                            limitCondition.push(` (${ tempSqlList.join(' OR ') }) `)
                        } else {
                            limitCondition.push(` 1=2 `)
                        }
                    }
                    await getSql()
                } else if (user.userType.toLowerCase() == 'unit') {
                    let hubNodeIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.hub, user.node);
                    if (hubNodeIdList.length) {
                        // Maybe not more than 1000 node in one hub
                        limitCondition.push(` l.unitId IN ( ? ) `)
                        replacements.push(hubNodeIdList)
                    }
                }
            }
            await getPermitSql()

            const getSearchSql = function () {
                if (activity) {
                    limitCondition.push(` l.activityName LIKE ` + sequelizeObj.escape("%"+activity+"%"))
                }
                if (purpose) {
                    limitCondition.push(` l.purpose LIKE ` + sequelizeObj.escape("%"+purpose+"%"))
                }
                if (taskStatus) {
                    if (taskStatus.toLowerCase() == 'waitcheck') taskStatus = 'pending'
                    if (taskStatus.toLowerCase() == 'completed') {
                        limitCondition.push(` ( l.status = 'returned' or l.status = 'completed' ) `)
                    } else {
                        limitCondition.push(` l.status = ? `)
                        replacements.push(taskStatus);
                    }
                }
                if (selectedDate) {
                    limitCondition.push(` (DATE(l.startDate) <= ? AND DATE(l.endDate) >= ? ) `)
                    replacements.push(selectedDate)
                    replacements.push(selectedDate)
                }
                if (group) {
                    limitCondition.push(` l.groupId=? `)
                    replacements.push(group)
                }
    
                if (taskId) {
                    limitCondition.push(` l.taskId like ` + sequelizeObj.escape("%"+taskId+"%"))
                }
                if (hub) {
                    limitCondition.push(` l.hub=? `)
                    replacements.push(hub);
                }
                if (node) {
                    limitCondition.push(` l.node=? `)
                    replacements.push(node);
                }
                if (driverName) {
                    limitCondition.push(` l.driverName like ` + sequelizeObj.escape("%"+driverName+"%"))
                }
                if (vehicleNo) {
                    limitCondition.push(` l.vehicleNo like ` + sequelizeObj.escape("%"+vehicleNo+"%"))
                }
            }
            getSearchSql()

            if (limitCondition.length) {
                sql += ' WHERE ' + limitCondition.join(' AND ');
                sql2 += ' WHERE ' + limitCondition.join(' AND ');
            }

            if (sortBy == 'driverName' && sort) {
                if (sort.toLowerCase() == 'asc') {
                    sql += ` ORDER BY l.driverName ASC `
                } else {
                    sql += ` ORDER BY l.driverName DESC `
                }
            } else {
                sql += ` ORDER BY l.startDate desc `
            }

            let totalList = await sequelizeObj.query(sql2, { type: QueryTypes.SELECT, replacements });

            sql += ` limit ?, ?`
            replacements.push(Number(pageNum));
            replacements.push(Number(pageLength));
            let taskList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements });

            let pageList = await userService.getUserPageList(userId, 'Task Dashboard', 'CV')
            let operationList = pageList.map(item => `${ item.action }`).join(',')
            for (let task of taskList) {
                task.operation = operationList
            }

            // get location, activity and purpose
            let sysTaskId = taskList.map(item => item.taskId)
            if (sysTaskId.length) {
                let result = await sequelizeSystemObj.query(`
                    SELECT jt.id, jt.pickupDestination, jt.dropoffDestination, g.groupName, r.purposeType AS purpose, r.additionalRemarks AS activityName 
                    FROM job_task jt
                    LEFT JOIN request r ON r.id = jt.requestId
                    LEFT JOIN \`group\` g ON g.id = r.groupId
                    WHERE jt.id IN (?)
                `, { type: QueryTypes.SELECT, replacements: [ sysTaskId ]})

                for (let task of taskList) {
                    result.some(item => {
                        if (item.id == task.taskId) {
                            // task = { ...task, ...item }
                            task.pickupDestination = item.pickupDestination
                            task.dropoffDestination = item.dropoffDestination
                            task.groupName = item.groupName
                            return true
                        }
                    })
                }
            }
            
            return res.json({ respMessage: taskList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });

        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    startLoan: async function (req, res) {
        try {
            let { loanId, actualStartTime } = req.body;
            let oldLoan = await loan.findOne({ where: { id: loanId } })
            await loan.update({ actualStartTime }, { where: { id: loanId } })
            let newLoan = await loan.findOne({ where: { id: loanId } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Loan Task',
                businessId: loanId,
                optType: 'Start',
                beforeData: `${ JSON.stringify([oldLoan]) }`,
                afterData: `${ JSON.stringify([newLoan]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'edit loan start time.'
            })
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    completeLoan: async function (req, res) {
        try {
            let { loanId, actualEndTime } = req.body;
            let oldLoan = await loan.findOne({ where: { id: loanId } })
            await loan.update({ actualEndTime }, { where: { id: loanId } })
            let newLoan = await loan.findOne({ where: { id: loanId } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Loan Task',
                businessId: loanId,
                optType: 'Complete',
                beforeData: `${ JSON.stringify([oldLoan]) }`,
                afterData: `${ JSON.stringify([newLoan]) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'edit loan end time.'
            })
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    }
}