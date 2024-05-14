const log = require('../log/winston.js').logger('Alert Process');
const util = require('../util/utils.js');
const CONTENT = require('../util/content.js');

const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf.js');

const moment = require('moment');

const { Unit } = require('../model/unit.js');
const { Vehicle } = require('../model/vehicle.js');

const { Track } = require('../model/event/track.js');
const { TrackHistory } = require('../model/event/trackHistory.js');
const { DevicePositionHistory, DevicePositionHistoryBackup } = require('../model/event/devicePositionHistory.js');
const { DeviceOffenceHistory } = require('../model/event/deviceOffenceHistory.js');
const { DriverPositionHistory, DriverPositionHistoryBackup } = require('../model/event/driverPositionHistory.js');
const { DriverOffenceHistory } = require('../model/event/driverOffenceHistory.js');

const outputService = require('../services/outputService');

process.on('message', async deviceProcess => {
    // { deviceList: [], driverList: [] }
    log.info(`Message from parent(${moment().format('YYYY-MM-DD HH:mm:ss')}): `)
    log.info(JSON.stringify(deviceProcess, null, 4))
    try {
        const deviceList = deviceProcess.deviceList
        const driverList = deviceProcess.driverList
        
        await updateOBDAlert(deviceList);
        await updateMobileAlert(driverList);

        process.send({ success: true })
        process.exit(0)
    } catch (error) {
        log.error(error);
        process.send({ success: false, error })
    }
})


const updateOBDAlert = async function (targetList) {
    try {
        if (!targetList.length) {
            log.info(`updateOBDAlertByChildProcess => no data.`)
            return
        } else {
            log.info(`updateOBDAlertByChildProcess`, JSON.stringify(targetList, null, 4))
        }

        // Find out vehicleList be related 
        let deviceIdList = targetList.map(item => item.deviceId)
        let vehicleList = await Vehicle.findAll({ where: { deviceId: deviceIdList }, raw: true })

        for (let target of targetList) {
            // Find out vehicle
            let vehicle = vehicleList.find(item => item.deviceId == target.deviceId)
            if (!vehicle) {
                log.warn(`DeviceID ${ target.deviceId } has no record in Table(Vehicle)`)
                continue
            } else {
                log.warn(`DeviceID ${ target.deviceId } => VehicleNo ${ vehicle.vehicleNo }`)
            }

            // Find out task by date (not loan task, so need vehicleNumber & driverId while search task)
            let taskList = await sequelizeObj.query(`
                SELECT taskId, dataFrom, driverId, vehicleNumber, vehicleNumber AS vehicleNo,
                DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime,
                hub, node, groupId
                FROM task
                WHERE vehicleNumber = '${ vehicle.vehicleNo }'
                AND '${ target.createdAt }' >= mobileStartTime
                AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )
                AND driverId IS NOT NULL

                UNION

                SELECT CONCAT('DUTY-', dutyId) AS taskId, 'SYSTEM' AS dataFrom, driverId, vehicleNo, vehicleNo AS vehicleNumber,
                DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime,
                hub, node, groupId
                FROM urgent_indent
                WHERE vehicleNo = '${ vehicle.vehicleNo }'
                AND '${ target.createdAt }' >= mobileStartTime
                AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )
                AND driverId IS NOT NULL
            `, { type: QueryTypes.SELECT })
            // Calculate every task's alert record

            if (!taskList.length) {
                log.info(`DeviceID ${ target.deviceId } do not has task at ${ target.createdAt } `)
                continue
            }

            for (let task of taskList) {
                // generate timezone
                let timezone = [ task.mobileStartTime ]
                if (task.mobileEndTime) {
                    // If through two or more days, will ignore at commonStoreEventHistoryForIdle
                    timezone.push(task.mobileEndTime)
                } else {
                    timezone.push(moment().format('YYYY-MM-DD 23:59:59'))
                }
                log.warn(`TaskID: ${ task.taskId } => TimeZone ${ timezone }`)

                let deviceGPSList = await outputService.readFromFile(target.deviceId, null, timezone)
                let alertList = await commonGenerateNoGoZoneAlert(deviceGPSList, target.deviceId, vehicle.vehicleNo, task.hub, task.node, task.groupId)

                if (alertList.length) {
                    alertList = alertList.map(item => {
                        item.taskId = task.taskId
                        item.vehicleNo = task.vehicleNo
                        return item
                    })
                    log.warn(`Alert List => `, JSON.stringify(alertList, null, 4))
                    await sequelizeObj.transaction(async transaction => {
                        await TrackHistory.destroy({ where: { 
                            deviceId: target.deviceId, 
                            violationType: CONTENT.ViolationType.NoGoZoneAlert, 
                            occTime: { [Op.between]: timezone } 
                        } })
    
                        await commonStoreEventHistoryForAlert(alertList, 'obd');
                        await commonStoreEventForAlert(alertList, { dataFrom: 'obd' });
                        await commonStoreEventPositionHistoryForOBD(alertList, deviceGPSList)
                    })
                } else {
                    log.info(`updateOBDAlertByChildProcess => DeviceId: ${ target.deviceId }(VehicleNo: ${ vehicle.vehicleNo }) has no alert record on taskId: ${ task.taskId }.(mobileStartTime: ${ task.mobileStartTime }, mobileEndTime: ${ task.mobileEndTime })`)
                }
            }
        }
    } catch (error) {
        log.error(`updateOBDAlertByChildProcess => `, error)
    }
}

const updateMobileAlert = async function (targetList) {
    try {
        if (!targetList.length) {
            log.info(`updateMobileAlertByChildProcess => no data.`)
            return
        } else {
            log.info(`updateMobileAlertByChildProcess`, JSON.stringify(targetList, null, 4))
        }

        for (let target of targetList) {
            // Find out task by date (not loan task, so need vehicleNumber & driverId while search task)
            let taskList = await sequelizeObj.query(`
                SELECT taskId, dataFrom, driverId, vehicleNumber, vehicleNumber AS vehicleNo, 
                DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime,
                hub, node, groupId
                FROM task
                WHERE driverId = ${ target.driverId }
                AND vehicleNumber = '${ target.vehicleNo }'
                AND '${ target.createdAt }' >= mobileStartTime
                AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )

                UNION

                SELECT CONCAT('DUTY-', dutyId) AS taskId, 'SYSTEM' AS dataFrom, driverId, vehicleNo, vehicleNo AS vehicleNumber,
                DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime,
                hub, node, groupId
                FROM urgent_indent
                WHERE driverId = ${ target.driverId }
                AND vehicleNo = '${ target.vehicleNo }'
                AND '${ target.createdAt }' >= mobileStartTime
                AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )
            `, { type: QueryTypes.SELECT })

            // Calculate every task's Alert record
            for (let task of taskList) {
                // generate timezone
                let timezone = [ task.mobileStartTime ]            
                if (task.mobileEndTime) {
                    // If through two or more days, will ignore at commonStoreEventHistoryForIdle
                    timezone.push(task.mobileEndTime)
                } else {
                    timezone.push(moment().format('YYYY-MM-DD 23:59:59'))
                }
                log.warn(`TaskID: ${ task.taskId } => TimeZone ${ timezone }`)

                let driverGPSList = await outputService.readFromFile(task.driverId, task.vehicleNo, timezone)
                let alertList = await commonGenerateNoGoZoneAlert(driverGPSList, task.driverId, task.vehicleNo, task.hub, task.node, task.groupId)

                if (alertList.length) {
                    alertList = alertList.map(item => {
                        item.taskId = task.taskId
                        item.vehicleNo = task.vehicleNo
                        return item
                    })
                    log.warn(`Alert List => `, JSON.stringify(alertList, null, 4))
                    await sequelizeObj.transaction(async transaction => {
                        await TrackHistory.destroy({ where: { 
                            deviceId: `${ target.driverId }`, 
                            violationType: CONTENT.ViolationType.NoGoZoneAlert, 
                            occTime: { [Op.between]: timezone } 
                        } })
    
                        await commonStoreEventHistoryForAlert(alertList, 'mobile');
                        await commonStoreEventForAlert(alertList, { dataFrom: 'mobile' });
                        await commonStoreEventPositionHistoryForMobile(alertList, driverGPSList)
                    })
                } else {
                    log.info(`updateMobileAlertByChildProcess => DriverId: ${ target.driverId }(VehicleNo: ${ target.vehicleNo }) has no alert record on taskId: ${ task.taskId }.(mobileStartTime: ${ task.mobileStartTime }, mobileEndTime: ${ task.mobileEndTime })`)
                }
            }
        }

    } catch (error) {
        log.error(`updateMobileAlertByChildProcess => `, error)
    }
}

const getNoGoZoneList = async function (hub, node, groupId) {
    try {
        let sql = `
            SELECT nz.*, u.unitId, u.hub, u.node, u.userType,
            GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
            FROM nogo_zone nz
            LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
            LEFT JOIN user u on nz.owner = u.userId
            LEFT JOIN unit un on un.id = u.unitId
            WHERE nz.deleted = 0 and nz.alertType = 1 and nz.enable = 1
        `

        if (groupId) {
            sql += ` AND (u.unitId = '${ groupId }' AND u.userType = '${ CONTENT.USER_TYPE.CUSTOMER }') `
        } else {
            sql += ` AND (un.unit <=> '${ hub }' and un.subUnit <=> '${ node }' AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') `
        }

        sql += ` GROUP BY nz.id `

        let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
        return noGoZoneList
    } catch (error) {
        return []
    }
}

const getUnitIdByUnitAndSubUnit = async function (hub, node) {
    let unitId
    if(!hub) {
        let unit = await Unit.findAll()
        unitId = unit.map(item => { return item.id });
        unitId = Array.from(new Set(unitId));
        // unitId = (unitId.toString()).split(',')
    } else {
        if(node){
            let unit = await Unit.findOne({ where: { unit: hub, subUnit: node } })
            unitId = [ unit.id ];
        } else {
            let unit = await Unit.findAll({ where: { unit: hub } })
            unitId = unit.map(item => { return item.id });
            unitId = Array.from(new Set(unitId));
            // unitId = (unitId.toString()).split(',')
        }
    }
    
    return unitId
}

const getNoGoZoneListByHubNode = async function (hub, node) {
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
        let permitUnitIdList = await getUnitIdByUnitAndSubUnit(hub, node);
        // hub
        let permitUnitIdList2 = await getUnitIdByUnitAndSubUnit(hub);
        sql += ` AND (
            (u.unitId IN (${ permitUnitIdList }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') 
            OR
            (u.unitId IN (${ permitUnitIdList2 }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') 
            OR
            (u.userType IN ('${ CONTENT.USER_TYPE.HQ }', '${ CONTENT.USER_TYPE.ADMINISTRATOR }'))
        )`

        sql += ` GROUP BY nz.id `
        log.info(sql)
        let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
        return noGoZoneList
    } catch (error) {
        log.error(error)
        return []
    }
}
const getNoGoZoneListByGroup = async function (groupId) {
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
            (u.unitId = ${ groupId } and u.userType = '${ CONTENT.USER_TYPE.CUSTOMER }') 
            OR
            (u.userType IN ('${ CONTENT.USER_TYPE.HQ }', '${ CONTENT.USER_TYPE.ADMINISTRATOR }'))
        )`

        sql += ` GROUP BY nz.id `
        let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
        return noGoZoneList
    } catch (error) {
        return []
    }
}

const checkAlertTime = function (noGoZone, dateTime) {
    const checkAlertDate = function (noGoZone, dateTime) {
        let currentDate = moment(dateTime).format('YYYY-MM-DD')
        if (moment(currentDate, 'YYYY-MM-DD').isBetween(moment(noGoZone.startDate, 'YYYY-MM-DD'), moment(noGoZone.endDate, 'YYYY-MM-DD'), null, [])) {
            return true
        }
        return false
    }

    // DATA => 'YYYY-MM-DD HH:mm:ss'
    const checkWeek = function (selectedWeeks, date) {
        let week = moment(date).day()
        let weeks = selectedWeeks.split(',').map(item => Number.parseInt(item))
        if (weeks.indexOf(week) > -1) {
            return true
        }
        return false
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

    if (checkAlertDate(noGoZone, dateTime) && checkWeek(selectedWeeks, dateTime) && checkTime(selectedTimes, dateTime)) {
        // log.warn(`********************************`)
        // log.warn(selectedWeeks)
        // log.warn(selectedTimes)
        // log.warn(dateTime)
        // log.warn(`********************************`)
        return true
    }

    return false
}

const commonGenerateNoGoZoneAlert = async function (list, id, vehicleNo, hub, node, groupId) {
    try {
        // TODO: At least two record can cal descSpeed
        if (!list.length || list.length === 1) return [];
        let alertList = [];

        let noGoZoneList = []
        if (groupId) {
            noGoZoneList = await getNoGoZoneListByGroup(groupId)
        } else {
            noGoZoneList = await getNoGoZoneListByHubNode(hub, node);
        }
        for (let noGoZone of noGoZoneList) {
            log.info(noGoZone.zoneName)
            let alertRecord = {
                deviceId: id, 
                vehicleNo,
                violationType: CONTENT.ViolationType.NoGoZoneAlert, 
                zoneId: noGoZone.id
            }
            let preStatus = 0;
            for (let position of list) {
                if (!checkAlertTime(noGoZone, position.createdAt)) {
                    // current time is not in alert timezone
                    continue;
                }

                let result = util.isPointInPolygon([position.lat, position.lng], JSON.parse(noGoZone.polygon))
                
                if (result) {
                    if (preStatus == 0) {
                        // first time in zone
                        alertRecord.occTime = position.createdAt
                        alertRecord.startTime = position.createdAt
                        alertRecord.vin = position.vin
                        alertRecord.startSpeed = position.speed
                        alertRecord.speed = position.speed
                        alertRecord.lat = position.lat
                        alertRecord.lng = position.lng

                        preStatus = 1
                    } else {
                        // still in
                    }
                } else {
                    if (preStatus == 1) {
                        // out
                        alertRecord.endTime = position.createdAt

                        let timezone = moment(alertRecord.endTime).diff(moment(alertRecord.startTime));
                        alertRecord.stayTime = Math.floor(timezone / 1000)

                        // store this record and start new one
                        alertList.push(alertRecord)

                        // re-init
                        preStatus = 0; 
                        alertRecord = {
                            deviceId: id, 
                            vehicleNo,
                            violationType: CONTENT.ViolationType.NoGoZoneAlert, 
                            zoneId: noGoZone.id
                        }
                    } else {

                        // still out
                    }
                }
            }
            if (alertRecord.startTime && !alertRecord.endTime) {
                // in no go zone, not out yet
                alertList.push(alertRecord)
            }
        }

        return alertList
    } catch (error) {
        log.error('(commonGenerateAlert): ', error)
    }
}

const commonStoreEventForAlert = async function (list, option) {
    if (!list || list.length == 0) return;

    let latestIdle = list[list.length - 1];

    let result = await TrackHistory.findAll({
        where: {
            deviceId: latestIdle.deviceId,
            violationType: latestIdle.violationType, 
            vehicleNo: latestIdle.vehicleNo ?? null,
        }
    })
    let count = result.length;

    try {
        let result = await Track.findOne({
            where: {
                deviceId: latestIdle.deviceId,
                violationType: latestIdle.violationType, 
                vehicleNo: latestIdle.vehicleNo ?? null,
            }
        })
        if (result) {
            await result.update({
                count: count, 
                dataFrom: option.dataFrom, 
                startTime: latestIdle.startTime, 
                endTime: latestIdle.endTime, 
                diffSecond: moment(latestIdle.endTime).diff(moment(latestIdle.startTime)) / 1000, 
                occTime: latestIdle.occTime, 
                lastOccTime: latestIdle.endTime, 
                speed: latestIdle.speed, 
                startSpeed: latestIdle.startSpeed, 
                endSpeed: latestIdle.endSpeed, 
                lat: latestIdle.lat, 
                lng: latestIdle.lng 
            })
        } else {
            await Track.create({ 
                deviceId: latestIdle.deviceId,
                count: count, 
                vehicleNo: latestIdle.vehicleNo, 
                violationType: latestIdle.violationType,
                dataFrom: option.dataFrom, 
                startTime: latestIdle.startTime, 
                endTime: latestIdle.endTime, 
                diffSecond: moment(latestIdle.endTime).diff(moment(latestIdle.startTime)) / 1000, 
                occTime: latestIdle.occTime, 
                lastOccTime: latestIdle.endTime, 
                speed: latestIdle.speed, 
                startSpeed: latestIdle.startSpeed, 
                endSpeed: latestIdle.endSpeed, 
                lat: latestIdle.lat, 
                lng: latestIdle.lng 
            })
        }
        
    } catch (error) {
        log.error(error)
    }
}
const commonStoreEventHistoryForAlert = async function (list, from) {
    try {
        if (!list || list.length == 0) return;
        let records = [];
        for (let data of list) {
            data.dataFrom = from
            data.diffSecond = moment(data.endTime).diff(moment(data.startTime)) / 1000
            records.push(data)
        }
        return await TrackHistory.bulkCreate(records, { updateOnDuplicate: ['lat', 'lng', 'speed', 'startSpeed', 'endSpeed', 'startTime', 'endTime', 'diffSecond', 'stayTime', 'accSpeed', 'decSpeed'] });
    } catch (error) {
        log.error(error)
    }
}

const commonStoreEventPositionHistoryForOBD = async function (list, obdGpsList) {
    try {
        let records = [], idSet = new Set();
        for (let data of list) {
            // TODO: add this record in 30s into offenceHistory table
            let targetOffenceHistoryList = await obdGpsList.filter(item => {
                if (item.deviceId == data.deviceId)  {
                    if (moment(item.createdAt).isAfter(moment(data.startTime).subtract(15, 's')) 
                    || moment(item.createdAt).isBefore(moment(data.endTime).add(15, 's'))) {
                        return true;
                    }
                }
            })
            log.warn(`Find position record for offence history => ${ data.deviceId } total (${ targetOffenceHistoryList.length }) count`)
            log.warn(`Find position record for offence history => startTime: ${ moment(data.startTime).subtract(15, 's').format('YYYY-MM-DD HH:mm:ss') }, endTime: ${ moment(data.endTime).add(15, 's').format('YYYY-MM-DD HH:mm:ss') } `)

            for (let target of targetOffenceHistoryList) {
                if (idSet.has(target.id)) continue;
                else {
                    idSet.add(target.id)
                    records.push({ 
                        id: target.id, 
                        deviceId: target.deviceId, 
                        speed: target.speed, 
                        lat: target.lat, 
                        lng: target.lng, 
                        rpm: target.rpm, 
                        createdAt: target.createdAt 
                    });
                }
            }
        }
        if (records.length) {
            log.warn(`Store obd position record for offence history => total (${ records.length }) count`)
            await DeviceOffenceHistory.bulkCreate(records, { updateOnDuplicate: ['lat', 'lng'] });
        }
    } catch (error) {
        log.error('commonStoreEventPositionHistoryForOBD: ', error)
    }
}
const commonStoreEventPositionHistoryForMobile = async function (list, driverGPSList) {
    try {
        let records = [], idSet = new Set();
        
        // console.log(driverGPSList)
        // console.log(list)
        for (let data of list) {
            // TODO: add this record in 30s into offenceHistory table
            let targetOffenceHistoryList = await driverGPSList.filter(item => {
                if (item.driverId == data.deviceId && item.vehicleNo == data.vehicleNo)  {
                    if (moment(item.createdAt).isAfter(moment(data.startTime).subtract(15, 's')) 
                    || moment(item.createdAt).isBefore(moment(data.endTime).add(15, 's'))) {
                        return true;
                    }
                }
            })
            log.warn(`Find position record for offence history => deviceId: ${ data.deviceId }, vehicleNo: ${ data.vehicleNo } total (${ targetOffenceHistoryList.length }) count`)
            log.warn(`Find position record for offence history => startTime: ${ moment(data.startTime).subtract(15, 's').format('YYYY-MM-DD HH:mm:ss') }, endTime: ${ moment(data.endTime).add(15, 's').format('YYYY-MM-DD HH:mm:ss') } `)

            for (let target of targetOffenceHistoryList) {
                if (idSet.has(target.id)) continue;
                else {
                    idSet.add(target.id)
                    records.push({ 
                        id: target.id, 
                        driverId: target.driverId, 
                        vehicleNo: target.vehicleNo, 
                        speed: target.speed, 
                        lat: target.lat, 
                        lng: target.lng, 
                        rpm: target.rpm, 
                        createdAt: target.createdAt 
                    })
                }
            }
        }
        
        if (records.length) {
            log.warn(`Store mobile position record for offence history => total (${ records.length }) count`)
            await DriverOffenceHistory.bulkCreate(records, { updateOnDuplicate: ['lat', 'lng'] });
        }
    } catch (error) {
        log.error('commonStoreEventPositionHistoryForMobile: ', error)
    }
}