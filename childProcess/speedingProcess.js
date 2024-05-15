const log = require('../log/winston').logger('Speeding Process');
const { QueryTypes, Op } = require('sequelize');
const moment = require('moment');
const util = require('../util/utils');

const CONTENT = require('../util/content');
const { sequelizeObj } = require('../db/dbConf')

const { commonFindOutTaskId } = require('../services/offenceService');

const { Track } = require('../model/event/track.js');
const { TrackHistory } = require('../model/event/trackHistory.js');
const { DevicePositionHistory, DevicePositionHistoryBackup } = require('../model/event/devicePositionHistory.js');
const { DeviceOffenceHistory } = require('../model/event/deviceOffenceHistory.js');
const { DriverPositionHistory, DriverPositionHistoryBackup } = require('../model/event/driverPositionHistory.js');
const { DriverOffenceHistory } = require('../model/event/driverOffenceHistory.js');

process.on('message', async deviceProcess => {
    // { deviceList: [], driverList: [] }
    log.warn(`Message from parent(${ moment().format('YYYY-MM-DD HH:mm:ss') }): `, JSON.stringify(deviceProcess))
    try {
        const deviceList = deviceProcess.deviceList
        const driverList = deviceProcess.driverList
        for (let device of deviceList) {
            let record = await Track.findOne({
                where: {
                    deviceId: device.deviceId,
                    violationType: CONTENT.ViolationType.Speeding
                }
            })
            // This is the flag use to different current data from old data
            let flagOccTime = record ? record.lastOccTime : null;
            let flagCount = record ? record.count : 0;
            let timezone = [ device.startRecordTime, device.endRecordTime ]
            let list = await getDataForGenerateSpeeding({ deviceId: device.deviceId, limitSpeed: device.limitSpeed ?? 60, flagOccTime, timezone });
            await generateSpeeding({ deviceId: device.deviceId, list, flagCount, flagOccTime, dbRecord: record })
            await util.wait(10)
        }
        for (let driver of driverList) {
            let record = await Track.findOne({
                where: {
                    deviceId: driver.driverId,
                    vehicleNo: driver.vehicleNo,
                    violationType: CONTENT.ViolationType.Speeding
                }
            })
            // This is the flag use to different current data from old data
            let flagOccTime = record ? record.occTime : null;
            let flagCount = record ? record.count : 0;
            let timezone = [ driver.startRecordTime, driver.endRecordTime ]
            let list = await getDataForGenerateSpeedingByMobile({ driverId: driver.driverId, vehicleNo: driver.vehicleNo, limitSpeed: driver.limitSpeed ?? 60, flagOccTime, timezone });
            await generateSpeedingByMobile({ driverId: driver.driverId, vehicleNo: driver.vehicleNo, list, flagCount, flagOccTime, dbRecord: record });
            await util.wait(10)
        }
        process.send({ success: true })
        process.exit(0)
    } catch (error) {
        log.error(error);
        process.send({ success: false, error })
    }
})

process.on('exit', function () {
    log.warn(`Child process exit ...`)
})

// ************************************************************
// OBD
const getDataForGenerateSpeeding = async function (option) {
    return await sequelizeObj.query(`
        SELECT ph2.* FROM (
            SELECT ROW_NUMBER() OVER( ORDER BY ph1.createdAt ASC) AS rowNo, ph1.* FROM device_position_history_backup AS ph1 
            WHERE ph1.deviceId = '${ option.deviceId }'
            ${ option.flagOccTime ? ` AND ph1.createdAt >= '${ moment(option.flagOccTime).format('YYYY-MM-DD HH:mm:ss') }' ` : '' }
            AND ph1.createdAt >= '${ moment(option.timezone[0]).format('YYYY-MM-DD HH:mm:ss') }'
            AND ph1.createdAt <= '${ moment(option.timezone[1]).format('YYYY-MM-DD HH:mm:ss') }'
            ORDER BY ph1.createdAt ASC
        ) AS ph2 WHERE ph2.speed > ${ option.limitSpeed } ORDER BY ph2.createdAt asc
    `, { type: QueryTypes.SELECT })
}
const generateSpeeding = async function (option) {
    if (!option.list.length) return;

    let speedingList = commonGenerateContinuousSpeeding(option.list, option.deviceId);

    let needMinusCount = false;
    if (option.dbRecord) {
        // TODO: check with last one record
        // log.info('```````````````````````````')
        // log.info(speedingList[0].startTime)
        // log.info(option.dbRecord.endTime)
        // log.info(moment(speedingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2)
        // log.info('```````````````````````````')
        if (speedingList.length && moment(speedingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2) {
            needMinusCount = true;

            speedingList[0].occTime = option.dbRecord.occTime;
            speedingList[0].startTime = option.dbRecord.startTime;
            speedingList[0].startSpeed = option.dbRecord.startSpeed;
            let diffSecond = moment(speedingList[0].endTime).diff(moment(speedingList[0].startTime))/1000
            speedingList[0].diffSecond = diffSecond
            if ([CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc].includes(speedingList[0].violationType)) {
                if (CONTENT.ViolationType.HardBraking === speedingList[0].violationType) {
                    speedingList[0].diffSpeed = speedingList[0].startSpeed - speedingList[0].endSpeed
                    speedingList[0].decSpeed = speedingList[0].diffSpeed / diffSecond
                } else if (CONTENT.ViolationType.RapidAcc === speedingList[0].violationType) {
                    speedingList[0].diffSpeed = speedingList[0].endSpeed - speedingList[0].startSpeed
                    speedingList[0].accSpeed = speedingList[0].diffSpeed / diffSecond
                }
            }
        }
    }

    if (speedingList.length) {
        await sequelizeObj.transaction(async transaction => {
            option.dataFrom = 'obd'
            option.needMinusCount = needMinusCount
            speedingList = await commonFindOutTaskId(speedingList, 'obd')
            await commonStoreEventForSpeeding(speedingList, option)
            await commonStoreEventHistoryForSpeeding(speedingList, 'obd');
            await commonStoreEventPositionHistoryForOBD(speedingList);
        })
    }
}

// ************************************************************
// Mobile
const getDataForGenerateSpeedingByMobile = function (option) {
    return sequelizeObj.query(`
        SELECT ph2.* FROM (
            SELECT ROW_NUMBER() OVER( ORDER BY ph1.createdAt ASC) AS rowNo, ph1.* FROM driver_position_history_backup AS ph1 
            WHERE ph1.driverId = '${ option.driverId }' AND ph1.vehicleNo = '${ option.vehicleNo }' 
            ${ option.flagOccTime ? ` AND ph1.createdAt >= '${ moment(option.flagOccTime).format('YYYY-MM-DD HH:mm:ss') }' ` : '' }
            AND ph1.createdAt >= '${ moment(option.timezone[0]).format('YYYY-MM-DD HH:mm:ss') }'
            AND ph1.createdAt <= '${ moment(option.timezone[1]).format('YYYY-MM-DD HH:mm:ss') }'
            ORDER BY ph1.createdAt ASC
        ) AS ph2 WHERE ph2.speed > ${ option.limitSpeed } ORDER BY ph2.createdAt ASC;
    `, { type: QueryTypes.SELECT })
}
const generateSpeedingByMobile = async function (option) {
    if (!option.list.length) return;

    let speedingList = commonGenerateContinuousSpeeding(option.list, option.driverId);

    let needMinusCount = false;
    if (option.dbRecord) {
        // TODO: check with last one record
        // log.info('```````````````````````````')
        // log.info(speedingList[0].startTime)
        // log.info(option.dbRecord.endTime)
        // log.info(moment(speedingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2)
        // log.info('```````````````````````````')
        if (speedingList.length && moment(speedingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2) {
            needMinusCount = true;

            speedingList[0].occTime = option.dbRecord.occTime;
            speedingList[0].startTime = option.dbRecord.startTime;
            speedingList[0].startSpeed = option.dbRecord.startSpeed;
            let diffSecond = moment(speedingList[0].endTime).diff(moment(speedingList[0].startTime))/1000
            speedingList[0].diffSecond = diffSecond
            if ([CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc].includes(speedingList[0].violationType)) {
                if (CONTENT.ViolationType.HardBraking === speedingList[0].violationType) {
                    speedingList[0].diffSpeed = speedingList[0].startSpeed - speedingList[0].endSpeed
                    speedingList[0].decSpeed = speedingList[0].diffSpeed / diffSecond
                } else if (CONTENT.ViolationType.RapidAcc === speedingList[0].violationType) {
                    speedingList[0].diffSpeed = speedingList[0].endSpeed - speedingList[0].startSpeed
                    speedingList[0].accSpeed = speedingList[0].diffSpeed / diffSecond
                }
            }
        }
    }

    if (speedingList.length) {
        // TODO: insert vehicleNo into data
        speedingList = speedingList.map(speeding => {
            speeding.vehicleNo = option.vehicleNo;
            return speeding
        })

        await sequelizeObj.transaction(async transaction => {
            option.dataFrom = 'mobile';
            option.needMinusCount = needMinusCount;
            speedingList = await commonFindOutTaskId(speedingList, 'mobile')
            await commonStoreEventForSpeeding(speedingList, option)
            await commonStoreEventHistoryForSpeeding(speedingList, 'mobile');
            await commonStoreEventPositionHistoryForMobile(speedingList);
        })
    }
}

// ************************************************************
// Common
const commonGenerateContinuousSpeeding = function (list, id) {
    if (!list.length) return [];

    let speedingList = [];
    if (list.length === 1) {
        // Only one record
        speedingList.push({
            deviceId: id,
            violationType: CONTENT.ViolationType.Speeding,
            speed: list[0].speed,
            lat: list[0].lat,
            lng: list[0].lng,
            occTime: list[0].createdAt,
            startTime: list[0].createdAt,
            endTime: list[0].createdAt,
            startSpeed: list[0].speed,
            endSpeed: list[0].speed,
        })
        return speedingList;
    } 
    // More than one record
    let index = -1;
    let startNode = null;
    let tempSpeed = 0
    for (let data of list) {

        if (data.speed > tempSpeed) tempSpeed = data.speed

        index++;
        if (index === 0) {
            // First record, store as startNode
            data.occTime = data.createdAt;
            data.startTime = data.createdAt;
            data.endTime = data.createdAt;
            data.startSpeed= data.speed;
            data.endSpeed= data.speed;
            startNode = data;
            continue;
        } 
        
        // Next record
        // Check if continuous rowNo record
        if (data.rowNo - 1 === list[index - 1].rowNo) {
            // Continuous
            // Check if last record
            if (index === list.length - 1) {
                // Last record
                speedingList.push({
                    deviceId: id,
                    violationType: CONTENT.ViolationType.Speeding,
                    // speed: data.speed,
                    speed: tempSpeed,
                    lat: data.lat,
                    lng: data.lng,
                    occTime: startNode.occTime,
                    startTime: startNode.startTime,
                    startSpeed: startNode.startSpeed,
                    endTime: data.createdAt, // Last record, no 'endTime', 'endSpeed' Filed yet
                    endSpeed: data.speed, // Last record, no 'endTime', 'endSpeed' Filed yet
                })
                
                tempSpeed = 0

            } else {
                // Not last record
                data.endTime = data.createdAt // Last record, no 'endTime', 'endSpeed' Filed yet
                data.endSpeed = data.speed // Last record, no 'endTime', 'endSpeed' Filed yet
            }
            
            continue;
        } 
        
        // Not Continuous
        // Add pre zone Speeding
        speedingList.push({
            deviceId: id,
            violationType: CONTENT.ViolationType.Speeding,
            // speed: list[index - 1].speed,
            speed: tempSpeed,
            lat: list[index - 1].lat,
            lng: list[index - 1].lng,
            occTime: startNode.occTime,
            startTime: startNode.startTime,
            startSpeed: startNode.startSpeed,
            endTime: list[index - 1].endTime,
            endSpeed: list[index - 1].endSpeed,
        })

        tempSpeed = 0
        
        // Check if last record
        if (index === list.length - 1) {
            // Last record
            speedingList.push({
                deviceId: id,
                violationType: CONTENT.ViolationType.Speeding,
                // speed: data.speed,
                speed: tempSpeed,
                lat: data.lat,
                lng: data.lng,
                occTime: data.createdAt,
                startTime: data.createdAt,
                endTime: data.createdAt, // Last record, no 'endTime', 'endSpeed' Filed yet
                startSpeed: data.speed, 
                endSpeed: data.speed, // Last record, no 'endTime', 'endSpeed' Filed yet
            })

            tempSpeed = 0
            
        } else {
            // Not last record
            data.occTime = data.createdAt;
            data.startTime = data.createdAt;
            data.endTime = data.createdAt;
            data.startSpeed= data.speed;
            data.endSpeed= data.speed;
            startNode = data;
        }
    }
    return speedingList;
}

const commonStoreEventForSpeeding = async function (list, option) {
    let latestSpeeding = list[list.length - 1];
    let count = list.length + option.flagCount; 
    if (option.needMinusCount) count--;
    let result = await Track.findOne({ 
        where: {
            deviceId: latestSpeeding.deviceId,
            violationType: latestSpeeding.violationType,
            vehicleNo: latestSpeeding.vehicleNo ?? null,
        }
    })
    if (result) {
        await result.update({
            count: count,
            dataFrom: option.dataFrom,
            startTime: latestSpeeding.startTime, 
            endTime: latestSpeeding.endTime, 
            diffSecond: moment(latestSpeeding.endTime).diff(moment(latestSpeeding.startTime)) / 1000, 
            occTime: latestSpeeding.occTime, 
            lastOccTime: latestSpeeding.endTime, 
            speed: latestSpeeding.speed, 
            startSpeed: latestSpeeding.startSpeed, 
            endSpeed: latestSpeeding.endSpeed, 
            lat: latestSpeeding.lat, 
            lng: latestSpeeding.lng
        })
    } else {
        await Track.create({
            deviceId: latestSpeeding.deviceId,
            count: count,
            vehicleNo: latestSpeeding.vehicleNo,
            dataFrom: option.dataFrom,
            violationType: latestSpeeding.violationType, 
            startTime: latestSpeeding.startTime, 
            endTime: latestSpeeding.endTime, 
            diffSecond: moment(latestSpeeding.endTime).diff(moment(latestSpeeding.startTime)) / 1000, 
            occTime: latestSpeeding.occTime, 
            lastOccTime: latestSpeeding.endTime, 
            speed: latestSpeeding.speed, 
            startSpeed: latestSpeeding.startSpeed, 
            endSpeed: latestSpeeding.endSpeed, 
            lat: latestSpeeding.lat, 
            lng: latestSpeeding.lng 
        })
    }
    
}
const commonStoreEventHistoryForSpeeding = async function (list, from) {
    try {
        let records = []
        for (let speed of list) {
            speed.dataFrom = from
            speed.diffSecond = moment(speed.endTime).diff(moment(speed.startTime)) / 1000
            records.push(speed)
        }
        await TrackHistory.bulkCreate(records, { updateOnDuplicate: ['lat', 'lng'] })
        log.info(`commonStoreEventHistoryForSpeeding: => ${ JSON.stringify(records, null, 4) }`)
    } catch (error) {
        log.error('commonStoreEventHistoryForSpeeding: ', error)
    }
}
const commonStoreEventPositionHistoryForOBD = async function (list) {
    try {
        let records = [], idSet = new Set();
        for (let data of list) {
            // TODO: add this record in 30s into offenceHistory table
            let targetOffenceHistoryList = await DevicePositionHistoryBackup.findAll({
                where: {
                    deviceId: data.deviceId,
                    createdAt: {
                        [Op.gte]: moment(data.startTime).subtract(15, 's').format('YYYY-MM-DD HH:mm:ss'),
                        [Op.lte]: moment(data.endTime).add(15, 's').format('YYYY-MM-DD HH:mm:ss'),
                    }
                }
            })
            log.warn(`Find position record for offence history => ${ data.deviceId } total (${ targetOffenceHistoryList.length }) count`)
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
const commonStoreEventPositionHistoryForMobile = async function (list) {
    try {
        let records = [], idSet = new Set();
        for (let data of list) {
            // TODO: add this record in 30s into offenceHistory table
            let targetOffenceHistoryList = await DriverPositionHistoryBackup.findAll({
                where: {
                    driverId: data.deviceId,
                    vehicleNo: data.vehicleNo,
                    createdAt: {
                        [Op.gte]: moment(data.startTime).subtract(15, 's').format('YYYY-MM-DD HH:mm:ss'),
                        [Op.lte]: moment(data.endTime).add(15, 's').format('YYYY-MM-DD HH:mm:ss'),
                    }
                }
            })
            log.warn(`Find position record for offence history => deviceId: ${ data.deviceId }, vehicleNo: ${ data.vehicleNo } total (${ targetOffenceHistoryList.length }) count`)

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

module.exports = {
    getDataForGenerateSpeeding,
    generateSpeeding,
    getDataForGenerateSpeedingByMobile,
    generateSpeedingByMobile,
    
    commonGenerateContinuousSpeeding,

    commonStoreEventForSpeeding,
    commonStoreEventHistoryForSpeeding,
    commonStoreEventPositionHistoryForOBD,
    commonStoreEventPositionHistoryForMobile,
}