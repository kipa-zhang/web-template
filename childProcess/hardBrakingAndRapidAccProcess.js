const log = require('../log/winston').logger('HardBraking & RapidAcc Process');
const { QueryTypes, Op } = require('sequelize');
const moment = require('moment');
const util = require('../util/utils');

const CONTENT = require('../util/content');
const conf = require('../conf/conf');
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
    log.warn(`Message from parent(${moment().format('YYYY-MM-DD HH:mm:ss')}): `, JSON.stringify(deviceProcess))
    try {
        const deviceList = deviceProcess.deviceList
        const driverList = deviceProcess.driverList

        const initData = function (record) {
            let result = {
                flagOccTime,
                flagCount
            }

            result.flagOccTime = record ? record.lastOccTime : null
            result.flagCount = record ? record.count : 0

            return result
        }

        for (let device of deviceList) {
            let hardBrakingRecord = await Track.findOne({
                where: {
                    deviceId: device.deviceId,
                    violationType: CONTENT.ViolationType.HardBraking
                }
            })
            // This is the flag use to different current data from old data
            // let flagOccTime = hardBrakingRecord ? hardBrakingRecord.lastOccTime : null;
            // let flagCount = hardBrakingRecord ? hardBrakingRecord.count : 0;
            let result = initData(hardBrakingRecord)
            let timezone = [ device.startRecordTime, device.endRecordTime ]
            let list = await getDataFormDB({ deviceId: device.deviceId, flagOccTime: result.flagOccTime, timezone });

            await generateHardBraking({ deviceId: device.deviceId, list, flagCount: result.flagCount, dbRecord: hardBrakingRecord })
            let rapidAccRecord = await Track.findOne({
                where: {
                    deviceId: device.deviceId,
                    violationType: CONTENT.ViolationType.RapidAcc
                }
            })
            // This is the flag use to different current data from old data
            // let flagOccTime2 = rapidAccRecord ? rapidAccRecord.lastOccTime : null;
            // let flagCount2 = rapidAccRecord ? rapidAccRecord.count : 0;
            let result2 = initData(rapidAccRecord)
            let list2 = await getDataFormDB({ deviceId: device.deviceId, flagOccTime: result2.flagOccTime, timezone });
            await generateRapidAcc({ deviceId: device.deviceId, list: list2, flagCount: result2.flagCount, dbRecord: rapidAccRecord });
            await util.wait(10)
        }
        for (let driver of driverList) {
            let hardBrakingRecord = await Track.findOne({
                where: {
                    deviceId: driver.driverId,
                    vehicleNo: driver.vehicleNo,
                    violationType: CONTENT.ViolationType.HardBraking
                }
            })
            // This is the flag use to different current data from old data
            // let flagOccTime = hardBrakingRecord ? hardBrakingRecord.lastOccTime : null;
            // let flagCount = hardBrakingRecord ? hardBrakingRecord.count : 0;
            let result = initData(hardBrakingRecord)
            let timezone = [ driver.startRecordTime, driver.endRecordTime ]

            let list = await getDataFormDBByMobile({ driverId: driver.driverId, vehicleNo: driver.vehicleNo, flagOccTime: result.flagOccTime, timezone });
            
            await generateHardBrakingByMobile({ driverId: driver.driverId, vehicleNo: driver.vehicleNo, list, flagCount: result.flagCount, dbRecord: hardBrakingRecord })
            let rapidAccRecord = await Track.findOne({
                where: {
                    deviceId: driver.driverId,
                    vehicleNo: driver.vehicleNo,
                    violationType: CONTENT.ViolationType.RapidAcc
                }
            })
            // This is the flag use to different current data from old data
            // let flagOccTime2 = rapidAccRecord ? rapidAccRecord.lastOccTime : null;
            // let flagCount2 = rapidAccRecord ? rapidAccRecord.count : 0;
            let result2 = initData(rapidAccRecord)
            let list2 = await getDataFormDBByMobile({ driverId: driver.driverId, vehicleNo: driver.vehicleNo, flagOccTime: result2.flagOccTime, timezone })
            await generateRapidAccByMobile({ driverId: driver.driverId, vehicleNo: driver.vehicleNo, list: list2, flagCount: result2.flagCount, dbRecord: rapidAccRecord });
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
const getDataFormDB = async function (option) {
    return await sequelizeObj.query(`
        SELECT ROW_NUMBER() OVER( ORDER BY ph1.createdAt ASC) AS rowNo, ph1.* FROM device_position_history_backup AS ph1
        WHERE ph1.deviceId = '${ option.deviceId }'
        ${ option.flagOccTime ? ` AND ph1.createdAt >= '${ moment(option.flagOccTime).format('YYYY-MM-DD HH:mm:ss') }' ` : '' }
        AND ph1.createdAt >= '${ moment(option.timezone[0]).format('YYYY-MM-DD HH:mm:ss') }'
        AND ph1.createdAt <= '${ moment(option.timezone[1]).format('YYYY-MM-DD HH:mm:ss') }'
        ORDER BY ph1.createdAt ASC
    `, { type: QueryTypes.SELECT })
}
const generateHardBraking = async function (option) {
    if (!option.list.length) return;

    let tempHardBrakingList = commonGenerateHardBraking(option.list, option.deviceId, CONTENT.ViolationType.HardBraking)
    let hardBrakingList = commonGenerateContinuousList(tempHardBrakingList);
    
    // TODO: check if continuous with pre-offence
    let needMinusCount = false;
    if (option.dbRecord) {
        // TODO: check with last one record
        // log.info('```````````````````````````')
        // log.info(hardBrakingList[0].startTime)
        // log.info(option.dbRecord.endTime)
        // log.info(moment(hardBrakingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2)
        // log.info('```````````````````````````')
        if (hardBrakingList.length && moment(hardBrakingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2) {
            needMinusCount = true;

            hardBrakingList[0].occTime = option.dbRecord.occTime;
            hardBrakingList[0].startTime = option.dbRecord.startTime;
            hardBrakingList[0].startSpeed = option.dbRecord.startSpeed;
            let diffSecond = moment(hardBrakingList[0].endTime).diff(moment(hardBrakingList[0].startTime))/1000
            hardBrakingList[0].diffSecond = diffSecond
            if ([CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc].includes(hardBrakingList[0].violationType)) {
                if (CONTENT.ViolationType.HardBraking === hardBrakingList[0].violationType) {
                    hardBrakingList[0].diffSpeed = hardBrakingList[0].startSpeed - hardBrakingList[0].endSpeed
                    hardBrakingList[0].decSpeed = hardBrakingList[0].diffSpeed / diffSecond
                } else if (CONTENT.ViolationType.RapidAcc === hardBrakingList[0].violationType) {
                    hardBrakingList[0].diffSpeed = hardBrakingList[0].endSpeed - hardBrakingList[0].startSpeed
                    hardBrakingList[0].accSpeed = hardBrakingList[0].diffSpeed / diffSecond
                }
            }
        }
    }

    // TODO: update db 
    if (hardBrakingList.length) {
        await sequelizeObj.transaction(async transaction => {
            option.dataFrom = 'obd'
            option.needMinusCount = needMinusCount
            hardBrakingList = await commonFindOutTaskId(hardBrakingList, 'obd')
            await commonStoreEventForHardBraking(hardBrakingList, option);
            await commonStoreEventHistoryForHardBraking(hardBrakingList, 'obd');
            await commonStoreEventPositionHistoryForOBD(hardBrakingList);
        })
    }
}
const generateRapidAcc = async function (option) {
    if (!option.list.length) return;

    let tempRapidAccList = commonGenerateRapidAcc(option.list, option.deviceId, CONTENT.ViolationType.RapidAcc)
    let rapidAccList = commonGenerateContinuousList(tempRapidAccList)

    // TODO: check if continuous with pre-offence
    let needMinusCount = false;
    if (option.dbRecord) {
        // TODO: check with last one record
        // log.info('```````````````````````````')
        // log.info(rapidAccList[0].startTime)
        // log.info(option.dbRecord.endTime)
        // log.info(moment(rapidAccList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2)
        // log.info('```````````````````````````')
        if (rapidAccList.length && moment(rapidAccList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2) {
            needMinusCount = true;

            rapidAccList[0].occTime = option.dbRecord.occTime;
            rapidAccList[0].startTime = option.dbRecord.startTime;
            rapidAccList[0].startSpeed = option.dbRecord.startSpeed;
            let diffSecond = moment(rapidAccList[0].endTime).diff(moment(rapidAccList[0].startTime))/1000
            rapidAccList[0].diffSecond = diffSecond
            if ([CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc].includes(rapidAccList[0].violationType)) {
                if (CONTENT.ViolationType.HardBraking === rapidAccList[0].violationType) {
                    rapidAccList[0].diffSpeed = rapidAccList[0].startSpeed - rapidAccList[0].endSpeed
                    rapidAccList[0].decSpeed = rapidAccList[0].diffSpeed / diffSecond
                } else if (CONTENT.ViolationType.RapidAcc === rapidAccList[0].violationType) {
                    rapidAccList[0].diffSpeed = rapidAccList[0].endSpeed - rapidAccList[0].startSpeed
                    rapidAccList[0].accSpeed = rapidAccList[0].diffSpeed / diffSecond
                }
            }
        }
    }

    // TODO: update db 
    if (rapidAccList.length) {
        await sequelizeObj.transaction(async transaction => {
            option.dataFrom = 'obd'
            option.needMinusCount = needMinusCount
            rapidAccList = await commonFindOutTaskId(rapidAccList, 'obd')
            await commonStoreEventForRapidAcc(rapidAccList, option);
            await commonStoreEventHistoryForRapidAcc(rapidAccList, 'obd');
            await commonStoreEventPositionHistoryForOBD(rapidAccList);
        })   
    }
}

// ************************************************************
// Mobile
const getDataFormDBByMobile = async function (option) {
    return await sequelizeObj.query(`
        SELECT ROW_NUMBER() OVER( ORDER BY ph1.createdAt ASC) AS rowNo, ph1.* FROM driver_position_history_backup AS ph1
        WHERE ph1.driverId = '${ option.driverId }' AND ph1.vehicleNo = '${ option.vehicleNo }'
        ${ option.flagOccTime ? ` AND ph1.createdAt >= '${ moment(option.flagOccTime).format('YYYY-MM-DD HH:mm:ss') }' ` : '' }
        AND ph1.createdAt >= '${ moment(option.timezone[0]).format('YYYY-MM-DD HH:mm:ss') }'
        AND ph1.createdAt <= '${ moment(option.timezone[1]).format('YYYY-MM-DD HH:mm:ss') }'
        ORDER BY ph1.createdAt ASC
    `, { type: QueryTypes.SELECT })
}
const generateHardBrakingByMobile = async function (option) {
    if (!option.list.length) return;
    
    let tempHardBrakingList = commonGenerateHardBraking(option.list, option.driverId, CONTENT.ViolationType.HardBraking)
    let hardBrakingList = commonGenerateContinuousList(tempHardBrakingList)
    
    // TODO: check if continuous with pre-offence
    let needMinusCount = false;
    if (option.dbRecord) {
        // TODO: check with last one record
        // log.info('```````````````````````````')
        // log.info(hardBrakingList[0].startTime)
        // log.info(option.dbRecord.endTime)
        // log.info(moment(hardBrakingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2)
        // log.info('```````````````````````````')
        if (hardBrakingList.length && moment(hardBrakingList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2) {
            needMinusCount = true;

            hardBrakingList[0].occTime = option.dbRecord.occTime;
            hardBrakingList[0].startTime = option.dbRecord.startTime;
            hardBrakingList[0].startSpeed = option.dbRecord.startSpeed;
            let diffSecond = moment(hardBrakingList[0].endTime).diff(moment(hardBrakingList[0].startTime))/1000
            hardBrakingList[0].diffSecond = diffSecond
            if ([CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc].includes(hardBrakingList[0].violationType)) {
                if (CONTENT.ViolationType.HardBraking === hardBrakingList[0].violationType) {
                    hardBrakingList[0].diffSpeed = hardBrakingList[0].startSpeed - hardBrakingList[0].endSpeed
                    hardBrakingList[0].decSpeed = hardBrakingList[0].diffSpeed / diffSecond
                } else if (CONTENT.ViolationType.RapidAcc === hardBrakingList[0].violationType) {
                    hardBrakingList[0].diffSpeed = hardBrakingList[0].endSpeed - hardBrakingList[0].startSpeed
                    hardBrakingList[0].accSpeed = hardBrakingList[0].diffSpeed / diffSecond
                }
            }
        }
    }

    // TODO: update db 
    if (hardBrakingList.length) {
        // TODO： insert vehicleNo into data
        hardBrakingList = hardBrakingList.map(hardBraking => {
            hardBraking.vehicleNo = option.vehicleNo;
            return hardBraking
        })

        await sequelizeObj.transaction(async transaction => {
            option.dataFrom = 'mobile';
            option.needMinusCount = needMinusCount;
            hardBrakingList = await commonFindOutTaskId(hardBrakingList, 'mobile')
            await commonStoreEventForHardBraking(hardBrakingList, option);
            await commonStoreEventHistoryForHardBraking(hardBrakingList, 'mobile');
            await commonStoreEventPositionHistoryForMobile(hardBrakingList);
        })
    }
}
const generateRapidAccByMobile = async function (option) {
    if (!option.list.length) return;

    let tempRapidAccList = commonGenerateRapidAcc(option.list, option.driverId, CONTENT.ViolationType.RapidAcc)
    let rapidAccList = commonGenerateContinuousList(tempRapidAccList)

    // TODO: check if continuous with pre-offence
    let needMinusCount = false;
    if (option.dbRecord) {
        // TODO: check with last one record
        // log.info('```````````````````````````')
        // log.info(rapidAccList[0].startTime)
        // log.info(option.dbRecord.endTime)
        // log.info(moment(rapidAccList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2)
        // log.info('```````````````````````````')
        if (rapidAccList.length && moment(rapidAccList[0].startTime).diff(moment(option.dbRecord.endTime), 's') < 2) {
            needMinusCount = true;

            rapidAccList[0].occTime = option.dbRecord.occTime;
            rapidAccList[0].startTime = option.dbRecord.startTime;
            rapidAccList[0].startSpeed = option.dbRecord.startSpeed;
            let diffSecond = moment(rapidAccList[0].endTime).diff(moment(rapidAccList[0].startTime))/1000
            rapidAccList[0].diffSecond = diffSecond
            if ([CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc].includes(rapidAccList[0].violationType)) {
                if (CONTENT.ViolationType.HardBraking === rapidAccList[0].violationType) {
                    rapidAccList[0].diffSpeed = rapidAccList[0].startSpeed - rapidAccList[0].endSpeed
                    rapidAccList[0].decSpeed = rapidAccList[0].diffSpeed / diffSecond
                } else if (CONTENT.ViolationType.RapidAcc === rapidAccList[0].violationType) {
                    rapidAccList[0].diffSpeed = rapidAccList[0].endSpeed - rapidAccList[0].startSpeed
                    rapidAccList[0].accSpeed = rapidAccList[0].diffSpeed / diffSecond
                }
            }
        }
    }
    
    // TODO: update db 
    if (rapidAccList.length) {
        // TODO： insert vehicleNo into data
        rapidAccList = rapidAccList.map(rapidAcc => {
            rapidAcc.vehicleNo = option.vehicleNo;
            return rapidAcc
        })

        await sequelizeObj.transaction(async transaction => { 
            option.dataFrom = 'mobile'
            option.needMinusCount = needMinusCount
            rapidAccList = await commonFindOutTaskId(rapidAccList, 'mobile')
            await commonStoreEventForRapidAcc(rapidAccList, option);
            await commonStoreEventHistoryForRapidAcc(rapidAccList, 'mobile');
            await commonStoreEventPositionHistoryForMobile(rapidAccList);
        })
    }
}

// ************************************************************
// Common
const commonGenerateHardBraking = function (list, id, violationType) {
    // TODO: At least two record can cal descSpeed
    if (!list.length || list.length === 1) return [];

    let index = -1, preNode = null;
    let hardBrakingList = [];
    for (let data of list) {
        index++;
        if (index === 0) {
            continue;
        } else {
            preNode = list[index - 1]
        }

        if (preNode.speed > data.speed) {
            // In hardBraking
            // Check descSpeed
            let diffSpeed = preNode.speed - data.speed ;
            let diffSecond = Math.floor(moment(data.createdAt).diff(moment(preNode.createdAt)) / 1000);
            let decSpeed = diffSecond ? (diffSpeed / diffSecond) : 0
            if (decSpeed >= conf.HardBraking) {
                hardBrakingList.push({
                    rowNo: preNode.rowNo,
                    deviceId: id,
                    violationType,
                    speed: preNode.speed,
                    lat: preNode.lat,
                    lng: preNode.lng,
                    occTime: preNode.createdAt,
                    startSpeed: preNode.speed,
                    endSpeed: data.speed,
                    startTime: preNode.createdAt,
                    endTime: data.createdAt,
                })
            }
        } else {
            // Not in hardBraking
            continue;
        }
    }
    return hardBrakingList;
}
const commonGenerateRapidAcc = function (list, id, violationType) {
    // TODO: At least two record can cal descSpeed
    if (!list.length || list.length === 1) return [];

    let index = -1, preNode = null;
    let rapidAccList = [];
    for (let data of list) {
        index++;
        if (index === 0) {
            continue;
        } else {
            preNode = list[index - 1]
        }

        if (data.speed > preNode.speed) {
            // In hardBraking
            // Check descSpeed
            let diffSpeed = data.speed - preNode.speed;
            let diffSecond = Math.floor(moment(data.createdAt).diff(moment(preNode.createdAt)) / 1000);
            let ascSpeed = diffSecond ? (diffSpeed / diffSecond) : 0
            if (ascSpeed >= conf.RapicAcc) {
                rapidAccList.push({
                    rowNo: preNode.rowNo,
                    deviceId: id,
                    violationType,
                    speed: preNode.speed,
                    lat: preNode.lat,
                    lng: preNode.lng,
                    occTime: preNode.createdAt,
                    startSpeed: preNode.speed,
                    endSpeed: data.speed,
                    startTime: preNode.createdAt,
                    endTime: data.createdAt,
                })
            }
        } else {
            // Not in hardBraking
            continue;
        }
    }
    return rapidAccList;
}
const commonGenerateContinuousList = function (list) {
    // TODO: find continuous decSpeed and calculate as single one
    let result = [];
    if (list.length === 1) {
        // Only one record
        let currentNode = list[0]
        result.push({
            deviceId: currentNode.deviceId,
            violationType: currentNode.violationType,
            speed: currentNode.speed,
            lat: currentNode.lat,
            lng: currentNode.lng,
            occTime: currentNode.occTime,
            startSpeed: currentNode.startSpeed, 
            endSpeed: currentNode.endSpeed, 
            startTime: currentNode.startTime, 
            endTime: currentNode.endTime,
        })
        return result
    } 
    // More than one record
    let index = -1, startNode = null;
    for (let data of list) {
        index++;
        if (index === 0) {
            // First record, just store in startNode
            startNode = data;
            continue;
        } else if (data.rowNo - 1 === list[index - 1].rowNo) {
            // Check if continuous rowNo record
            // This is continuous node
            // Check if last record
            if (index === list.length - 1) {
                // Last one record
                result.push({
                    deviceId: data.deviceId,
                    violationType: data.violationType,
                    speed: data.speed,
                    lat: data.lat,
                    lng: data.lng,
                    occTime: startNode.occTime,
                    startSpeed: startNode.startSpeed, 
                    startTime: startNode.startTime, 
                    endSpeed: data.endSpeed, 
                    endTime: data.endTime,
                })
            } else {
                // Not last one record
                continue;
            }
        } else {
            // This is not continuous node
            result.push({
                deviceId: data.deviceId,
                violationType: data.violationType,
                speed: data.speed,
                lat: data.lat,
                lng: data.lng,
                occTime: startNode.occTime,
                startSpeed: startNode.startSpeed, 
                endSpeed: list[index - 1].endSpeed, 
                startTime: startNode.startTime, 
                endTime: list[index - 1].endTime,
            })
            // Check if last record
            if (index === list.length - 1) {
                // Last one record
                result.push({
                    deviceId: data.deviceId,
                    violationType: data.violationType,
                    speed: data.speed,
                    lat: data.lat,
                    lng: data.lng,
                    occTime: data.occTime,
                    startSpeed: data.startSpeed, 
                    endSpeed: data.endSpeed, 
                    startTime: data.startTime, 
                    endTime: data.endTime,
                })
            } else {
                // Not last one record
                startNode = data;
                continue;
            }
        }
    }
    return result;
}
const commonStoreEventForHardBraking = async function (list, option) {
    let latestRecord = list[list.length - 1]

    let count = list.length + option.flagCount; 
    if (option.needMinusCount) count--;

    let diffSecond = moment(latestRecord.endTime).diff(moment(latestRecord.startTime)) / 1000
    let diffSpeed = latestRecord.startSpeed - latestRecord.endSpeed
    let decSpeed = diffSpeed / diffSecond
    if (Number.isNaN(decSpeed) || decSpeed === Infinity || decSpeed === -Infinity) {
        decSpeed = 0;
    }

    let result = await Track.findOne({ 
        where: {
            deviceId: latestRecord.deviceId,
            violationType: latestRecord.violationType,
            vehicleNo: latestRecord.vehicleNo ?? null,
        }
    })
    if (result) {
        await result.update({
            count: count, 
            dataFrom: option.dataFrom,
            occTime: latestRecord.startTime, 
            lastOccTime: latestRecord.endTime, 
            speed: latestRecord.speed, 
            lat: latestRecord.lat, 
            lng: latestRecord.lng, 
            decSpeed, 
            diffSecond, 
            diffSpeed, 
            accSpeed: 0, 
            startSpeed: latestRecord.startSpeed, 
            startTime: latestRecord.startTime, 
            endSpeed: latestRecord.endSpeed, 
            endTime: latestRecord.endTime, 
        })
    } else {
        await Track.create({ 
            deviceId: latestRecord.deviceId, 
            count: count, 
            vehicleNo: latestRecord.vehicleNo, 
            violationType: latestRecord.violationType, 
            dataFrom: option.dataFrom,
            occTime: latestRecord.startTime, 
            lastOccTime: latestRecord.endTime, 
            speed: latestRecord.speed, 
            lat: latestRecord.lat, 
            lng: latestRecord.lng, 
            decSpeed, 
            diffSecond, 
            diffSpeed, 
            accSpeed: 0, 
            startSpeed: latestRecord.startSpeed, 
            startTime: latestRecord.startTime, 
            endSpeed: latestRecord.endSpeed, 
            endTime: latestRecord.endTime, 
        })
    }
    
}
const commonStoreEventForRapidAcc = async function (list, option) {
    let latestRecord = list[list.length - 1]

    let count = list.length + option.flagCount; 
    if (option.needMinusCount) count--;

    let diffSecond = moment(latestRecord.endTime).diff(moment(latestRecord.startTime)) / 1000
    let diffSpeed = latestRecord.endSpeed - latestRecord.startSpeed
    let accSpeed = diffSpeed / diffSecond
    if (Number.isNaN(accSpeed) || accSpeed === Infinity || accSpeed === -Infinity) {
        accSpeed = 0;
    }

    let result = await Track.findOne({ 
        where: {
            deviceId: latestRecord.deviceId,
            violationType: latestRecord.violationType,
            vehicleNo: latestRecord.vehicleNo ?? null,
        }
    })
    if (result) {
        await result.update({
            count: count, 
            dataFrom: option.dataFrom,
            occTime: latestRecord.startTime, 
            lastOccTime: latestRecord.endTime, 
            speed: latestRecord.speed, 
            lat: latestRecord.lat, 
            lng: latestRecord.lng, 
            decSpeed: 0, 
            diffSecond, 
            diffSpeed, 
            accSpeed, 
            startSpeed: latestRecord.startSpeed, 
            startTime: latestRecord.startTime, 
            endSpeed: latestRecord.endSpeed, 
            endTime: latestRecord.endTime, 
        })
    } else {
        await Track.create({ 
            deviceId: latestRecord.deviceId, 
            count: count, 
            vehicleNo: latestRecord.vehicleNo, 
            violationType: latestRecord.violationType, 
            dataFrom: option.dataFrom,
            occTime: latestRecord.startTime, 
            lastOccTime: latestRecord.endTime, 
            speed: latestRecord.speed, 
            lat: latestRecord.lat, 
            lng: latestRecord.lng, 
            decSpeed: 0, 
            diffSecond, 
            diffSpeed, 
            accSpeed, 
            startSpeed: latestRecord.startSpeed, 
            startTime: latestRecord.startTime, 
            endSpeed: latestRecord.endSpeed, 
            endTime: latestRecord.endTime, 
        })
    }
    

}
const commonStoreEventHistoryForHardBraking = async function (list, from) {
    try {
        let records = []
        for (let data of list) {
            data.dataFrom = from;
            data.diffSecond = moment(data.endTime).diff(moment(data.startTime)) / 1000
            data.diffSpeed = data.startSpeed - data.endSpeed
            data.decSpeed = data.diffSpeed / data.diffSecond
            if (Number.isNaN(data.decSpeed) || data.decSpeed === Infinity || data.decSpeed === -Infinity || !data.decSpeed) {
                data.decSpeed = 0;
            }
            records.push(data)
        }
        await TrackHistory.bulkCreate(records, { updateOnDuplicate: ['lat', 'lng'] });
        log.info(`commonStoreEventHistoryForHardBraking: => ${ JSON.stringify(records, null, 4) }`)
    } catch (error) {
        log.error('commonStoreEventHistoryForHardBraking: ', error)
    }
}
const commonStoreEventHistoryForRapidAcc = async function (list, from) {
    try {
        let records = []
        for (let data of list) {
            data.dataFrom = from;
            data.diffSecond = moment(data.endTime).diff(moment(data.startTime))/1000
            data.diffSpeed = data.endSpeed - data.startSpeed
            data.accSpeed = data.diffSpeed / data.diffSecond
            if (Number.isNaN(data.accSpeed) || data.accSpeed === Infinity || data.accSpeed === -Infinity || !data.accSpeed) {
                data.accSpeed = 0;
            }
            records.push(data)
        }
        await TrackHistory.bulkCreate(records, { updateOnDuplicate: ['lat', 'lng'] });
        log.info(`commonStoreEventHistoryForRapidAcc: => ${ JSON.stringify(records, null, 4) }`)
    } catch (error) {
        log.error('commonStoreEventHistoryForRapidAcc: ', error)
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
            log.warn(`Find position record for offence history => deviceId: ${ data.deviceId } total (${ targetOffenceHistoryList.length }) count`)
            for (let target of targetOffenceHistoryList) {
                if (idSet.has(target.id)) continue;
                else {
                    idSet.add(target.id)
                    records.push({ 
                        id: target.id, 
                        deviceId: target.deviceId, 
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
    getDataFormDB,
    generateHardBraking,
    generateRapidAcc,

    getDataFormDBByMobile,
    generateHardBrakingByMobile,
    generateRapidAccByMobile,
    
    commonGenerateHardBraking,
    commonGenerateRapidAcc,
    commonGenerateContinuousList,

    commonStoreEventForHardBraking,
    commonStoreEventForRapidAcc,
    commonStoreEventHistoryForHardBraking,
    commonStoreEventHistoryForRapidAcc,
    commonStoreEventPositionHistoryForOBD,
    commonStoreEventPositionHistoryForMobile,
}