const log = require('../log/winston').logger('Missing Process');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const moment = require('moment');

const { Vehicle } = require('../model/vehicle.js');
const { DriverPosition } = require('../model/driverPosition');

const { Track } = require('../model/event/track');
const { TrackHistory } = require('../model/event/trackHistory.js');

const { TO_Operation } = require('../model/toOperation');

const outputService = require('../services/outputService');

process.on('message', async deviceProcess => {
    // { deviceList: [], driverList: [] }
    log.info(`Message from parent(${moment().format('YYYY-MM-DD HH:mm:ss')}): `)
    log.info(JSON.stringify(deviceProcess, null, 4))
    try {
        const deviceList = deviceProcess.deviceList
        const driverList = deviceProcess.driverList
        
        await updateOBDMissing(deviceList)
        await updateMobileMissing(driverList)

        process.send({ success: true })
        process.exit(0)
    } catch (error) {
        log.error(error);
        process.send({ success: false, error })
    }
})

const updateOBDMissing = async function (targetList) {
    // 1. > judgeMissingTime
    if (!targetList.length) {
        log.info(`updateOBDMissing => no data.`)
        return
    } else {
        log.info(`updateOBDMissing`, JSON.stringify(targetList, null, 4))
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
            DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
            FROM task
            WHERE vehicleNumber = '${ vehicle.vehicleNo }'
            AND '${ target.createdAt }' >= mobileStartTime
            AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )
            AND driverId IS NOT NULL

            UNION

            SELECT dutyId as taskId, 'SYSTEM' AS dataFrom, driverId, vehicleNo, vehicleNo AS vehicleNumber,
            DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
            DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
            FROM urgent_duty
            WHERE vehicleNo = '${ vehicle.vehicleNo }'
            AND '${ target.createdAt }' >= mobileStartTime
            AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )
            AND driverId IS NOT NULL
        `, { type: QueryTypes.SELECT })
        // Calculate every task's missing record

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
            let missingList = await commonGenerateMissing(deviceGPSList, target.deviceId, null)
            
            await analysisMissing(missingList)
        }
    }
}
const analysisMissing = async function () {
    if (missingList.length) {
        missingList = missingList.map(item => {
            item.taskId = task.taskId
            item.vehicleNo = task.vehicleNo
            return item
        })
        log.warn(`Missing List => `, JSON.stringify(missingList, null, 4))   
        await sequelizeObj.transaction(async transaction => {
            let track = await Track.findOne({ where: { deviceId: target.deviceId, violationType: CONTENT.ViolationType.Missing } })
            let result = await commonStoreEventHistoryForMissing(missingList, 'obd');
            log.info(JSON.stringify(result, null, 4))

            // flagCount is not correct here, need think about if exist same data(from result above)
            let flagCount = track ? track.count : 0
            await commonStoreEventForMissing(missingList, { flagCount, dataFrom: 'obd' });
        })
    } else {
        log.info(`updateOBDMissing => DeviceId: ${ target.deviceId }(VehicleNo: ${ vehicle.vehicleNo }) has no missing record on taskId: ${ task.taskId }.(mobileStartTime: ${ task.mobileStartTime }, mobileEndTime: ${ task.mobileEndTime })`)
    }
}

const updateMobileMissing = async function (targetList) {
    // 1. > judgeMissingTime
    // 2. No NetWork or No GPS Permission
    try {
        if (!targetList.length) {
            log.info(`updateMobileMissing => no data.`)
            return
        } else {
            log.info(`updateMobileMissing`, JSON.stringify(targetList, null, 4))
        }

        for (let target of targetList) {
            // Find out task by date (not loan task, so need vehicleNumber & driverId while search task)
            let taskList = await sequelizeObj.query(`
                SELECT taskId, dataFrom, driverId, vehicleNumber, vehicleNumber AS vehicleNo, 
                DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
                FROM task
                WHERE driverId = ${ target.driverId }
                AND vehicleNumber = '${ target.vehicleNo }'
                AND '${ target.createdAt }' >= mobileStartTime
                AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )

                UNION

                SELECT dutyId as taskId, 'SYSTEM' AS dataFrom, driverId, vehicleNo, vehicleNo AS vehicleNumber,
                DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
                FROM urgent_duty
                WHERE driverId = ${ target.driverId }
                AND vehicleNo = '${ target.vehicleNo }'
                AND '${ target.createdAt }' >= mobileStartTime
                AND (mobileEndTime IS NULL OR '${ target.createdAt }' <= mobileEndTime )
            `, { type: QueryTypes.SELECT })

            // Calculate every task's missing record
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
                let missingList = await commonGenerateMissing(driverGPSList, task.driverId, task.vehicleNo)

                if (missingList.length) {
                    missingList = missingList.map(item => {
                        item.taskId = task.taskId
                        item.vehicleNo = task.vehicleNo
                        return item
                    })
                    log.warn(`Missing List => `, JSON.stringify(missingList, null, 4))
                    await sequelizeObj.transaction(async transaction => {
                        let track = await Track.findOne({ where: { deviceId: task.driverId, vehicleNo: task.vehicleNo, violationType: CONTENT.ViolationType.Missing } })
                        let result = await commonStoreEventHistoryForMissing(missingList, 'mobile');
                        log.info(JSON.stringify(result, null, 4))

                        // flagCount is not correct here, need think about if exist same data(from result above)
                        let flagCount = track ? track.count : 0
                        await commonStoreEventForMissing(missingList, { flagCount, dataFrom: 'mobile' });
                    })
                } else {
                    log.info(`updateMobileMissing => DriverId: ${ target.driverId }(VehicleNo: ${ target.vehicleNo }) has no missing record on taskId: ${ task.taskId }.(mobileStartTime: ${ task.mobileStartTime }, mobileEndTime: ${ task.mobileEndTime })`)
                }
            }

        }

    } catch (error) {
        log.error(`updateMobileMissingByChildProcess => `, error)
    }
}

const commonGenerateMissing = async function (list, id, vehicleNo) {
    // TODO: At least two record can cal descSpeed
    if (!list.length || list.length === 1) return [];
    let idleList = [];
    let index = -1;

    let noSignalStartIndex = -1;
    for (let data of list) {
        index++;

        if (index !== list.length - 1) {
            let timezone = moment(list[index + 1].createdAt).diff(moment(data.createdAt));

            if (timezone > conf.judgeMissingTime) {
                if (vehicleNo) {
                    // Mobile
                    let checkResult = await checkMissingByTimeZone([data.createdAt, list[index + 1].createdAt], id, vehicleNo)
                    if (checkResult.result) {
                        idleList.push({ 
                            deviceId: id, 
                            violationType: CONTENT.ViolationType.Missing, 
                            missingType: checkResult.reason,
                            startTime: data.createdAt, 
                            endTime: list[index + 1].createdAt, 
                            speed: data.speed, 
                            vin: data.vin, 
                            lat: data.lat, 
                            lng: data.lng, 
                            occTime: data.createdAt, 
                            stayTime: Math.floor(timezone / 1000) 
                        })
                    }
                } else {
                    // OBD
                    idleList.push({ 
                        deviceId: id, 
                        violationType: CONTENT.ViolationType.Missing, 
                        startTime: data.createdAt, 
                        endTime: list[index + 1].createdAt, 
                        speed: data.speed, 
                        vin: data.vin, 
                        lat: data.lat, 
                        lng: data.lng, 
                        occTime: data.createdAt, 
                        stayTime: Math.floor(timezone / 1000) 
                    })
                } 
            }

            const generateData = function () {
                // Checkout Missing Type => No Signal(GPS Time is same)
                if (list[index + 1].gpsTime == data.gpsTime) {
                    if (noSignalStartIndex == -1) {
                        // TODO: find out start record
                        noSignalStartIndex = index
                    } else {
                        // TODO: go on find out end record
                    }
                } else {
                    if (noSignalStartIndex == -1) {
                        // TODO: find out start record
                    } else {
                        // TODO: find out end record
                        // Check time
                        let timezone = moment(data.createdAt).diff(moment(list[noSignalStartIndex].createdAt));
                        if (timezone > conf.judgeMissingTime) {
                            idleList.push({ 
                                deviceId: id, 
                                violationType: CONTENT.ViolationType.Missing, 
                                startTime: list[noSignalStartIndex].createdAt, 
                                missingType: list[noSignalStartIndex + 1].gpsService == 1 ? 'No GPS Signal' : 'No GPS Service',
                                endTime: data.createdAt, 
                                speed: list[noSignalStartIndex].speed, 
                                vin: list[noSignalStartIndex].vin, 
                                lat: list[noSignalStartIndex].lat, 
                                lng: list[noSignalStartIndex].lng, 
                                occTime: list[noSignalStartIndex].createdAt, 
                                stayTime: Math.floor((moment(data.createdAt).diff(moment(list[noSignalStartIndex].createdAt))) / 1000) 
                            })
                        } else {
                            // TODO: leave this record, find next start node
                        }

                        // TODO: reload flag
                        noSignalStartIndex = -1;
                    }
                }
            }
            generateData()
        }    
    }
    return idleList;
}
const commonStoreEventForMissing = async function (list, option) {
    let latestIdle = list[list.length - 1];

    let count = list.length + option.flagCount; 
    if (option.needMinusCount) count--;

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
const commonStoreEventHistoryForMissing = async function (list, from) {
    try {
        let records = [];
        for (let data of list) {
            data.dataFrom = from
            data.diffSecond = moment(data.endTime).diff(moment(data.startTime)) / 1000
            records.push(data)
        }
        return await TrackHistory.bulkCreate(records, { updateOnDuplicate: ['lat', 'lng'] });
    } catch (error) {
        log.error(error)
    }
}

// TODO: Check effective in timezone
// While pause, return { result: true, reason: 'Pause' }
// While no permission, return { result: true, reason: 'No GPS Permission' }
// Default return { result: true, reason: 'Missing' }
const checkMissingByTimeZone = async function (timezone, driverId, vehicleNo) {
    let driverPosition = await DriverPosition.findOne({ where: { driverId, vehicleNo } })
    if (!driverPosition) {
        log.warn(`(checkMissingByTimeZone)No driver position record here => driverId: ${ driverId }, vehicleNo: ${ vehicleNo }`)
        return { result: false };
    }

    if (!timezone || timezone.length < 2) {
        log.info(`(checkMissingByTimeZone) timezone is not correct => ${ JSON.stringify(timezone) }`)
        return { result: false };
    }
    
    // Calculate by timezone
    let toOperation = await TO_Operation.findAll({ where: { driverId, startTime: { [Op.startsWith]: moment(timezone[0]).format('YYYY-MM-DD') } }})
    for (let operation of toOperation) {
        // 1. Missing Type => Pause 
        if (operation.type == 1 
            && operation.description.toLowerCase().indexOf('pause') > -1 
            && operation.endTime
            && moment(timezone[0]).isSameOrAfter(moment(operation.startTime).subtract(5, 's')) 
            && moment(timezone[1]).isSameOrBefore(moment(operation.endTime).add(5, 's'))
        ) {
            return { result: false, reason: 'Pause' }
        }

        // 2. Missing Type => No GPS Permission 
        if (operation.type == 0 
            && operation.description.toLowerCase().indexOf('permission') > -1 
            && operation.endTime
            && moment(timezone[0]).isSameOrAfter(moment(operation.startTime).subtract(5, 's'))
            && moment(timezone[1]).isSameOrBefore(moment(operation.endTime).add(5, 's'))
        ) {
            // TODO: mobile will upload position every 5 seconds, sometime seconds is not correct
            return { result: true, reason: 'No GPS Permission' }
        }
    }

    return { result: true, reason: 'Network' }
}