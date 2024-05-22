const log = require('../log/winston').logger('Track Service');
const conf = require('../conf/conf');
const util = require('../util/utils');

const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const moment = require('moment');
const { fork } = require('child_process')

const { Device } = require('../model/device');
const { DriverPosition } = require('../model/driverPosition');
const { DevicePositionHistory, DevicePositionHistoryBackup } = require('../model/event/devicePositionHistory');
const { DriverPositionHistory, DriverPositionHistoryBackup } = require('../model/event/driverPositionHistory');

const outputService = require('./outputService');
const missingService = require('../services/missingService');
const noGoZoneAlertService = require('../services/noGoZoneAlertService');

const prepareBaseData = async function () {
    const checkRecordWithSameCreatedTime = async function () {
        try {
            // Attention: Only need check OBD
            // Attention: Only need check OBD
            // Attention: Only need check OBD

            log.warn(`checkRecordWithSameCreatedTime => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
            
            let continueCheckSameCreatedTime = true;
            // use to record how many times run while
            let index = 0;
            // record how many data affected
            let count = 0;
            // use while here to ensure there are no same createdTime
            while (continueCheckSameCreatedTime) {
                index++;
                log.warn(`checkRecordWithSameCreatedTime => index: ${ index } `)
                let result1 = await sequelizeObj.query(`
                    SELECT tt.idList FROM (
                        SELECT COUNT(*) AS \`count\`, GROUP_CONCAT(id) AS idList 
                        FROM device_position_history_backup
                        GROUP BY deviceId, createdAt
                    ) tt
                    WHERE tt.count > 1
                `, { type: QueryTypes.SELECT })
                if (result1 && result1.length) {
                    // find out all id
                    let list = []
                    for (let data of result1) {
                        let idList = data.idList.split(',')
                        if (idList.length > 1) {
                            // keep last one record
                            list = list.concat(idList.slice(0, idList.length -1))
                        }
                    }
                    // record how many data affected
                    count += list.length
    
                    // delete 
                    // maybe more than 1000 record, so use while
                    await sequelizeObj.query(`
                        DELETE FROM device_position_history_backup WHERE id IN (?)
                    `, { type: QueryTypes.DELETE, replacements: [ list ] })
                } else {
                    // do not exist same createdAt record, finish while
                    continueCheckSameCreatedTime = false
                }
            }
    
            log.warn(`checkRecordWithSameCreatedTime affect data count => ${ count }`);
            log.warn(`checkRecordWithSameCreatedTime => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        } catch (error) {
            throw error;
        }
    }
    const destroyRecordWithNullGPS = async function () {
        try {
            log.warn(`destroyRecordWithNullGPS => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
    
            await sequelizeObj.query(`
                DELETE FROM device_position_history_backup WHERE lat IS NULL or lng IS NULL;
            `, { type: QueryTypes.DELETE })
    
            await sequelizeObj.query(`
                DELETE FROM driver_position_history_backup WHERE lat IS NULL or lng IS NULL;
            `, { type: QueryTypes.DELETE })
    
            log.warn(`destroyRecordWithNullGPS => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        } catch (error) {
            throw error;
        }
    }
    const destroyRecordTemp = async function () {
        try {
            log.warn(`destroyRecordTemp => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
            
            // clear temp data
            await DevicePositionHistoryBackup.destroy({ where: { deviceId: '0' } }); 
            await DriverPositionHistoryBackup.destroy({ where: { driverId: 0 } });

            log.warn(`destroyRecordTemp => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        } catch (error) {
            throw error
        }
    }
    const destroyRecordWithNullCreatedTime = async function () {
        try {
            log.warn(`destroyRecordWithNullCreatedTime => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)

            let result1 = await DevicePositionHistoryBackup.destroy({ where: { createdAt: { [Op.is]: null } } }); // OBD use system time
            let result2 = await DriverPositionHistoryBackup.destroy({ where: { createdAt: { [Op.is]: null } } }); // Impossible 
            log.warn(`DevicePositionHistory(createdAt is null) affect data count => ${ result1 }`);
            log.warn(`DriverPositionHistory(createdAt is null) affect data count => ${ result2 }`);

            log.warn(`destroyRecordWithNullCreatedTime => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        } catch (error) {
            throw error
        }
    }

    try {
        log.info(`prepareBaseData(${ moment().format('YYYY-MM-DD HH:mm:ss') })  start.`);
        
        await destroyRecordTemp();
        await destroyRecordWithNullCreatedTime();
        await checkRecordWithSameCreatedTime();
        await destroyRecordWithNullGPS();

        log.info(`prepareBaseData(${ moment().format('YYYY-MM-DD HH:mm:ss') }) end.`);
    } catch (error) {
        throw error
    }
}

const prepareData = async function () {
    // deviceList 
    const deviceList = await sequelizeObj.query(`
    SELECT d.deviceId, v.vehicleNo, dp.createdAt, dp.createdAt AS startRecordTime, 
    IFNULL(v.limitSpeed, 60) AS limitSpeed, 
    IF(DATE_ADD(dp.createdAt, INTERVAL ${ conf.Calculate_TimeZone } MINUTE) < d.updatedAt, 
        DATE_ADD(dp.createdAt, INTERVAL ${ conf.Calculate_TimeZone } MINUTE), 
            d.updatedAt) AS endRecordTime
    FROM device d
    LEFT JOIN (
        SELECT vehicleNo, deviceId, limitSpeed
        FROM vehicle

        UNION 

        SELECT vehicleNo, deviceId, limitSpeed
        FROM vehicle_history
    ) v ON v.deviceId = d.deviceId
    LEFT JOIN (
        SELECT deviceId, createdAt, id 
        FROM device_position_history_backup
        GROUP BY deviceId
    ) dp ON dp.deviceId = d.deviceId 
    WHERE dp.id IS NOT NULL 
    `, { type: QueryTypes.SELECT })
    log.info(`updateTrackDashboardInfoByChildProcess deviceList => find out ${ deviceList.length } `)
    log.info(`updateTrackDashboardInfoByChildProcess deviceList => detail ${ JSON.stringify(deviceList, null, 4) } `)


    // driverList
    const driverList = await sequelizeObj.query(`
        SELECT d.driverId, v.vehicleNo, dp.createdAt, d.vehicleNo, dp.createdAt AS startRecordTime, 
        IFNULL(v.limitSpeed, 60) AS limitSpeed, 
        IF(DATE_ADD(dp.createdAt, INTERVAL ${ conf.Calculate_TimeZone } MINUTE) < d.updatedAt, 
            DATE_ADD(dp.createdAt, INTERVAL ${ conf.Calculate_TimeZone } MINUTE), 
                d.updatedAt) AS endRecordTime
        FROM driver_position d
        LEFT JOIN (
            SELECT vehicleNo, deviceId, limitSpeed
            FROM vehicle

            UNION 

            SELECT vehicleNo, deviceId, limitSpeed
            FROM vehicle_history
        ) v ON v.vehicleNo = d.vehicleNo
        LEFT JOIN (
            SELECT driverId, vehicleNo, createdAt, id 
            FROM driver_position_history_backup
            GROUP BY driverId, vehicleNo
        ) dp ON dp.driverId = d.driverId AND dp.vehicleNo = d.vehicleNo
        WHERE dp.id IS NOT NULL 
    `, { type: QueryTypes.SELECT })

    log.info(`updateTrackDashboardInfoByChildProcess driverList => find out ${ driverList.length } `)
    log.info(`updateTrackDashboardInfoByChildProcess driverList => detail ${ JSON.stringify(driverList, null, 4) } `)


    for (let device of deviceList) {
        device.createdAt = moment(device.createdAt).format('YYYY-MM-DD HH:mm:ss')
        device.startRecordTime = moment(device.startRecordTime).format('YYYY-MM-DD HH:mm:ss')
        device.endRecordTime = moment(device.endRecordTime).format('YYYY-MM-DD HH:mm:ss')
    }
    for (let driver of driverList) {
        driver.createdAt = moment(driver.createdAt).format('YYYY-MM-DD HH:mm:ss')
        driver.startRecordTime = moment(driver.startRecordTime).format('YYYY-MM-DD HH:mm:ss')
        driver.endRecordTime = moment(driver.endRecordTime).format('YYYY-MM-DD HH:mm:ss')
    }

    return { deviceList, driverList }   
}

const OutputDataList = async function (deviceList, driverList) {
    try {
        log.warn(`OutputDataList => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        log.warn(`OutputDataList => deviceList.length ${ deviceList.length } `)
        log.warn(`OutputDataList => driverList.length ${ driverList.length } `)
        for (let device of deviceList) {
            let list = await DevicePositionHistoryBackup.findAll({ 
                where: { 
                    deviceId: device.deviceId, 
                    createdAt: {
                        [Op.between]: [ 
                            moment(device.startRecordTime).format('YYYY-MM-DD HH:mm:ss'), 
                            moment(device.endRecordTime).format('YYYY-MM-DD HH:mm:ss') 
                        ] 
                    } 
                } 
            })

            log.warn(`OutputDataList => deviceId: ${ device.deviceId } (${ list.length } records) `)	
            // While no record, continue
            if (!list.length) {
                continue
            }
            await outputService.writeIntoFile(list, device.deviceId)
        } 
        for (let driver of driverList) {
            let list = await DriverPositionHistoryBackup.findAll({ 
                where: { 
                    driverId: driver.driverId, 
                    vehicleNo: driver.vehicleNo,
                    createdAt: { 
                        [Op.between]: [ 
                            moment(driver.startRecordTime).format('YYYY-MM-DD HH:mm:ss'), 
                            moment(driver.endRecordTime).format('YYYY-MM-DD HH:mm:ss') 
                        ] 
                    } 
                } 
            })

            log.warn(`OutputDataList => driverId: ${ driver.driverId } (${ list.length } records) `)
            // While no record, continue
            if (!list.length) {
                continue
            }
            await outputService.writeIntoFile(list, driver.driverId)
        }
        log.warn(`OutputDataList => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
    } catch (error) {
        throw error;
    }
}

const calculateBaseOffenceList = async function (deviceList, driverList) {
    try {
        log.info(`(calculateOffenceList ${ moment().format('YYYY-MM-DD HH:mm:ss') } ): start update hardBraking & rapidAcc & speeding!`);

        // Separate list into conf.Calculate_Block every block
        let deviceBlock = Math.floor(deviceList.length / conf.Calculate_Block) + 1;
        let driverBlock = Math.floor(driverList.length / conf.Calculate_Block) + 1;

        log.warn(`deviceBlock length => ${ deviceBlock } `);
        log.warn(`driverBlock length => ${ driverBlock } `);

        let deviceFork = {}
        if (deviceList.length) {
            for (let block = 0; block < deviceBlock; block++) {
                log.warn(`Start deviceFork => block: ${ block }`);
                deviceFork[block] = { hr: 0, sp: 0, miss: 0 }
    
                // New Child Process
                const hardBrakingAndRapidAccForked = fork('./childProcess/hardBrakingAndRapidAccProcess.js')
                const speedingForked = fork('./childProcess/speedingProcess.js')
                // const missingForked = fork('./childProcess/missingProcess.js')
    
                hardBrakingAndRapidAccForked.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block }) `, JSON.stringify(msg));
                    if (msg.success) {
                        deviceFork[block].hr = 1; // hr: hardBraking
                        // if (deviceFork[block].hr && deviceFork[block].sp && deviceFork[block].miss) {
                        if (deviceFork[block].hr && deviceFork[block].sp) {
                            // delete record
                            await deleteRecord(deviceList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), [])
                            log.warn(`End deviceFork => block: ${ block }`);
                        } else {
                            log.warn(`waiting for another fork...(Block => ${ block })`);
                        }
                    }
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // hardBrakingAndRapidAccForked.disconnect();
                })
                hardBrakingAndRapidAccForked.send({ deviceList: deviceList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), driverList: [] })
            
                speedingForked.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block }) `, JSON.stringify(msg));
                    if (msg.success) {
                        deviceFork[block].sp = 1; // sp: speeding
                        // if (deviceFork[block].hr && deviceFork[block].sp && deviceFork[block].miss) {
                        if (deviceFork[block].hr && deviceFork[block].sp) {
                            // delete record
                            await deleteRecord(deviceList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), [])
                            log.warn(`End deviceFork => block: ${ block }`);
                        } else {
                            log.warn(`waiting for another fork...(Block => ${ block })`);
                        }
                    }
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // speedingForked.disconnect();
                })
                speedingForked.send({ deviceList: deviceList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), driverList: [] })
            }
        }

        let driverFork = {}
        if (driverList.length) {
            for (let block = 0; block < driverBlock; block++) {
                log.warn(`Start driverFork => block: ${ block }`);
                driverFork[block] = { hr: 0, sp: 0, miss: 0 }
    
                // New Child Process
                const hardBrakingAndRapidAccForked = fork('./childProcess/hardBrakingAndRapidAccProcess.js')
                const speedingForked = fork('./childProcess/speedingProcess.js')
                // const missingForked = fork('./childProcess/missingProcess.js')
    
                hardBrakingAndRapidAccForked.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block })`, JSON.stringify(msg));
                    if (msg.success) {
                        driverFork[block].hr = 1; // hr: hardBraking
                        // if (driverFork[block].hr && driverFork[block].sp && driverFork[block].miss) {
                        if (driverFork[block].hr && driverFork[block].sp) {
                            // delete record
                            await deleteRecord([], driverList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block))
                            log.warn(`End driverFork => block: ${ block }`);
                        } else {
                            log.warn(`waiting for another fork...(Block => ${ block })`);
                        }
                    }
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // hardBrakingAndRapidAccForked.disconnect();
                })
                hardBrakingAndRapidAccForked.send({ deviceList: [], driverList: driverList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block) })
            
                speedingForked.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block })`, JSON.stringify(msg));
                    if (msg.success) {
                        driverFork[block].sp = 1; // sp: speeding
                        // if (driverFork[block].hr && driverFork[block].sp && driverFork[block].miss) {
                        if (driverFork[block].hr && driverFork[block].sp) {
                            // delete record
                            await deleteRecord([], driverList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block))
                            log.warn(`End driverFork => block: ${ block }`);
                        } else {
                            log.warn(`waiting for another fork...(Block => ${ block })`);
                        }
                    }
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // speedingForked.disconnect();
                })
                speedingForked.send({ deviceList: [], driverList: driverList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block) })
            }        
        }

        
        log.info(`(calculateOffenceList ${ moment().format('YYYY-MM-DD HH:mm:ss') } ): end update hardBraking & rapidAcc & speeding!`);
    } catch (error) {
        throw error
    }
}

const deleteRecord = async function (deviceList, driverList) {
    try {
        log.warn(`deleteRecord => deviceList: ${ JSON.stringify(deviceList, null, 4) } - (${ moment().format('YYYY-MM-DD HH:mm:ss') })`)
        for (let device of deviceList) {
            let result = await DevicePositionHistoryBackup.destroy({ 
                where: { 
                    deviceId: device.deviceId, 
                    createdAt: { 
                        [Op.between]: [ 
                            moment(device.startRecordTime).format('YYYY-MM-DD HH:mm:ss'), 
                            moment(device.endRecordTime).format('YYYY-MM-DD HH:mm:ss') 
                        ] 
                    } 
                } 
            })
            log.warn(`deleteRecord => deviceId: ${ device.deviceId } (${ result } records)`)
        }

        log.warn(`deleteRecord => driverList: ${ JSON.stringify(driverList, null, 4) } - (${ moment().format('YYYY-MM-DD HH:mm:ss') })`)
        for (let driver of driverList) {
            let result = await DriverPositionHistoryBackup.destroy({
                where: {
                    driverId: driver.driverId, 
                    vehicleNo: driver.vehicleNo,
                    createdAt: { 
                        [Op.between]: [ 
                            driver.startRecordTime, 
                            driver.endRecordTime 
                        ]
                    } 
                } 
            })
            log.warn(`deleteRecord => driverId: ${ driver.driverId }, vehicleNo: ${ driver.vehicleNo } (${ result } records)`)
        }
    } catch (error) {
        log.error(`deleteRecord: `, error);
        throw error;
    }
}

module.exports = {
    commonFindOutTaskId: async function (list, dataFrom) {
        try {
            for (let offence of list) {
                offence.occTime = moment(offence.occTime).format('YYYY-MM-DD HH:mm:ss')

                let taskList = []
                if (dataFrom == 'mobile') { 
                    taskList = await sequelizeObj.query(`
                        SELECT taskId, dataFrom, driverId, vehicleNumber, vehicleNumber AS vehicleNo, 
                        DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                        DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
                        FROM task
                        WHERE driverId = ${ offence.deviceId }
                        AND vehicleNumber = '${ offence.vehicleNo }'
                        AND '${ offence.occTime }' >= mobileStartTime
                        AND (mobileEndTime IS NULL OR '${ offence.occTime }' <= mobileEndTime )
    
                        UNION
    
                        SELECT CONCAT('DUTY-', dutyId) AS taskId, 'SYSTEM' AS dataFrom, driverId, vehicleNo, vehicleNo AS vehicleNumber,
                        DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                        DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
                        FROM urgent_indent
                        WHERE driverId = ${ offence.deviceId }
                        AND vehicleNo = '${ offence.vehicleNo }'
                        AND '${ offence.occTime }' >= mobileStartTime
                        AND (mobileEndTime IS NULL OR '${ offence.occTime }' <= mobileEndTime )
                    `, {
                        type: QueryTypes.SELECT
                    })
                } else if (dataFrom == 'obd') {
                    taskList = await sequelizeObj.query(`
                        SELECT taskId, dataFrom, driverId, vehicleNumber, vehicleNumber AS vehicleNo,
                        DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                        DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
                        FROM task
                        WHERE vehicleNumber = '${ offence.vehicleNo }'
                        AND '${ offence.occTime }' >= mobileStartTime
                        AND (mobileEndTime IS NULL OR '${ offence.occTime }' <= mobileEndTime )
                        AND driverId IS NOT NULL
    
                        UNION
    
                        SELECT CONCAT('DUTY-', dutyId) AS taskId, 'SYSTEM' AS dataFrom, driverId, vehicleNo, vehicleNo AS vehicleNumber,
                        DATE_FORMAT(mobileStartTime, '%Y-%m-%d %H:%i:%s') AS mobileStartTime, 
                        DATE_FORMAT(mobileEndTime, '%Y-%m-%d %H:%i:%s') AS mobileEndTime
                        FROM urgent_indent
                        WHERE vehicleNo = '${ offence.vehicleNo }'
                        AND '${ offence.occTime }' >= mobileStartTime
                        AND (mobileEndTime IS NULL OR '${ offence.occTime }' <= mobileEndTime )
                        AND driverId IS NOT NULL                
                    `, {
                        type: QueryTypes.SELECT
                    })
                }
    
                if (taskList.length) {
                    offence.taskId = taskList[0].taskId
                }
            }

            return list
        } catch (error) {
            throw error
        }
    },
    calculateOffenceList: async function () {
        try {
            
            // 1.prepare work
            await prepareBaseData();
            
            // 2.prepare data
            let { deviceList, driverList } = await prepareData();
    
            // 3.output data
            await OutputDataList(deviceList, driverList);
    
            // 4.wait a moment
            await util.wait(2000);
    
            // 5.missing
            await missingService.calculateMissingList(deviceList, driverList)
    
            // 6.no go zone alert
            await noGoZoneAlertService.calculateAlertList(deviceList, driverList)
    
            // 7.speeding/hardBarking/rapidAcc
            await calculateBaseOffenceList(deviceList, driverList)
    
        } catch (error) {
            log.error(`calculateOffenceList => `, error)
        }
    }
}