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

const CheckRecordWithSameCreatedTime = async function () {
    try {
        log.warn(`CheckRecordWithSameCreatedTime => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        
        let continueCheckSameCreatedTime = true, index = 0;
        let count = 0;
        while (continueCheckSameCreatedTime) {
            index++;
            log.warn(`CheckRecordWithSameCreatedTime => index: ${ index } `)
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
                        list = list.concat(idList.slice(0, idList.length -1))
                    }
                }
                count += list.length

                // Still exist same createdAt data
                // delete again
                await sequelizeObj.query(`
                    DELETE FROM device_position_history_backup WHERE id IN (?)
                `, { type: QueryTypes.DELETE, replacements: [ list ] })
            } else {
                continueCheckSameCreatedTime = false
            }
        }

        log.warn(`CheckRecordWithSameCreatedTime affect data count => ${ count }`);
        log.warn(`CheckRecordWithSameCreatedTime => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
    } catch (error) {
        log.error(`CheckRecordWithSameCreatedTime: `, error);
        throw error;
    }
}

const DestroyRecordWithNullGPS = async function () {
    try {
        log.warn(`DeleteRecordWithNullCreatedTime => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)

        await sequelizeObj.query(`
            DELETE FROM device_position_history_backup WHERE lat IS NULL or lng IS NULL;
        `, { type: QueryTypes.DELETE })

        await sequelizeObj.query(`
            DELETE FROM driver_position_history_backup WHERE lat IS NULL or lng IS NULL;
        `, { type: QueryTypes.DELETE })

        log.warn(`DeleteRecordWithNullCreatedTime => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
    } catch (error) {
        log.error(`DeleteRecordWithNullCreatedTime: `, error);
        throw error;
    }
}

const CheckRecordWithSameGPS = async function (deviceList, driverList) {
    try {
        // deviceList
        log.warn(`(CheckRecordWithSameGPS) Device => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        for (let device of deviceList) {
            // Check width same gps data
            const recordList = await sequelizeObj.query(`
                SELECT * FROM (
                    SELECT COUNT(*) AS positionCount, GROUP_CONCAT(id) AS idList, lat, lng, deviceId, createdAt 
                    FROM device_position_history_backup 
                    WHERE deviceId = '${ device.deviceId }' 
                    AND createdAt BETWEEN '${ moment(device.startRecordTime).format('YYYY-MM-DD HH:mm:ss') }' AND '${ moment(device.endRecordTime).format('YYYY-MM-DD HH:mm:ss') }'
                    GROUP BY lat, lng
                ) AS t 
                WHERE t.positionCount > 1
            `, { type: QueryTypes.SELECT })
            log.warn(`(CheckRecordWithSameGPS) Device => deviceId (${ device.deviceId })=> same time count (${ recordList.length }) `)

            // While no record, continue
            if (!recordList.length) continue

            for (let record of recordList) {
                let firstRecord = await sequelizeObj.query(`
                    SELECT id AS id FROM device_position_history_backup 
                    WHERE lat = '${ record.lat }' AND lng = '${ record.lng }' AND deviceId = '${ device.deviceId }' 
                    ORDER BY id ASC LIMIT 1
                `, { type: QueryTypes.SELECT })
                let lastRecord = await sequelizeObj.query(`
                    SELECT id AS id FROM device_position_history_backup 
                    WHERE lat = '${ record.lat }' AND lng = '${ record.lng }' AND deviceId = '${ device.deviceId }' 
                    ORDER BY id DESC LIMIT 1
                `, { type: QueryTypes.SELECT })
                // Delete data with same gps, but speed is not 0
                if (firstRecord.length && lastRecord.length) {
                    await sequelizeObj.query(`
                        DELETE FROM device_position_history_backup 
                        WHERE speed != 0 
                        AND id between ${ firstRecord[0].id } AND ${ lastRecord[0].id }
                    `, { type: QueryTypes.DELETE })
                }
            }
        }
        log.warn(`(CheckRecordWithSameGPS) Device => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)

        // driverList
        log.warn(`(CheckRecordWithSameGPS) Mobile => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        for (let driver of driverList) {
            // Check width same gps data
            const recordList = await sequelizeObj.query(`
                SELECT * FROM (
                    SELECT COUNT(*) AS positionCount, GROUP_CONCAT(id) AS idList, lat, lng, driverId, vehicleNo, createdAt 
                    FROM driver_position_history_backup 
                    WHERE driverId = '${ driver.driverId }' AND vehicleNo = '${ driver.vehicleNo }'
                    AND createdAt BETWEEN '${ moment(driver.startRecordTime).format('YYYY-MM-DD HH:mm:ss') }' AND '${ moment(driver.endRecordTime).format('YYYY-MM-DD HH:mm:ss') }'
                    GROUP BY lat, lng
                ) AS t 
                WHERE t.positionCount > 1
            `, { type: QueryTypes.SELECT })
            log.warn(`(CheckRecordWithSameGPS) Mobile => deviceId (${ driver.driverId })=> same time count (${ recordList.length }) `)

            // While no record, continue
            if (!recordList.length) continue

            for (let record of recordList) {
                let firstRecord = await sequelizeObj.query(`
                    SELECT id AS id FROM driver_position_history_backup 
                    WHERE lat = '${ record.lat }' AND lng = '${ record.lng }' AND driverId = '${ driver.driverId }' AND vehicleNo = '${ driver.vehicleNo }'
                    ORDER BY id ASC LIMIT 1
                `, { type: QueryTypes.SELECT })
                let lastRecord = await sequelizeObj.query(`
                    SELECT id AS id FROM driver_position_history_backup 
                    WHERE lat = '${ record.lat }' AND lng = '${ record.lng }' AND driverId = '${ driver.driverId }'  AND vehicleNo = '${ driver.vehicleNo }'
                    ORDER BY id DESC LIMIT 1
                `, { type: QueryTypes.SELECT })
                // Delete data with same gps, but speed is not 0
                if (firstRecord[0].id && lastRecord[0].id) {
                    await sequelizeObj.query(`
                        DELETE FROM driver_position_history_backup 
                        WHERE speed != 0 
                        AND id between ${ firstRecord[0].id } AND ${ lastRecord[0].id }
                    `, { type: QueryTypes.DELETE })
                }
            }
            
        }
        log.warn(`(CheckRecordWithSameGPS) Mobile => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        
    } catch (error) {
        log.error(`CheckRecordWithSameGPS: `, error);
        throw error;
    }
}
const OutputDataList = async function (deviceList, driverList) {
    try {
        log.warn(`OutputDataList => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        log.warn(`OutputDataList => deviceList.length ${ deviceList.length } `)
        log.warn(`OutputDataList => driverList.length ${ driverList.length } `)
        for (let device of deviceList) {
            let list = await DevicePositionHistoryBackup.findAll({ 
                where: 
                    { 
                        deviceId: device.deviceId, 
                        createdAt: { 
                            [Op.between]: [ moment(device.startRecordTime).format('YYYY-MM-DD HH:mm:ss'), moment(device.endRecordTime).format('YYYY-MM-DD HH:mm:ss') ] 
                        } 
                    } 
            })

            log.warn(`OutputDataList => deviceId: ${ device.deviceId } (${ list.length } records) `)	
            // While no record, continue
            if (!list.length) {
                continue
            }

            // Add 2023-10-13
            // device.data = list;

            await outputService.writeIntoFile(list, device.deviceId)
        } 
        for (let driver of driverList) {                                             
            let list = await DriverPositionHistoryBackup.findAll({ 
                where: 
                    { 
                        driverId: driver.driverId, 
                        vehicleNo: driver.vehicleNo,
                        createdAt: { 
                            [Op.between]: [ moment(driver.startRecordTime).format('YYYY-MM-DD HH:mm:ss'), moment(driver.endRecordTime).format('YYYY-MM-DD HH:mm:ss') ] 
                        } 
                } })

            log.warn(`OutputDataList => driverId: ${ driver.driverId } (${ list.length } records) `)
            // While no record, continue
            if (!list.length) {
                continue
            }
                
            // Add 2023-10-13
            // driver.data = list

            await outputService.writeIntoFile(list, driver.driverId)
        }
        log.warn(`OutputDataList => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
    } catch (error) {
        log.error(`OutputDataList: `, error);
        throw error;
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
                        [Op.between]: [ moment(device.startRecordTime).format('YYYY-MM-DD HH:mm:ss'), moment(device.endRecordTime).format('YYYY-MM-DD HH:mm:ss') ] 
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
                    createdAt: { [Op.between]: [ driver.startRecordTime, driver.endRecordTime ] } 
                } 
            })
            log.warn(`deleteRecord => driverId: ${ driver.driverId }, vehicleNo: ${ driver.vehicleNo } (${ result } records)`)
        }
    } catch (error) {
        log.error(`deleteRecord: `, error);
        throw error;
    }
}

module.exports.updateTrackDashboardInfoByChildProcess = async function () {
    try {
        log.info(`(initTrackDashboardInfo ${moment().format('YYYY-MM-DD HH:mm:ss')} ): start update hardBraking & rapidAcc & speeding!`);

        await DevicePositionHistoryBackup.destroy({ where: { deviceId: '0' } }); // clear temp data
        await DriverPositionHistoryBackup.destroy({ where: { driverId: 0 } }); // clear temp data 

        // Delete all record that createdAt is null;
        log.warn(`CheckRecordWithNullCreatedTime => start time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)
        let result1 = await DevicePositionHistoryBackup.destroy({ where: { createdAt: { [Op.is]: null } } }); // OBD use system time
        let result2 = await DriverPositionHistoryBackup.destroy({ where: { createdAt: { [Op.is]: null } } }); // Impossible 
        log.warn(`DevicePositionHistory(createdAt is null) affect data count => ${ result1 }`);
        log.warn(`DriverPositionHistory(createdAt is null) affect data count => ${ result2 }`);
        log.warn(`CheckRecordWithNullCreatedTime => end time (${ moment().format('YYYY-MM-DD HH:mm:ss') }) `)

        // TODO: Use for clear record with same createdAt timestamp
        // Attention: Only need check OBD
        await CheckRecordWithSameCreatedTime();
        await DestroyRecordWithNullGPS();
        
        const deviceList = await sequelizeObj.query(`
            SELECT d.deviceId, IFNULL(v.limitSpeed, 60) AS limitSpeed, dp.createdAt AS startRecordTime, 
            IF(DATE_ADD(dp.createdAt,INTERVAL ${ conf.Calculate_TimeZone } MINUTE) < d.updatedAt, DATE_ADD(dp.createdAt,INTERVAL ${ conf.Calculate_TimeZone } MINUTE), d.updatedAt) AS endRecordTime
            FROM device d
            LEFT JOIN vehicle v ON v.deviceId = d.deviceId
            LEFT JOIN (
                SELECT deviceId, createdAt, id 
                FROM device_position_history_backup
                GROUP BY deviceId
            ) dp ON dp.deviceId = d.deviceId 
            WHERE dp.id IS NOT NULL 
        `, { type: QueryTypes.SELECT })
        log.info(`updateTrackDashboardInfoByChildProcess deviceList => find out ${ deviceList.length } `)
        log.info(`updateTrackDashboardInfoByChildProcess deviceList => detail ${ JSON.stringify(deviceList, null, 4) } `)

        // 1 min 4 sec
        const driverList = await sequelizeObj.query(`
            SELECT d.driverId, IFNULL(v.limitSpeed, 60) AS limitSpeed, d.vehicleNo, dp.createdAt AS startRecordTime, 
            IF(DATE_ADD(dp.createdAt,INTERVAL ${ conf.Calculate_TimeZone } MINUTE) < d.updatedAt, DATE_ADD(dp.createdAt,INTERVAL ${ conf.Calculate_TimeZone } MINUTE), d.updatedAt) AS endRecordTime
            FROM driver_position d
            LEFT JOIN vehicle v ON v.vehicleNo = d.vehicleNo
            LEFT JOIN (
                SELECT driverId, vehicleNo, createdAt, id 
                FROM driver_position_history_backup
                GROUP BY driverId, vehicleNo
            ) dp ON dp.driverId = d.driverId AND dp.vehicleNo = d.vehicleNo
            WHERE dp.id IS NOT NULL 
        `, { type: QueryTypes.SELECT })

        log.info(`updateTrackDashboardInfoByChildProcess driverList => find out ${ driverList.length } `)
        log.info(`updateTrackDashboardInfoByChildProcess driverList => detail ${ JSON.stringify(driverList, null, 4) } `)

        // TODO: Use for clear record with same gps but speed is not 0
        // await CheckRecordWithSameGPS(deviceList, driverList);

        // TODO: Move data into files
        await OutputDataList(deviceList, driverList);

        // wait for output
        log.warn(`*****************************`);
        log.warn(`wait for output`);
        log.warn(`*****************************`);
        await util.wait(2000);
        log.warn(`*****************************`);
        log.warn(`finish wait `);
        log.warn(`*****************************`);


        
    } catch (error) {
        log.error(error)
    }
}

