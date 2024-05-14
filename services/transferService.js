const log = require('../log/winston.js').logger('DB Service');

const moment = require('moment');
const { QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf.js');

const { DevicePositionHistory, DevicePositionHistoryBackup } = require('../model/event/devicePositionHistory.js');
const { DriverPositionHistory, DriverPositionHistoryBackup } = require('../model/event/driverPositionHistory.js');

const transferTable = async function () {
    try {
        log.warn(`transferTable start => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`) 
        await sequelizeObj.transaction(async (t) => {

            log.info(`TRANSFER driver_position_history start => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)

            // get tempDriverPositionId
            let tempDriverPositionId = null
            let checkResult1 = await sequelizeObj.query(`
                select * from driver_position_history order by id desc limit 1 
            `, { type: QueryTypes.SELECT })
            if (checkResult1 && checkResult1.length) {
                tempDriverPositionId = checkResult1[0].id
            } else {
                // In case pre-wrong data
                let checkResult1_1 = await sequelizeObj.query(`
                    select * from driver_offence_history order by id desc limit 1 
                `, { type: QueryTypes.SELECT })
                if (checkResult1_1 && checkResult1_1.length) {
                    tempDriverPositionId = checkResult1_1[0].id
                }
            }

            // transfer table data
            let result1 = await sequelizeObj.query(`
                INSERT INTO driver_position_history_backup SELECT * FROM driver_position_history
            `, { type: QueryTypes.INSERT })
            log.info(`TRANSFER driver_position_history result => ${ result1 }`)

            // clear table
            await sequelizeObj.query(`
                TRUNCATE TABLE driver_position_history
            `, { type: QueryTypes.DELETE })

            // insert max id data
            if (tempDriverPositionId) {
                await DriverPositionHistory.create({ id: tempDriverPositionId + 1, driverId: 0 })
            }

            log.info(`TRUNCATE driver_position_history end => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)


            // ********************************************************************************************************


            log.info(`TRANSFER device_position_history start => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)

            // get tempDevicePositionId
            let tempDevicePositionId = null
            let checkResult2 = await sequelizeObj.query(`
                select * from device_position_history order by id desc limit 1 
            `, { type: QueryTypes.SELECT })
            if (checkResult2 && checkResult2.length) {
                tempDevicePositionId = checkResult2[0].id
            } else {
                // In case pre-wrong data
                let checkResult2_1 = await sequelizeObj.query(`
                    select * from device_offence_history order by id desc limit 1 
                `, { type: QueryTypes.SELECT })
                if (checkResult2_1 && checkResult2_1.length) {
                    tempDevicePositionId = checkResult2_1[0].id
                }
            }

            // clear table
            let result2 = await sequelizeObj.query(`
                INSERT INTO device_position_history_backup SELECT * FROM device_position_history
            `, { type: QueryTypes.INSERT })
            log.info(`TRANSFER device_position_history result => ${ result2 }`)

            await sequelizeObj.query(`
                TRUNCATE TABLE device_position_history
            `, { type: QueryTypes.DELETE })

            // insert max id data
            if (tempDevicePositionId) {
                await DevicePositionHistory.create({ id: tempDevicePositionId + 1, deviceId: 0 })
            }

            log.info(`TRUNCATE device_position_history end => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)

        })
        log.warn(`transferTable end => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`) 
    } catch (error) {
        log.error(error)
    }
}

module.exports.transferTable = transferTable