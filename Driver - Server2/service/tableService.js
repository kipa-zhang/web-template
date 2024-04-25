const log = require('../log/winston').logger('DB Service');
const conf = require('../conf/conf.js');

const { QueryTypes } = require('sequelize');
const { sequelizePositionObj } = require('../db/dbConf_position');
const moment = require('moment');

const createMobileTable = async function (tableName) {
    try {
        await sequelizePositionObj.query(` 
            CREATE TABLE \`${ tableName }\` (
            \`id\` bigint(20) NOT NULL AUTO_INCREMENT,
            \`driverId\` int NOT NULL,
            \`vehicleNo\` varchar(55) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
            \`lat\` float(21,18) DEFAULT '0.000000000000000000',
            \`lng\` float(21,18) DEFAULT '0.000000000000000000',
            \`speed\` int DEFAULT '0',
            \`rpm\` int DEFAULT '0',
            \`createdAt\` datetime DEFAULT CURRENT_TIMESTAMP,
            \`gpsTime\` datetime DEFAULT NULL COMMENT 'time while get gps',
            \`receiveTime\` datetime DEFAULT NULL COMMENT 'time while server receive data',
            \`gpsPermission\` tinyint(1) DEFAULT '0' COMMENT '0: not allow, 1: allow (Always be 1 here, only has permission can update here)',
            \`gpsService\` tinyint(1) DEFAULT '0' COMMENT '0: closed, 1: open',
            \`network\` tinyint(1) DEFAULT '0' COMMENT '0: closed, 1: open',
            PRIMARY KEY (\`id\`) USING BTREE
          ) ENGINE=InnoDB;        
        `, { type: QueryTypes.INSERT })
        await sequelizePositionObj.query(`
            ALTER TABLE \`${ tableName }\` AUTO_INCREMENT = ${ Number.parseInt(tableName.split('_').at(-1) + moment().format('mm') + '00000000') };
        `, { type: QueryTypes.UPDATE })
        log.info(`Table(Mobile) ${ tableName } create success.`)
    } catch (error) {
        log.error(`createMobileTable(${ tableName }) =>`, error)
        throw new Error(`createMobileTable(${ tableName })`)
    }
}

// driverId: user.driverId,
// vehicleNo,
// lat: Number.parseFloat(_latestPosition.lat.replace(',', '.')),
// lng: Number.parseFloat(_latestPosition.lng.replace(',', '.')),
// speed: _latestPosition.speed,
// createdAt: moment(Number.parseInt(position.sendTime)).format('YYYY-MM-DD HH:mm:ss'),
// gpsTime: moment(Number.parseInt(_latestPosition.gpstime ?? position.sendTime)).format('YYYY-MM-DD HH:mm:ss'),
// receiveTime: receiveTime,
// gpsPermission: position.gpsPermission,
// gpsService: position.gpsService,
// network: position.network,
const createMobilePositionRecords = async function (tableName, positionList) {
    try {
        let positionListStr = positionList.map(item => 
            `(${ item.driverId }, '${ item.vehicleNo }', ${ item.lat }, ${ item.lat }, ${ item.speed }, 
            '${ item.createdAt }', '${ item.gpsTime }', '${ item.receiveTime }', '${ item.gpsPermission }', '${ item.gpsService }', ${ item.network })`)
        positionListStr = positionListStr.join(',')
        await sequelizePositionObj.query(`
            INSERT INTO \`driver_position_history_${ tableName }\`(
                driverId, vehicleNo, lat, lng, speed, createdAt, gpsTime, receiveTime, gpsPermission, gpsService, network )
            VALUES ${ positionListStr }
        `, { type: QueryTypes.INSERT })
    } catch (error) {
        throw error
    }
}

const createOBDTable = async function (tableName) {
    try {
        await sequelizePositionObj.query(` 
        CREATE TABLE \`${ tableName }\` (
            \`id\` bigint(20) NOT NULL AUTO_INCREMENT,
            \`deviceId\` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
            \`lat\` float(13,10) DEFAULT '0.0000000000',
            \`lng\` float(13,10) DEFAULT '0.0000000000',
            \`speed\` float DEFAULT '0',
            \`rpm\` float DEFAULT '0',
            \`createdAt\` datetime DEFAULT NULL,
            \`vin\` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
            \`networkState\` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '0',
            \`deviceTime\` datetime DEFAULT NULL,
            \`deviceTimems\` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
            PRIMARY KEY (\`id\`) USING BTREE
          ) ENGINE=InnoDB ;               
        `, { type: QueryTypes.INSERT })
        await sequelizePositionObj.query(`
            ALTER TABLE \`${ tableName }\` AUTO_INCREMENT = ${ Number.parseInt(tableName.split('_').at(-1) + moment().format('mm') + '00000000') };
        `, { type: QueryTypes.UPDATE })
        log.info(`Table(OBD) ${ tableName } create success.`)
    } catch (error) {
        log.error(`createOBDTable(${ tableName }) =>`, error)
        throw `createOBDTable(${ tableName }) failed`
    }
}

// deviceId:currentDeviceParams.deviceId,
// lat:currentDeviceParams.lat,
// lng:currentDeviceParams.lng,
// speed:currentDeviceParams.speed,
// rpm:currentDeviceParams.rpm,
// createdAt: currentDeviceParams.createdAt,
// vin:null,
// networkState:currentDeviceParams.deviceNetworkState,
// deviceTime:currentDeviceParams.deviceTime,
// deviceTimems:currentDeviceParams.deviceTimems,
const createOBDPositionRecords = async function (tableName, positionList) {
    try {
        let positionListStr = positionList.map(item => 
            `'(${ item.deviceId }', ${ item.lat }, ${ item.lat }, ${ item.speed }, ${ item.rpm }, 
            '${ item.createdAt }', '${ item.networkState }', '${ item.deviceTime }', '${ item.deviceTimems }')`)
        positionListStr = positionListStr.join(',')
        await sequelizePositionObj.query(`
            INSERT INTO \`device_position_history_${ tableName }\`(
                deviceId, lat, lng, speed, rpm, createdAt, networkState, deviceTime, deviceTimems )
            VALUES ${ positionListStr }
        `, { type: QueryTypes.INSERT })
    } catch (error) {
        throw error
    }
}

const checkTable = async function (tableName, tableType = 'MOBILE') {
    try {
        tableName = tableType == 'MOBILE' ? `driver_position_history_${ tableName }` : `device_position_history_${ tableName }`
        let result = await sequelizePositionObj.query(` SHOW TABLES LIKE '${ tableName }'; `, { type: QueryTypes.SELECT })
        if (!result.length) {
            // Table do not exist
            if (tableType == 'MOBILE') {
                await createMobileTable(tableName);
            } else if (tableType == 'OBD') {
                await createOBDTable(tableName);
            } else {
                throw `checkTable => tableType ${ tableType } is not correct.`
            }
        }
    } catch (error) {
        throw error
    }
}

const destroyTable = async function (tableName) {
    try {
        await sequelizePositionObj.query(` DROP TABLE \`${ tableName }\`; `, { type: QueryTypes.DELETE })
        log.info(`destroyTable ${ tableName } success`)
    } catch (error) {
        log.error(`destroyTable ${ tableName }(${ tableType }) failed`)
        log.error(`destroyTable =>`, error)
        throw `destroyTable ${ tableName }(${ tableType }) failed`
    }
}

const getTableList = async function (type = 'MOBILE') {
    try {
        let tableList = await sequelizePositionObj.query(`
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA='${ conf.dbPositionConf.database }' 
            AND TABLE_NAME LIKE '${ type == 'MOBILE' ? 'driver_position_history_' : 'device_position_history_' }%';
        `, { type: QueryTypes.SELECT })
        log.info(tableList)
        return tableList
    } catch (error) {
        log.error(error)
    }
}

module.exports = {
    getTableList,
    destroyTable,
    checkTable,
    createMobilePositionRecords,
    createOBDPositionRecords,
}