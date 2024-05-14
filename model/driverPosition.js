const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverPosition = dbConf.sequelizeObj.define('driverPosition', {
    driverId: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
    },
    vehicleNo: {
        type: DataTypes.STRING(55),
        primaryKey: true,
    },
    unitId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    lat: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    lng: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    speed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    rpm: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    state: {
        type: DataTypes.STRING(55),
        defaultValue: null
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW,
        comment: 'created from mobile'
    },
    gpsTime: {
        type: DataTypes.DATE,
        comment: 'time while get gps'
    },
    receiveTime: {
        type: DataTypes.DATE,
        comment: 'time while server receive data'
    },
    gpsPermission: {
        type: DataTypes.TINYINT,
        comment: '0: not allow, 1: allow',
        defaultValue: 0,
    },
    gpsService: {
        type: DataTypes.TINYINT,
        comment: '0: closed, 1: open',
        defaultValue: 0,
    },
    network: {
        type: DataTypes.TINYINT,
        comment: '0: closed, 1: open (Always be 1 here, only has network can update here)',
        defaultValue: 0,
    },
    missingType: {
        type: DataTypes.STRING(55),
        comment: 'Network, No GPS Signal, No GPS Service, No GPS Permission, Pause, SOS',
    },
    creator: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
    realtimeSpeeding: {
        type: DataTypes.TINYINT,
        defaultValue: 0,
    },
    realtimeAlert: {
        type: DataTypes.TINYINT,
        defaultValue: 0,
    },
}, {
    // other options
    tableName: 'driver_position',
    timestamps: false,
});
