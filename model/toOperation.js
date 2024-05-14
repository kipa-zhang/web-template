const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.TO_Operation = dbConf.sequelizeObj.define('to_operation', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    vehicleNo: {
        type: DataTypes.STRING(255)
    },
    taskId: {
        type: DataTypes.STRING(255)
    },
    type: {
        type: DataTypes.STRING(255),
        comment: '0: [gpsPermission, gpsService, network], 1: [pause, sos, resume]'
    },
    description: {
        type: DataTypes.STRING(255)
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
        comment: '0: closed, 1: open',
        defaultValue: 0,
    },
    startTime: {
        type: DataTypes.DATE
    },
    endTime: {
        type: DataTypes.DATE
    },
}, {
    // other options
    timestamps: true
});