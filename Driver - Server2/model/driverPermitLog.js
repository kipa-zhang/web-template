const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverPermitLog = dbConf.sequelizeObj.define('driverPermitLog', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    optType: {
        type: DataTypes.STRING(40),
    },
    optTime: {
        type: DataTypes.DATE,
    },
    remarks: {
        type: DataTypes.STRING(256),
    },
    reason: {
        type: DataTypes.STRING(64),
        defaultValue: null
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    creator: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    }
}, {
    // other options
    tableName: 'driver_permit_log',
    timestamps: false,
});
