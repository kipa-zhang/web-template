const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.VehicleKeyOptRecord = dbConf.sequelizeObj.define('vehicleKeyOptRecord', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    vehicleNo: {
        type: DataTypes.STRING(55),
        primaryKey: true,
    },
    taskId: {
        type: DataTypes.STRING(20), 
        primaryKey: true,
    },
    keyTagId: {
        type: DataTypes.STRING(64),
        defaultValue: null,
    },
    optType: {
        type: DataTypes.STRING(16)
    },
    optStatus: {
        type: DataTypes.STRING(32),
        defaultValue: 'Completed'
    },
    optPage: {
        type: DataTypes.STRING(10)
    },
    siteId: {
        type: DataTypes.STRING(4),
    },
    slotId: {
        type: DataTypes.STRING(4),
    },
	optTime: {
        type: DataTypes.DATE
    },
    dataFrom: {
        type: DataTypes.STRING(10),
    },
    optBy: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
    driverId: {
        type: DataTypes.INTEGER(12)
    },
    createdAt: {
        type: DataTypes.DATE
    },
    remarks: {
        type: DataTypes.STRING(200),
    },
    dataJson: {
        type: DataTypes.TEXT
    },
    reason: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    }
}, {
    tableName: 'key_opt_record',
    timestamps: false,
})