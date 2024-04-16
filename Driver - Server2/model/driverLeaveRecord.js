const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.DriverLeaveRecord = dbConf.sequelizeObj.define('driverLeaveRecord', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.BIGINT(15),
    },
	startTime: {
        type: DataTypes.DATE,
    },
    endTime: {
        type: DataTypes.DATE,
    },
	dayType: {
        type: DataTypes.STRING(10),
    },
	optTime: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),  
    },
    reason: {
        type: DataTypes.STRING(20),
    },
    remarks: {
        type: DataTypes.STRING(200),
    },
    creator: {
        type: DataTypes.INTEGER(12),
        defaultValue: 0
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    status: {
        type: DataTypes.INTEGER(1),
        defaultValue: 1,
    },
    cancelRemarks : {
        type: DataTypes.STRING(200),
    }
}, {
    tableName: 'driver_leave_record',
    timestamps: false,
})