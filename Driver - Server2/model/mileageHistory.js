const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.MileageHistory = dbConf.sequelizeObj.define('mileage_history', {
    taskId: {
        type: DataTypes.STRING(20), 
        primaryKey: true,
    },
    date: {
        type: DataTypes.DATEONLY,
    },
    startTime: {
        type: DataTypes.DATE,
    },
	endTime: {
        type: DataTypes.DATE,
    },
    startMileage: {
        type: DataTypes.FLOAT(20, 1),
    },
	endMileage: {
        type: DataTypes.FLOAT(20, 1),
    },
    deviceId: {
        type: DataTypes.STRING(50),
    },
    vehicleNo: {
        type: DataTypes.STRING(50),
    },
	driverId: {
        type: DataTypes.INTEGER(11),
    },
    mileageTraveled: {
        type: DataTypes.FLOAT(20, 1),
    },
    mobileMileageTraveled: {
        type: DataTypes.FLOAT(20, 1),
    },
    obdMileageTraveled: {
        type: DataTypes.FLOAT(20, 1),
    },
  }, {
    // other options
    timestamps: true,
});