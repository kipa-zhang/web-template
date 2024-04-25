const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.DriverMonthAchievement = dbConf.sequelizeObj.define('driver_month_achievement', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
    },
    month: {
        type: DataTypes.STRING(8),
    },
    platformsTrained: {
        type: DataTypes.INTEGER(8),
    },
    totalMileage: {
        type: DataTypes.FLOAT(20, 1),
    },
    taskNum: {
        type: DataTypes.INTEGER(8),
    },
    taskPerfectHours: {
        type: DataTypes.FLOAT(20, 1),
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    }
  }, {
    // other options
    timestamps: true
});