const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.DriverTask = dbConf.sequelizeObj.define('driverTask', {
    id: {
        type: DataTypes.INTEGER, 
        autoIncrement: true,
        primaryKey: true,
    },
    vehicleRelationId: {
        type: DataTypes.INTEGER, 
        allowNull: false,
    },
    routeNo: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    arrivedInfo: {
        type: DataTypes.TEXT,
        defaultValue: null,
    },
    estimateStartTime: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    estimateEndTime: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    startTime: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    endTime: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    status: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    state: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    stateTime: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    mobileState: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    mobileStateTime: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    highPriority: {
        type: DataTypes.TINYINT,
        defaultValue: 0,
    },
}, {
    // other options
    tableName: 'driver_task',
    timestamps: true,
});
