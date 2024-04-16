const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.TaskAssignRecord = dbConf.sequelizeObj.define('taskAssignRecord', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.BIGINT(15),
    },
    newDriverId: {
        type: DataTypes.BIGINT(15),
    },
    vehicleNumber: {
        type: DataTypes.STRING(15),
    },
    newVehicleNumber: {
        type: DataTypes.STRING(15),
    },
	taskId: {
        type: DataTypes.STRING(20),
    },
	optType: {
        type: DataTypes.STRING(10),
        defaultValue: 'assign'
    },
	optTime: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),  
    },
    reasons: {
        type: DataTypes.STRING(20),
    },
    remarks: {
        type: DataTypes.STRING(200),
    }
}, {
    tableName: 'driver_task_assign_record',
    timestamps: false,
})