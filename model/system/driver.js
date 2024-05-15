const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');

module.exports.Driver = sequelizeSystemObj.define('driver', {
    taskId: {
        type: DataTypes.BIGINT,
        primaryKey: true,
    },
    driverId: {
        type: DataTypes.BIGINT,
    },
    permitType: {
        type: DataTypes.STRING(255),
    },
    status: {
        type: DataTypes.STRING(150),
    },
    name: {
        type: DataTypes.STRING(150),
    },
    nric: {
        type: DataTypes.STRING(9),
    },
    contactNumber: {
        type: DataTypes.STRING(30),
    },
    data: {
        type: DataTypes.TEXT,
    },
    driverFrom: {
        type: DataTypes.STRING(255),
    }
}, {
    timestamps: true,
});