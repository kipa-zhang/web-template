const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.DevicePositionHistory = dbConf.sequelizeObj.define('device_position_history', {
    id: {
        type: DataTypes.INTEGER, 
        primaryKey: true,
        autoIncrement: true
    },
    deviceId: {
        type: DataTypes.STRING(55),
        allowNull: false,
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
    createdAt: {
        type: DataTypes.DATE,
    },
    deviceTime: {
        type: DataTypes.DATE,
    },
}, {
    // other options
    tableName: 'device_position_history',
    timestamps: false,
});
