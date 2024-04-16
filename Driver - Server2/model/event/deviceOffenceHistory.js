const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.DeviceOffenceHistory = dbConf.sequelizeObj.define('deviceOffenceHistory', {
    id: {
        type: DataTypes.BIGINT(20), 
        primaryKey: true,
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
        defaultValue: DataTypes.NOW
    }
}, {
    // other options
    tableName: 'device_offence_history',
    timestamps: false,
    indexes: [
        {
            name: 'index_of_deviceId_createdAt',
            fields: ['deviceId', 'createdAt']
        }
    ],
});
