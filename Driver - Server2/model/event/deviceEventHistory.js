const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.DeviceEventHistory = dbConf.sequelizeObj.define('deviceEventHistory', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    deviceId: {
        type: DataTypes.STRING(55),
    },
    event: {
        type: DataTypes.STRING(55),
    },
    vin: {
        type: DataTypes.STRING(55),
    },
    createdAt: {
        type: DataTypes.DATE,
    }
}, {
    // other options,
    tableName: 'device_event_history',
    timestamps: false,
    indexes: [
        {
            name: 'idx_of_deviceId_event',
            fields: ['deviceId', 'event']
        }
    ],
});
