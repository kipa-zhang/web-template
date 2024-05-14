const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Device = dbConf.sequelizeObj.define('device', {
    deviceId: {
        type: DataTypes.STRING(55),
        primaryKey: true,
    },
    vin: {
        type: DataTypes.STRING(55),
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
    state: {
        type: DataTypes.STRING(55),
        defaultValue: null
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW,
    },
    creator: {
        type: DataTypes.INTEGER(11),
    }
}, {
    // other options
    tableName: 'device',
    timestamps: false,
});
