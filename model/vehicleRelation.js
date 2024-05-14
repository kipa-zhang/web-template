const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.VehicleRelation = dbConf.sequelizeObj.define('vehicleRelation', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    vehicleNo: {
        type: DataTypes.STRING(45),
        defaultValue: null,
    },
    deviceId: {
        type: DataTypes.STRING(45),
        defaultValue: null,
    },
    limitSpeed: {
        type: DataTypes.INTEGER,
        defaultValue: 60,
    },
}, {
    tableName: 'vehicle_relation',
    timestamps: true,
    indexes: [
        {
            name: 'idx_of_driverId_vehicleNo_deviceId',
            fields: ['driverId', 'vehicleNo', 'deviceId']
        }
    ],
});
