const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.VehicleCategory = dbConf.sequelizeObj.define('vehicle_category', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
    },
    vehicleName: {
        type: DataTypes.STRING(100),
    },
    vehicleClass: {
        type: DataTypes.STRING(16),
    },
    description: {
        type: DataTypes.STRING(200),
    },
    category: {
        type: DataTypes.STRING(200),
    },
    belongTo: {
        type: DataTypes.STRING(20),
    },
    baseLineQty: {
        type: DataTypes.TINYINT(4),
    },
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    status: {
        type: DataTypes.STRING(20),
        defaultValue: 'enable'
    },
}, {
    // other options
    tableName: 'vehicle_category',
    timestamps: true,
});
