const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverMileage = dbConf.sequelizeObj.define('driverMileage', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    permitType: {
        type: DataTypes.STRING(40),
    },
    mileage: {
        type: DataTypes.FLOAT(10, 1),
    },
}, {
    // other options
    tableName: 'driver_mileage',
    timestamps: true,
});
