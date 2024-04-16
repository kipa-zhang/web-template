const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverPermit = dbConf.sequelizeObj.define('driverPermit', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    permitNo: {
        type: DataTypes.STRING(40),
    },
    permitType: {
        type: DataTypes.STRING(40),
    },
    dateOfIssue: {
        type: DataTypes.DATE,
    },
    category: {
        type: DataTypes.STRING(5),
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    }
}, {
    // other options
    tableName: 'driver_permit',
    timestamps: false,
});
