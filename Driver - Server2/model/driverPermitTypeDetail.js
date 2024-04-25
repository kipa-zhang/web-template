const { DataTypes, QueryTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverPermitTypeDetail = dbConf.sequelizeObj.define('driver_permittype_detail', {
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
        type: DataTypes.STRING(55),
    },
    baseMileage: {
        type: DataTypes.FLOAT(10, 1),
    },
    passDate: {
        type: DataTypes.DATE,
    },
    attemptNums: {
        type: DataTypes.TINYINT(2),
    },
    testerCode: {
        type: DataTypes.STRING(40),
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    creator: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    operation: {
        type: DataTypes.VIRTUAL
    }
}, {
    // other options
    tableName: 'driver_permittype_detail',
    timestamps: false,
});
