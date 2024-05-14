const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.Driver = dbConf.sequelizeObj.define('driver', {
    driverId: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    loginName: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    driverName: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    nric: {
        type: DataTypes.STRING(8),
    },
    contactNumber: {
        type: DataTypes.STRING(55),
    },
    permitType: {
        type: DataTypes.STRING(255),
    },
    unitId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    totalMileage: {
        type: DataTypes.INTEGER(12),
        defaultValue: 0,
    },
    vocation: {
        type: DataTypes.STRING(55),
    },
    rank: {
        type: DataTypes.STRING(55),
    },
    enlistmentDate: {
        type: DataTypes.DATE,
    },
    operationallyReadyDate: {
        type: DataTypes.DATE,
    },
    birthday: {
        type: DataTypes.DATE, 
    },
    bloodType: {
        type: DataTypes.STRING(5), 
    },
    totalMileage: {
        type: DataTypes.INTEGER(12),
        defaultValue: 0,
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    creator: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
    vehicleNo: {
        type: DataTypes.VIRTUAL,
    },
    lat: {
        type: DataTypes.VIRTUAL,
    },
    lng: {
        type: DataTypes.VIRTUAL,
    },
    state: {
        type: DataTypes.STRING(55)
    }, 
    status: {
        type: DataTypes.STRING(20),
    },
    overrideStatus: {
        type: DataTypes.STRING(20),
    },
    overrideStatusTime: {
        type: DataTypes.DATE,
    },
}, {
    // other options
    tableName: 'driver',
    timestamps: false,
});
