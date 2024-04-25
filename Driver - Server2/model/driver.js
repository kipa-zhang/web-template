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
        type: DataTypes.STRING(55),
    },
    contactNumber: {
        type: DataTypes.STRING(55),
    },
    email: {
        type: DataTypes.STRING(64),
    },
    permitType: {
        type: DataTypes.STRING(255),
    },
    vehicleType: {
        type: DataTypes.STRING(3000),
    },
    vocation: {
        type: DataTypes.STRING(55),
    },
    rank: {
        type: DataTypes.STRING(55),
    },
    unit: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    unitId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    groupId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
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
    status: {
        type: DataTypes.STRING(20),
    },
    licensingStatus: {
        type: DataTypes.STRING(20),
        defaultValue: 'Not Ready'
    },
    lastSOSDateTime: {
        type: DataTypes.DATE
    },
    state: {
        type: DataTypes.STRING(55)
    },
    permitStatus: {
        type: DataTypes.STRING(10),
        defaultValue: "valid"
    }, 
    permitInvalidReason: {
        type: DataTypes.STRING(64),
        defaultValue: null
    },
}, {
    // other options
    tableName: 'driver',
    timestamps: false,
});
