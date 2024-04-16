const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');

module.exports.Request = sequelizeSystemObj.define('request', {
    id: {
        type: DataTypes.STRING(11),
        primaryKey: true,
    },
    startDate: {
        type: DataTypes.STRING(50),
    },
    estimatedTripDuration: {
        type: DataTypes.STRING(20),
    },
    noOfTrips: {
        type: DataTypes.STRING(11),
    },
    additionalRemarks: {
        type: DataTypes.STRING(100),
    },
    createdBy: {
        type: DataTypes.BIGINT,
    },
    creatorRole: {
        type: DataTypes.STRING(45),
    },
    groupId: {
        type: DataTypes.BIGINT,
    },
    typeOfIndent: {
        type: DataTypes.STRING(100),
    },
    trips: {
        type: DataTypes.VIRTUAL,
    },
    purposeType: {
        type: DataTypes.STRING(100),
    },
    poNumber: {
        type: DataTypes.STRING(100),
    },
    groupName: {
        type: DataTypes.VIRTUAL,
    },
}, {
    timestamps: true,
});