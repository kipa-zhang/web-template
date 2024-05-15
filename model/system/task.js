const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');

module.exports.Task = sequelizeSystemObj.define('job_task', {
    id: {
        type: DataTypes.BIGINT,
        autoIncrement: true,
        primaryKey: true,
    },
    externalTaskId: {
        type: DataTypes.BIGINT,
    },
    externalJobId: {
        type: DataTypes.BIGINT,
    },
    requestId: {
        type: DataTypes.STRING(11),
    },
    tripId: {
        type: DataTypes.INTEGER,
    },
    startDate: {
        type: DataTypes.STRING(40),
    },
    endDate: {
        type: DataTypes.STRING(40),
    },
    pickupDestination: {
        type: DataTypes.STRING(200),
    },
    dropoffDestination: {
        type: DataTypes.STRING(200),
    },
    poc: {
        type: DataTypes.STRING(200),
    },
    pocNumber: {
        type: DataTypes.STRING(12),
    },
    executionDate: {
        type: DataTypes.STRING(10),
    },
    executionTime: {
        type: DataTypes.STRING(10),
    },
    duration: {
        type: DataTypes.STRING(5),
    },
    taskStatus: {
        type: DataTypes.STRING(60),
    },
    driverId: {
        type: DataTypes.BIGINT,
    },
    mobileStartTime: {
        type: DataTypes.DATE,
    },
    arrivalTime: {
        type: DataTypes.DATE,
    },
    endTime: {
        type: DataTypes.DATE,
    },
    departTime: {
        type: DataTypes.DATE,
    },
    copyFrom: {
        type: DataTypes.BIGINT,
    },
    success: {
        type: DataTypes.BOOLEAN,
    },
    guid: {
        type: DataTypes.STRING(15),
    },
    jobStatus: {
        type: DataTypes.STRING(25),
    },
    returnData: {
        type: DataTypes.TEXT,
    },
    sendData: {
        type: DataTypes.TEXT,
    },
    trackingId: {
        type: DataTypes.STRING(45),
    },
    contractPartNo: {
        type: DataTypes.STRING(100),
    },
    serviceProviderId: {
        type: DataTypes.STRING(4),
    },
    selectableTsp: {
        type: DataTypes.STRING(20),
    },
    tspChangeTime: {
        type: DataTypes.DATE,
    },
    notifiedTime: {
        type: DataTypes.DATE,
    },
    driverNo: {
        type: DataTypes.BIGINT,
    },
    noMoreArbitrate: {
        type: DataTypes.BIGINT,
    },
    endorse: {
        type: DataTypes.BIGINT,
    },
    cancellationTime: {
        type: DataTypes.DATE,
    },
    funding: {
        type: DataTypes.STRING(200),
    },
    poNumber:{
        type: DataTypes.STRING(100),
    },
    isChange: {
        type: DataTypes.BOOLEAN,
    }
}, {
    // other options
    timestamps: true,
});