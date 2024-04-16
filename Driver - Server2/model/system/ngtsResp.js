const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');

module.exports.NGTSResp = sequelizeSystemObj.define('ngts_resp', {
    id: {
        type: DataTypes.BIGINT,
        primaryKey: true,
        autoIncrement: true
    },
    atmsTaskId: {
        type: DataTypes.BIGINT,
    },
    ngtsTripId: {
        type: DataTypes.STRING(23),
    },
    referenceId: {
        type: DataTypes.STRING(23),
        allowNull: false,
    },
    transacationType: {
        type: DataTypes.STRING(45),
    },
    transacationDateTime: {
        type: DataTypes.DATE,
    },
    responseStatus: {
        type: DataTypes.STRING(1),
        defaultValue: 'A',
    },
    serviceMode: {
        type: DataTypes.STRING(50),
    },
    resourceId: {
        type: DataTypes.STRING(20),
    },
    resourceQuantity: {
        type: DataTypes.INTEGER,
    },
    startDateTime: {
        type: DataTypes.DATE,
    },
    endDateTime: {
        type: DataTypes.DATE,
    },
    pocUnitCode: {
        type: DataTypes.STRING(20),
    },
    pocName: {
        type: DataTypes.STRING(66),
    },
    pocMobileNumber: {
        type: DataTypes.STRING(8),
    },
    reportingLocationId: {
        type: DataTypes.STRING(255),
    },
    destinationLocationId: {
        type: DataTypes.STRING(255),
    },
    preparkQuantity: {
        type: DataTypes.INTEGER,
    },
    preparkDateTime: {
        type: DataTypes.DATE,
    },
    ngtsJobId: {
        type: DataTypes.INTEGER,
    },
    ngtsJobStatus: {
        type: DataTypes.STRING(25),
    },
    driverId: {
        type: DataTypes.INTEGER,
    },
    driverName: {
        type: DataTypes.STRING(255),
    },
    driverMobileNumber: {
        type: DataTypes.STRING(8),
    },
    vehicleNumber: {
        type: DataTypes.STRING(25),
    },
    operatorId: {
        type: DataTypes.INTEGER,
    },
    isSend: {
        type: DataTypes.STRING(1),
        defaultValue: 'N',
    },
    trackingId: {
        type: DataTypes.STRING(45),
    },
}, {
    timestamps: true,
});