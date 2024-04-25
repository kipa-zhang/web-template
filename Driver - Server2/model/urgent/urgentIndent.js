const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.UrgentIndent = dbConf.sequelizeObj.define('urgentIndent', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
    },
    dutyId: {
        type: DataTypes.BIGINT(15),
    },
    status: {
        type: DataTypes.STRING(50),
    },
	startTime: {
        type: DataTypes.DATE,
    },
	endTime: {
        type: DataTypes.DATE,
    },
    mobileStartTime: {
        type: DataTypes.DATE,
    },
	mobileEndTime: {
        type: DataTypes.DATE,
    },
    vehicleType: {
        type: DataTypes.STRING(55),
    },
    reportingLocation: {
        type: DataTypes.STRING(200),
    },
    reportingGPS: {
        type: DataTypes.STRING(200),
    },
    poc: {
        type: DataTypes.STRING(55),
    },
    mobileNumber: {
        type: DataTypes.STRING(55),
    },
    indentId: {
        type: DataTypes.STRING(55),
    },
    requestId: {
        type: DataTypes.STRING(55),
    },
    hub: {
        type: DataTypes.STRING(50),
    },
    node: {
        type: DataTypes.STRING(50),
    },
    groupId: {
        type: DataTypes.INTEGER,
    },
    vehicleNo: {
        type: DataTypes.STRING(15),
    },
    driverId: {
        type: DataTypes.BIGINT(15),
    }, 
    amendedBy: {
        type: DataTypes.INTEGER(11),
    },
    cancelBy: {
        type: DataTypes.STRING(250),
    },
    cancelledCause: {
        type: DataTypes.STRING(250),
    },
    cancelledDateTime: {
        type: DataTypes.DATE,
    },
}, {
    tableName: 'urgent_indent',
})