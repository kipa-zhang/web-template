const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.MobileTrip = dbConf.sequelizeObj.define('mobile_trip', {
    id: {
        type: DataTypes.INTEGER, 
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.BIGINT(15),
    },
    vehicleNumber: {
        type: DataTypes.STRING(15),
    },
	indentStartTime: {
		type: DataTypes.DATE,
	},
    indentEndTime: {
		type: DataTypes.DATE,
	},
    purpose: {
        type: DataTypes.STRING(200),
    },
    pickupDestination: {
        type: DataTypes.STRING(200),
    },
    pickupGPS: {
        type: DataTypes.STRING(200),
    },
    dropoffDestination: {
        type: DataTypes.STRING(200),
    },
    dropoffGPS: {
        type: DataTypes.STRING(200),
    },
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    dataFrom: {
        type: DataTypes.STRING(20),
        defaultValue: 'MOBILE'
    },
    status: {
        type: DataTypes.STRING(20),
    },
    cancelledDateTime: {
        type: DataTypes.DATE
    },
    cancelledCause: {
        type: DataTypes.STRING(255),
    },
    cancelledBy: {
        type: DataTypes.BIGINT(20), 
    },
    groupId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    unitId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    approveDateTime: {
        type: DataTypes.DATE
    },
    approveBy: {
        type: DataTypes.BIGINT(20), 
    }
}, {
    timestamps: false,
})