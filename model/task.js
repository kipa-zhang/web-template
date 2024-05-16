const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Task = dbConf.sequelizeObj.define('task', {
    taskId: {
        type: DataTypes.STRING(20), 
        primaryKey: true,
    },
    driverId: {
        type: DataTypes.BIGINT(15),
    },
    vehicleNumber: {
        type: DataTypes.STRING(15),
    },
	driverStatus: {
        type: DataTypes.STRING(55),
    },
	vehicleStatus: {
        type: DataTypes.STRING(55),
    },
	indentId: {
        type: DataTypes.STRING(15),
    },
	indentStartTime: {
		type: DataTypes.DATE,
	},
    indentEndTime: {
		type: DataTypes.DATE,
	},
    mobileStartTime: {
		type: DataTypes.DATE,
	},
    mobileEndTime: {
		type: DataTypes.DATE,
	},
    purpose: {
        type: DataTypes.STRING(200),
    },
    activity: {
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
    routePoints: {
        type: DataTypes.BLOB
    },
    routeDistance: {
        type: DataTypes.FLOAT
    },
    routeTimeNeed: {
        type: DataTypes.FLOAT
    },
    routeNavigation: {
        type: DataTypes.BLOB
    },
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    hub: {
        type: DataTypes.STRING(50),
    },
    node: {
        type: DataTypes.STRING(50),
    },
    dataFrom: {
        type: DataTypes.STRING(20),
    },
    reassignReasons: {
        type: DataTypes.STRING(50),
    },
    reassignRemarks : {
        type: DataTypes.STRING(200),
    },
    reassignAt: {
        type: DataTypes.DATE,
    },
}, {
    tableName: 'task',
    timestamps: false,
})