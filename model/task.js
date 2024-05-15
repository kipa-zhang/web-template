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
    routeNo: {
        type: DataTypes.STRING(45),
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
    purpose: {
        type: DataTypes.STRING(200),
    },
    pickupDestination: {
        type: DataTypes.STRING(200),
    },
    dropoffDestination: {
        type: DataTypes.STRING(200),
    },
    mobileStartTime: {
		type: DataTypes.DATE,
	},
    mobileEndTime: {
		type: DataTypes.DATE,
	},
    pickupGPS: {
        type: DataTypes.STRING(200),
    },
    dropoffGPS: {
        type: DataTypes.STRING(200),
    },
    routePoints: {
        type: DataTypes.TEXT
    },
    routeDistance: {
        type: DataTypes.FLOAT
    },
    routeTimeNeed: {
        type: DataTypes.FLOAT
    },
    routeNavigation: {
        type: DataTypes.TEXT
    },
    groupId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    taskReady: {
        type: DataTypes.VIRTUAL
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
    startLateReason: {
        type: DataTypes.TEXT,
    }
}, {
    tableName: 'task',
    timestamps: false,
})