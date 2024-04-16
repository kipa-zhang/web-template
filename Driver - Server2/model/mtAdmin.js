const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.MtAdmin = dbConf.sequelizeObj.define('mt_admin', {
    id: {
        type: DataTypes.BIGINT(15), 
        primaryKey: true,
        autoIncrement: true,
    },
    purpose: {
        type: DataTypes.STRING(50),
    },
    activityName: {
        type: DataTypes.STRING(100),
    },
    unitId: {
        type: DataTypes.BIGINT(15),
    },
    startDate: {
        type: DataTypes.DATE,
    },
	endDate: {
        type: DataTypes.DATE,
    },
	vehicleNumber: {
        type: DataTypes.STRING(255),
    },
	vehicleType: {
        type: DataTypes.STRING(255),
    },
	driverName: {
		type: DataTypes.STRING(255),
	},
    driverId: {
        type: DataTypes.BIGINT(15),
    },
	mobileNumber: {
		type: DataTypes.STRING(30),
        defaultValue: ''
	},
	remarks: {
		type: DataTypes.TEXT,
	},
    category: {
		type: DataTypes.STRING(100),
	},
    serviceMode: {
		type: DataTypes.STRING(50),
	},
    reportingLocation: {
		type: DataTypes.STRING(255),
	},
    destination: {
		type: DataTypes.STRING(255),
	},
    reportingLocationLat: {
        type: DataTypes.STRING(45),
    },
    reportingLocationLng: {
        type: DataTypes.STRING(45),
    },
    destinationLat: {
        type: DataTypes.STRING(45),
    },
    destinationLng: {
        type: DataTypes.STRING(45),
    },
    poc: {
        type: DataTypes.STRING(255),
        defaultValue: ''
    },
    mbUnit: {
        type: DataTypes.STRING(128),
    },
    indentId: {
        type: DataTypes.STRING(128),
    },
    driverNum: {
        type: DataTypes.STRING(128),
        defaultValue: 1
    },
    needVehicle: {
        type: DataTypes.TINYINT(1),
        defaultValue: 1
    },
    dataType: {
        type: DataTypes.STRING(4),
        defaultValue: 'mt'
    },
    cancelledDateTime: {
        type: DataTypes.DATE
    },
    cancelledCause: {
        type: DataTypes.STRING(255),
    },
    amendedBy: {
        type: DataTypes.BIGINT(20), 
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
}, {
    tableName: 'mt_admin',
    timestamps: false,
})