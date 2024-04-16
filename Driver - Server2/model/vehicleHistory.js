const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.VehicleHistory = dbConf.sequelizeObj.define('vehicle_history', {
    vehicleNo: {
        type: DataTypes.STRING(55),
        primaryKey: true,
    },
    unitId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    groupId: {
        type: DataTypes.STRING(55),
    },
    vehicleCategory: {
        type: DataTypes.STRING(55),
    },
    deviceId: {
        type: DataTypes.STRING(45),
        defaultValue: null,
    },
    vehicleType: {
        type: DataTypes.STRING(55),
    },
    permitType: {
        type: DataTypes.STRING(255),
    },
    vin: {
        type: DataTypes.STRING(55),
    },
    dimensions: {
        type: DataTypes.STRING(100),
    },
    totalMileage: {
        type: DataTypes.INTEGER(12),
        defaultValue: 0,
    },
    keyTagId: {
        type: DataTypes.STRING(64),
        defaultValue: null,
    },
    nextWpt1Time: {
		type: DataTypes.DATE,
	},
    wpt1CompleteTime: {
		type: DataTypes.DATE,
	},
    nextWpt2Time: {
		type: DataTypes.DATE,
	},
    wpt2CompleteTime: {
		type: DataTypes.DATE,
	},
    nextWpt3Time: {
		type: DataTypes.DATE,
	},
    wpt3CompleteTime: {
		type: DataTypes.DATE,
	},
    nextMptTime: {
		type: DataTypes.DATE,
	},
    nextPmTime: {
		type: DataTypes.DATE,
	},
    pmMaxMileage: {
        type: DataTypes.FLOAT(20, 1),
    },
    pmCycleMonth: {
        type: DataTypes.INTEGER(12),
    },
    nextAviTime: {
		type: DataTypes.DATE,
	},
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    status: {
        type: DataTypes.STRING(20),
        defaultValue: 'Deployable'
    },
    overrideStatus: {
        type: DataTypes.STRING(20),
    },
    overrideStatusTime: {
        type: DataTypes.DATE,
    },
    limitSpeed: {
        type: DataTypes.INTEGER,
        defaultValue: 60,
    }
}, {
    // other options
    tableName: 'vehicle_history',
    timestamps: true,
});
