const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Vehicle = dbConf.sequelizeObj.define('vehicle', {
    vehicleNo: {
        type: DataTypes.STRING(55),
        primaryKey: true,
    },
    unitId: {
        type: DataTypes.INTEGER(11),
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
    nextAviTime: {
		type: DataTypes.DATE,
	},
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    status: {
        type: DataTypes.STRING(20),
    },
    limitSpeed: {
        type: DataTypes.INTEGER,
        defaultValue: 60,
    },
}, {
    // other options
    tableName: 'vehicle',
    timestamps: true,
});
