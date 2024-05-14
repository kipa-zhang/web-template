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
    deviceId: {
        type: DataTypes.STRING(55),
    },
    vehicleCategory: {
        type: DataTypes.STRING(55),
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
    nextWpt1Time: {
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
    overrideStatus: {
        type: DataTypes.STRING(20),
    },
    overrideStatusTime: {
        type: DataTypes.DATE,
    },
    limitSpeed: {
        type: DataTypes.INTEGER,
        defaultValue: 60,
    },
    onhold: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
    },
}, {
    // other options
    tableName: 'vehicle',
    timestamps: true,
});
