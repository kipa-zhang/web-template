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
        allowNull: false,
    },
    activityName: {
        type: DataTypes.STRING(100),
    },
    unitId: {
        type: DataTypes.BIGINT(15),
        allowNull: false,
    },
    startDate: {
        type: DataTypes.DATE,
        allowNull: false,
    },
	endDate: {
        type: DataTypes.DATE,
        allowNull: false,
    },
	vehicleNumber: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
	vehicleType: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
	driverName: {
		type: DataTypes.STRING(255),
        allowNull: false,
	},
    driverId: {
        type: DataTypes.BIGINT(15),
        allowNull: false,
    },
	mobileNumber: {
		type: DataTypes.STRING(30),
        allowNull: false,
	},
	remarks: {
		type: DataTypes.TEXT,
        allowNull: false,
	},
    category: {
		type: DataTypes.STRING(100),
        allowNull: false,
	},
    serviceMode: {
		type: DataTypes.STRING(50),
        allowNull: false,
	},
    reportingLocation: {
		type: DataTypes.STRING(255),
        allowNull: false,
	},
    destination: {
		type: DataTypes.STRING(255),
        allowNull: false,
	},
    secured: {
        type: DataTypes.BOOLEAN,
        allowNull: false,
    },
    reportingLocationLat: {
        type: DataTypes.STRING(45),
        allowNull: false,
    },
    reportingLocationLng: {
        type: DataTypes.STRING(45),
        allowNull: false,
    },
    destinationLat: {
        type: DataTypes.STRING(45),
        allowNull: false,
    },
    destinationLng: {
        type: DataTypes.STRING(45),
        allowNull: false,
    },
    poc: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    arrivalTime: {
        type: DataTypes.DATE,
    },
    departTime: {
        type: DataTypes.DATE,
    },
    endTime: {
        type: DataTypes.DATE,
    },
}, {
    tableName: 'mt_admin',
    timestamps: false,
})