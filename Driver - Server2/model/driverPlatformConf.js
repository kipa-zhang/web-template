const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverPlatformConf = dbConf.sequelizeObj.define('driver_platform_conf', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    permitType: {
        type: DataTypes.STRING(55),
    },
    vehicleType: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    baseMileage: {
        type: DataTypes.FLOAT(10, 2),
    },
    totalMileage: {
        type: DataTypes.FLOAT(10, 2),
    },
    lastDrivenDate: {
        type: DataTypes.DATE,
    },
    assessmentDate: {
        type: DataTypes.DATE,
    },
    approveStatus: {
        type: DataTypes.STRING(10),
        defaultValue: 'Edited',
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    creator: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    remarks: {
        type: DataTypes.STRING(255),
    },
    operation: {
        type: DataTypes.VIRTUAL
    }
}, {
    // other options
    tableName: 'driver_platform_conf',
    timestamps: false,
});
