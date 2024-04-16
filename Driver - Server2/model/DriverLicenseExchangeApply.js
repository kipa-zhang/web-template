const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.DriverLicenseExchangeApply = dbConf.sequelizeObj.define('licenseExchangeApply', {
    applyId: {
        type: DataTypes.BIGINT, 
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
    },
    driverName: {
        type: DataTypes.STRING(255),
    },
    nric: {
        type: DataTypes.STRING(8),
    },
    permitType: {
        type: DataTypes.STRING(255),
    },
    permitTypeMileage: {
        type: DataTypes.STRING(128),
    },
    enlistmentDate: {
        type: DataTypes.DATE,
    },
    birthday: {
        type: DataTypes.DATE, 
    },
    driverDemeritPoints: {
        type: DataTypes.TINYINT, 
        defaultValue: 0
    },
    permitTypeDemeritPoints: {
        type: DataTypes.TINYINT, 
        defaultValue: 0
    },
    applyDate: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    approveDate: {
        type: DataTypes.DATE,
    },
    emailConfirm: {
        type: DataTypes.STRING(64),
    },
    approveBy: {
        type: DataTypes.INTEGER(12)
    },
    status: {
        type: DataTypes.STRING(20),
        defaultValue: 'Pending Approval',
    },
    creator: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    submitDate: {
        type: DataTypes.DATE,
    },
    submitBy: {
        type: DataTypes.INTEGER(12)
    },
    endorseDate: {
        type: DataTypes.DATE,
    },
    endorseBy: {
        type: DataTypes.INTEGER(12)
    },
    verifyDate: {
        type: DataTypes.DATE,
    },
    verifyBy: {
        type: DataTypes.INTEGER(12)
    },
    recommendDate: {
        type: DataTypes.DATE,
    },
    recommendBy: {
        type: DataTypes.INTEGER(12)
    },
    rejectDate: {
        type: DataTypes.DATE,
    },
    rejectBy: {
        type: DataTypes.INTEGER(12)
    },
    rejectReason: {
        type: DataTypes.STRING(256)
    },
    successDate: {
        type: DataTypes.DATE,
    },
    successBy: {
        type: DataTypes.INTEGER(12)
    },
    failDate: {
        type: DataTypes.DATE,
    },
    failBy: {
        type: DataTypes.INTEGER(12)
    },
    failReason: {
        type: DataTypes.STRING(256)
    },
    pendingDate: {
        type: DataTypes.DATE,
    },
    pendingBy: {
        type: DataTypes.INTEGER(12)
    },
    pendingReason: {
        type: DataTypes.STRING(256)
    }
}, {
    tableName: 'driver_license_exchange_apply',
    timestamps: false,
})