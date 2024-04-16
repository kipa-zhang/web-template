const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');

module.exports.PurchaseOrder = sequelizeSystemObj.define('purchase_order', {
    taskId: {
        type: DataTypes.BIGINT,
        primaryKey: true,
        allowNull: false,
    },
    jobId: {
        type: DataTypes.BIGINT,
    },
    tripPrice: {
        type: DataTypes.STRING(35),
    },
    hourlyPrice: {
        type: DataTypes.STRING(35),
    },
    isPeak: {
        type: DataTypes.BOOLEAN,
    },
    isLate: {
        type: DataTypes.BOOLEAN,
    },
    isWeekend: {
        type: DataTypes.BOOLEAN,
    },
    hasDriver: {
        type: DataTypes.BOOLEAN,
    },
    blockPeriod: {
        type: DataTypes.STRING(35),
    },
    blockPrice: {
        type: DataTypes.STRING(35),
    },
    blockHourly: {
        type: DataTypes.STRING(35),
    },
    OTBlockPeriod: {
        type: DataTypes.STRING(35),
    },
    OTBlockPrice: {
        type: DataTypes.STRING(35),
    },
    OTHourly: {
        type: DataTypes.STRING(35),
    },
    dailyPrice: {
        type: DataTypes.STRING(35),
    },
    weeklyPrice: {
        type: DataTypes.STRING(35),
    },
    monthlyPrice: {
        type: DataTypes.STRING(35),
    },
    yearlyPrice: {
        type: DataTypes.STRING(35),
    },
    transportCost: {
        type: DataTypes.STRING(35),
    },
    surchargeLessThen48: {
        type: DataTypes.STRING(35),
    },
    surchargeGenterThen12: {
        type: DataTypes.STRING(35),
    },
    surchargeLessThen12: {
        type: DataTypes.STRING(35),
    },
    surchargeLessThen4: {
        type: DataTypes.STRING(35),
    },
    surchargeDepart: {
        type: DataTypes.STRING(35),
    },
    transCostSurchargeLessThen4: {
        type: DataTypes.STRING(35),
    },
    total: {
        type: DataTypes.STRING(35),
    }
}, {
    timestamps: true,
});

module.exports.InitialPurchaseOrder = sequelizeSystemObj.define('initial_purchase_order', {
    taskId: {
        type: DataTypes.BIGINT,
        primaryKey: true,
        allowNull: false,
    },
    jobId: {
        type: DataTypes.BIGINT,
    },
    tripPrice: {
        type: DataTypes.STRING(35),
    },
    hourlyPrice: {
        type: DataTypes.STRING(35),
    },
    isPeak: {
        type: DataTypes.BOOLEAN,
    },
    isLate: {
        type: DataTypes.BOOLEAN,
    },
    isWeekend: {
        type: DataTypes.BOOLEAN,
    },
    hasDriver: {
        type: DataTypes.BOOLEAN,
    },
    blockPeriod: {
        type: DataTypes.STRING(35),
    },
    blockPrice: {
        type: DataTypes.STRING(35),
    },
    blockHourly: {
        type: DataTypes.STRING(35),
    },
    OTBlockPeriod: {
        type: DataTypes.STRING(35),
    },
    OTBlockPrice: {
        type: DataTypes.STRING(35),
    },
    OTHourly: {
        type: DataTypes.STRING(35),
    },
    dailyPrice: {
        type: DataTypes.STRING(35),
    },
    weeklyPrice: {
        type: DataTypes.STRING(35),
    },
    monthlyPrice: {
        type: DataTypes.STRING(35),
    },
    yearlyPrice: {
        type: DataTypes.STRING(35),
    },
    transportCost: {
        type: DataTypes.STRING(35),
    },
    surchargeLessThen48: {
        type: DataTypes.STRING(35),
    },
    surchargeGenterThen12: {
        type: DataTypes.STRING(35),
    },
    surchargeLessThen12: {
        type: DataTypes.STRING(35),
    },
    surchargeLessThen4: {
        type: DataTypes.STRING(35),
    },
    surchargeDepart: {
        type: DataTypes.STRING(35),
    },
    transCostSurchargeLessThen4: {
        type: DataTypes.STRING(35),
    },
    total: {
        type: DataTypes.STRING(35),
    }
}, {
    timestamps: true,
});