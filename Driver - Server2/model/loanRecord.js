const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.loanRecord = dbConf.sequelizeObj.define('loan_record', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    indentId: {
        type: DataTypes.STRING(60),
        allowNull: null
    },
    taskId: {
        type: DataTypes.STRING(55),
        allowNull: null
    },
    driverId: {
        type: DataTypes.INTEGER,
        defaultValue: null
    },
    vehicleNo: {
        type: DataTypes.STRING(55),
        defaultValue: null
    },
    startDate: {
        type: DataTypes.DATE,
        defaultValue: null
    },
    endDate: {
        type: DataTypes.DATE,
        defaultValue: null
    },
    groupId: {
        type: DataTypes.INTEGER,
        allowNull: null
    },
    returnDate: {
        type: DataTypes.DATE,
        defaultValue: null
    },
    creator: {
        type: DataTypes.INTEGER,
        defaultValue: null
    },
    returnBy: {
        type: DataTypes.INTEGER,
        defaultValue: null
    },
    returnRemark: {
        type: DataTypes.STRING(55),
        defaultValue: null
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    actualStartTime: {
        type: DataTypes.DATE,
    },
    actualEndTime: {
        type: DataTypes.DATE,
    },
    unitId: {
        type: DataTypes.INTEGER,
        defaultValue: null,
    },
    purpose: {
        type: DataTypes.STRING(60),
        defaultValue: null
    },
    activity: {
        type: DataTypes.STRING(200),
        defaultValue: null
    }
}, {
    tableName: 'loan_record',
    timestamps: true,
});
