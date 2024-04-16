const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.loan = dbConf.sequelizeObj.define('loan', {
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
    creator: {
        type: DataTypes.INTEGER,
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
    tableName: 'loan',
    timestamps: true,
});
