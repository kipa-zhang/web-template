const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.HOTO = dbConf.sequelizeObj.define('hoto', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    vehicleNo: {
        type: DataTypes.STRING(255),
    },
    driverId: {
        type: DataTypes.STRING(255),
    },
    fromHub: {
        type: DataTypes.STRING(255),
    },
    fromNode: {
        type: DataTypes.STRING(255),
    },
    toHub: {
        type: DataTypes.STRING(255),
    },
    toNode: {
        type: DataTypes.STRING(255),
    },
    hotoDateTime: {
        type: DataTypes.DATE,
        allowNull: null,
    },
    returnDateTime: {
        type: DataTypes.DATE,
    },
    startDateTime: {
        type: DataTypes.DATE,
    },
    endDateTime: {
        type: DataTypes.DATE,
    },
    unitId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    status: {
        type: DataTypes.STRING(30),
        defaultValue: null,
    },
    requestId: {
        type: DataTypes.INTEGER,
        defaultValue: null,
    },
    creator: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    }
}, {
    tableName: 'hoto',
    timestamps: true,
});
