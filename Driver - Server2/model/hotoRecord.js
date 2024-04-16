const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.HOTORecord = dbConf.sequelizeObj.define('hoto_record', {
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
    returnBy: {
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
    tableName: 'hoto_record',
    timestamps: true,
});
