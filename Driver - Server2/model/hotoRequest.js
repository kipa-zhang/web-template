const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.HOTORequest = dbConf.sequelizeObj.define('hoto_request', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    activityName: {
        type: DataTypes.STRING(100),
        defaultValue: null
    },
    purpose: {
        type: DataTypes.STRING(100),
        allowNull: null
    },
    hub: {
        type: DataTypes.STRING(50),
        defaultValue: null
    },
    node: {
        type: DataTypes.STRING(50),
        defaultValue: null
    },
    startTime: {
        type: DataTypes.DATE,
        allowNull: null
    },
    endTime: {
        type: DataTypes.DATE,
        allowNull: null
    },
    resource: {
        type: DataTypes.STRING(26),
        allowNull: null
    },
    resourceQty: {
        type: DataTypes.INTEGER,
        allowNull: null
    },
    explanation: {
        type: DataTypes.STRING(255),
        defaultValue: null
    },
    status: {
        type: DataTypes.STRING(30),
        defaultValue: null
    },
    vehicleType: {
        type: DataTypes.STRING(55),
        defaultValue: null
    },
    creator: {
        type: DataTypes.INTEGER,
        allowNull: null
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    }
}, {
    tableName: 'hoto_request',
    timestamps: true,
});
