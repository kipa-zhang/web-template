const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.ODD = dbConf.sequelizeObj.define('odd', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    taskId: {
        type: DataTypes.STRING(20), 
    },
    contentFrom: {
        type: DataTypes.STRING(255),
    },
    content: {
        type: DataTypes.TEXT,
        allowNull: false,
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    rectifyBy: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    rectifyAt: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    remarks: {
        type: DataTypes.STRING(255),
    },
}, {
    tableName: 'odd',
    timestamps: true,
});