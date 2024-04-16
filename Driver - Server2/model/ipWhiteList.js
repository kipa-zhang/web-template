const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.SystemIPWhiteList = dbConf.sequelizeObj.define('systemIPWhitelist', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    ip: {
        type: DataTypes.STRING(32),
        allowNull: false,
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    creator: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
}, {
    // other options
    tableName: 'system_ip_whitelist',
    timestamps: false,
});
