const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

const CONTENT = require('../util/content');
const utils = require('../util/utils');

module.exports.Route = dbConf.sequelizeObj.define('route', {
    routeNo: {
        type: DataTypes.STRING(55), 
        primaryKey: true,
    },
    routeName: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    fromAddress: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    toAddress: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    fromPosition: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    toPosition: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    state: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    reRouted: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0,
    },
    reRoutedByIncidentNo: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    line: {
        type: DataTypes.TEXT,
        defaultValue: null,
    },
    lineColor: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    navigation: {
        type: DataTypes.TEXT,
        defaultValue: null,
    },
    distance: {
        type: DataTypes.INTEGER(5),
        defaultValue: null,
    },
    timeNeed: {
        type: DataTypes.INTEGER(5),
        defaultValue: null,
    },
    userZoneId: {
        type: DataTypes.INTEGER(5),
        defaultValue: null,
    },
    routeInfo: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    creator: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    updater: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
}, {
    // other options
    tableName: 'route',
    timestamps: true,
});
