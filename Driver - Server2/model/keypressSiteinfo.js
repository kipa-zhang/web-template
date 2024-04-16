const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.KeypressSiteinfo = dbConf.sequelizeObj.define('keypressSiteinfo', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },
    siteId: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    unitId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    encryptionKey: {
        type: DataTypes.STRING(32),
    },
    locationName: {
        type: DataTypes.STRING(150),
        defaultValue: null,
    },
    boxName: {
        type: DataTypes.STRING(150),
        defaultValue: null,
    },
    type: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
    },
}, {
    tableName: 'keypress_site_info',
    timestamps: false
});
