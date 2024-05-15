const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.KeypressBoxDetailInfo = dbConf.sequelizeObj.define('keypressBoxDetailInfo', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },
    siteId: {
        type: DataTypes.STRING(8),
        defaultValue: null,
    },
    slotId: {
        type: DataTypes.TINYINT(4),
        defaultValue: null,
    },
    keyTagId: {
        type: DataTypes.STRING(64),
        defaultValue: null,
    },
    status: {
        type: DataTypes.STRING(10),
        defaultValue: 'in',
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    updatedAt: {
        type: DataTypes.DATE
    }
}, {
    tableName: 'keypress_box_detail_info',
    timestamps: false
});
