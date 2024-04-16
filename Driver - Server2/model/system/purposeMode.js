const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf_system');

module.exports.PurposeMode = dbConf.sequelizeSystemObj.define('purpose_mode', {
    id: {
        type: DataTypes.BIGINT,
        primaryKey: true,
        autoIncrement: true,
    },
    name: {
        type: DataTypes.STRING(100),
    },
    isMandatory: {
        type: DataTypes.BOOLEAN,
    }
}, {
    timestamps: false,
});