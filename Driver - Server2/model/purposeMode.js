const { DataTypes, QueryTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.PurposeMode = dbConf.sequelizeObj.define('purpose_mode', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    purposeName: {
        type: DataTypes.STRING(255),
        allowNull: false,
    }
}, {
    tableName: 'purpose_mode',
    timestamps: false,
});
