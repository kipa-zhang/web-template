const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');

module.exports.Group = sequelizeSystemObj.define('group', {
    id: {
        type: DataTypes.BIGINT,
        autoIncrement: true,
        primaryKey: true,
    },
    groupName: {
        type: DataTypes.STRING(200),
    },
    serviceType: {
        type: DataTypes.STRING(200),
    },
}, {
    timestamps: false,
});