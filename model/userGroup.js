const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.UserGroup = dbConf.sequelizeObj.define('userGroup', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },
    groupName: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    userId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
}, {
    tableName: 'user_group',
    timestamps: true,
});
