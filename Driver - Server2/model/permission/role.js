const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.Role = dbConf.sequelizeObj.define('role', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    roleName: {
        type: DataTypes.STRING(100), 
    },
    pageList: {
        type: DataTypes.STRING(100), 
    },
    creator: {
        type: DataTypes.INTEGER(11),
    },
    updater: {
        type: DataTypes.INTEGER(11),
    }
  }, {
    // other options
    timestamps: true
});
