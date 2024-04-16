const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.ModulePageWhite = dbConf.sequelizeObj.define('module_page_white', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    link: {
        type: DataTypes.STRING(200),
    }
  }, {
    // other options
    timestamps: false
});
