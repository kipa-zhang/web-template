const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.ModulePage = dbConf.sequelizeObj.define('module_page', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    parentId: {
        type: DataTypes.STRING(100), 
    },
    module: {
        type: DataTypes.STRING(100), 
    },
    page: {
        type: DataTypes.STRING(100), 
    },
    action: {
        type: DataTypes.STRING(100),
    },
    link: {
        type: DataTypes.STRING(200),
    }
  }, {
    // other options
    timestamps: true
});
