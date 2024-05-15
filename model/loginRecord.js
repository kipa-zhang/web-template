const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.LoginRecord = dbConf.sequelizeObj.define('loginRecord', {
    userId: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
    },
    token: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    mobileType: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    appVersion: {
        type: DataTypes.STRING(55),
        allowNull: false,
    }
}, {
    // other options
    tableName: 'login_record',
    timestamps: true,
});
