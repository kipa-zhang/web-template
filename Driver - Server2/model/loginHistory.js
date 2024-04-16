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
    ip: {
        type: DataTypes.STRING(55),
    }
}, {
    // other options
    tableName: 'login_record',
    timestamps: true,
});
