const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.UserNotice = dbConf.sequelizeObj.define('userNotice', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    userId: {
        type: DataTypes.INTEGER(11), 
    },
    routeNo: {
        type: DataTypes.STRING(255),
    },
    content: {
        type: DataTypes.STRING(255),
    },
}, {
    // other options
    tableName: 'user_notice',
    timestamps: true,
});
