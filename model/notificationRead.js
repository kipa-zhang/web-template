const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.NotificationRead = dbConf.sequelizeObj.define('notification_read', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    notificationId: {
        type: DataTypes.INTEGER(11), 
        allowNull: false,
    },
    userId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
  }, {
    // other options
    tableName: 'notification_read',
    timestamps: true,
});
