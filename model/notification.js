const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Notification = dbConf.sequelizeObj.define('notification', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    title: {
        type: DataTypes.STRING(255), 
        allowNull: false,
    },
    type: {
        type: DataTypes.STRING(255), 
        allowNull: false,
    },
	creator: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    startDateTime: {
        type: DataTypes.DATE,
        allowNull: false,
    },
	endDateTime: {
        type: DataTypes.DATE,
        allowNull: false,
    },
    scheduledTime: {
        type: DataTypes.TIME,
    },
    description: {
        type: DataTypes.TEXT, 
    },
    link: {
        type: DataTypes.TEXT, 
    },
    coverImage: {
        type: DataTypes.STRING(255),
    },
    coverImageBase64: {
        type: DataTypes.BLOB,
    },
    mainImage: {
        type: DataTypes.STRING(255), 
    },
    mainImageBase64: {
        type: DataTypes.BLOB,
    },
    laptopHubNodeList: {
        type: DataTypes.TEXT, 
    },
    driverHubNodeList: {
        type: DataTypes.TEXT, 
    },
    deleted: {
        type: DataTypes.TINYINT,
        defaultValue: 0 
    },
    sended: {
        type: DataTypes.TINYINT,
        defaultValue: 0 
    },
    toCategory: {
        type: DataTypes.STRING(55), 
    }, 
    toType: {
        type: DataTypes.STRING(55), 
    },
    platform: {
        type: DataTypes.STRING(255), 
    },
    groupId: {
        type: DataTypes.STRING(255), 
    },
  }, {
    // other options
    tableName: 'notification',
    timestamps: true,
});
