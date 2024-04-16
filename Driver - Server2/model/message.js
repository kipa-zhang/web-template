const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Message = dbConf.sequelizeObj.define('message', {
    id: {
        type: DataTypes.INTEGER(11),
        autoIncrement: true,
        primaryKey: true,
    },
    fromUser: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    toUser: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    chatType: {
        type: DataTypes.STRING(55),
    },
    messageType: {
        type: DataTypes.STRING(55),
    },
    content: {
        type: DataTypes.TEXT,
    },
    contentSize: {
        type: DataTypes.INTEGER(5),
    },
    messageTime: {
        type: DataTypes.DATE,
    },
    received: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0,
    },
    read: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0,
    },
}, {
    tableName: 'message',
    timestamps: true,
});
