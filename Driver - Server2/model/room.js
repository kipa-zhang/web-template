const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Room = dbConf.sequelizeObj.define('room', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },
    roomName: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
}, {
    tableName: 'user_room',
    timestamps: true,
});
