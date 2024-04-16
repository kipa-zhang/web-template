const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.RoomMember = dbConf.sequelizeObj.define('roomMember', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },
    roomId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    roomMember: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
}, {
    tableName: 'user_room_member',
    timestamps: true,
});
