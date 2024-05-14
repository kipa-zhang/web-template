const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Friend = dbConf.sequelizeObj.define('friend', {
    driverId: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
    },
    friendId: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
    },
}, {
    tableName: 'friend',
    timestamps: false,
});
