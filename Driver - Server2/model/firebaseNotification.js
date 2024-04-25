const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.FirebaseNotification = dbConf.sequelizeObj.define('firebase_notification', {
    id: {
        type: DataTypes.BIGINT,
        autoIncrement: true,
        primaryKey: true,
    },
    taskId: {
        type: DataTypes.STRING(20),
    },
    driverId: {
        type: DataTypes.INTEGER,
    },
    vehicleNo: {
        type: DataTypes.STRING(55),
    },
    type: {
        type: DataTypes.STRING(255),
    },
    title: {
        type: DataTypes.STRING(55),
    },
    content: {
        type: DataTypes.STRING(255),
    },
    success: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0,
    },
}, {
    timestamps: true,
});
