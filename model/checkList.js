const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.CheckList = dbConf.sequelizeObj.define('check_list', {
    id: {
        type: DataTypes.BIGINT,
        primaryKey: true,
    },
    taskId: {
        type: DataTypes.STRING(20),
    },
    indentId: {
        type: DataTypes.STRING(100),
    },
    driverId: {
        type: DataTypes.INTEGER,
    },
    vehicleNo: {
        type: DataTypes.STRING(55),
    },
    checkListName: {
        type: DataTypes.STRING(55),
    },
}, {
    timestamps: true,
});
