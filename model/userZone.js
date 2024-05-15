const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.UserZone = dbConf.sequelizeObj.define('userZone', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    zoneName: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    polygon: {
        type: DataTypes.TEXT,
        allowNull: false,
    },
    color: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    owner: {
        type: DataTypes.INTEGER(11), 
        allowNull: false,
    },
}, {
    // other options
    tableName: 'user_zone',
    timestamps: true,
});
