const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Waypoint = dbConf.sequelizeObj.define('waypoint', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    waypointName: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    lat: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    lng: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    type: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0
    },
    owner: {
        type: DataTypes.INTEGER(11),
        defaultValue: 0
    },
    needCalculate: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0
    },
    tips: {
        type: DataTypes.STRING(55),
    },
    creator: {
        type: DataTypes.INTEGER(12),
        defaultValue: 0
    }
}, {
    // other options
    tableName: 'waypoint',
    timestamps: true,
});
