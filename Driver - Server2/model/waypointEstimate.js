const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.WaypointEstimate = dbConf.sequelizeObj.define('waypointEstimate', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    waypointId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    arriveTime: {
        type: DataTypes.DATE,
    },
    estimateTime: {
        type: DataTypes.DATE,
    }
}, {
    // other options
    tableName: 'waypoint_estimate',
    timestamps: true,
});
