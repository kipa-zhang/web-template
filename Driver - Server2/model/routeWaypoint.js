const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.RouteWaypoint = dbConf.sequelizeObj.define('routeWaypoint', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },
    routeNo: {
        type: DataTypes.STRING(55), 
        allowNull: false,
    },
    waypointId: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
}, {
    // other options
    tableName: 'route_waypoint',
    timestamps: false,
});
