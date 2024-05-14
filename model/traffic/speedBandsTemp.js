const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.SpeedBandsTemp = dbConf.sequelizeObj.define('speed_bands_temp', {
    LinkID: {
        type: DataTypes.STRING(20),
        primaryKey: true,
    },
    RoadName: {
        type: DataTypes.STRING(200), 
    },
	RoadCategory: {
        type: DataTypes.STRING(10), 
    },
    SpeedBand: {
        type: DataTypes.INTEGER(10), 
    },
    MinimumSpeed: {
        type: DataTypes.STRING(10), 
    },
    MaximumSpeed: {
        type: DataTypes.STRING(10), 
    },
    StartLon: {
        type: DataTypes.STRING(50), 
    },
    StartLat: {
        type: DataTypes.STRING(50), 
    },
    EndLon: {
        type: DataTypes.STRING(50), 
    },
    EndLat: {
        type: DataTypes.STRING(50), 
    },
  }, {
    // other options
    timestamps: false
});
