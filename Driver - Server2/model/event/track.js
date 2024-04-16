const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.Track = dbConf.sequelizeObj.define('track', {
    deviceId: {
        type: DataTypes.STRING(55),
        allowNull: false,
		primaryKey: true,
    },
    violationType: {
        type: DataTypes.STRING(55),
        allowNull: false,
        primaryKey: true,
    },
    occTime: {
        type: DataTypes.DATE,
        allowNull: false,
        primaryKey: true,
    },
	vehicleNo: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
	count: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0,
    },
    dataFrom: {
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
	speed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
	startSpeed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    startTime: {
        type: DataTypes.DATE,
    },
    endSpeed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    endTime: {
        type: DataTypes.DATE,
    },
    diffSecond: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    stayTime: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    accSpeed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    decSpeed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    
	lastOccTime: {
        type: DataTypes.DATE,
    }
  }, {
    // other options
    timestamps: false,
    indexes: [
        {
            name: 'idx_deviceId_violationType',
            fields: ['deviceId', 'violationType']
        }
    ],
});
