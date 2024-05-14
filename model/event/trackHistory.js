const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.TrackHistory = dbConf.sequelizeObj.define('trackHistory', {
    deviceId: {
        type: DataTypes.STRING,
        allowNull: false,
		primaryKey: true,
    },
    violationType: {
        type: DataTypes.STRING,
        allowNull: false,
        primaryKey: true,
    },
    occTime: {
        type: DataTypes.DATE,
        allowNull: false,
        primaryKey: true,
    },
    zoneId: {
        type: DataTypes.INTEGER,
    },
    missingType: {
        type: DataTypes.STRING,
    },
    vehicleNo: {
        type: DataTypes.STRING,
        defaultValue: null,
    },
    taskId: {
        type: DataTypes.STRING,
        defaultValue: null,
    },
    dataFrom: {
        type: DataTypes.STRING,
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
    endSpeed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    startTime: {
        type: DataTypes.DATE,
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
    }
  }, {
    // other options
    tableName: 'track_history',
    timestamps: false,
    indexes: [
        {
            name: 'idx_deviceId_violationType_occTime',
            fields: ['deviceId', 'violationType', 'occTime']
        },
        {
            name: 'idx_deviceId_occTime',
            fields: ['deviceId', 'occTime']
        }
    ],
});
