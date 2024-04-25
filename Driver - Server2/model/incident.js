const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Incident = dbConf.sequelizeObj.define('incident', {
    incidentNo: {
        type: DataTypes.STRING(55),
        primaryKey: true,
    },
    incidentName: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    incidentType: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    affectRoute: {
        type: DataTypes.TEXT,
        defaultValue: null,
    },
    userZoneId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    occTime: {
        type: DataTypes.DATE,
        allowNull: false,
    },
    endTime: {
        type: DataTypes.DATE,
    },
    blockPeriod: {
        type: DataTypes.INTEGER(5),
        allowNull: false,
        defaultValue: 0,
    },
    lat: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    lng: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    unitId: {
        type: DataTypes.INTEGER(11),
    },
    address: {
        type: DataTypes.STRING(55),
    },
    description: {
        type: DataTypes.TEXT,
    },
    images: {
        type: DataTypes.STRING(255),
    },
    imageBlobs: {
        type: DataTypes.BLOB('medium'),
    },
    state: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    userZoneId: {
        type: DataTypes.STRING(55),
    },
    affectRoute: {
        type: DataTypes.TEXT,
    },
    creator: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    onclickTime: {
        type: DataTypes.DATE,
    },
    receiveTime: {
        type: DataTypes.DATE,
    },
}, {
    tableName: 'incident',
    timestamps: true,
});
