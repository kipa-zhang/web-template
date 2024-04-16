const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.IncidentDetail = dbConf.sequelizeObj.define('incident_detail', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    sosId: {
        type: DataTypes.INTEGER(11),
        allowNull: null
    },
    description: {
        type: DataTypes.STRING(255),
        defaultValue: null
    },
    followUpActions: {
        type: DataTypes.STRING(255),
        defaultValue: null
    },
    negligence: {
        type: DataTypes.INTEGER(11),
        allowNull: null
    },
    closedOn: {
        type: DataTypes.STRING(255),
        defaultValue: null
    },
    locationOfIncident: {
        type: DataTypes.STRING(25),
        allowNull: null
    },
    locationType: {
        type: DataTypes.STRING(25),
        allowNull: null
    },
    local: {
        type: DataTypes.INTEGER(11),
        allowNull: null
    },
    weekRange: {
        type: DataTypes.STRING(25),
        allowNull: null
    },
    detailTime: {
        type: DataTypes.TIME,
        allowNull: null
    },
    weekNumber: {
        type:  DataTypes.INTEGER(11),
        allowNull: null
    },
    monthNumber: {
        type:  DataTypes.INTEGER(11),
        allowNull: null
    },
    workYear: {
        type:  DataTypes.STRING(25),
        allowNull: null
    },
    incidentPeakHour: {
        type:  DataTypes.STRING(25),
        allowNull: null
    },
    incidentTime: {
        type:  DataTypes.STRING(25),
        allowNull: null
    },
    weather: {
        type:  DataTypes.STRING(25),
        defaultValue: null
    },
    trafficCondition: {
        type:  DataTypes.STRING(25),
        defaultValue: null
    },
    typeOfDetail: {
        type:  DataTypes.STRING(25),
        defaultValue: null
    },
    secondOrderOfDetail: {
        type:  DataTypes.STRING(40),
        defaultValue: null
    },
    directionOfMovement: {
        type:  DataTypes.STRING(25),
        defaultValue: null
    },
    typeOfManoeuvre: {
        type:  DataTypes.STRING(25),
        defaultValue: null
    },
    locationOflmpact: {
        type:  DataTypes.STRING(25),
        defaultValue: null
    },
    lssueDemeritPoints: {
        type:  DataTypes.STRING(25),
        defaultValue: null
    },
    suspensionPeriod: {
        type:  DataTypes.STRING(40),
        defaultValue: null
    },
    creator: {
        type: DataTypes.INTEGER,
        defaultValue: null
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    }
}, {
    tableName: 'incident_detail',
    timestamps: true,
});
