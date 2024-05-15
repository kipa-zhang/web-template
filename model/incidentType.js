const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.IncidentType = dbConf.sequelizeObj.define('incidentType', {
    id: {
        type: DataTypes.INTEGER(2),
        primaryKey: true,
    },
    incidentType: {
        type: DataTypes.STRING(55),
        allowNull: false,
    }
}, {
    tableName: 'incident_type',
    timestamps: false,
});
