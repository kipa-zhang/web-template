const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.RiskAssessment = dbConf.sequelizeObj.define('risk_assessment', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    riskType: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    assessment: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    level: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
}, {
    tableName: 'risk_assessment',
    timestamps: true,
});
