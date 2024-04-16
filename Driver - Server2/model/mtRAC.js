const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.MT_RAC = dbConf.sequelizeObj.define('mt_rac', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    taskId: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    riskAssessment: {
        type: DataTypes.STRING(255),
    },
    riskLevel: {
        type: DataTypes.STRING(55),
    }, 
    driverDeclaration: {
        type: DataTypes.STRING(255),
    },
    needCommander: {
        type: DataTypes.TINYINT(1),
        default: 0,
    },
    commander: {
        type: DataTypes.STRING(100),
    },
    commanderContactNumber: {
        type: DataTypes.INTEGER(15),
    },
    commanderSignature: {
        type: DataTypes.BLOB,
    },
    commanderSignatureDateTime: {
        type: DataTypes.DATE,
    },
    officer: {
        type: DataTypes.STRING(100),
    },
    officerSignature: {
        type: DataTypes.BLOB,
    },
    officerSignatureDateTime: {
        type: DataTypes.DATE,
    },
    mitigation: {
        type: DataTypes.STRING(100),
    },
    submittedBy: {
        type: DataTypes.INTEGER(11),
    },
    submittedDateTime: {
        type: DataTypes.DATE,
    },
}, {
    tableName: 'mt_rac',
    timestamps: true,
});
