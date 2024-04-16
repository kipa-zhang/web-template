const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverAssessmentRecord = dbConf.sequelizeObj.define('driverAssessmentRecord', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    assessmentType: {
        type: DataTypes.STRING(64),
    },
    issueDate: {
        type: DataTypes.STRING(16),
    },
    /**
     * Pass, Fail
     * default value: fail
     */
    status: {
        type: DataTypes.STRING(16),
        defaultValue: 'Fail',
        comment: `Pass, Fail`
    },
    approveStatus: {
        type: DataTypes.STRING(10),
        defaultValue: 'Edited',
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    creator: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
    remarks: {
        type: DataTypes.STRING(128),
    },
}, {
    // other options
    tableName: 'driver_assessment_record',
    timestamps: true,
});
