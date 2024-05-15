const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.OperationRecord = dbConf.sequelizeObj.define('operationRecord', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    operatorId: {
        type: DataTypes.BIGINT(15),
    },
    businessType: {
        type: DataTypes.STRING(128),
        defaultValue: 'common'
    },
    businessId: {
        type: DataTypes.STRING(32),
    },
    optType: {
        type: DataTypes.STRING(32),
        defaultValue: 'common'
    },
    beforeData: {
        type: DataTypes.TEXT,
    },
    afterData: {
        type: DataTypes.TEXT,
    },
	optTime: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),  
    },
    remarks: {
        type: DataTypes.STRING(200),
    }
}, {
    tableName: 'operation_record',
    timestamps: false,
})