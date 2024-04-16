const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.EventRecord = dbConf.sequelizeObj.define('eventRecord', {
    description: {
        type: DataTypes.STRING(255), 
        primaryKey: true,
    },
    createdAt: {
        type: DataTypes.DATE, 
    },
}, {
    // other options
    tableName: 'event_record',
    timestamps: false,
});
