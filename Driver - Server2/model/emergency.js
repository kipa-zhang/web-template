const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.Emergency = dbConf.sequelizeObj.define('emergency', {
    driverId: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
    },
    username: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    relationship: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    nric: {
        type: DataTypes.STRING(8),
    },
    contactNumber: {
        type: DataTypes.STRING(55),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
}, {
    // other options
    tableName: 'emergency',
    timestamps: false,
});
