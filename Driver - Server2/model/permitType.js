const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.PermitType = dbConf.sequelizeObj.define('permitType', {
    permitType: {
        type: DataTypes.STRING(55),
        primaryKey: true,
    },
    vehicleType: {
        type: DataTypes.INTEGER(2000),
        defaultValue: ''
    },
    parent: {
        type: DataTypes.STRING(55),
        defaultValue: ''
    },
    eligibilityMileage: {
        type: DataTypes.FLOAT(20, 1),
    }
}, {
    // other options
    tableName: 'permitType',
    timestamps: false,
});
