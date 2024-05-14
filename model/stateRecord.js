const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.StateRecord = dbConf.sequelizeObj.define('stateRecord', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    userId: {
        type: DataTypes.INTEGER(11), 
    },
    state: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    content: {
        type: DataTypes.STRING(255),
    },
}, {
    // other options
    tableName: 'state_record',
    timestamps: true,
});
