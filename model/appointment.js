const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Appointment = dbConf.sequelizeObj.define('appointment', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    appointmentType: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    appointmentName: {
        type: DataTypes.STRING(255),
        allowNull: false,
    }
}, {
    // other options
    tableName: 'appointment',
    timestamps: false,
});
