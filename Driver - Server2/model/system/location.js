const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');

module.exports.Location = sequelizeSystemObj.define('location', {
    id: {
        type: DataTypes.BIGINT,
        primaryKey: true,
        autoIncrement: true
    },
    locationName: {
        type: DataTypes.STRING(150),
        allowNull: false,
    },
    belongTo: {
        type: DataTypes.STRING(50),
    },
    secured: {
        type: DataTypes.BOOLEAN,
        allowNull: false,
    },
    lat: {
        type: DataTypes.STRING(45)
    },
    lng: {
        type: DataTypes.STRING(45)
    },
    zip: {
        type: DataTypes.STRING(100)
    },
    country: {
        type: DataTypes.STRING(100)
    },
}, {
    timestamps: false,
});