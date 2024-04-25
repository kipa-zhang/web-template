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
    // node: {
    //     type: DataTypes.STRING(100)
    // },
    // hub: {
    //     type: DataTypes.STRING(100)
    // },
}, {
    timestamps: false,
});