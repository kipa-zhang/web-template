const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.DriverOffenceHistory = dbConf.sequelizeObj.define('driverOffenceHistory', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    vehicleNo: {
        type: DataTypes.STRING,
        defaultValue: null,
    },
    lat: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    lng: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    speed: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    rpm: {
        type: DataTypes.INTEGER(5),
        defaultValue: 0
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    }
}, {
    // other options
    tableName: 'driver_offence_history',
    timestamps: false,
    indexes: [
        {
            name: 'index_of_driverId_createdAt',
            fields: ['driverId', 'createdAt']
        }
    ],
});
