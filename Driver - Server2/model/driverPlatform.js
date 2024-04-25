const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.DriverPlatform = dbConf.sequelizeObj.define('driver_platform', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        allowNull: false,
    },
    vehicleNo: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    taskId: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    permitType: {
        type: DataTypes.STRING(55),
    },
    vehicleType: {
        type: DataTypes.STRING(55),
    },
    totalMileage: {
        type: DataTypes.FLOAT(10, 2),
    },
    tripDate: {
        type:DataTypes.DATE,
    }
}, {
    // other options
    tableName: 'driver_platform',
});
