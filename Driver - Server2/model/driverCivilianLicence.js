const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.DriverCivilianLicence = dbConf.sequelizeObj.define('driver_civilian_licence', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },  
    driverId: {
        type: DataTypes.INTEGER,
    },
    cardSerialNumber:  {
        type: DataTypes.STRING(50),
    },
    civilianLicence: {
        type: DataTypes.STRING(50),
    },
    dateOfIssue: {
        type: DataTypes.DATE,
    },
    status: {
        type: DataTypes.STRING(50),
    },
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
}, {
    tableName: 'driver_civilian_licence',
})