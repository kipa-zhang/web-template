const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.DriverPositionHistory = dbConf.sequelizeObj.define('driver_position_history', {
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
        defaultValue: DataTypes.NOW,
        comment: 'created from mobile'
    },
    gpsTime: {
        type: DataTypes.DATE,
        comment: 'time while get gps'
    },
    receiveTime: {
        type: DataTypes.DATE,
        comment: 'time while server receive data'
    },
    gpsPermission: {
        type: DataTypes.TINYINT,
        comment: '0: not allow, 1: allow (Always be 1 here, only has permission can update here)',
        defaultValue: 0,
    },
    gpsService: {
        type: DataTypes.TINYINT,
        comment: '0: closed, 1: open',
        defaultValue: 0,
    },
    network: {
        type: DataTypes.TINYINT,
        comment: '0: closed, 1: open',
        defaultValue: 0,
    },
}, {
    // other options
    tableName: 'driver_position_history',
    timestamps: false,
});
