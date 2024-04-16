const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.SystemConf = dbConf.sequelizeObj.define('systemConf', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    groupName: {
        type: DataTypes.STRING(45), 
    },
    mobileUploadPositionFrequency: {
        type: DataTypes.INTEGER(5),
        defaultValue: 10,
    },
    mobileReceiveIncidentFrequency: {
        type: DataTypes.INTEGER(5),
        defaultValue: 10,
    },
    mobilePeerUnitFrequency: {
        type: DataTypes.INTEGER(5),
        defaultValue: 10,
    },
    allowAudioImgFile: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0,
    },
    allowAudioRadioCall: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0,
    },
    allowNotice: {
        type: DataTypes.TINYINT(1),
        defaultValue: 1,
    },
}, {
    // other options
    tableName: 'system_conf',
    timestamps: true,
});
