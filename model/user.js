const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

const utils = require('../util/utils');
const CONTENT = require('../util/content');

module.exports.User = dbConf.sequelizeObj.define('user', {
    userId: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    username: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    fullName: {
        type: DataTypes.STRING(100),
        allowNull: false,
    },
    nric: {
        type: DataTypes.STRING(55),
        defaultValue: null,
        get(){
            let val = this.getDataValue('nric');
            let newValue = val
            if(val) {
                if(val.length > 9) newValue = utils.decodeAESCode(val);
            }
            return newValue
        }
    },
    userIcon: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    unitId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    ord: {
        type: DataTypes.DATE,
    },
    password: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    pwdErrorTimes: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0
    },
    userType: {
        type: DataTypes.STRING(55),
        defaultValue: CONTENT.USER_TYPE.UNIT,
    },
    online: {
        type: DataTypes.TINYINT(1),
        defaultValue: 0,
    },
    driverId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    role: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    sgid: {
        type: DataTypes.STRING(100),
        defaultValue: null,
    },
    jwtToken: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    firebaseToken: {
        type: DataTypes.TEXT,
        defaultValue: null,
    },
    enable: {
        type: DataTypes.TINYINT(1),
        defaultValue: 1,
    },
    lastLoginTime: {
        type: DataTypes.DATE 
    },
    unLockTime: {
        type: DataTypes.DATE 
    },
    lastChangePasswordDate: {
        type: DataTypes.DATE,
        defaultValue: null
    },
    operation: {
        type: DataTypes.VIRTUAL 
    },
    contactNumber: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    }
}, {
    // other options
    tableName: 'user',
    timestamps: true,
});
