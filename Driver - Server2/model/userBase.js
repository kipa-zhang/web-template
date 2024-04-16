const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

const utils = require('../util/utils');
const CONTENT = require('../util/content');

module.exports.UserBase = dbConf.sequelizeObj.define('userBase', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
        autoIncrement: true,
    },
    cvUserId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    mvUserId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    loginName: {
        type: DataTypes.STRING(100),
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
    contactNumber: {
        type: DataTypes.STRING(25),
        defaultValue: null,
    },
    email: {
        type: DataTypes.STRING(100),
        defaultValue: null,
    },
    ord: {
        type: DataTypes.DATE,
    },
    password: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    status: {
        type: DataTypes.STRING(30),
        defaultValue: null,
    },
    cvRole: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    cvGroupId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    cvGroupName: {
        type: DataTypes.STRING(200),
        defaultValue: null,
    },
    cvServiceProviderId: {
        type: DataTypes.STRING(200),
        defaultValue: null,
    },
    cvServiceTypeId: {
        type: DataTypes.STRING(200),
        defaultValue: null,
    },
    mvUserType: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    mvUnitId: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    mvGroupId: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    mvGroupName: {
        type: DataTypes.STRING(200),
        defaultValue: null,
    },
    mvRoleName: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    },
    dataFrom: {
        type: DataTypes.STRING(20),
        defaultValue: 'SYSTEM'
    },
    creator: {
        type: DataTypes.INTEGER(11),
    },
    createdAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW(),
    },
    approveDate: {
        type: DataTypes.DATE,
    },
    approveBy: {
        type: DataTypes.INTEGER(12)
    },
    rejectDate: {
        type: DataTypes.DATE,
    },
    rejectBy: {
        type: DataTypes.INTEGER(12)
    },
    rejectReason: {
        type: DataTypes.STRING(256)
    },
    remarks: {
        type: DataTypes.STRING(200)
    },
    cvApproveDate: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    cvApproveBy: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    cvRejectDate: {
        type: DataTypes.DATE,
        defaultValue: null,
    },
    cvRejectBy: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    cvRejectReason: {
        type: DataTypes.STRING(256),
        defaultValue: null,
    },
    hq: {
        type: DataTypes.STRING(55),
        defaultValue: null,
    }
}, {
    // other options
    tableName: 'user_base',
    timestamps: true,
});
