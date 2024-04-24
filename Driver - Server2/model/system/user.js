const { DataTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../../db/dbConf_system');
const moment = require('moment');
const CONTENT = require('../../util/content.js');

module.exports.USER = sequelizeSystemObj.define('user', {
    id: {
        type: DataTypes.BIGINT,
        autoIncrement: true,
        primaryKey: true,
    },
    nric: {
        type: DataTypes.STRING(11),
    },
    username: {
        type: DataTypes.STRING(100),
    },
    password: {
        type: DataTypes.STRING(150),
    },
    loginName:{
        type: DataTypes.STRING(8),
    },
    lastLoginTime: {
        type: DataTypes.DATE,
    },
    times: {
        type: DataTypes.INTEGER,
    },
    lastChangePasswordDate: {
        type: DataTypes.DATE,
    },
    status: {
        type: DataTypes.STRING(30),
        get() {
            let status = this.getDataValue('status');
            let activeTime = this.getDataValue('activeTime');
            let lastLoginTime = this.getDataValue('lastLoginTime');
            let createdAt = this.getDataValue('createdAt');
            return CheckUserStatus(status, activeTime, lastLoginTime, createdAt)
        }
    },
    historyPassword: {
        type: DataTypes.TEXT,
    },
    activeTime: {
        type: DataTypes.DATE,
    },
    group: {
        type: DataTypes.BIGINT,
    },
    role:{
        type: DataTypes.BIGINT,
    },
    contactNumber: {
        type: DataTypes.STRING(25),
    },
    email: {
        type: DataTypes.STRING(100),
    },
    token: {
        type: DataTypes.TEXT,
    },
    roleName: {
        type: DataTypes.VIRTUAL,
    },
    groupName: {
        type: DataTypes.VIRTUAL,
    },
    serviceProviderId: {
        type: DataTypes.STRING(200),
    },
    serviceTypeId: {
        type: DataTypes.STRING(200),
    },
    sgid: {
        type: DataTypes.STRING(100),
    },
    ord: {
        type: DataTypes.DATE,
    },
}, {
    timestamps: true,
});

module.exports.UserManagementReport = sequelizeSystemObj.define('user_management_report', {
    id: {
        type: DataTypes.BIGINT,
        autoIncrement: true,
        primaryKey: true,
    },
    userId: {
        type: DataTypes.BIGINT,
    },
    activity: {
        type: DataTypes.STRING(50),
    },
    operateDate: {
        type: DataTypes.DATE,
    },
    operatorId: {
        type: DataTypes.BIGINT,
    },
    operatorUserBaseId: {
        type: DataTypes.BIGINT,
    },
    triggeredBy: {
        type: DataTypes.STRING(255),
    },
    beforeData: {
        type: DataTypes.TEXT,
    },
    afterData: {
        type: DataTypes.TEXT,
    },
    remark: {
        type: DataTypes.STRING(1100),
    },
}, {
    timestamps: false,
});

const CheckUserStatus = function (status, activeTime, lastLoginTime, createdAt) {
    lastLoginTime = lastLoginTime == null ? createdAt : lastLoginTime
    let day90 = 90
    let day180 = 180

    if (status == CONTENT.CV_USER_STATUS.Active) {
        lastLoginTime = moment(lastLoginTime).isSameOrAfter(moment(activeTime)) ? lastLoginTime : activeTime
    } else if (status == CONTENT.CV_USER_STATUS.LockOut) {
        if (CheckIfDaysPassed(lastLoginTime, day180)) {
            return CONTENT.CV_USER_STATUS.Deactivated
        } else {
            return CONTENT.CV_USER_STATUS.LockOut
        }
    } else if (status == CONTENT.CV_USER_STATUS.Deactivated) {
        return CONTENT.CV_USER_STATUS.Deactivated
    }

    if (CheckIfDaysPassed(lastLoginTime, day180)) {
        return CONTENT.CV_USER_STATUS.Deactivated
    } else if (CheckIfDaysPassed(lastLoginTime, day90)) {
        return CONTENT.CV_USER_STATUS.LockOut
    } else {
        return CONTENT.CV_USER_STATUS.Active
    }
}

const CheckIfDaysPassed = function (lastLoginTime, day) {
    return moment(new Date()).diff(moment(new Date(lastLoginTime)), "d") >= day
}