const log = require('../log/winston').logger('Sync cv mv user util');
const utils = require('../util/utils');
const CONTENT = require('../util/content');
const conf = require('../conf/conf');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const moment = require('moment');
const { UserBase } = require('../model/userBase.js');
const _SysUser = require('../model/system/user.js');
const { User } = require('../model/user');

const syncCVMVUser = async function () {
    try {
        //clear old sync data
        await sequelizeObj.query(`
            delete from user_base ub where ub.dataFrom = 'SYNC';
        `, { type: QueryTypes.DELETE })

        //matched cvUser, mvUser
        let matchedUserList = await sequelizeObj.query(`
            select ub.cvUserId, ub.mvUserId from user_base ub
        `, { type: QueryTypes.SELECT })
        let matchedCvUserId = [];
        let matchedMvUserId = [];
        if (matchedUserList && matchedUserList.length > 0) {
            for (let temp of matchedUserList) {
                if (temp.cvUserId) {
                    matchedCvUserId.push(temp.cvUserId);
                }
                if (temp.mvUserId) {
                    matchedMvUserId.push(temp.mvUserId);
                }
            }
        }

        //all cv user
        let cvUserList = await sequelizeSystemObj.query(`
            SELECT
                cv.id as cvUserId,
                cv.username as fullName,
                cv.nric as nric,
                cv.contactNumber as contactNumber,
                cv.loginName as loginName,
                cv.\`password\` as \`password\`,
                cv.lastLoginTime as lastLoginTime,
                cv.role as cvRole,
                cv.\`group\` as cvGroupId,
                cv.serviceProviderId as cvServiceProviderId,
                cv.serviceTypeId as cvServiceTypeId,
                cv.\`status\` as status,
                cv.createdAt
            FROM user cv where 1=1 ${ matchedCvUserId.length > 0 ? ` and id not in ('${ matchedCvUserId.join("','") }')` : '' }
        `, { type: QueryTypes.SELECT })
        if (cvUserList && cvUserList.length > 0) {
            for (let temp of cvUserList) {
                temp.cvUserKey = ((temp.loginName ?? '') + (temp.fullName ?? '')).toUpperCase();
            }
        }

        let cvGroupList = await sequelizeSystemObj.query(`
            SELECT id, groupName from \`group\`;
        `, { type: QueryTypes.SELECT })

        //all mv server user
        let mvUserList = await sequelizeObj.query(`
            SELECT
                mv.userId as mvUserId,
                mv.nric as nric,
                mv.fullName as fullName,
                mv.username as loginName,
                mv.lastLoginTime as lastLoginTime,
                mv.userType as mvUserType,
                mv.unitId as mvUnitId,
                mv.contactNumber as contactNumber,
                mv.role as mvRoleName,
                mv.\`password\` as \`password\`,
                mv.\`enable\` as status,
                mv.createdAt
            FROM USER mv
            where mv.userType != 'MOBILE' ${ matchedMvUserId.length > 0 ? ` and userId not in ('${ matchedMvUserId.join("','") }')` : '' }
        `, { type: QueryTypes.SELECT })
        if (mvUserList && mvUserList.length > 0) {
            for (let temp of mvUserList) {
                temp.mvUserKey = ((temp.loginName ?? '') + (temp.fullName ?? '')).toUpperCase();
            }
        }

        //{cvUser:{}, mvUser: {}};
        let cvAndMvUserList = [];
        matchedMvUserId = [];
        cvUserList.forEach(function (cvUser) { 
            let mvUser = mvUserList.find(mvUser => mvUser.mvUserKey == cvUser.cvUserKey);
            
            if (mvUser) {
                if (matchedMvUserId.indexOf(mvUser.mvUserId) != -1) {
                    cvAndMvUserList.push({cvUser});
                    log.info(`cv user:${cvUser.cvUserId} no matches mvUser, mvUser:${mvUser.mvUserId} has been matched to other cv user!`);
                } else {
                    cvAndMvUserList.push({cvUser, mvUser});
                    log.info(`cv user:${cvUser.cvUserId} has match mvUser:${mvUser.mvUserId}.`);
                    matchedMvUserId.push(mvUser.mvUserId);
                }
            } else {
                cvAndMvUserList.push({cvUser});
                log.info(`cv user:${cvUser.cvUserId} no matches mvUser.`);
            }
        });
        mvUserList.forEach(function (mvUser) { 
            if (matchedMvUserId.indexOf(mvUser.mvUserId) == -1) {
                cvAndMvUserList.push({mvUser});
                log.info(`mv user:${mvUser.mvUserId} no matches cvUser.`);
            }
        });

        let newUserBaseList = [];
        for(let temp of cvAndMvUserList) {
            let userBase = buildUserBase(temp.cvUser, temp.mvUser, cvGroupList);
            log.info(`build new user base cvUserId:${userBase.cvUserId}, mvUserId:${userBase.mvUserId}.`);
            newUserBaseList.push(userBase);

            if (newUserBaseList.length > 20) {
                await UserBase.bulkCreate(newUserBaseList);
                newUserBaseList = [];
            }
        }

        if (newUserBaseList.length > 0) {
            await UserBase.bulkCreate(newUserBaseList);
        }
    } catch (error) {
        log.error('(uploadDVLOA) : ', error);
    }
}

const buildUserBase = function(cvUser, mvUser, cvGroupList) {
    let userBase = {};

    userBase.fullName = mvUser ? mvUser.fullName : cvUser.fullName;
    userBase.loginName = mvUser ? mvUser.loginName : cvUser.loginName;
    userBase.contactNumber = cvUser ? cvUser.contactNumber : mvUser.contactNumber;
    userBase.nric = mvUser ? mvUser.nric : '';
    if (userBase.nric && userBase.nric.length <= 9) {
        userBase.nric = utils.generateAESCode(userBase.nric).toUpperCase();
    }
    userBase.status = 'Approved';
    userBase.lastLoginTime = cvUser ? cvUser.lastLoginTime : null;

    let userCreateAt = null;
    if (cvUser) {
        userBase.cvUserId = cvUser.cvUserId;
        userBase.cvRole = cvUser.cvRole;
        userBase.cvGroupId = cvUser.cvGroupId;
        if (cvUser.cvGroupId) {
            let cvGroup = cvGroupList.find(item => item.id == cvUser.cvGroupId);
            if (cvGroup) {
                userBase.cvGroupName = cvGroup.groupName;
            }
        }
        userBase.cvServiceProviderId = cvUser.cvServiceProviderId;
        userBase.cvServiceTypeId = cvUser.cvServiceTypeId;
        if (cvUser.status == 'Deactivated') {
            userBase.status = 'Disabled';
        }
        userCreateAt = cvUser.createdAt;
    }
    if (mvUser) {
        userBase.mvUserId = mvUser.mvUserId;
        userBase.mvUserType = mvUser.mvUserType;
        if (userBase.mvUserType == 'CUSTOMER') {
            userBase.mvGroupId = mvUser.mvUnitId;
            if (userBase.mvGroupId) {
                let mvGroup = cvGroupList.find(item => item.id == userBase.mvGroupId);
                if (mvGroup) {
                    userBase.mvGroupName = mvGroup.groupName;
                }
            }
        } else {
            userBase.mvUnitId = mvUser.mvUnitId;
        }
        userBase.mvRoleName = mvUser.mvRoleName;
        if (mvUser.status == '0') {
            userBase.status = 'Disabled';
        }
        userCreateAt = mvUser.createdAt;
    }

    userBase.dataFrom = 'SYNC';
    userBase.createdAt = moment(userCreateAt).format('YYYY-MM-DD HH:mm:ss');
    userBase.approveDate = moment(userCreateAt).format('YYYY-MM-DD HH:mm:ss');
    userBase.remarks = 'Sync cv and mv user!'

    return userBase;
}

syncCVMVUser();

