const log = require('../log/winston.js').logger('Upload TO util');
const utils = require('./utils.js');
const CONTENT = require('./content.js');
const conf = require('../conf/conf.js');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf.js')
const { sequelizeSystemObj } = require('../db/dbConf_system.js')

const path = require('path');
// const fs = require('fs');
const fs = require('graceful-fs');
const moment = require('moment');
const xlsx = require('node-xlsx');
const formidable = require('formidable');

const { Driver } = require('../model/driver.js');
const { User } = require('../model/user.js');
const { DriverAssessmentRecord } = require('../model/driverAssessmentRecord.js');
const { DriverPermitTypeDetail } = require('../model/driverPermitTypeDetail.js');
const { DriverPlatformConf } = require('../model/driverPlatformConf.js');
const { DriverCivilianLicence } = require('../model/driverCivilianLicence.js');

const SUPPORT_ROLE_LIST = ["TO", "TL", "DV", "LOA"];
const SUPPORT_CATEGORY_LIST = ["Category A Assessment", "Category B Assessment", "Category C Assessment", "Category D Assessment"];
const SUPPORT_CATEGORY_STATUS = ["Fail", "Pass"];
const SUPPORT_CIVILIAN_LICENCE = ['CL 2B', 'CL 2A', 'CL 2', 'CL 3A', 'CL 3'];

const driverRoleVocation = {
    "TO": [ "BASE", "BASE (Trainee)", "CBT", "CBT (Trainee)", "CS/CSS", "CS/CSS (Trainee)", "SVC", "SVC (Trainee)", 
        "STO", "STO (Trainee)", "COXSWAIN", "COXSWAIN (Trainee)", "-" ],
    "TL": [ "ADV", "Trainee", "Basic", "-" ],
    "DV": [ "-" ],
    "LOA": [ "-" ]
}

const uploadDriver = async function () {
    try {
        let filePath = "C:\\Users\\lf-ne\\Desktop\\TO_format_202401_template.xlsx"
        let sheetList = xlsx.parse(filePath);
        log.info("sheet num: " + sheetList ? sheetList.length : 0);
        if (sheetList && sheetList.length > 4) {
            let driverDataList = buildDriverData(sheetList);
            log.info("total driver num: " + driverDataList.length);
            log.info("total driver info: " + JSON.stringify(driverDataList));
            let effectiveDriverDataList = await checkAndFilterDriverData(driverDataList);
            log.info("effective driver num: " + effectiveDriverDataList.length);
            log.info("effective driver info: " + JSON.stringify(effectiveDriverDataList));
            for (let temp of effectiveDriverDataList) {
                saveDriverData(temp);
            }
        } else {
            log.warn("file sheet num error!");
        }
    } catch (error) {
        log.error('(uploadDriver) : ', error);
    }
}

/**
    {
        fullName: "",
        fullNric: "",
        role: "",
        vocation: "-",
        groupId: "",
        dateOfBirth: "",
        contactNo: "",
        permitNo: "",
        dateOfIssue: "",
        categoryList: [{categoryName: "", assessmentDate: "", status: ""}],
        permitTypeList: [{permitType: "", mileage: ""}],
        platformList: [{vehicleType: "", assessmentDate: "", mileage: ""}],
        civilianLicenceList: [{}],
    } 
 */
const buildDriverData = function (sheetList) {
    let driverBaseData = sheetList[0].data;
    let driverCategoryData = sheetList[1].data;
    let driverPermitTypeData = sheetList[2].data;
    let driverPlatformData = sheetList[3].data;
    let driverCivilianLicenceData = sheetList[4].data;

    let driverDataList = [];
    let categoryList = [];
    let permitTypeList = [];
    let platformList = [];
    let civilianLicenceList = [];
    if (driverCategoryData && driverCategoryData.length > 0) {
        for (let index = 0; index < driverCategoryData.length; index++) {
            if (index == 0 || !driverCategoryData[index][0] || !driverCategoryData[index][1]) {
                continue;
            }
            categoryList.push({
                row: index,
                fullName: driverCategoryData[index][0],
                nric: driverCategoryData[index][1],
                categoryName: driverCategoryData[index][2],
                assessmentDate: driverCategoryData[index][3],
                status: driverCategoryData[index][4]
            })
        }
    }
    if (driverPermitTypeData && driverPermitTypeData.length > 0) {
        for (let index = 0; index < driverPermitTypeData.length; index++) {
            if (index == 0 || !driverPermitTypeData[index][0] || !driverPermitTypeData[index][1]) {
                continue;
            }
            permitTypeList.push({
                row: index,
                fullName: driverPermitTypeData[index][0],
                nric: driverCategoryData[index][1],
                permitType: driverPermitTypeData[index][2],
                passDate: driverPermitTypeData[index][3],
                numberOfAttempts: driverPermitTypeData[index][4],
                testerCode: driverPermitTypeData[index][5]
            })
        }
    }
    if (driverPlatformData && driverPlatformData.length > 0) {
        for (let index = 0; index < driverPlatformData.length; index++) {
            if (index == 0 || !driverPlatformData[index][0] || !driverPlatformData[index][1]) {
                continue;
            }
            platformList.push({
                row: index,
                fullName: driverPlatformData[index][0],
                nric: driverCategoryData[index][1],
                vehicleType: driverPlatformData[index][2],
                assessmentDate: driverPlatformData[index][3],
                mileage: driverPlatformData[index][4],
                lastDrivenDate: driverPlatformData[index][5]
            })
        }
    }
    if (driverCivilianLicenceData && driverCivilianLicenceData.length > 0) {
        for (let index = 0; index < driverCivilianLicenceData.length; index++) {
            if (index == 0 || !driverCivilianLicenceData[index][0] || !driverCivilianLicenceData[index][1]) {
                continue;
            }
            civilianLicenceList.push({
                row: index,
                fullName: driverCivilianLicenceData[index][0],
                nric: driverCivilianLicenceData[index][1],
                civilianLicence: driverCivilianLicenceData[index][2],
                dateOfIssue: driverCivilianLicenceData[index][3],
                cardSerialNo: driverCivilianLicenceData[index][4]
            })
        }
    }
    if (driverBaseData && driverBaseData.length > 0) {
        for (let index = 0; index < driverBaseData.length; index++) {
            if (index == 0 || !driverBaseData[index][0] || !driverBaseData[index][1]) {
                continue;
            }
            let fullName = driverBaseData[index][0].trim();
            let fullNric = driverBaseData[index][1].trim();
            let loginName = fullNric.substring(0,1) + fullNric.substring(fullNric.length - 4) + fullName.replace(/\s*/g, '').substring(0,3);
            let driverData = {
                row: index,
                fullName: fullName,
                fullNric: fullNric,
                loginName: loginName,
                role: driverBaseData[index][2] ? driverBaseData[index][2].trim().toUpperCase() : '',
                vocation: driverBaseData[index][3] ? driverBaseData[index][3].trim() : '',
                groupId: driverBaseData[index][4] ? (driverBaseData[index][4] + '').trim() : '',
                hub: driverBaseData[index][5] ? driverBaseData[index][5].trim().toUpperCase() : '',
                node: driverBaseData[index][6] ? driverBaseData[index][6].trim().toUpperCase() : '',
                enlistmentDate: driverBaseData[index][7] ? driverBaseData[index][7].trim() : '',
                ordDate: driverBaseData[index][8] ? driverBaseData[index][8].trim() : '',
                dateOfBirth: driverBaseData[index][9] ? driverBaseData[index][9].trim() : '',
                contactNo: driverBaseData[index][10] ? (driverBaseData[index][10] + '').trim() : '',
                permitNo: driverBaseData[index][11] ? driverBaseData[index][11].trim() : '',
                dateOfIssue: driverBaseData[index][12] ? driverBaseData[index][12].trim() : '',
                email: driverBaseData[index][13] ? driverBaseData[index][13].trim() : ''
            };
            driverData.categoryList = categoryList.filter(item => item.fullName == driverData.fullName && item.nric == driverData.fullNric);
            driverData.permitTypeList = permitTypeList.filter(item => item.fullName == driverData.fullName && item.nric == driverData.fullNric);
            driverData.platformList = platformList.filter(item => item.fullName == driverData.fullName && item.nric == driverData.fullNric);
            driverData.civilianLicenceList = civilianLicenceList.filter(item => item.fullName == driverData.fullName && item.nric == driverData.fullNric);

            if (driverData.role && (driverData.role == 'TO' || driverData.role == 'TL')) {
                driverData.groupId = null;
            }

            driverDataList.push(driverData);
        }
    } else {
        log.warn("sheet 1 is empty!");
    }

    return driverDataList;
}

const checkAndFilterDriverData = async function(driverDataList) {
    let supportGroupList = await sequelizeSystemObj.query(
        `select g.id, g.groupName from \`group\` g `,
        {
            type: QueryTypes.SELECT
        }
    );
    let supportUnitList = await sequelizeObj.query(
        `select id, UPPER(unit) as unit, UPPER(subUnit) as subUnit from unit `,
        {
            type: QueryTypes.SELECT
        }
    );
    let supportPermitTypeList = await sequelizeObj.query(
        `select permitType,parent from permittype `,
        {
            type: QueryTypes.SELECT
        }
    );
    let supportVehicleTypeList = await sequelizeObj.query(
        `select DISTINCT vehicleName as vehicleType from vehicle_category `,
        {
            type: QueryTypes.SELECT
        }
    );
    let resultDriverList = [];
    for (let driverData of driverDataList) {
        let fullName = driverData.fullName;
        let fullNric = driverData.fullNric;
        let loginName = driverData.loginName;
        let driver = await Driver.findOne({where: {loginName: loginName}});
        let errorMsg = ``;
        if (driver) {
            errorMsg += `Driver ${fullName}-${fullNric} has exist; `;
        }
        if (!SUPPORT_ROLE_LIST.includes(driverData.role)) {
            errorMsg += `Just support driver role[${JSON.stringify(SUPPORT_ROLE_LIST)}]; `;
        }
        if (!fullName) {
            errorMsg += `Driver Full Name is empty; `;
        }
        if (!fullNric) {
            errorMsg += `Driver Full NRIC is empty; `;
        } else {
            let regular = /^[S,T]\d{7}[A-Z]$/ ;     
            if (regular.test(fullNric) == false) {
                errorMsg += `Driver Full NRIC[${fullNric}] format error; `;
            } 
        }
        if (!driverData.contactNo) {
            errorMsg += `Driver Contact No. is empty; `;
        } else {
            let firstNumber = driverData.contactNo.substring(0, 1);
            if (!(driverData.contactNo.length == 8 && (firstNumber == "8" || firstNumber == "9"))) {
                errorMsg += `Driver Contact No. must be 8 number and start with 8 or 9; `;
            }  
        }
        if (!driverData.email) {
            errorMsg += `Driver Email Address is empty; `;
        } else {
            let regular = /^[^\s@]+@[^\s@]+\.[^\s@]+$/ ;     
            if (regular.test(driverData.email) == false) {
                errorMsg += `Driver Email Address[${driverData.email}] format error; `;
            }  
        }

        if (!driverData.vocation) {
            driverData.vocation = '-';
        }
        
        let spuportVocation = driverRoleVocation[driverData.role];
        if (!spuportVocation || !spuportVocation.includes(driverData.vocation)) {
            errorMsg += `${driverData.role} driver just support vocation[${JSON.stringify(spuportVocation)}]; `;
        }

        if (driverData.groupId) {
            let group = supportGroupList.find(item => (item.id == driverData.groupId || item.groupName == driverData.groupId));
            if (group) {
                driverData.groupId = group.id;
                driverData.groupName = group.groupName;
            } else {
                errorMsg += `Unsupport driver Unit Code[${driverData.groupId}]; `;
            }
        }

        if (driverData.groupId) {
            driverData.hub = null;
            driverData.node = null;
        }
        let driverUnit = null;
        if (driverData.hub) {
            if (driverData.node) {
                driverUnit = supportUnitList.find(item => item.unit == driverData.hub && item.subUnit == driverData.node);
            } else {
                driverUnit = supportUnitList.find(item => item.unit == driverData.hub && !item.subUnit);
            }
        }
        if (driverData.role == 'TO' || driverData.role == 'TL') {
            if (!driverData.hub) {
                errorMsg += `TO/TL driver hub is empty; `;
            }
            if (driverUnit) {
                driverData.unitId = driverUnit.id;
            } else {
                errorMsg += `TO/TL driver hub/node[${driverData.hub}/${driverData.node ?? ''}] is not exist; `;
            }
        } else if (driverData.role == 'DV' || driverData.role == 'LOA') {
            if (!driverData.groupId) {
                if (driverUnit) {
                    driverData.unitId = driverUnit.id;
                } else {
                    errorMsg += `DV/LOA driver need select one group or unit; `;
                }
            }
        }
        
        if (driverData.dateOfBirth && !moment(driverData.dateOfBirth, 'DD/MM/YYYY', true).isValid()) {
            errorMsg += `DOB [${driverData.dateOfBirth}] format error: support format DD/MM/YYYY; `;
        }
        if (driverData.dateOfIssue && !moment(driverData.dateOfIssue, 'DD/MM/YYYY', true).isValid()) {
            errorMsg += `Date of Issue[${driverData.dateOfIssue}] format error: support format DD/MM/YYYY; `;
        }
        if (driverData.enlistmentDate && !moment(driverData.enlistmentDate, 'DD/MM/YYYY', true).isValid()) {
            errorMsg += `Enlistment Date[${driverData.enlistmentDate}] format error: support format DD/MM/YYYY; `;
        }
        if (driverData.ordDate && !moment(driverData.ordDate, 'DD/MM/YYYY', true).isValid()) {
            errorMsg += `ORD Date[${driverData.ordDate}] format error: support format DD/MM/YYYY; `;
        }

        if (driverData.categoryList && driverData.categoryList.length > 0) {
            for (let temp of driverData.categoryList) {
                if (!temp.categoryName) {
                    errorMsg += `Category Assessment is empty; `;
                } else if (!SUPPORT_CATEGORY_LIST.includes(temp.categoryName)) {
                    errorMsg += `Just support category[${JSON.stringify(SUPPORT_CATEGORY_LIST)}]; `;
                }
                if (!temp.status) {
                    errorMsg += `Category Assessment Status is empty; `;
                } else if (!SUPPORT_CATEGORY_STATUS.includes(temp.status)) {
                    errorMsg += `Just support category status[${JSON.stringify(SUPPORT_CATEGORY_STATUS)}]; `;
                }
                if (!temp.assessmentDate) {
                    errorMsg += `Category Assessment Date is empty; `;
                } else if (!moment(temp.assessmentDate, 'DD/MM/YYYY', true).isValid()) {
                    errorMsg += `Category Assessment Date[${temp.assessmentDate}] format error: support format DD/MM/YYYY; `;
                }
            }
        }
        if (driverData.permitTypeList && driverData.permitTypeList.length > 0) {
            for (let temp of driverData.permitTypeList) {
                if (!temp.permitType) {
                    errorMsg += `Driver Test Type is empty; `;
                }
                let permitType = supportPermitTypeList.find(item => item.permitType == temp.permitType);
                if (!permitType) {
                    errorMsg += `Unsupport Driver Test Type[${temp.permitType}]; `;
                }
                if (!temp.passDate) {
                    errorMsg += `Driver Test Type Pass Date is empty; `;
                } else if (!moment(temp.passDate, 'DD/MM/YYYY', true).isValid()) {
                    errorMsg += `Driver Test Type Pass Date[${temp.passDate}] format error: support format DD/MM/YYYY; `;
                }
                if (!temp.numberOfAttempts) {
                    errorMsg += `Driver Test Type Number of Attempts is empty; `;
                } else {
                    let regular = /^[0-9]*$/;
                    if (regular.test(temp.numberOfAttempts) == false) {
                        errorMsg += `Driver Test Type Number of Attempts[${temp.numberOfAttempts}] not number; `;
                    } 
                }
                if (!temp.testerCode) {
                    errorMsg += `Driver Test Type Tester Code is empty; `;
                }
            }
        }
        if (driverData.platformList && driverData.platformList.length > 0) {
            for (let temp of driverData.platformList) {
                if (!temp.vehicleType) {
                    errorMsg += `Driver Platform Vehicle Type is empty; `;
                }
                let vehicleType = supportVehicleTypeList.find(item => item.vehicleType == temp.vehicleType);
                if (!vehicleType) {
                    errorMsg += `Unsupport Driver Platform vehicle type[${temp.vehicleType}]; `;
                }
                if (!temp.assessmentDate) {
                    errorMsg += `Driver Platform Assessment Date is empty; `;
                } else if (!moment(temp.assessmentDate, 'DD/MM/YYYY', true).isValid()) {
                    errorMsg += `Driver Platform Assessment Date[${temp.assessmentDate}] format error: support format DD/MM/YYYY; `;
                }
                // if (!temp.mileage) {
                //     errorMsg += `Driver Platform Mileage is empty; `;
                // } else {
                //     let regular = /^\d+(\.\d{1,2})?$/;
                //     if (regular.test(temp.mileage) == false) {
                //         errorMsg += `Driver Platform Mileage[${temp.mileage}] not number; `;
                //     } 
                // }
                if (!temp.lastDrivenDate) {
                    errorMsg += `Driver Platform Last Driven Date is empty; `;
                } else if (!moment(temp.lastDrivenDate, 'DD/MM/YYYY', true).isValid()) {
                    errorMsg += `Driver Platform Last Driven Date[${temp.lastDrivenDate}] format error: support format DD/MM/YYYY; `;
                }
            }
        }
        if (driverData.civilianLicenceList && driverData.civilianLicenceList.length > 0) {
            let cardSerialNo = null;
            for (let temp of driverData.civilianLicenceList) {
                if (!temp.civilianLicence) {
                    errorMsg += `Driver Civilian Licence is empty; `;
                } else {
                    let civilianLicence = SUPPORT_CIVILIAN_LICENCE.find(item => item == temp.civilianLicence.toUpperCase());
                    if (!civilianLicence) {
                        errorMsg += `Unsupport Driver Civilian Licence[${temp.civilianLicence}]; `;
                    }
                }
                
                if (!temp.dateOfIssue) {
                    errorMsg += `Driver Civilian Licence Date of Issue is empty; `;
                } else if (!moment(temp.dateOfIssue, 'DD/MM/YYYY', true).isValid()) {
                    errorMsg += `Driver Civilian Licence Date of Issue[${temp.dateOfIssue}] format error: support format DD/MM/YYYY; `;
                }
                if (!cardSerialNo && temp.cardSerialNo) {
                    cardSerialNo = temp.cardSerialNo;
                }
            }
            if (cardSerialNo) {
                for (let temp of driverData.civilianLicenceList) {
                    temp.cardSerialNo = cardSerialNo;
                }
            } else {
                errorMsg += `Driver Civilian Licence Card Serial No. is empty; `;
            }
        }

        if(errorMsg) {
            errorMsg = `Row ${driverData.row}: ` + errorMsg;
            log.warn(errorMsg);
        } else {
            resultDriverList.push(driverData);
        }
    }
    return resultDriverList;
}

const saveDriverData = async function(effectiveDriverData) {
    try {
        let fullNric = effectiveDriverData.fullNric;
        let nric = utils.generateAESCode(fullNric).toUpperCase()
        let driver = {
            loginName: effectiveDriverData.loginName,
            driverName: effectiveDriverData.fullName,
            nric: nric,
            contactNumber: effectiveDriverData.contactNo,
            vocation: effectiveDriverData.vocation,
            unit: effectiveDriverData.groupId ? effectiveDriverData.groupId : null,
            groupId: effectiveDriverData.groupId,
            unitId: effectiveDriverData.unitId,
            birthday: effectiveDriverData.dateOfBirth ? moment(effectiveDriverData.dateOfBirth, 'DD/MM/YYYY').format("YYYY-MM-DD") : null,
            enlistmentDate: effectiveDriverData.enlistmentDate ? moment(effectiveDriverData.enlistmentDate, 'DD/MM/YYYY').format("YYYY-MM-DD") : null,
            operationallyReadyDate: effectiveDriverData.ordDate ? moment(effectiveDriverData.ordDate, 'DD/MM/YYYY').format("YYYY-MM-DD") : null,
            permitNo: effectiveDriverData.permitNo,
            permitIssueDate: effectiveDriverData.dateOfIssue ? moment(effectiveDriverData.dateOfIssue, 'DD/MM/YYYY').format("YYYY-MM-DD") : null,
            email: effectiveDriverData.email,
            creator: 1
        }

        let user = {
            fullName: effectiveDriverData.fullName,
            username: effectiveDriverData.loginName,
            unitId: effectiveDriverData.unitId ? effectiveDriverData.unitId : effectiveDriverData.groupId,
            userType: 'MOBILE',
            role: effectiveDriverData.role,
            nric: nric
        }

        let pwdStr = (nric.substr((nric.length)-4, 4)) + effectiveDriverData.contactNo.substr(0, 4);
        log.info(`Driver ${effectiveDriverData.loginName}, password: ${pwdStr}`);
        user.password = utils.generateMD5Code(pwdStr).toUpperCase();
        let categoryList = [];
        for (let temp of effectiveDriverData.categoryList) {
            let exist = categoryList.find(item => item.categoryName == temp.categoryName);
            if (!exist) {
                categoryList.push({
                    assessmentType: temp.categoryName,
                    issueDate: moment(temp.assessmentDate, 'DD/MM/YYYY').format("YYYY-MM-DD"),
                    status: temp.status,
                    approveStatus: 'Approved',
                    creator: 1
                });
            }
        }

        let permitTypeDetailList = [];
        let driverPermitType = '';
        for (let temp of effectiveDriverData.permitTypeList) {
            let exist = permitTypeDetailList.find(item => item.permitType == temp.permitType);
            if (exist) {
                continue;
            }
            permitTypeDetailList.push({
                permitType: temp.permitType,
                passDate: moment(temp.passDate, 'DD/MM/YYYY').format("YYYY-MM-DD"),
                attemptNums: temp.numberOfAttempts,
                testerCode: temp.testerCode,
                approveStatus: 'Approved',
                creator: 1
            });
            driverPermitType = driverPermitType ? `,${temp.permitType}` : temp.permitType;
        }
        let platformList = [];
        for (let temp of effectiveDriverData.platformList) {
            let exist = platformList.find(item => item.vehicleType == temp.vehicleType);
            if (exist) {
                continue;
            }
            //let mileage = temp.mileage ? temp.mileage : 0;
            platformList.push({
                vehicleType: temp.vehicleType,
                // baseMileage: Number(mileage),
                // totalMileage: Number(mileage),
                assessmentDate: moment(temp.assessmentDate, 'DD/MM/YYYY').format("YYYY-MM-DD"),
                lastDrivenDate: moment(temp.lastDrivenDate, 'DD/MM/YYYY').format("YYYY-MM-DD"),
                approveStatus: 'Approved',
                creator: 1
            });
        }
        let civilianLicenceList = [];
        for (let temp of effectiveDriverData.civilianLicenceList) {
            let exist = civilianLicenceList.find(item => item.civilianLicence == temp.civilianLicence);
            if (exist) {
                continue;
            }
            civilianLicenceList.push({
                civilianLicence: temp.civilianLicence,
                dateOfIssue: moment(temp.dateOfIssue, 'DD/MM/YYYY').format("YYYY-MM-DD"),
                cardSerialNumber: temp.cardSerialNo,
                status: 'Approved',
                creator: 1
            });
        }
        driver.permitType = driverPermitType;
        await sequelizeObj.transaction(async transaction => {
            let newDriver = await Driver.create(driver);
            let driverId = newDriver.driverId;
            user.driverId = driverId;
            await User.create(user);
            for (let temp of categoryList) {
                temp.driverId = driverId;
            }
            for (let temp of permitTypeDetailList) {
                temp.driverId = driverId;
            }
            for (let temp of platformList) {
                temp.driverId = driverId;
            }
            for (let temp of civilianLicenceList) {
                temp.driverId = driverId;
            }

            if (categoryList && categoryList.length > 0) {
                await DriverAssessmentRecord.bulkCreate(categoryList);
            }
            if (permitTypeDetailList && permitTypeDetailList.length > 0) {
                await DriverPermitTypeDetail.bulkCreate(permitTypeDetailList);
            }
            if (platformList && platformList.length > 0) {
                await DriverPlatformConf.bulkCreate(platformList);
            }
            if (civilianLicenceList && civilianLicenceList.length > 0) {
                await DriverCivilianLicence.bulkCreate(civilianLicenceList);
            }
        });

    } catch(error) {
        log.error(`Row ${effectiveDriverData.row} driver ${effectiveDriverData.fullName + '-' + effectiveDriverData.fullNric} save fail: ` + (error && error.message ? error.message : 'unknow system error!'));
        log.error(error)
    }
}

uploadDriver();