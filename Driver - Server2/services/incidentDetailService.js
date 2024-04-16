const log = require('../log/winston').logger('IncidentDetail Service');
const moment = require('moment');
const utils = require('../util/utils');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const { IncidentDetail } = require('../model/incidentDetail');
const { SOS } = require('../model/sos');

const userService = require('../services/userService');
const { OperationRecord } = require('../model/operationRecord');

module.exports.getWeather = async function (req, res) {
    try {
        let list = ['Clear', 'Slight Rain', 'Heavy Rain', 'Foggy', 'Gloomy']
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getTrafficCondition = async function (req, res) {
    try {
        let list = ['Light', 'Moderate', 'Heavy', 'Jam']
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getTypeOfDelail = async function (req, res) {
    try {
        let list = ['Admin(M-Admin)', 'Adin Exercise', 'Admin Ops', 'Event', 'Ex', 'Ops', 'Safety', 'Training']
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getSecondOrderOfDetail = async function (req, res) {
    try {
        let list = ['Convoy', 'Curency', 'DCP', 'Driving Tig', 'Escort', 'Escort (VIP)', 'Evacuation',' Fam Trg','Functional Drive House Visit',
            'JGL Petrodl', 'JIT Trg', 'Maintenance', 'Movement', 'MP Ops', 'Ops', 'Orientation Trg', 'Park Down', 
            'POL run', 'Prowl', 'Ration','Recee', 'Recovery', 'Refresher','rehearsal', 'Port Ops', 'RTU', 'Safely', 'Store Run', 'Trooplift', 
            'Vehicle Prep', 'WPT', 'Movement (report to unit)', 'Movement (to Trg area)', 'Movement (Ex)'
        ]
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getDirectionOfMovement = async function (req, res) {
    try {
        let list = ['Forward', 'Reverse', 'Stationary']
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getTypeOfManoeuvre = async function (req, res) {
    try {
        let list = ['Left Tumn','Right Tum', 'Right Tumn (DRT)', 'Straight', 'Lane Change (Let)', 'Lane Change (Right)', 
        '3-point turn', 'Parking','Reversing', 'Overtaking', 'U-Turn', 'Stationary']
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getLocalionOfImpact = async function (req, res) {
    try {
        let list = ['Front', 'Front Left', 'Front Right', 'Rear', 'Rear Left', 
            'Rear Right', 'Left', 'Right', 'Top Front', 'Top Rear', 'Top Lef', 'Top Right']
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getLocationType  = async function (req, res) {
    try {
        let list = ['Mlinor Road', 'Carpark', 'Expressway', 'Slip Road', 'Loading Bay', 'POL Point', 'Camp VAC', 'Vessel', 'Washing Bay',
            'Workshop', 'Parade Square', 'Hanger'
        ]
        return res.json(utils.response(1, list));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getWeeklyDateByDate = async function (req, res) {
    try {
        let weekList = [];
        let date = new Date(moment().format('yyyy-MM-DD'));
        if(date.getDay()=="0"){
            date.setDate(date.getDate() -6);
        }else {
            date.setDate(date.getDate() - date.getDay() + 1);
        }
        let myDate=date.getDate();
        let myMonth=date.getMonth() + 1;
        if(date.getDate()<10){
            myDate= '0'+ myDate;
        }
        if(date.getMonth() + 1<10){
            myMonth='0'+myMonth;
        }
        weekList.push(date.getFullYear() + "-" + myMonth+ "-" + myDate);
        for(let i=0;i<6;i++) {
            date.setDate(date.getDate() + 1);
            myDate=date.getDate();
            myMonth=date.getMonth() + 1;
            if(date.getDate()<10){
                myDate= '0'+ myDate;
            }
            if(date.getMonth() + 1<10){
                myMonth='0'+myMonth;
            }
            weekList.push(date.getFullYear() + "-" + myMonth+ "-" +myDate);
        }
        let incidentPeakHour = 'NO';
        let incidentTime = null;
        if((moment().format('YYYY-MM-DD HH:mm') >= moment(`${ moment().format('YYYY-MM-DD') } 07:30`).format('YYYY-MM-DD HH:mm') && 
            moment().format('YYYY-MM-DD HH:mm') <= moment(`${ moment().format('YYYY-MM-DD') } 09:30`).format('YYYY-MM-DD HH:mm')) 
        || (moment().format('YYYY-MM-DD HH:mm') >= moment(`${ moment().format('YYYY-MM-DD') } 17:30`).format('YYYY-MM-DD HH:mm') &&
            moment().format('YYYY-MM-DD HH:mm') <= moment(`${ moment().format('YYYY-MM-DD') } 19:30`).format('YYYY-MM-DD HH:mm'))) incidentPeakHour = 'Yes'

        if((moment().format('YYYY-MM-DD HH:mm') >= moment(`${ moment().format('YYYY-MM-DD') } 07:01`).format('YYYY-MM-DD HH:mm') && 
            moment().format('YYYY-MM-DD HH:mm') <= moment(`${ moment().format('YYYY-MM-DD') } 18:59`).format('YYYY-MM-DD HH:mm'))){
            incidentTime = 'Day'
        } else if((moment().format('YYYY-MM-DD HH:mm') >= moment(`${ moment().format('YYYY-MM-DD') } 19:00`).format('YYYY-MM-DD HH:mm') && 
                   moment().format('YYYY-MM-DD HH:mm') <= moment(`${ moment().format('YYYY-MM-DD') } 07:00`).format('YYYY-MM-DD HH:mm'))) {
            incidentTime = 'Night'
        } 
        let obj = { 
            weekRange: `${ moment(weekList[0]).format('DD/MM/YYYY') } - ${ moment(weekList[weekList.length-1]).format('DD/MM/YYYY') }`, 
            weekNumber: moment().format('w'), 
            monthNumber: moment().format('M'),
            workYear: `${ moment().format('YYYY') } Q${ moment().format('Q') }`,
            incidentPeakHour: incidentPeakHour,
            incidentTime: incidentTime
        }
        return res.json(utils.response(1, obj));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getIncidentDetailBySosId = async function (req, res) {
    try {
        let sosId = req.body.sosId;
        let incidentDetail = await IncidentDetail.findOne({ where: { sosId: sosId } })
        return res.json(utils.response(1, incidentDetail));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.createIncidentDetail = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        if(!userId)  return res.json(utils.response(0, `The current user does not exist.`));
        let incidentObj = req.body.incidentObj;
        await sequelizeObj.transaction(async transaction => {
            incidentObj.creator = userId;
            incidentObj.createdAt = moment().format('YYYY-MM-DD HH:mm:ss');
            incidentObj.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss');
            let oldSos = null;
            let newSos = null;
            if (incidentObj.lssueDemeritPoints) {
                oldSos = await SOS.findOne({ where: { id: incidentObj.sosId } })
                await SOS.update({ demeritPoint: incidentObj.lssueDemeritPoints, optBy: userId, optAt: moment().format('YYYY-MM-DD HH:mm:ss') },{ where: { id: incidentObj.sosId } })
                newSos = await SOS.findOne({ where: { id: incidentObj.sosId } })
            }
            const icidentDetail = await IncidentDetail.create(incidentObj)
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'SOS',
                businessId: icidentDetail.id,
                optType: 'Incident Detail New',
                beforeData: `${ JSON.stringify(['', oldSos ? oldSos : '']) }`,
                afterData: `${ JSON.stringify([incidentObj, newSos ? newSos : '']) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'new incident detail.'
            })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updateIncidentDetailById = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        if(!userId)  return res.json(utils.response(0, `The current user does not exist.`));
        let pageList = await userService.getUserPageList(req.cookies.userId, 'SOS')
        let operationList = pageList.map(item => `${ item.page }`)

        let incidentObj = req.body.incidentObj;
        let incidentId = req.body.incidentId;
        await sequelizeObj.transaction(async transaction => {
            let oldIncidentObj = await IncidentDetail.findOne({ where: { id: incidentId } })
            incidentObj.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss');
            let oldSos = null;
            let newSos = null;
            if (operationList.includes('Edit Issue')) {
                if (incidentObj.lssueDemeritPoints) {
                    oldSos = await SOS.findOne({ where: { id: incidentObj.sosId } })
                    await SOS.update({ demeritPoint: incidentObj.lssueDemeritPoints, optBy: userId, optAt: moment().format('YYYY-MM-DD HH:mm:ss') },{ where: { id: incidentObj.sosId } })
                    newSos = await SOS.findOne({ where: { id: incidentObj.sosId } })
                }
                await IncidentDetail.update(incidentObj, { where: { id: incidentId } })
            } else {
                delete incidentObj.lssueDemeritPoints
                delete incidentObj.suspensionPeriod
                await IncidentDetail.update(incidentObj, { where: { id: incidentId } })
            }
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'SOS',
                businessId: incidentId,
                optType: 'Incident Detail Edit',
                beforeData: `${ JSON.stringify([oldIncidentObj, oldSos ? oldSos : '']) }`,
                afterData: `${ JSON.stringify([incidentObj, newSos ? newSos : '']) }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'edit incident detail.'
            })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updateIncidentIssueById = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        if(!userId)  return res.json(utils.response(0, `The current user does not exist.`));
        let incidentObj = req.body.incidentObj;
        let incidentId = req.body.incidentId;
        await sequelizeObj.transaction(async transaction => {
            incidentObj.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss');
            await SOS.update({ demeritPoint: incidentObj.lssueDemeritPoints, optBy: userId, optAt: moment().format('YYYY-MM-DD HH:mm:ss') },{ where: { id: incidentObj.sosId } })
            await IncidentDetail.update({ lssueDemeritPoints: incidentObj.lssueDemeritPoints, suspensionPeriod: incidentObj.suspensionPeriod }, { where: { id: incidentId } })
        }).catch(error => {
            throw error
        })
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}