const log = require('../log/winston').logger('Location Service');
const utils = require('../util/utils');

const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { MtAdmin } = require('../model/mtAdmin');
const { Task } = require('../model/task');
const { UrgentIndent } = require('../model/urgent/urgentIndent.js');

const _SystemLocation = require('../model/system/location');
const _SystemTask = require('../model/system/task');
const { OperationRecord } = require('../model/operationRecord.js');

const userService = require('../services/userService');

module.exports = {
    getLocationList: async function (req, res) {
        let location = req.body.location;

        let sql = ` 
            SELECT l.*, pickup.pickupCount, dropoff.dropoffCount 
            FROM location l
            LEFT JOIN (
                SELECT COUNT(*) AS pickupCount, jt.pickupDestination FROM job_task jt GROUP BY pickupDestination
            ) pickup ON pickup.pickupDestination = l.locationName
            LEFT JOIN (
                SELECT COUNT(*) AS dropoffCount, jt.dropoffDestination FROM job_task jt GROUP BY dropoffDestination
            ) dropoff ON dropoff.dropoffDestination = l.locationName 
        `

        let limitSql = []
        let replacements = []
        const initLocation = function () {
            if (location) {
                if (location.locationId) {
                    limitSql.push(` l.id = ? `)
                    replacements.push(location.locationId)
                } else if (location.locationName) {
                    limitSql.push(` l.locationName LIKE `+ sequelizeSystemObj.escape("%" + location.locationName +"%"))
                }
    
                if (location.belongTo) {
                    limitSql.push(` l.belongTo = ? `)
                    replacements.push(location.belongTo)
                }
            }
        }
        initLocation()

        if (limitSql.length) {
            sql += ` WHERE ${ limitSql.join(' AND ') } `
        }

        let locationList = await sequelizeSystemObj.query(sql, { type: QueryTypes.SELECT, replacements });

        let sql2 = `
            SELECT reportingLocation, destination FROM mt_admin GROUP BY reportingLocation, destination
        `
        let locationGroupList = await sequelizeObj.query(sql2, { type: QueryTypes.SELECT });

        let pageList = await userService.getUserPageList(req.cookies.userId, 'Driver Control', 'Location')
        let operationList = pageList.map(item => `${ item.action }`).join(',')
        
        locationList = locationList.map(location => {
            location.operation = operationList
            locationGroupList.forEach(group => {
                if (group.reportingLocation == location.locationName) {
                    if (location.pickupCount) location.pickupCount++;
                    else location.pickupCount = 1;
                }
                if (group.destination == location.locationName) {
                    if (location.dropoffCount) location.dropoffCount++;
                    else location.dropoffCount = 1;
                }
            })

            return location
        })
        
        return res.json(utils.response(1, locationList));
    },
    updateLocation: async function (req, res) {
        let location = req.body;
        if (!location.id) {
            log.error(`Location id does not exist!`);
            return res.json(utils.response(0, `Location id does not exist!`));
        }

        const checkLocationName = async function (location) {
            // check locationName
            let checkResult = await _SystemTask.Task.findAll({ where: { 
                [Op.or]: [
                    { pickupDestination: location.locationName },
                    { dropoffDestination: location.locationName }
                ]
            } })
            if (checkResult?.length > 0) {
                log.warn(`${ location.locationName } already used in system, can not change locationName here!`)
                
                // Update task table
                await Task.update({ pickupGPS: `${ location.lat },${ location.lng }` }, { where: { pickupDestination: location.locationName } })
                await Task.update({ dropoffGPS: `${ location.lat },${ location.lng }` }, { where: { dropoffDestination: location.locationName } })
            }

            let checkResult2 = await MtAdmin.findAll({ where: { 
                [Op.or]: [
                    { reportingLocation: location.locationName },
                    { destination: location.locationName }
                ]
            } })
            if (checkResult2?.length > 0) {
                log.warn(`${ location.locationName } already used in MT-ADMIN, can not change locationName here!`)
                
                // update mt-admin table
                await MtAdmin.update({ reportingLocationLat: location.lat, reportingLocationLng: location.lng }, { where: { reportingLocation: location.locationName } })
                await MtAdmin.update({ destinationLat: location.lat, destinationLng: location.lng }, { where: { destination: location.locationName } })
            }

            let checkResult3 = await Task.findAll({ where: { 
                [Op.or]: [
                    { dropoffDestination: location.locationName },
                    { pickupDestination: location.locationName }
                ]
            } })
            if (checkResult3?.length > 0) {
                log.warn(`${ location.locationName } already used in Task, can not change locationName here!`)
                
                // update task table
                await Task.update({ dropoffGPS: `${ location.lat },${ location.lng }` }, { where: { dropoffDestination: location.locationName } })
                await Task.update({ pickupGPS: `${ location.lat },${ location.lng }` }, { where: { pickupDestination: location.locationName } })
            }

            let checkResult4 = await UrgentIndent.findAll({ where: { reportingLocation: location.locationName }})
            if (checkResult4?.length > 0) {
                log.warn(`${ location.locationName } already used in Urgent Indent, can not change locationName here!`)
                
                // update task table
                await UrgentIndent.update({ reportingGPS: `${ location.lat },${ location.lng }` }, { where: { reportingLocation: location.locationName } })
            }
        }
        
        await checkLocationName(location);
        let result = await _SystemLocation.Location.findAll({ where: { locationName: location.locationName, id: { [Op.ne]: location.id } } })
        if (result.length) {
            log.warn(`${ location.locationName } already used , can not edit here!`)
            return res.json(utils.response(0, `${ location.locationName } already used , can not edit here!`));
        }
        await _SystemLocation.Location.update(location, { where: { id: location.id } });

        await OperationRecord.create({
            operatorId: req.cookies.userId,
            businessType: 'Update Location',
            businessId: location.id,
            optType: 'Update',
            afterData: JSON.stringify(location), 
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            remarks: null
        })
        return res.json(utils.response(1, `success`));
    },
    deleteLocation: async function (req, res) {
        let locationId = req.body.id;
        if (!locationId) {
            log.error(`Location id does not exist!`);
            return res.json(utils.response(0, `Location id does not exist!`));
        }

        const checkLocationName = async function (location) {
            // check locationName
            let checkResult = await _SystemTask.Task.findAll({ where: 
                { 
                    [Op.or]: [
                        { pickupDestination: location.locationName },
                        { dropoffDestination: location.locationName }
                    ],
                    taskStatus: {
                        [Op.notIn]: [ 'cancelled', 'completed' ]
                    }
                } 
            })
            if (checkResult?.length > 0) {
                log.warn(`Location ${ location.locationName } already used in system, can not delete here!`)
                throw new Error(`Location has been used and is unable to delete.`)
            }

            let checkResult2 = await Task.findAll({ 
                where: { 
                    [Op.or]: [
                        { pickupDestination: location.locationName },
                        { dropoffDestination: location.locationName }
                    ],
                    driverStatus: {
                        [Op.notIn]: [ 'cancelled', 'completed' ]
                    }
                } 
            })
            if (checkResult2?.length > 0) {
                log.warn(`${ location.locationName } already used in MT-ADMIN, can not delete here!`)
                throw new Error(`Location has been used and is unable to delete.`)
            }

            let checkResult3 = await UrgentIndent.findAll({ 
                where: { 
                    reportingLocation: location.locationName,
                    status: {
                        [Op.notIn]: [ 'cancelled', 'completed' ]
                    }
                } 
            })
            if (checkResult3?.length > 0) {
                log.warn(`${ location.locationName } already used in Urgent, can not delete here!`)
                throw new Error(`Location has been used and is unable to delete.`)
            }
        }

        let location = await _SystemLocation.Location.findByPk(locationId)
        try {
            await checkLocationName(location);
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
        await _SystemLocation.Location.destroy({ where: { id: locationId } })

        await OperationRecord.create({
            operatorId: req.cookies.userId,
            businessType: 'Delete Location',
            businessId: locationId,
            optType: 'Delete',
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            remarks: null
        })

        return res.json(utils.response(1, `success`));
    },
    createLocation: async function (req, res) {
        let location = req.body;
        let result = await _SystemLocation.Location.findAll({ where: { locationName: location.locationName } })
        if (result.length) {
            log.warn(`Location ${ location.locationName } has already been created.`)
            return res.json(utils.response(0, `Location ${ location.locationName } has already been created.`));
        }
        await _SystemLocation.Location.create(location);

        await OperationRecord.create({
            operatorId: req.cookies.userId,
            businessType: 'Create Location',
            optType: 'New',
            afterData: JSON.stringify(location), 
            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            remarks: null
        })

        return res.json(utils.response(1, `success`));
    },
}
