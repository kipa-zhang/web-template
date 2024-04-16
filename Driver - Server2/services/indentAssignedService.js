const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');
const SOCKET = require('../socket/socket');
const ACTIVEMQ = require('../activemq/activemq');

const moment = require('moment');
let axios = require('axios');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { Route } = require('../model/route');
const { RouteWaypoint } = require('../model/routeWaypoint');
const { Driver } = require('../model/driver');
const { CheckList } = require('../model/checkList');
const { DriverTask } = require('../model/driverTask');
const { DriverPosition } = require('../model/driverPosition');
const { VehicleRelation } = require('../model/vehicleRelation.js');
const { User } = require('../model/user.js');
const { UserGroup } = require('../model/userGroup.js');
const { Unit } = require('../model/unit.js');
const { Friend } = require('../model/friend.js');

const groupService = require('./groupService');
const userService = require('./userService');
const unitService = require('./unitService');
const assignService2 = require('./assignService2');
const { UserZone } = require('../model/userZone.js');
const { Vehicle } = require('../model/vehicle.js');
const { Task } = require('../model/task.js');

const _SystemDriver = require('../model/system/driver');
const _SystemVehicle = require('../model/system/vehicle');
const _SystemLocation = require('../model/system/location');
const _SystemTask = require('../model/system/task');
const _SystemRequest = require('../model/system/request');
const _SystemGroup = require('../model/system/group');
const { use } = require('../routes/assign.js');
const { loadFromBuffer } = require('images');

let TaskUtils = {
    getTypeOfVehicle: async function () {
        const typeOfVehicleList = await sequelizeSystemObj.query(
            `SELECT DISTINCT typeOfVehicle from contract_rate order by typeOfVehicle`,
            {
                type: QueryTypes.SELECT
            }
        );
        return typeOfVehicleList
    },
    findOutHubNodeOption: function (hub, node) {
        // Checkout limit option
        let option = {};
        if (!hub) {
            if (!node) {
                option = null;
            } else {
                option.subUnit = node;
            }
        } else {
            if (!node) {
                option.unit = hub;
            } else {
                option.unit = hub;
                option.subUnit = node;
            }
        }
        return option;
    },
    QueryAndFilterJobList: async function (reqParams) {
        let pageNum = reqParams.pageNum;
        let pageLength = reqParams.pageLength;
        let { execution_date, created_date, unit, status, driverStatus, tripNo, vehicleType } = reqParams
        let replacements = []
        let sql = `SELECT
            b.id as taskId, c.driverId, c.\`status\`, c.\`name\`, c.nric, c.contactNumber, d.vehicleNumber, e.\`name\` as tsp, 
            a.tripNo, a.noOfDriver,a.vehicleType, b.taskStatus, a.periodStartDate, a.periodEndDate,b.poc, b.pocNumber,
            sm.value as serviceMode, a.pickupDestination, a.dropoffDestination, a.approve, a.driver, g.groupName,b.createdAt, 
            st.name as resourceType, b.mobiusUnit
            FROM
                job a
            LEFT JOIN job_task b ON a.id = b.tripId
            LEFT JOIN driver c ON b.id = c.taskId
            LEFT JOIN vehicle d ON b.id = d.taskId
            LEFT JOIN service_provider e ON ifnull(b.serviceProviderId, a.serviceProviderId) = e.id
            LEFT JOIN request f ON a.requestId = f.id
            LEFT JOIN \`group\` g ON f.groupId = g.id AND  FIND_IN_SET(a.serviceTypeId, g.serviceType)
            LEFT JOIN service_mode sm ON a.serviceModeId = sm.id
            LEFT JOIN service_type st ON st.id = sm.service_type_id
            where 1=1 and a.approve = 1 and lower(st.category) = 'mv' and b.taskStatus = 'assigned'`
        if (status != "" && status != null) {
            if (!driverStatus) {
                sql += ` and b.taskStatus = ?`
                replacements.push(status)
            } else {
                // Mobius Driver Server (Available assign task)
                sql += ` and (b.taskStatus = ? OR (b.driverStatus = ? OR b.driverStatus IS NULL ))`
                replacements.push(status, driverStatus)
            }
        }
        if (vehicleType != "" && vehicleType != null) {
            sql += ` and a.vehicleType = ?`
            replacements.push(vehicleType)
        }
        if (created_date != "" && created_date != null) {
            sql += ` and DATE_FORMAT(b.createdAt,'%Y-%m-%d') = ?`
            replacements.push(created_date)
        }
        if (execution_date != "" && execution_date != null) {
            if (execution_date.indexOf('~') != -1) {
                const dates = execution_date.split(' ~ ')
                sql += ` and (b.executionDate >= ? and b.executionDate <= ?)`
                replacements.push(dates[0])
                replacements.push(dates[1])
            } else {
                sql += ` and b.executionDate = ?`
                replacements.push(execution_date)
            }
        }
    
        if (tripNo != "" && tripNo != null) {
            sql += ` and a.tripNo like ?`
            replacements.push(`%${tripNo}%`)
        }
        if (unit) {
            sql += ` and b.mobiusUnit in (?) `
            replacements.push(unit);
        } 
        
        let result = await sequelizeSystemObj.query(sql, {
            replacements: replacements,
            type: QueryTypes.SELECT,
        });

        if((pageNum || pageNum == 0) && pageLength){
            sql += ` order by b.id desc limit ?, ?`
            replacements.push(Number(pageNum))
            replacements.push(Number(pageLength))
        }
        let pageResult = await sequelizeSystemObj.query(sql,
            {
                replacements: replacements,
                type: QueryTypes.SELECT
            }
        );

       
        return { data: pageResult, recordsFiltered: result.length, recordsTotal: result.length }
    }
}

module.exports = {
    getVehicleType: async function (req, res) {
        try {
           let result = await TaskUtils.getTypeOfVehicle()
            return res.json(utils.response(1, result));     
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
        
    },
    getAssignableTaskList: async function (req, res) {
        try {
            let option = req.body;
            let hubNodeOption = await TaskUtils.findOutHubNodeOption(option.hub, option.node)
            let unitList = await Unit.findAll({where: hubNodeOption});
            option.unit = unitList.map(u => u.id)
            let result = await TaskUtils.QueryAndFilterJobList(option)
            return res.json({ data: result.data, recordsFiltered: result.recordsFiltered, recordsTotal: result.recordsFiltered }); 
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
}