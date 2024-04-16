const log = require('../log/winston').logger('Route Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const SOCKET = require('../socket/socket');
const ActiveMQ = require('../activemq/activemq');

const userService = require('../services/userService');
const groupService = require('../services/groupService');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { Route } = require('../model/route');
const { RouteWaypoint } = require('../model/routeWaypoint');
const { Waypoint } = require('../model/waypoint');
const { Pois } = require('../model/pois');
const { User } = require('../model/user');
const { Incident } = require('../model/incident');
const { UserZone } = require('../model/userZone.js');
const { Unit } = require('../model/unit.js');
const { OperationRecord } = require('../model/operationRecord.js');

module.exports.getRouteList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let routeName = req.body.routeName;
        let routeList = [];
        let user = await User.findByPk(userId);
        if (user) {
            if (routeName) {
                routeList = await Route.findAll({ where: { routeName: { [Op.substring]: routeName } } });
            } else {
                routeList = await Route.findAll();
            }

            let pageList = await userService.getUserPageList(req.cookies.userId, 'Driver Control', 'View Route')
            let operationList = pageList.map(item => `${ item.action }`).join(',')

            for (let route of routeList) {
                route.operation = operationList
            }

        } else {
            log.warn(`UserId ${ userId } does not exist.`)
        }
        return res.json(utils.response(1, routeList));    
    } catch (error) {
        log.error('(getRouteList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.getRoute = async function (req, res) {
    try {
        let routeNo = req.body.routeNo;
        let route = await Route.findByPk(routeNo);
        route = route.dataValues;
        delete route.navigation;
        delete route.line;
        route.fromPosition = JSON.parse(route.fromPosition)
        route.toPosition = JSON.parse(route.toPosition)
        let waypointList = await RouteWaypoint.findAll({ where: { routeNo } })
        route.waypointList = waypointList
        return res.json(utils.response(1, route));    
    } catch (error) {
        log.error('(getRouteList) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.askRouteLine = async function (req, res) {
    try {
        let fromPosition = req.body.fromPosition;
        let toPosition = req.body.toPosition;
        // [{ id: 0, type: 0, point: { lat, lng } },...]
        let waypointList = req.body.waypointList; 
        let msg = {};
        msg.FromAddr = fromPosition.lat + ':' + fromPosition.lng;
        msg.ToAddr = toPosition.lat + ':' + toPosition.lng;
        waypointList.forEach(function (waypoint, index) {
            msg['Waypoint' + (index + 1) + '_' + waypoint.type] = waypoint.position.lat + ':' + waypoint.position.lng;
        });
        log.info('(askRouteLine) msg: ', JSON.stringify(msg));
        ActiveMQ.publicAskRoute(Buffer.from(JSON.stringify(msg)));

        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(askRouteLine) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.reRouteLine = async function (req, res) {
    try {
        let fromPoint = req.body.fromPoint;
        let toPoint = req.body.toPoint;
        let waypointList = req.body.waypointList;
        let incidentNo = req.body.incidentNo;

        let incident = await Incident.findByPk(incidentNo);

        let newMsg = `Obstacle:${ incident.lat }:${ incident.lng },FromAddr:${ fromPoint.lat }:${ fromPoint.lng },ToAddr:${ toPoint.lat }:${ toPoint.lng },`;
        let waypointMsgList = [];
        waypointList.forEach(function (wayPoint, index) {
            waypointMsgList.push(`Waypoint${ index + 1 }_${ wayPoint.type }:${ wayPoint.point.lat }:${ wayPoint.point.lng }`)
        });
        newMsg = newMsg + waypointMsgList.join(',');

        log.info('(reRoute) msg: ', newMsg);
        ActiveMQ.publicReRoute(Buffer.from(newMsg));

        return res.json(utils.response(1, 'success'));
    } catch (err) {
        log.error('(reRoute) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.createRoute = async function (req, res) {
    try {
        let route = req.body.route;
        let waypointList = route.waypointList;
        await sequelizeObj.transaction(async transaction => {
            // update route
            route.routeNo = 'Route-' + moment().format('YYYYMMDD') + utils.generateUniqueKey();
            route.state = CONTENT.ROUTE_STATUS.UNASSIGNED;
            route.creator = req.cookies.userId;
            route.timeNeed = route.time;
            route.fromPosition = JSON.stringify(route.fromPosition)
            route.toPosition = JSON.stringify(route.toPosition)
            route.line = JSON.stringify(route.line)

            await Route.create(route);
            // update route waypoint
            let routeWaypointList = waypointList.map(routeWaypoint => {
                return { routeNo: route.routeNo, waypointId: routeWaypoint.id }
            })
            await RouteWaypoint.bulkCreate(routeWaypointList);

            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Create Route',
                businessId: null,
                optType: 'New',
                afterData: `${ JSON.stringify(route) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));   
    } catch (error) {
        log.error('(createRoute) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.deleteRoute = async function (req, res) {
    try {
        let routeNo = req.body.routeNo;
        await sequelizeObj.transaction(async transaction => {
            // find out route
            let route = await Route.findByPk(routeNo);
            if (!route) {
                throw Error(`RouteNo ${ routeNo } does not exist`)
            } 
            // check route state
            if (route.state === CONTENT.ROUTE_STATUS.ASSIGNED) {
                throw Error(`RouteNo ${ routeNo }'s state is ${ route.state }, can not delete!`)
            }
            // clear route info
            await Route.destroy({ where: { routeNo } });
            await RouteWaypoint.destroy({ where: { routeNo } })
            // broadcast to browser
            SOCKET.publicSocketMsg(CONTENT.BROADCAST_EVENT.ROUTE_UPDATE, null);

            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Delete Route',
                businessId: routeNo,
                optType: 'Delete',
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (error) {
        log.error('(deleteRoute) : ', error);
        return res.json(utils.response(0, error));
    }
};

module.exports.updateRoute = async function (req, res) {
    try {
        let route = req.body.route;
        let waypointList = route.waypointList;
        await sequelizeObj.transaction(async transaction => {
            let oldRoute = await Route.findByPk(route.routeNo)
            await Route.update({
                routeName: route.routeName,
                fromAddress: route.fromAddress,
                toAddress: route.toAddress,
                fromPosition: JSON.stringify(route.fromPosition),
                toPosition: JSON.stringify(route.toPosition),
                line: JSON.stringify(route.line),
                lineColor: route.lineColor,
                distance: route.distance,
                timeNeed: route.timeNeed,
            }, { where: { routeNo: route.routeNo } });
            let routeWaypointList = waypointList.map(routeWaypoint => {
                return { routeNo: route.routeNo, waypointId: routeWaypoint.id }
            })
            await RouteWaypoint.destroy({ where: { routeNo: route.routeNo } })
            await RouteWaypoint.bulkCreate(routeWaypointList);

            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Update Route',
                businessId: route.routeNo,
                optType: 'Update',
                beforeData: JSON.stringify(oldRoute), 
                afterData: JSON.stringify(route), 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })
        }).catch(error => {
            log.error(error);
            throw error
        });
        return res.json(utils.response(1, 'success'));  
    } catch (err) {
        log.error('(updateRoute) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.copyRoute = async function (req, res) {
    try {
        let routeNo = req.body.routeNo;
        await sequelizeObj.transaction(async transaction => {
            const checkRoute = async function (routeNo) {
                let routeResult = await Route.findByPk(routeNo)
                if (!routeResult) {
                    throw Error(`RouteNo ${ routeNo } does not exist.`)
                }
                return routeResult;
            }

            let route = await checkRoute(routeNo);
            route = route.dataValues;
            
            // create Route
            route.routeNo = 'Route-' + moment().format('YYYYMMDD') + utils.generateUniqueKey();
            route.state = CONTENT.ROUTE_STATUS.UNASSIGNED;

            // create RouteWaypoint
            let routeWaypointList = await RouteWaypoint.findAll({ where: { routeNo } })
            let newRouteWaypoint = []
            for (let routeWaypoint of routeWaypointList) {
                newRouteWaypoint.push({ routeNo: route.routeNo, waypointId: routeWaypoint.waypointId })
            }
            await RouteWaypoint.bulkCreate(newRouteWaypoint);
            await Route.create({
                routeNo: route.routeNo,
                routeName: route.routeName,
                fromAddress: route.fromAddress,
                toAddress: route.toAddress,
                fromPosition: route.fromPosition,
                toPosition: route.toPosition,
                line: route.line,
                lineColor: route.lineColor,
                distance: route.distance,
                timeNeed: route.timeNeed,
                state: CONTENT.ROUTE_STATUS.UNASSIGNED,
            });

            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Copy Route',
                businessId: routeNo,
                optType: 'Copy',
                afterData: JSON.stringify(route), 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));  
    } catch (error) {
        log.error('(copyRoute) : ', error);
        return res.json(utils.response(0, error));
    }
};

// ****************************************************
// Waypoint

module.exports.getWaypointList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let user = await User.findByPk(userId)
        let waypointList = [];
        
        if (user) {
            waypointList = await Waypoint.findAll();
        } else {
            log.warn(`UserId ${ userId } does not exist.`)
        }

        let pageList = await userService.getUserPageList(req.cookies.userId, 'Driver Control', 'Waypoint')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        for (let waypoint of waypointList) {
            waypoint.point = { lat: waypoint.lat, lng: waypoint.lng };
            delete waypoint.lat;
            delete waypoint.lng;

            waypoint.operation = operationList
        }

        return res.json(utils.response(1, waypointList));    
    } catch (err) {
        log.error('(getWaypointList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.createWaypoint = async function (req, res) {
    try {
        let waypoint = req.body.waypoint;
        await sequelizeObj.transaction(async transaction => {
            const checkWaypoint = async function (waypoint) {
                let waypointResult = await Waypoint.findOne({ where: { waypointName: waypoint.waypointName } })
                return waypointResult;
            }
            let waypointResult = await checkWaypoint(waypoint);
            if (waypointResult) waypoint.id = waypointResult.id;
            waypoint.owner = req.cookies.userId;
            await Waypoint.upsert(waypoint);

            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Create/Update Waypoint',
                businessId: null,
                optType: 'New/Update',
                afterData: JSON.stringify(waypoint), 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));   
    } catch (err) {
        log.error('(createWaypoint) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.deleteWaypoint = async function (req, res) {
    try {
        let waypoint = req.body.waypoint;
        await sequelizeObj.transaction(async transaction => {
            const checkWaypoint = async function (waypoint) {
                // check if exist in routeWaypoint
                let routeWaypointResult = await RouteWaypoint.findOne({ where: { waypointId: waypoint.id } })
                if (routeWaypointResult) {
                    log.warn(`Waypoint  ${ waypoint.id } is been used yet, can not delete.`)
                    throw Error(`Waypoint is been used yet, can not delete.`);
                }
            }
            
            await checkWaypoint(waypoint);
            await Waypoint.destroy({ where: { id: waypoint.id } })     
            
            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Delete Waypoint',
                businessId: waypoint.id,
                optType: 'Delete',
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(deleteWaypoint) : ', err);
        return res.json(utils.response(0, err));
    }
};

module.exports.updateWaypoint = async function (req, res) {
    try {
        let waypoint = req.body.waypoint;
        await sequelizeObj.transaction(async transaction => {
            const checkWaypoint = async function (waypoint) {
                // check if exist in routeWaypoint
                let waypointResult = await Waypoint.findOne({ where: { id: waypoint.id } })
                if (!waypointResult) {
                    throw Error(`Waypoint ${ waypointResult.waypointName } does not exist.`)
                }
            }
            
            await checkWaypoint(waypoint);
            await Waypoint.update(waypoint, { where: { id: waypoint.id } })
            SOCKET.publicSocketMsg(CONTENT.BROADCAST_EVENT.WAY_POINT_UPDATE, null);

            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Update Waypoint',
                businessId: waypoint.id,
                optType: 'Update',
                afterData: JSON.stringify(waypoint), 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));   
    } catch (err) {
        log.error('(updateWaypoint) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

// ****************************************************
// Pois


/**
 * User directly input pointName
 */
module.exports.getPointByPositionName = async function(req, res) {
    try {
        let position = req.body.position;
        log.info('(getPointByPositionName) position: ', position);
        let result = await sequelizeSystemObj.query(`
            SELECT * FROM location WHERE locationName LIKE ? LIMIT 20
        `, { type: QueryTypes.SELECT, replacements:["%" + position + "%"] })
        
        if (result.length > 0) {
            return res.json(utils.response(1, { lat: result[0].lat, lng: result[0].lng }));
        } else {
            return res.json(utils.response(1, null));
        }
    } catch (err) {
        log.error('(getPointByPositionName) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

/**
 * User input part of pointName
 */
module.exports.getNameByPositionName = async function (req, res) {
    try {
        let position = req.body.position;
        log.info('(getNameByPositionName) position: ', position);
        let allList = await sequelizeSystemObj.query(`
            SELECT *  FROM location WHERE locationName LIKE `+sequelizeSystemObj.escape("%"+position+"%")+` LIMIT 20
        `, { type: QueryTypes.SELECT })
        log.info(allList);
        return res.json(utils.response(1, allList.slice(0, 20)));
    } catch (err) {
        log.error('(getNameByPositionName) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};