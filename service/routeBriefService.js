const log = require('../log/winston').logger('RouteBrief Service');

const utils = require('../util/utils');
const { Task } = require('../model/task');
const { Route } = require('../model/route');

module.exports = {
    GetRouteInfo: async function (req, res) {
        let { taskId } = req.body
        let task = await Task.findOne({ where: { taskId } })
        if (!task) {
            log.warn(`TaskId ${taskId} do not exist.`)
            return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
        }

        let result = {
            reportingLocation: "",
            destination: "",
            line: null,
            distance: "",
            timeNeed: "",
        }
        let { routeNo, pickupDestination, dropoffDestination, routePoints, routeDistance, routeTimeNeed } = task
        if (!routeNo) {
            result.reportingLocation = pickupDestination
            result.destination = dropoffDestination
            result.line = routePoints ? JSON.parse(routePoints) : null
            result.distance = `(${routeDistance / 1000}km)`
            result.timeNeed = `${routeTimeNeed}(min)`
        } else {
            let route = await Route.findByPk(routeNo)
            let { fromAddress, toAddress, line, distance, timeNeed } = route
            result.reportingLocation = fromAddress
            result.destination = toAddress
            result.line = line ? JSON.parse(line) : null
            result.distance = `(${distance / 1000}km)`
            result.timeNeed = `${timeNeed}(min)`
        }
        return res.json(utils.response(1, result));
    },
}