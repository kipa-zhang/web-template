const utils = require('../util/utils');
const axios = require('axios');
const conf = require('../conf/conf');

const log = require('../log/winston').logger('Traffic Service');

const { SpeedBands } = require('../model/traffic/speedBands');
const { SpeedBandsTemp } = require('../model/traffic/speedBandsTemp');
const { sequelizeObj } = require('../db/dbConf');

const { cacheData, cacheCommonData } = require('../cache_tool/dashboard.js');

const header = {
    "AccountKey": "3c4imVWJT6u+yh3WphD2OQ==",
    "accept": "application/json",
}

module.exports.header = header

module.exports.getTrafficList = function (req, res) {
    // Will not more than 500
    let option = {
        headers: header
    }
    if (conf.openProxy) {
        option.proxy = conf.proxy
    }
    axios.get("http://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents", option
    ).then(response => {

        cacheCommonData({ url: req.originalUrl }, response.data.value)

        return res.json(utils.response(1, response.data.value));
    }).catch(error => {
        log.error('(getTrafficList) : ', error);
        return res.json(utils.response(0, 'Server error!'));
    });
}

module.exports.getTrafficImages = function (req, res) {
    // Will not more than 500
    let option = {
        headers: header
    }
    if (conf.openProxy) {
        option.proxy = conf.proxy
    }
    axios.get("https://api.data.gov.sg/v1/transport/traffic-images", option
    ).then(response => {
        
        cacheCommonData({ url: req.originalUrl }, response.data.items)

        return res.json(utils.response(1, response.data.items));
    }).catch(error => {
        log.error('(getTrafficImages) : ', error);
        return res.json(utils.response(0, 'Server error!'));
    });
}

module.exports.getEstTravelTimes = function (req, res) {
    let option = {
        headers: header
    }
    if (conf.openProxy) {
        option.proxy = conf.proxy
    }
    axios.get("http://datamall2.mytransport.sg/ltaodataservice/EstTravelTimes", option
    ).then(response => {
        return res.json(utils.response(1, response.data.items));
    }).catch(error => {
        log.error('(getEstTravelTimes) : ', error);
        return res.json(utils.response(0, 'Server error!'));
    });
}

module.exports.getTrafficSpeedBands = async function (req, res) {
        /**
         *  Refresh every 5 minutes
         *  A – Expressways
            B – Major Arterial Roads
            C – Arterial Roads
            D – Minor Arterial Roads
            E – Small Roads
            F – Slip Roads
            G – No category info available 

            Speed Bands Information. Total: 8
            1 – indicates speed range from 0 < 9
            2 – indicates speed range from 10 < 19
            3 – indicates speed range from 20 < 29
            4 – indicates speed range from 30 < 39
            5 – indicates speed range from 40 < 49
            6 – indicates speed range from 50 < 59
            7 – indicates speed range from 60 < 69
            8 – speed range from 70 or more
        */
    try {
        let { roadCategory, speedBand } = req.body;
        let option = {}
        if (roadCategory) option.RoadCategory = roadCategory.split(',') 
        if (speedBand) option.SpeedBand = speedBand.split(',') 

        let result = await SpeedBands.findAll({
            where: option
        })
        return res.json(utils.response(1, result));
    } catch (error) {
        console.error(error)
    }
}