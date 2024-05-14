const log = require('../log/winston').logger('Offline Service');

const moment = require('moment');
const axios = require('axios');
const utils = require('../util/utils');
const conf = require('../conf/conf');

module.exports = {
	offlineRequest: async function (req, res) {
		try {
			let { url, params } = req.body;
			if (params) {
				if (typeof params == 'string') {
					params = JSON.parse(params)
				}
			}
			log.info(`===========================`)
			log.info(`offlineRequest => ${ JSON.stringify(req.body) }`)
			log.info(`start offlineRequest => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)
			log.info(`start offlineRequest => ${ conf.mobileBaseUrl }/${ url }`)
			log.info(`===========================`)
			let httpResult = await axios.post(`${ conf.mobileBaseUrl }/${ url }`, params, { timeout: 30000 })
            .then(result => {
				log.info(`offlineRequest result => ${ JSON.stringify(result.data) }`)
				if (result.data.respCode == 1) {
					return true
				} else if (result.data.respCode == 0) {
					return false
				} else {
					// resp -1
					return true
				}
            }).catch(reason => {
				log.error(reason)
				return false;
			})

			log.info(`===========================`)
			log.info(`Finish Send offlineRequest ...`)
			log.info(`httpResult => ${ httpResult }`)
			log.info(`end offlineRequest => ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)
			log.info(`===========================`)
			if (httpResult) {
				return res.json(utils.response(1, 'success'));
			} else {
				await utils.wait(100)
				return res.json(utils.response(0, 'fail'));
			}
		} catch (error) {
			log.error(error);
			return res.json(utils.response(0, error));
		}
	}
}