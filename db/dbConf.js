const log = require('../winston/logger').logger('DB Helper');
const conf = require('../conf/conf.js');

const { Sequelize } = require('sequelize');
const createNamespace = require('cls-hooked').createNamespace;
const transportNamespace = createNamespace('transport');
Sequelize.useCLS(transportNamespace);

module.exports.sequelizeObj = new Sequelize(conf.dbConf.database, conf.dbConf.user, conf.dbConf.password, {
    host: conf.dbConf.host,
    port: conf.dbConf.port,
    dialect: 'mysql',
    logging: msg => {
		log.info(msg)
	},
    define: {
        freezeTableName: true
    },
    pool: {
        max: conf.dbConf.connectionLimit,
        min: 0,
        acquire: 30000,
        idle: 10000
    },
    dialectOptions: {
        charset: 'utf8mb4'
    },
	timezone: '+08:00'
});
