const log = require('../log/winston').logger('DB Helper');

const conf = require('../conf/conf.js');

const { Sequelize } = require('sequelize');
const createNamespace = require('cls-hooked').createNamespace;
const transportNamespace = createNamespace('mobile-system');
Sequelize.useCLS(transportNamespace);

module.exports.sequelizeSystemObj = new Sequelize(conf.dbSystemConf.database, conf.dbSystemConf.user, conf.dbSystemConf.password, {
    host: conf.dbSystemConf.host,
    port: conf.dbSystemConf.port,
    dialect: 'mysql',
    logging: msg => {
        //not print select sql
        if (msg && msg.indexOf('SELECT') == -1 && msg.indexOf('select') == -1) {
            log.info(msg)
        }
    },
    define: {
        freezeTableName: true
    },
    pool: {
        max: conf.dbSystemConf.connectionLimit,
        min: 0,
        acquire: 30000,
        idle: 10000
    },
    dialectOptions: {
        charset: 'utf8mb4'
    },
    timezone: '+08:00'
});

