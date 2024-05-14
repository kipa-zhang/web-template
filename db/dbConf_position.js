const conf = require('../conf/conf.js');

const { Sequelize } = require('sequelize');
const createNamespace = require('cls-hooked').createNamespace;
const transportNamespace = createNamespace('mobile-gps');
Sequelize.useCLS(transportNamespace);

module.exports.sequelizePositionObj = new Sequelize(conf.dbPositionConf.database, conf.dbPositionConf.user, conf.dbPositionConf.password, {
    host: conf.dbPositionConf.host,
    port: conf.dbPositionConf.port,
    dialect: 'mysql',
    logging: msg => {
        //not print select sql
        if (msg && msg.indexOf('SELECT') == -1 && msg.indexOf('select') == -1) {
            // log.info(msg)
        }
    },
    define: {
        freezeTableName: true
    },
    pool: {
        max: conf.dbPositionConf.connectionLimit/2,
        min: 0,
        acquire: 30000,
        idle: 10000
    },
    dialectOptions: {
        charset: 'utf8mb4'
    },
    timezone: '+08:00'
});