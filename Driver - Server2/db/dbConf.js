
const conf = require('../conf/conf.js');

const { Sequelize } = require('sequelize');
const createNamespace = require('cls-hooked').createNamespace;
const transportNamespace = createNamespace('transport-mobile');
Sequelize.useCLS(transportNamespace);

module.exports.sequelizeObj = new Sequelize(conf.dbConf.database, conf.dbConf.user, conf.dbConf.password, {
    host: conf.dbConf.host,
    port: conf.dbConf.port,
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
        max: conf.dbConf.connectionLimit/2,
        min: 0,
        acquire: 30000,
        idle: 10000
    },
    dialectOptions: {
        charset: 'utf8mb4'
    },
    timezone: '+08:00'
});
