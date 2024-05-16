module.exports.dbConf = {
    host: '192.168.1.188',
    user: 'root',
    password: 'root',
    port: 3306,
    database: 'mobius-driver',
    connectionLimit: 100
};

module.exports.serverPort = 10000;
module.exports.serverPortHttps = 11000;

module.exports.proxy = {
    protocol: 'http',
    host: '10.0.1.14',
    port: 3128
}