module.exports.dbConf = {
    host: 'localhost',
    user: 'root',
    password: 'root',
    port: 3306,
    database: 'mobius-driver',
    timezone: 'GMT%2B8',
    multipleStatements: true,
    connectionLimit: 500
};

module.exports.dbSystemConf = {
    host: 'localhost',
    user: 'root',
    password: 'root',
    port: 3306,
    database: 'tms4',
    timezone: 'GMT%2B8',
    multipleStatements: true,
    connectionLimit: 500
};

module.exports.firebaseServer = 'http://192.168.1.188:10000'
module.exports.systemServer = 'http://localhost:5001'

module.exports.activeMQConf = 'mqtt://192.168.1.188:1883';

module.exports.dataPath = "D://data"

module.exports.serverPort = 5000;
module.exports.serverPortHttps = 5080;

module.exports.VehicleMissingFrequency = 30; // seconds

module.exports.CheckWhiteIP = false;

module.exports.Use_Local_MapTile = false;

module.exports.HQ_Approve_Mvuser_Role = 'Admin Role,HQ - God Mode,CO,B2/S3';

module.exports.SgidClient = {
    SCOPES: process.env.SCOPES,
    CLIENT_ID: process.env.CLIENT_ID,
    CLIENT_SECRET: process.env.CLIENT_SECRET,
    HOSTNAME: process.env.HOSTNAME,
    REDIRECT_URL: process.env.REDIRECT_URL,
    PRIVATE_KEY: process.env.PRIVATE_KEY,
    PUBLIC_KEY: process.env.PUBLIC_KEY,
}

module.exports.ekey_press_qrkey = 'mobius-qrkey';
module.exports.ekey_press_server_url = 'http://localhost:57181/';

module.exports.openProxy = false
module.exports.proxy = {
    protocol: 'http',
    host: '10.0.1.14',
    port: 3128
}

module.exports.OPS_SUMMARY_HUB_LIST = ['HUB1', 'HUB2', 'Unit2'];