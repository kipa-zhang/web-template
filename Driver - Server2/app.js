const express = require('express');
const session = require('express-session');
const path = require('path');
const favicon = require('serve-favicon');
const cookieParser = require('cookie-parser');
const bodyParser = require('body-parser');
const ejs = require('ejs');
const cors = require('cors');
const log = require('./log/winston').logger('APP');
const utils = require('./util/utils');
const conf = require('./conf/conf');
const helmet = require('helmet');
const crypto = require('crypto');

const app = express();

app.use((req, res, next) => {
	res.locals.cspNonce = crypto.randomBytes(32).toString("hex");
	next();
});
app.use(helmet({
	contentSecurityPolicy: {
		useDefaults: false,
		directives: {
			// "default-src": ["'self' 'unsafe-inline' 'unsafe-eval'"],
			"default-src": ["'self'"],
			// "script-src": ['self', 'unsafe-inline', 'unsafe-eval'],
			"script-src": ["'self' 'unsafe-eval'", (req, res) => `'nonce-${ res.locals.cspNonce }'`],
			// "script-src-attr": ["'self' 'unsafe-inline' 'unsafe-eval'"],
			"script-src-attr": ["'self' 'unsafe-inline'"],
			"style-src": ["'self' 'unsafe-inline'"],
			// "style-src": ["'self' 'unsafe-inline' 'unsafe-eval'"],
			"connect-src": ["'self'"],
			"img-src": ["'self' data: https://gac-geo.googlecnapps.cn"],
			"form-action": ["'self'"],
			"frame-ancestors": ["'self'"],
			// 'upgrade-insecure-requests': [],
		},
		// reportOnly: true,
	},
}));

app.use(function(req, res, next) {
	if (['/._darcs', '/.bzr', '/.hg', '/BitKeeper', '/latest/meta-data/'].includes(req.url)) {
		const err = new Error('Not Found');
		err.status = 404;
		next(err);
	} else {
		next()
	}
});

app.set('views', path.join(__dirname, 'views'));
app.engine('.html', ejs.__express);
app.set('view engine', 'html');

app.use(favicon(path.join(__dirname, 'public', 'mobius.ico')));
app.use(bodyParser.json({limit: '10mb'}));
app.use(bodyParser.urlencoded({ limit: '10mb', extended: true }));
app.use(cookieParser());
app.use(session({
	secret: 'personal-session',
	resave: false,
	saveUninitialized: false
}));

const options = {
    maxAge: 30 * 24 * 3600 * 1000,
}
// app.use(express.static(path.join(__dirname, 'node_modules'), options));
app.use(express.static(path.join(__dirname, 'public', 'resources')));
app.use(express.static(path.join(__dirname, 'public', 'statics'), options));
app.use(cors({
	origin: `https://localhost`
}));

app.use (function (req, res, next) {
	// console.log(req.protocol)
	if (req.protocol == 'https') {
		// request was via https, so do no special handling
		next();
	} else {
		let host = req.get('host');
		if (host.indexOf(':') > -1) {
			host = host.split(':')[0] + `:${ conf.serverPortHttps }`
		}
		// request was via http, so redirect to https
		res.redirect('https://' + host + req.url);
	}
});

const home = require('./singpass/home')
const callback = require('./singpass/callback')
app.get('/home', home)
app.get('/callback', callback)

const urlInterceptor = require('./interceptor/urlInterceptor');
const IPInterceptor = require('./interceptor/IPInterceptor');
const tokenInterceptor = require('./interceptor/tokenInterceptor');
const roleInterceptor = require('./interceptor/roleInterceptor');
const dashboardInterceptor = require('./interceptor/dashboard');
app.use(tokenInterceptor);
app.use(urlInterceptor);
app.use(IPInterceptor);
app.use(roleInterceptor);
app.use(dashboardInterceptor);

const index = require('./routes/index');
const event = require('./routes/event');
const upload = require('./routes/upload');
const route = require('./routes/route');
const driver = require('./routes/driver');
const zone = require('./routes/zone');
const vehicle = require('./routes/vehicle');
const incident = require('./routes/incident');
const track = require('./routes/track');
const traffic = require('./routes/traffic');
const assign = require('./routes/assign');
const indentAssigned = require('./routes/indentAssigned');
const unit = require('./routes/unit');
const indent = require('./routes/indent');
const mtAdmin = require('./routes/mtAdmin');
const dashboard = require('./routes/dashboard');
const credit = require('./routes/credit');
const hqDashboard = require('./routes/hqDashboard');
const task = require('./routes/dashboard');
const resourcesDashboard = require('./routes/resourcesDashboard');
const hoto = require('./routes/hoto');
const loanOut = require('./routes/loanOut');
const notice = require('./routes/notice');
const incidentDetail = require('./routes/incidentDetail');
const urgent = require('./routes/urgent');
const user = require('./routes/user');
const reportCreator = require('./routes/reportCreator');

app.use('/', index);
app.use('/event', event);
app.use('/upload', upload);
app.use('/route', route);
app.use('/driver', driver);
app.use('/zone', zone);
app.use('/vehicle', vehicle);
app.use('/incident', incident);
app.use('/track', track);
app.use('/traffic', traffic);
app.use('/assign', assign);
app.use('/indentAssigned', indentAssigned);
app.use('/unit', unit);
app.use('/indent', indent);
app.use('/mtAdmin', mtAdmin);
app.use('/dashboard', dashboard);
app.use('/credit', credit);
app.use('/hqDashboard', hqDashboard);
app.use('/MV-Dashboard', task);
app.use('/resourcesDashboard', resourcesDashboard);
app.use('/hoto', hoto);
app.use('/loanOut', loanOut);
app.use('/notice', notice);
app.use('/incidentDetail', incidentDetail);
app.use('/urgent', urgent);
app.use('/user', user);
app.use('/reportCreator', reportCreator);

app.use(function(req, res, next) {
    if (req.url && req.url.startsWith('/map/Tiles')) {
        log.warn(`Local map tile: ${req.url} does not exist!`);
        return res.json(utils.response(0, `Local map tile: ${req.url} does not exist!`)); 
    }
    
    const err = new Error('Not Found');
    err.status = 404;
    next(err);
});

app.use(function (err, req, res, next) {
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};
  log.error(err);
  res.status(err.status || 500);
  if (err.status == 404) {
      res.render('404');
  } else {
      res.json(utils.response(0, 'server error')); 
  }
});

process.on('uncaughtException', function (e) {
    log.error(`uncaughtException`)
    log.error(e.message)
});
process.on('unhandledRejection', function (err, promise) {
    log.error(`unhandledRejection`);
    log.error(err.message);
})

/**
 * Need init these modules after system start!
 */
require('./activemq/activemq'); // 2023-2-1 Jasmin  => close mq

const { NotificationSchedule, UrgentSchedule, UpdateDutyStatus } = require('./firebase/schedule');
NotificationSchedule();
// UrgentSchedule();
UpdateDutyStatus();

module.exports = app;