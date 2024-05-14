const express = require('express');
const session = require('express-session');
const favicon = require('serve-favicon');
const path = require('path');
const logger = require('morgan');
const cookieParser = require('cookie-parser');
const bodyParser = require('body-parser');
const cors = require('cors');
let ejs = require('ejs');

require('./log/winston').initLogger()
const log = require('./log/winston').logger('APP');
const utils = require('./util/utils');

const index = require('./routes/index');
const keypress = require('./routes/keypress');
const offline = require('./routes/offline');
const urlInterceptor = require('./interceptor/urlInterceptor');
const taskStatusCheckInterceptor = require('./interceptor/taskStatusCheckInterceptor');

const mobileTORouter = require('./routes/mobileTO');
const mobileTripRouter = require('./routes/mobileTrip');

const app = express();
const cpu = require('./routes/cpu');
app.use('/cpu', cpu);

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.engine('html', ejs.__express);
app.set('view engine', 'html');

app.use(logger('dev'));
app.use(favicon(path.join(__dirname, 'public', 'mobius.ico')));
app.use(bodyParser.json({limit: '100mb'}));
app.use(bodyParser.urlencoded({ limit: '100mb', extended: true }));
app.use(cookieParser());
app.use(cors()); 

app.use(express.static(path.join(__dirname, 'public')));

app.use(function (req, res, next) {
  res.setTimeout(5 * 60 * 1000, function () {
      log.info('*************************');
      log.info('Request has timed out. ');
      log.info('HTTP Request URL: ', req.url);
      log.info('HTTP Request Body: ', JSON.stringify(req.body));
      log.info('*************************');
      res.json(utils.response(0, 'Request has timed out.'));
  });
  next();
});

app.use(urlInterceptor);
app.use(taskStatusCheckInterceptor);
app.use('/mobileTO', mobileTORouter);
app.use('/mobileTrip', mobileTripRouter);

app.use('/', index);
app.use('/keypress', keypress);
app.use('/offline', offline);

//singpass login
const singpassHome = require('./singpass/home')
const callback = require('./singpass/callback')
app.get('/singpassHome', singpassHome)
app.get('/callback', callback)

process.on('uncaughtException', function (e) {
  log.error(`uncaughtException`)
  log.error(e.message)
});
process.on('unhandledRejection', function (err, promise) {
  log.error(`unhandledRejection`);
  log.error(err.message);
})

app.use(function(req, res, next) {
    const err = new Error('Not Found');
    err.status = 404;
    next(err);
});

app.use(function(err, req, res, next) {
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};
  res.status(err.status || 500);
  log.error(`URL(${ req.originalUrl }) `, JSON.stringify(err));
  res.json(utils.response(0, err.message)); 
});

module.exports = app;