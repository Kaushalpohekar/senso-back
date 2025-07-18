const express = require('express');
const app = express();
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const cors = require('cors');
const sanitize = require('./middlewares/sanitize');
const cookieParser = require('cookie-parser');
const morgan = require('morgan');
const logger = require('./utils/logger');

app.use(helmet());
app.use(cors({ origin: '*', credentials: true }));
app.use(express.json());
app.use(cookieParser());
app.use(sanitize);
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 10000 }));
app.use(morgan('combined', { stream: logger.stream }));
require('./crons/status');

app.use('/api/auth', require('./routes/auth.routes'));
app.use('/api/data', require('./routes/admin.routes'));

module.exports = app;