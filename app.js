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

app.use('/senso-back', (req, res, next) => {
  next();
});

app.use('/senso-back/auth', require('./routes/auth.routes'));
app.use('/senso-back/data', require('./routes/admin.routes'));

module.exports = app;