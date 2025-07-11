const xss = require('xss');

function sanitizeInput(input) {
  if (typeof input === 'string') return xss(input);
  if (Array.isArray(input)) return input.map(sanitizeInput);
  if (typeof input === 'object' && input !== null) {
    const cleanObj = {};
    for (const key in input) {
      cleanObj[key] = sanitizeInput(input[key]);
    }
    return cleanObj;
  }
  return input;
}

function sanitizeMiddleware(req, res, next) {
  req.body = sanitizeInput(req.body);
  req.params = sanitizeInput(req.params);
  req.query = sanitizeInput({ ...req.query }); // avoids modifying getter directly
  next();
}

module.exports = sanitizeMiddleware;
