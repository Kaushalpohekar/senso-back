// require('dotenv').config();
// const app = require('./app');

// const PORT = process.env.PORT || 3300;
// const HOST = '0.0.0.0';

// app.listen(PORT, HOST, () => {
//   console.log(`Server running on http://${HOST}:${PORT}`);
// });
require('dotenv').config();
const fs = require('fs');
const https = require('https');
const http = require('http');
const app = require('./app');

const PORT = process.env.PORT || 3300;
const HOST = '0.0.0.0';

const sslOptions = {
  key: fs.readFileSync('/etc/letsencrypt/live/senso.senselive.io/privkey.pem', 'utf8'),
  cert: fs.readFileSync('/etc/letsencrypt/live/senso.senselive.io/fullchain.pem', 'utf8')
};

https.createServer(sslOptions, app).listen(PORT, HOST, () => {
  console.log(`✅ HTTPS Server running on https://${HOST}:${PORT}`);
});

http.createServer((req, res) => {
  res.writeHead(301, { Location: `https://${req.headers.host}${req.url}` });
  res.end();
}).listen(80, () => {
  console.log('🌐 HTTP redirecting to HTTPS');
});
