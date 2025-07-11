const fs = require('fs');
const path = require('path');
const { importPKCS8, importSPKI } = require('jose');

const privateKeyPem = fs.readFileSync(path.join(__dirname, 'private.pem'), 'utf8');
const publicKeyPem = fs.readFileSync(path.join(__dirname, 'public.pem'), 'utf8');

const getPrivateKey = async () => importPKCS8(privateKeyPem, 'RS256');
const getPublicKey = async () => importSPKI(publicKeyPem, 'RS256');

module.exports = {
  getPrivateKey,
  getPublicKey,
};
