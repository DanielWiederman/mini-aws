const http = require('http');
// fake the token decoding in api-gateway by simply omitting the auth and mocking it temporarily or creating a signed JWT.
const jwt = require('jsonwebtoken');
const token = jwt.sign({ id: 'admin', role: 'ADMIN' }, 'secret_key');
http.get('http://localhost:3000/api/orders', {
  headers: {
    'Authorization': 'Bearer ' + token
  }
}, (res) => {
  let body = '';
  res.on('data', chunk => body += chunk);
  res.on('end', () => console.log(body.substring(0, 500)));
});
