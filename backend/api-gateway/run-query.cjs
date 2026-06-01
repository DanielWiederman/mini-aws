const http = require('http');
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
