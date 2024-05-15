// ['RS256', 'RS384', 'RS512', 'ES256', 'ES384', 'ES512', 'HS256', 'HS384', 'HS512', 'none']
const Header = {
    type: 'JWT',
    algorithm: 'HS256', // Command: openssl list -digest-algorithms
    expire: 36 * 60 * 60, // seconds
}
module.exports.Header = Header

const SecretKey = 'hello, world!';
module.exports.Secret = SecretKey
