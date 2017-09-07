module.exports = {
  amqpUrl (opts) {
    var user = opts.user
    var pass = opts.pass
    var host = opts.host || 'localhost'
    var port = opts.port || 5672
    var heartbeat = opts.heartbeat || 10
    var vhost = opts.vhost || '%2f'
    return `amqp://${user && `${user}:${pass}@`}${host}:${port}/${vhost}?heartbeat=${heartbeat}`
  }
}
