var assert = require('assert')
var async = require('async')
var events = require('events')
var inherits = require('inherits')
var uuid = require('uuid/v4')

var Topology = require('./lib/topology')

module.exports = Kanin

function Kanin (opts) {
  assert(opts && opts.topology, 'topology must be provided')

  if (!(this instanceof Kanin)) return new Kanin(opts)

  events.EventEmitter.call(this)

  var self = this
  this.connection = null
  this.channel = null
  this.topology = new Topology({
    topology: opts.topology,
    onReply: this._onReply.bind(this)
  })

  this._consumers = []
  this._replyConsumerTag = null
  this._publishQueue = []
  this._requestQueue = []
  this._publishedRequests = []
  this._closed = false
  this._defaultRequestTimeout = 10000

  // This section is used purely to simulate connection errors...
  var previousHost
  this._startSimulateError = function () {
    previousHost = self.topology.connection.host
    self.topology.connection.host = 'something_unreachable'
    self.connection.connection.stream.emit(
      'error',
      new Error('simulated error')
    )
  }
  this._stopSimulateError = function () {
    self.topology.connection.host = previousHost
  }
}

inherits(Kanin, events.EventEmitter)

Kanin.prototype.configure = function (cb) {
  var self = this

  this.topology.configure((err, {connection, channel, replyConsumerTag}) => {
    if (err) {
      return cb(err)
    }

    self.connection = connection
    self.channel = channel
    self._replyConsumerTag = replyConsumerTag

    self.connection.on('error', err => {
      self._onConnectionError(err)
    })

    self.connection.on('close', err => {
      self._onConnectionClosed(err)
    })

    self.channel.on('error', err => {
      self._onChannelError(err)
    })

    // Internal events
    self.channel.on('consumer.cancelled', queueName => {
      async.series(
        [
          next => self.topology.recreateQueue(channel, queueName, next),
          next => {
            var {queue, options, onMessage} = self._consumers.find(
              c => c.queue === queueName
            )
            self._createConsumer(queue, options, onMessage, next)
          }
        ],
        err => {
          if (err) {
            self.emit(
              'error',
              new Error(`failed to recreate queue ${queueName}:`, err)
            )
          }
        }
      )
    })

    self.emit('connection.opened')
    cb()
  })
}

Kanin.prototype.close = function (cb) {
  this._closed = true
  this.connection ? this.connection.close(cb) : process.nextTick(cb)
  this.connection = null
}

Kanin.prototype.handle = function ({queue, options, onMessage}, cb) {
  var self = this
  if (!options) {
    options = {}
  }

  this._createConsumer(queue, options, onMessage, (err, consumerTag) => {
    if (err) {
      return cb(err)
    }
    options.consumerTag = consumerTag
    self._consumers.push({queue, options, onMessage})
    cb()
  })
}

Kanin.prototype.unsubscribeAll = function (cb) {
  var self = this
  var tags = this._consumers.map(c => c.options.consumerTag)

  if (this._replyConsumerTag) {
    tags.push(this._replyConsumerTag)
  }

  async.forEach(
    tags,
    (tag, next) => {
      self.channel.cancel(tag, err => {
        if (err) {
          return next(err)
        }
        if (tag === this._replyConsumerTag) {
          self._replyConsumerTag = null
        } else {
          var idx = self._consumers.findIndex(
            c => c.options.consumerTag === tag
          )
          self._consumers.splice(idx, 1)
        }
        next()
      })
    },
    cb
  )
}

Kanin.prototype.publish = function (exchange, message) {
  if (this.connection) {
    this._publish(exchange, message)
  } else {
    this._publishQueue.push({exchange, message})
  }
}

Kanin.prototype.request = function (exchange, message, cb) {
  var self = this
  var replyQueue = this.topology.replyQueue

  if (!replyQueue) {
    return cb(new Error('no reply queue has been configured!'))
  }

  // The request timeout will be wrong for requests which haven't been
  // sent due to lost connections. Depending on the reconnect time, the
  // request timeout will effectively be longer than planned.
  if (!this.connection) {
    return this._requestQueue.push({exchange, message, callback: cb})
  }

  var correlationId = uuid()
  var timeoutHandle = setTimeout(() => {
    var idx = self._publishedRequests.findIndex(
      r => r.correlationId === correlationId
    )

    if (idx === -1) {
      console.error('sent request not found, has it already been removed?')
      return
    }

    var req = self._publishedRequests[idx]
    self._publishedRequests.splice(idx, 1)
    process.nextTick(req.callback, new Error('request timeout'))
  }, message.timeout || this._defaultRequestTimeout)

  this._publish(exchange, {
    correlationId,
    replyTo: replyQueue.name,
    body: message.body,
    routingKey: message.routingKey
  })
  this._publishedRequests.push({correlationId, callback: cb, timeoutHandle})
}

Kanin.prototype._publish = function (exchange, message) {
  var json = JSON.stringify(message.body)

  this.channel.publish(exchange, message.routingKey, Buffer.from(json), {
    contentEncoding: 'utf8',
    contentType: 'application/json',
    correlationId: message.correlationId,
    expiration: message.expiration,
    replyTo: message.replyTo
  })
}

Kanin.prototype._createConsumer = function (queueName, options, onMessage, cb) {
  var self = this
  var count = options.prefetch || 0

  var wrappedMessageHandler = msg => {
    if (!msg) {
      return self.channel.emit('consumer.cancelled', queueName)
    }

    if (msg.properties.contentType === 'application/json') {
      msg.body = JSON.parse(msg.content)
    }

    msg.ack = self._ack.bind(self, msg)
    msg.nack = self._nack.bind(self, msg)
    msg.reject = self._reject.bind(self, msg)
    msg.reply = self._reply.bind(self, msg)

    onMessage(msg)
  }

  var global = false
  this.channel.prefetch(count, global)
  this.channel.consume(
    queueName,
    wrappedMessageHandler,
    {
      noAck: setDefault(options.noAck, false),
      exclusive: setDefault(options.exclusive, false),
      arguments: setDefault(options.arguments, null)
    },
    (err, ok) => {
      if (err) return cb(err)

      cb(null, ok.consumerTag)
    }
  )
}

Kanin.prototype._reconnect = function () {
  var self = this
  async.retry(
    {
      times: 10,
      interval: retryCount => 10 * Math.pow(2, retryCount)
    },
    cb => {
      if (!self._closed) {
        self.configure(cb)
      } else {
        cb()
      }
    },
    err => {
      if (err) {
        return self.emit('connection.unreachable')
      }

      // Abort reconnect if close has been called
      if (self._closed) {
        return
      }

      self._handleBackLog()
      self._reconnectConsumers(err => {
        if (err) {
          self.emit('error', err)
        }
      })
    }
  )
}

Kanin.prototype._reconnectConsumer = function (consumer, cb) {}

Kanin.prototype._reconnectConsumers = function (cb) {
  var self = this
  async.forEach(
    self._consumers,
    (consumer, next) => {
      self._createConsumer(
        consumer.queue,
        consumer.options,
        consumer.onMessage,
        next
      )
    },
    cb
  )
}

Kanin.prototype._ack = function (message) {
  this.channel && this.channel.ack(message)
}

var allUpTo = false
Kanin.prototype._nack = function (message) {
  var requeue = true
  this.channel && this.channel.nack(message, allUpTo, requeue)
}

Kanin.prototype._reject = function (message) {
  var requeue = false
  this.channel && this.channel.nack(message, allUpTo, requeue)
}

Kanin.prototype._reply = function (message, body) {
  var correlationId = message.properties.correlationId
  if (!correlationId) {
    return this.emit(
      'error',
      new Error('cannot reply to message without correlationId')
    )
  }

  var replyTo = message.properties.replyTo
  if (!replyTo) {
    return this.emit(
      'error',
      new Error('cannot reply to message without replyTo')
    )
  }

  if (!body) {
    return this.emit('error', new Error('must provide response body'))
  }

  // TODO: Save the response here?
  if (!this.channel) {
    return
  }

  var json = JSON.stringify(body)
  this.channel.sendToQueue(replyTo, Buffer.from(json), {
    contentType: 'application/json',
    contentEncoding: 'utf8',
    correlationId
  })
}

Kanin.prototype._onReply = function (message) {
  var correlationId = message.properties.correlationId
  if (!correlationId) {
    console.error('received response without message sent!')
  }

  var idx = this._publishedRequests.findIndex(
    r => r.correlationId === correlationId
  )
  if (idx === -1) {
    console.error('didnt find matching request!')
  }

  var req = this._publishedRequests[idx]
  clearTimeout(req.timeoutHandle)
  this._publishedRequests.splice(idx, 1)

  if (message.properties.contentType === 'application/json') {
    message.body = JSON.parse(message.content)
  }

  message.ack = this._ack.bind(this, message)
  message.nack = this._nack.bind(this, message)
  message.reject = this._reject.bind(this, message)

  req.callback(null, message)
}

Kanin.prototype._handleBackLog = function () {
  var self = this

  this._publishQueue.forEach(({exchange, message}) => {
    self.publish(exchange, message)
  })

  this._requestQueue.forEach(({exchange, message, callback}) => {
    self.request(exchange, message, callback)
  })
}

Kanin.prototype._onConnectionError = function (err) {
  var self = this
  this.connection.removeAllListeners()
  this.channel.removeAllListeners()
  this.connection = null
  this.channel = null

  self.emit('connection.error', err)
  self._reconnect()
}

Kanin.prototype._onConnectionClosed = function (err) {
  this.connection.removeAllListeners()
  this.channel.removeAllListeners()
  this.connection = null
  this.channel = null

  var isIntentionalClose = this._closed
  if (isIntentionalClose) {
    this.emit('connection.closed')
  } else {
    this.emit('connection.failed', err)
    this._reconnect()
  }
}

Kanin.prototype._onChannelError = function (err) {
  this.channel = null
  this.emit('channel.error', err)
}

function setDefault (x, val) {
  return x === undefined ? val : null
}