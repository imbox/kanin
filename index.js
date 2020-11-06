var assert = require('assert')
var async = require('async')
var events = require('events')
var safeStringify = require('fast-safe-stringify')
var util = require('util')
var uuid = require('uuid').v4

var Topology = require('./lib/topology')

module.exports = Kanin

function Kanin (opts) {
  assert(opts && opts.topology, 'topology must be provided')

  if (!(this instanceof Kanin)) return new Kanin(opts)

  events.EventEmitter.call(this)

  var self = this
  this.connection = null
  this.channel = null
  this.isReady = false
  this.topology = new Topology({
    topology: opts.topology,
    onReply: this._onReply.bind(this)
  })

  this._consumers = []
  this._replyConsumerTag = null
  this._sendQueue = []
  this._publishedRequests = []
  this._closed = false
  this._defaultRequestTimeout = opts.requestTimeout || 10000
  this._publishTimeout = opts.publishTimeout || 5000

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

util.inherits(Kanin, events.EventEmitter)

Kanin.prototype.configure = function (cb) {
  var self = this

  this.topology.configure((err, { connection, channel, replyConsumerTag }) => {
    if (err) {
      return cb(err)
    }

    self.isReady = true
    self.connection = connection
    self.channel = channel
    self._replyConsumerTag = replyConsumerTag

    self.connection.on('error', err => {
      self.isReady = false
      self._onConnectionError(err)
    })

    self.connection.on('close', err => {
      self._onConnectionClosed(err)
    })

    self.channel.on('error', err => {
      self.isReady = false
      self._onChannelError(err)
    })

    self.channel.on('drain', () => {
      process.nextTick(() => {
        self._handleBackLog(() => {})
      })
      self._onDrainEvent()
    })

    // Internal events
    self.channel.on('consumer.cancelled', queueName => {
      async.series(
        [
          next => self.topology.recreateQueue(channel, queueName, next),
          next => {
            var { queue, options, onMessage } = self._consumers.find(
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

Kanin.prototype.handle = function ({ queue, options, onMessage }, cb) {
  var self = this
  if (!options) {
    options = {}
  }

  this._createConsumer(queue, options, onMessage, (err, consumerTag) => {
    if (err) {
      return process.nextTick(cb, err)
    }
    options.consumerTag = consumerTag
    self._consumers.push({ queue, options, onMessage })
    process.nextTick(cb, null)
  })
}

Kanin.prototype.unsubscribeAll = function (cb) {
  if (!this.channel) {
    this._replyConsumerTag = null
    this._consumers.splice(0, this._consumers.length)
    return process.nextTick(cb, null)
  }

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

Kanin.prototype.publish = function (exchange, message, callback) {
  var contentType = message.contentType || 'application/json'
  var content
  if (contentType === 'application/json') {
    content = Buffer.from(JSON.stringify(message.body))
  } else if (contentType === 'text/plain') {
    content = Buffer.from(message.body)
  } else {
    return process.nextTick(
      callback,
      new Error('unrecognized contentType: ' + contentType)
    )
  }

  var correlationId = uuid()
  var options = {
    contentEncoding: 'utf8',
    contentType,
    correlationId,
    expiration: message.expiration,
    headers: message.headers
  }
  var routingKey = message.routingKey

  if (this.connection && this.channel && this.isReady) {
    this.isReady = this.channel.publish(exchange, routingKey, content, options)
    process.nextTick(callback)
  } else {
    var cb = once(callback)
    setTimeout(() => {
      cb(new Error('EPUBLISHTIMEOUT'))
    }, message.timeout || this._publishTimeout)
    this._sendQueue.push({
      exchange,
      routingKey,
      content,
      options,
      callback: cb,
      ttl: Date.now() + (message.timeout || this._publishTimeout)
    })
  }
}

Kanin.prototype.request = function (exchange, message, callback) {
  if (!callback) {
    throw new Error('callback missing!')
  }

  var replyQueue = this.topology.replyQueue
  if (!replyQueue) {
    return process.nextTick(
      callback,
      new Error('no reply queue has been configured!')
    )
  }

  var contentType = message.contentType || 'application/json'
  var content
  if (contentType === 'application/json') {
    content = Buffer.from(JSON.stringify(message.body))
  } else if (contentType === 'text/plain') {
    content = Buffer.from(message.body)
  } else {
    return process.nextTick(
      callback,
      new Error('unrecognized contentType: ' + contentType)
    )
  }

  var correlationId = uuid()
  var options = {
    contentEncoding: 'utf8',
    contentType,
    correlationId,
    expiration: message.expiration,
    headers: message.headers,
    messageId: correlationId, // For backwards compatibility with Rabbot
    replyTo: replyQueue.name
  }
  var routingKey = message.routingKey

  var self = this
  var timeoutHandle = setTimeout(() => {
    var idx = self._publishedRequests.findIndex(
      r => r.correlationId === correlationId
    )

    if (idx === -1) {
      return console.error(
        'sent request not found, has it already been removed?'
      )
    }

    var req = self._publishedRequests[idx]
    self._publishedRequests.splice(idx, 1)

    if (!req.callback) {
      return console.error('no callback found for request: ', req)
    }

    process.nextTick(req.callback, new Error('request timeout'))
  }, message.timeout || this._defaultRequestTimeout)

  this._publishedRequests.push({
    correlationId,
    callback,
    timeoutHandle
  })

  if (this.connection && this.channel && this.isReady) {
    this.isReady = this.channel.publish(exchange, routingKey, content, options)
  } else {
    this._sendQueue.push({
      exchange,
      routingKey,
      content,
      options,
      callback: noop,
      ttl: Date.now() + (message.timeout || this._defaultRequestTimeout)
    })
  }
}

Kanin.prototype.reply = function (message, body, callback) {
  var correlationId = message.properties.correlationId
  if (!correlationId) {
    return process.nextTick(
      callback,
      new Error(
        `cannot reply to message without correlationId: ${safeStringify(
          message
        )}`
      )
    )
  }

  var replyTo = message.properties.replyTo
  if (!replyTo) {
    return process.nextTick(
      callback,
      new Error(
        `cannot reply to message without replyTo: ${safeStringify(message)}`
      )
    )
  }

  if (!body) {
    return process.nextTick(callback, new Error('must provide response body'))
  }

  var content = Buffer.from(JSON.stringify(body))
  var options = {
    contentType: 'application/json',
    contentEncoding: 'utf8',
    correlationId
  }

  if (this.channel && this.isReady) {
    this.isReady = this.channel.sendToQueue(replyTo, content, options)
    process.nextTick(callback)
  } else {
    var cb = once(callback)
    setTimeout(() => {
      cb(new Error('EPUBLISHTIMEOUT'))
    }, this._publishTimeout)
    this._sendQueue.push({
      replyTo,
      content,
      options,
      type: 'reply',
      callback: cb,
      ttl: Date.now() + this._publishTimeout
    })
  }
}

Kanin.prototype._createConsumer = function (queueName, options, onMessage, cb) {
  var self = this
  var opts = {
    prefetch: setDefault(options.prefetch, 5),
    noAck: setDefault(options.noAck, false),
    exclusive: setDefault(options.exclusive, false),
    arguments: setDefault(options.arguments, null)
  }
  var queue = this.topology.queues.find(q => q.name === queueName)
  if (!queue) {
    return cb(new Error(queueName + ' not found in topology'))
  }

  var wrappedMessageHandler = msg => {
    if (!msg) {
      return self.channel.emit('consumer.cancelled', queueName)
    }

    if (msg.properties.contentType === 'application/json') {
      msg.body = JSON.parse(msg.content)
    } else if (msg.properties.contentType === 'text/plain') {
      msg.body = msg.content.toString()
    }

    if (opts.noAck === true) {
      msg.ack = noop
      msg.nack = noop
      msg.reject = noop
    } else {
      msg.ack = self._ack.bind(self, msg)
      msg.nack = self._nack.bind(self, msg)
      msg.reject = self._reject.bind(self, msg)
    }
    msg.reply = self.reply.bind(self, msg)

    // Hinder unhandled errors in `onMessage` to bubble up as channel errors
    // and cause unnecessary reconnections.
    try {
      onMessage(msg)
    } catch (err) {
      process.nextTick(() => {
        throw err
      })
    }
  }

  var isGlobal = false
  this.channel.prefetch(opts.prefetch, isGlobal)
  this.channel.consume(
    queueName,
    wrappedMessageHandler,
    {
      noAck: opts.noAck,
      exclusive: opts.exclusive,
      arguments: opts.arguments
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

      self._handleBackLog(() => {
        self._reconnectConsumers(err => {
          if (err) {
            self.emit('error', err)
          }
        })
      })
    }
  )
}

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

Kanin.prototype._onReply = function (message) {
  if (this.topology.replyQueue.noAck) {
    message.ack = noop
    message.nack = noop
    message.reject = noop
  } else {
    message.ack = this._ack.bind(this, message)
    message.nack = this._nack.bind(this, message)
    message.reject = this._reject.bind(this, message)
  }

  var correlationId = message.properties.correlationId
  if (!correlationId) {
    return console.error(
      `received reply without correlationId! ${JSON.stringify(message)}`
    )
  }

  var idx = this._publishedRequests.findIndex(
    r => r.correlationId === correlationId
  )
  if (idx === -1) {
    console.error(`reply without matching request ${JSON.stringify(message)}`)
    return message.reject()
  }

  var req = this._publishedRequests[idx]
  clearTimeout(req.timeoutHandle)
  this._publishedRequests.splice(idx, 1)

  if (message.properties.contentType === 'application/json') {
    message.body = JSON.parse(message.content)
  }

  process.nextTick(req.callback, null, message)
}

Kanin.prototype._handleBackLog = function (done) {
  var self = this
  var stillReady = true
  async.whilst(
    cb => {
      cb(null, self._sendQueue.length > 0 && stillReady)
    },
    cb => {
      const {
        exchange,
        content,
        options,
        routingKey,
        replyTo,
        type,
        callback,
        ttl
      } = self._sendQueue.shift()
      if (Date.now() > ttl) {
        cb()
      } else {
        if (type === 'reply') {
          stillReady = self.channel.sendToQueue(replyTo, content, options)
        } else {
          stillReady = self.channel.publish(
            exchange,
            routingKey,
            content,
            options
          )
        }
        process.nextTick(callback)
        cb()
      }
    },
    () => {
      this.isReady = stillReady
      done()
    }
  )
}

Kanin.prototype._onConnectionError = function (err) {
  var self = this
  this.connection && this.connection.removeAllListeners()
  this.channel && this.channel.removeAllListeners()
  this.connection = null
  this.channel = null

  self.emit('connection.error', err)
  self._reconnect()
}

Kanin.prototype._onConnectionClosed = function (err) {
  this.connection && this.connection.removeAllListeners()
  this.channel && this.channel.removeAllListeners()
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
  this.channel && this.channel.removeAllListeners()
  this.channel = null
  this.emit('channel.error', err)
  this._reconnect()
}

Kanin.prototype._onDrainEvent = function () {
  this.emit('channel.drain')
}

function setDefault (x, val) {
  return x !== undefined ? x : val
}

function noop () {}

function once (fn) {
  var called = false
  return (...args) => {
    if (called) return
    called = true
    return fn(...args)
  }
}
