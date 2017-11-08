var async = require('async')
var amqp = require('amqplib/callback_api')
var utils = require('./utils')

module.exports = Topology

function Topology (opts) {
  if (!(this instanceof Topology)) return new Topology(opts)

  this.connection = opts.topology.connection
  this.exchanges = opts.topology.exchanges
  this.queues = opts.topology.queues
  this.bindings = opts.topology.bindings
  this.replyQueue = opts.topology.replyQueue
  this.socketOptions = opts.topology.socketOptions

  this._onReply = opts.onReply
}

Topology.prototype.configure = function (cb) {
  var {exchanges, queues, bindings, replyQueue, socketOptions} = this
  var onReply = this._onReply
  var connectionOpts = this.connection

  var connection
  var channel
  var replyConsumerTag

  async.series(
    [
      next => {
        amqp.connect(
          utils.amqpUrl(connectionOpts),
          socketOptions,
          (err, conn) => {
            connection = conn
            next(err)
          }
        )
      },
      next => {
        connection.createChannel((err, ch) => {
          channel = ch
          next(err)
        })
      },
      next => {
        assertExchanges(channel, exchanges, next)
      },
      next => {
        assertQueues(channel, queues, next)
      },
      next => {
        bindQueues(channel, exchanges, queues, bindings, next)
      },
      next => {
        setupReplyQueue(channel, replyQueue, onReply, (err, consumerTag) => {
          replyConsumerTag = consumerTag
          next(err)
        })
      }
    ],
    err => cb(err, {connection, channel, replyConsumerTag})
  )
}

Topology.prototype.recreateQueue = function (channel, name, cb) {
  var queue = this.queues.find(q => q.name === name)
  var exchanges = this.exchanges
  var bindings = this.bindings.filter(b => b.target === name)

  async.series(
    [
      next => assertQueues(channel, [queue], next),
      next => bindQueues(channel, exchanges, [queue], bindings, next)
    ],
    cb
  )
}

function assertExchanges (channel, exchanges, cb) {
  async.forEach(
    exchanges,
    (e, next) => {
      channel.assertExchange(
        e.name,
        e.type,
        {
          durable: setDefault(e.durable, true),
          autoDelete: setDefault(e.autoDelete, false),
          arguments: setDefault(e.arguments, null)
        },
        next
      )
    },
    cb
  )
}

function assertQueues (channel, queues, cb) {
  async.forEach(
    queues,
    (q, next) => {
      channel.assertQueue(
        q.name,
        {
          exclusive: setDefault(q.exclusive, false),
          durable: setDefault(q.durable, true),
          autoDelete: setDefault(q.autoDelete, false),
          expires: setDefault(q.expires, null),
          arguments: setDefault(q.arguments, null)
        },
        next
      )
    },
    cb
  )
}

function bindQueues (channel, exchanges, queues, bindings, cb) {
  async.forEach(
    bindings,
    (binding, next) => {
      bindQueue(channel, exchanges, queues, binding, next)
    },
    cb
  )
}

function bindQueue (channel, exchanges, queues, binding, cb) {
  var queueOpts = queues.find(q => q.name === binding.target)
  if (!queueOpts) {
    return cb(new Error(`queue ${binding.target} not specified in topology`))
  }

  var exchangeOpts = exchanges.find(e => e.name === binding.exchange)
  if (!exchangeOpts) {
    return cb(
      new Error(`exchange ${binding.exchange} not specified in topology`)
    )
  }

  if (!binding.keys) {
    return cb(new Error(`binding is missing the keys property`))
  }

  var keys = Array.isArray(binding.keys) ? binding.keys : [binding.keys]
  async.forEach(
    keys,
    (key, callback) => {
      channel.bindQueue(binding.target, binding.exchange, key, null, callback)
    },
    cb
  )
}

function setupReplyQueue (channel, queue, onReply, cb) {
  if (!queue) {
    return cb()
  }

  if (!onReply) {
    return cb(
      new Error('reply message handler must be provided with replyQueue')
    )
  }

  async.series(
    [
      next => {
        channel.assertQueue(
          queue.name,
          {
            exclusive: setDefault(queue.exclusive, false),
            durable: setDefault(queue.durable, true),
            autoDelete: setDefault(queue.autoDelete, true),
            expires: setDefault(queue.expires, null),
            arguments: setDefault(queue.arguments, null)
          },
          next
        )
      },
      next => {
        var isGlobal = false
        channel.prefetch(0, isGlobal)
        channel.consume(
          queue.name,
          onReply,
          {
            noAck: setDefault(queue.noAck, false),
            exclusive: false,
            arguments: null
          },
          (err, ok) => {
            if (err) return cb(err)
            cb(null, ok.consumerTag)
          }
        )
      }
    ],
    cb
  )
}

function setDefault (x, val) {
  return x !== undefined ? x : val
}
