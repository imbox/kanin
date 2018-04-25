/* eslint-env mocha */
var amqp = require('amqplib/callback_api')
var uuid = require('uuid/v4')
var async = require('async')
var EventEmitter = require('events')
var should = require('should')

var Kanin = require('../')
var utils = require('../lib/utils')

var baseTopology = {
  connection: {
    host: 'localhost',
    port: 5672,
    user: 'kanin',
    pass: 'kanin'
  },

  socketOptions: {
    clientProperties: {
      service: 'kanin-test'
    }
  },

  queues: [
    {
      name: 'test-queue',
      autoDelete: false
    },
    {
      name: 'test-queue2',
      autoDelete: false
    }
  ],

  exchanges: [
    {
      name: 'test-exchange',
      type: 'topic',
      durable: true,
      autoDelete: false
    }
  ],

  bindings: [
    {
      exchange: 'test-exchange',
      target: 'test-queue',
      keys: ['test.1']
    },
    {
      exchange: 'test-exchange',
      target: 'test-queue2',
      keys: ['test.2']
    }
  ]
}

class EventArray extends EventEmitter {
  constructor () {
    super()
    this.data = []
  }

  push (obj) {
    this.data.push(obj)
    this.emit('change')
  }
}

describe('Kanin', function () {
  var connection
  var channel

  var purgeQueues = (mq, done) => {
    async.forEach(
      mq.topology.queues,
      (q, next) => {
        channel ? channel.purgeQueue(q.name, next) : next()
      },
      done
    )
  }

  var publish = (exchange, message) => {
    var json = JSON.stringify(message.body)
    channel.publish(exchange, message.routingKey, Buffer.from(json), {
      contentEncoding: 'utf8',
      contentType: 'application/json'
    })
  }

  before(function (done) {
    async.series(
      [
        next => {
          amqp.connect(utils.amqpUrl(baseTopology.connection), (err, conn) => {
            connection = conn
            next(err)
          })
        },
        next => {
          connection.createChannel((err, ch) => {
            channel = ch
            next(err)
          })
        }
      ],
      done
    )
  })

  describe('pubsub', function () {
    var mq

    beforeEach(function (done) {
      mq = new Kanin({ topology: baseTopology })
      done()
    })

    afterEach(function (done) {
      async.series(
        [next => mq.close(next), next => purgeQueues(mq, next)],
        done
      )
    })

    it('can be established', function (done) {
      mq.configure(err => {
        should.not.exist(err)
        done()
      })
    })

    it('can subscribe/publish message', function (done) {
      var messages = new EventArray()

      function onMessage (message) {
        message.ack()
        messages.push(message)
      }

      async.series(
        [
          next => mq.configure(next),
          next => {
            mq.handle({ queue: 'test-queue', onMessage: onMessage }, next)
          },
          next => {
            mq.publish('test-exchange', {
              routingKey: 'test.1',
              body: {
                key: 1
              }
            })
            messages.once('change', next)
          }
        ],
        err => {
          if (err) return done(err)

          messages.data[0].body.should.deepEqual({ key: 1 })
          done()
        }
      )
    })

    it('connection error with unacked messages', function (done) {
      this.timeout(0)
      var afterError = false
      var messages = []
      function onMessage (message) {
        messages.push(message)

        // Not acking any messages makes them stay in queue
        if (afterError) {
          message.ack()
        }
      }

      async.series(
        [
          next => mq.configure(next),
          next => {
            mq.handle(
              {
                queue: 'test-queue',
                options: { prefetch: 10 },
                onMessage: onMessage
              },
              next
            )
          },
          next => {
            for (var i = 0; i < 50; i++) {
              mq.publish('test-exchange', {
                routingKey: 'test.1',
                body: {
                  key: i
                }
              })
            }

            setTimeout(next, 100)
          },
          next => {
            mq._startSimulateError()
            setTimeout(next, 100)
          },
          next => {
            messages.forEach(m => m.ack())
            setTimeout(next, 100)
          },
          next => {
            afterError = true
            mq._stopSimulateError()

            // The messages should have been requeued and sent to our listener again...
            setTimeout(next, 1000)
          }
        ],
        err => {
          if (err) return done(err)

          messages.length.should.equal(60)
          done()
        }
      )
    })

    it('two consumers with different prefetch', function (done) {
      var msgList1 = []
      var msgList2 = []

      function onMessage1 (msg) {
        msgList1.push(msg)
      }

      function onMessage2 (msg) {
        msgList2.push(msg)
      }

      async.series(
        [
          next => mq.configure(next),
          next => {
            mq.handle(
              {
                queue: 'test-queue',
                options: { prefetch: 5 },
                onMessage: onMessage1
              },
              next
            )
          },
          next => {
            mq.handle(
              {
                queue: 'test-queue2',
                options: { prefetch: 2 },
                onMessage: onMessage2
              },
              next
            )
          },
          next => {
            for (var i = 0; i < 50; i++) {
              mq.publish('test-exchange', {
                routingKey: 'test.1',
                body: {
                  key: i
                }
              })
              mq.publish('test-exchange', {
                routingKey: 'test.2',
                body: {
                  key: i
                }
              })
            }
            setTimeout(next, 100)
          }
        ],
        err => {
          if (err) return done(err)

          msgList1.length.should.equal(5)
          msgList2.length.should.equal(2)

          done()
        }
      )
    })

    it('queue deleted', function (done) {
      var messages = new EventArray()
      async.series(
        [
          next => mq.configure(next),
          next => {
            mq.handle(
              {
                queue: 'test-queue',
                options: { prefetch: 1 },
                onMessage: msg => {
                  msg.ack()
                  messages.push(msg)
                }
              },
              next
            )
          },
          next => {
            channel.deleteQueue('test-queue', null, next)
          },
          next => {
            // Wait for queue to be recreated
            setTimeout(next, 50)
          },
          next => {
            messages.once('change', next)
            publish('test-exchange', {
              routingKey: 'test.1',
              body: {
                test: 1
              }
            })
          }
        ],
        err => {
          if (err) return done(err)

          messages.data[0].body.should.deepEqual({ test: 1 })
          messages.data.length.should.equal(1)
          done()
        }
      )
    })

    it('publish after unsubscribe all queues', function (done) {
      this.timeout(0)
      var message
      async.series(
        [
          next => mq.configure(next),
          next => {
            mq.handle(
              {
                queue: 'test-queue',
                options: { prefetch: 1 },
                onMessage: msg => {
                  msg.ack()
                  message = msg
                }
              },
              next
            )
          },
          next => mq.unsubscribeAll(next),
          next => {
            publish('test-exchange', {
              routingKey: 'test.1',
              body: {
                test: 1
              }
            })
            setTimeout(next, 50)
          }
        ],
        err => {
          if (err) {
            return done(err)
          }
          should.not.exist(message)
          done()
        }
      )
    })

    it.skip('[MANUAL] connection closed by admin gui', function (done) {
      this.timeout(0)
      var messages = new EventArray()
      async.series(
        [
          next => mq.configure(next),
          next => {
            mq.handle(
              {
                queue: 'test-queue',
                options: { prefetch: 1 },
                onMessage: msg => {
                  msg.ack()
                  messages.push(msg)
                }
              },
              next
            )
          },
          next => {
            console.log(
              'Manually close connection from admin, within 20 seconds!'
            )
            setTimeout(next, 20000)
          },
          next => {
            messages.once('change', next)
            publish('test-exchange', {
              routingKey: 'test.1',
              body: {
                test: 1
              }
            })
          }
        ],
        err => {
          if (err) return done(err)

          messages.data[0].body.should.deepEqual({ test: 1 })
          messages.data.length.should.equal(1)
          done()
        }
      )
    })
  })

  describe('rpc', function () {
    var requester
    var responder

    beforeEach(function (done) {
      var queueId = uuid().slice(0, 8)
      requester = new Kanin({
        topology: {
          connection: baseTopology.connection,
          exchanges: [
            {
              name: 'requests-exchange',
              type: 'topic',
              autoDelete: true
            }
          ],

          replyQueue: {
            name: `test-${queueId}-response-queue`,
            exclusive: true,
            noAck: false
          }
        }
      })

      responder = new Kanin({
        topology: {
          connection: baseTopology.connection,
          exchanges: [
            {
              name: 'requests-exchange',
              type: 'topic',
              autoDelete: true
            }
          ],
          queues: [
            {
              name: 'test-requests-queue',
              limit: 10
            }
          ],
          bindings: [
            {
              exchange: 'requests-exchange',
              target: 'test-requests-queue',
              keys: ['test.request']
            }
          ]
        }
      })

      async.series(
        [next => responder.configure(next), next => requester.configure(next)],
        done
      )
    })

    afterEach(function (done) {
      async.series(
        [
          next => requester.close(next),
          next => responder.close(next),
          next => purgeQueues(requester, next),
          next => purgeQueues(responder, next)
        ],
        done
      )
    })

    it('reply queue', function (done) {
      var messages = []
      function onRequest (message) {
        messages.push(message)
        message.reply({
          statusCode: 200,
          message: 'OK',
          data: {}
        })
        message.ack()
      }

      async.series(
        [
          next => {
            responder.handle(
              {
                queue: 'test-requests-queue',
                options: { prefetch: 5 },
                onMessage: onRequest
              },
              next
            )
          },
          next => {
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                body: {
                  text: 'a request'
                }
              },
              (err, response) => {
                if (err) {
                  return next(err)
                }
                response.ack()
                messages.push(response)
                next()
              }
            )
          }
        ],
        err => {
          if (err) return done(err)

          messages.map(m => m.body).should.deepEqual([
            { text: 'a request' },
            {
              statusCode: 200,
              message: 'OK',
              data: {}
            }
          ])
          messages.length.should.equal(2)
          done()
        }
      )
    })

    it('reply queue timeout', function (done) {
      var expectedErr
      var response
      var startTime
      async.series(
        [
          next => {
            responder.handle(
              {
                queue: 'test-requests-queue',
                options: { prefetch: 5 },
                onMessage: msg => msg.ack()
              },
              next
            )
          },
          next => {
            startTime = Date.now()
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                timeout: 50,
                body: {
                  text: 'a request'
                }
              },
              (err, _response) => {
                response = _response
                expectedErr = err
                next()
              }
            )
          }
        ],
        err => {
          if (err) return done(err)

          expectedErr.should.be.an.Error()
          should.not.exist(response)

          var timeDiff = Date.now() - startTime
          timeDiff.should.be.within(50, 100)
          done()
        }
      )
    })

    it.skip('[MANUAL] delete queues and connections and look for strange behaviour', function (done) {
      this.timeout(0)
      async.series(
        [
          next => {
            responder.handle(
              {
                queue: 'test-requests-queue',
                options: { prefetch: 5 },
                onMessage: msg => msg.ack()
              },
              next
            )
          },
          next => {
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                timeout: 50,
                body: {
                  text: 'a request'
                }
              },
              () => next()
            )
          }
        ],
        err => {
          if (err) return done(err)

          console.log(
            'Close connections and delete queues, etc. Everything should be recreated'
          )
          console.log('you must manually stop test with CTRL-C')
        }
      )
    })
  })

  describe('rpc without ack', function () {
    var requester
    var responder

    beforeEach(function (done) {
      var queueId = uuid().slice(0, 8)
      requester = new Kanin({
        topology: {
          connection: baseTopology.connection,
          exchanges: [
            {
              name: 'requests-exchange',
              type: 'topic',
              autoDelete: true
            }
          ],

          replyQueue: {
            name: `test-${queueId}-response-queue`,
            durable: true,
            autoDelete: false,
            exclusive: false,
            noAck: true
          }
        }
      })

      responder = new Kanin({
        topology: {
          connection: baseTopology.connection,
          exchanges: [
            {
              name: 'requests-exchange',
              type: 'topic',
              autoDelete: true
            }
          ],
          queues: [
            {
              name: 'test-requests-queue',
              limit: 10
            }
          ],
          bindings: [
            {
              exchange: 'requests-exchange',
              target: 'test-requests-queue',
              keys: ['test.request']
            }
          ]
        }
      })

      async.series(
        [next => responder.configure(next), next => requester.configure(next)],
        done
      )
    })

    afterEach(function (done) {
      async.series(
        [
          next => requester.close(next),
          next => responder.close(next),
          next => purgeQueues(requester, next),
          next => purgeQueues(responder, next)
        ],
        done
      )
    })

    it('does not crash', function (done) {
      var messages = []
      function onRequest (message) {
        messages.push(message)
        message.reply({
          statusCode: 200,
          message: 'OK',
          data: {}
        })
        message.ack()
      }

      async.series(
        [
          next => {
            responder.handle(
              {
                queue: 'test-requests-queue',
                options: { prefetch: 5 },
                onMessage: onRequest
              },
              next
            )
          },
          next => {
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                timeout: 30000,
                body: {
                  text: 'a request'
                }
              },
              (err, response) => {
                if (err) {
                  return next(err)
                }
                messages.push(response)
                next()
              }
            )
          },
          next => {
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                timeout: 30000,
                body: {
                  text: 'second request'
                }
              },
              (err, response) => {
                if (err) {
                  return next(err)
                }
                messages.push(response)
                next()
              }
            )
          }
        ],
        err => {
          if (err) return done(err)

          messages.map(m => m.body).should.deepEqual([
            { text: 'a request' },
            {
              statusCode: 200,
              message: 'OK',
              data: {}
            },
            { text: 'second request' },
            {
              statusCode: 200,
              message: 'OK',
              data: {}
            }
          ])
          messages.length.should.equal(4)
          done()
        }
      )
    })

    it('unrecognized request response', function (done) {
      var messages = []
      function onRequest (message) {
        messages.push(message)

        let prevCorrelationId = message.properties.correlationId
        let prevMessageId = message.properties.messageId

        message.properties.correlationId = 'spoofed'
        message.properties.messageId = 'spoofed'
        message.reply({
          statusCode: 200,
          message: 'OK',
          data: {}
        })

        message.properties.correlationId = prevCorrelationId
        message.properties.messageId = prevMessageId
        message.ack()
      }

      let requestError
      async.series(
        [
          next => {
            responder.handle(
              {
                queue: 'test-requests-queue',
                options: { prefetch: 5 },
                onMessage: onRequest
              },
              next
            )
          },
          next => {
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                timeout: 500,
                body: {
                  text: 'a request'
                }
              },
              err => {
                requestError = err
                next()
              }
            )
          }
        ],
        err => {
          if (err) return done(err)

          messages.map(m => m.body).should.deepEqual([{ text: 'a request' }])
          messages.length.should.equal(1)
          requestError.should.deepEqual(new Error('request timeout'))
          done()
        }
      )
    })

    it('ack in noAck queue', function (done) {
      var messages = []
      function onRequest (message) {
        messages.push(message)
        message.reply({
          statusCode: 200,
          message: 'OK',
          data: {}
        })
        message.ack()
      }

      async.series(
        [
          next => {
            responder.handle(
              {
                queue: 'test-requests-queue',
                options: { prefetch: 5 },
                onMessage: onRequest
              },
              next
            )
          },
          next => {
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                timeout: 500,
                body: {
                  text: 'a request'
                }
              },
              (err, response) => {
                if (err) {
                  return next(err)
                }
                response.ack()
                next()
              }
            )
          },
          next => {
            requester.request(
              'requests-exchange',
              {
                routingKey: 'test.request',
                timeout: 500,
                body: {
                  text: 'second request'
                }
              },
              (err, response) => {
                if (err) {
                  return next(err)
                }
                response.ack()
                next()
              }
            )
          }
        ],
        err => {
          if (err) return done(err)

          messages
            .map(m => m.body)
            .should.deepEqual([
              { text: 'a request' },
              { text: 'second request' }
            ])
          messages.length.should.equal(2)
          done()
        }
      )
    })
  })
})
