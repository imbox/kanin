# kanin
An opinionated RabbitMQ client built upon amqplib.

## Development
Requires `docker` and `docker-compose` to be installed. A RabbitMQ docker
container will be started with webadmin exposed on `localhost:15672`.

Start tests by runninig
```bash
$ npm test
```

To run tests but not restart the RabbitMQ container each time, first run
`npm run test:setup` and then each time you want to run the actual tests
`npm run test:run`
