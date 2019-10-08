FROM rabbitmq:3.6-management

RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl unzip \
  && rm -rf /var/lib/apt/lists/*

RUN set -x \
  && curl -sL -o /plugins/rabbitmq_delayed_message_exchange.zip https://dl.bintray.com/rabbitmq/community-plugins/3.6.x/rabbitmq_delayed_message_exchange/rabbitmq_delayed_message_exchange-20171215-3.6.x.zip \
  && unzip /plugins/rabbitmq_delayed_message_exchange.zip -d /plugins/ \
  && rabbitmq-plugins enable --offline \
    rabbitmq_delayed_message_exchange
