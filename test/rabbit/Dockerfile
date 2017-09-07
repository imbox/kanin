FROM rabbitmq:3.6-management

ENV PLUGIN_BASE=v3.6.x
ENV DELAYED_MESSAGE_VERSION=0.0.1

RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/*

RUN set -x \
  && curl -sL -o /plugins/rabbitmq_delayed_message_exchange-${DELAYED_MESSAGE_VERSION}.ez  http://www.rabbitmq.com/community-plugins/${PLUGIN_BASE}/rabbitmq_delayed_message_exchange-${DELAYED_MESSAGE_VERSION}.ez \
  && rabbitmq-plugins enable --offline \
    rabbitmq_delayed_message_exchange
