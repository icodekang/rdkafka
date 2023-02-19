#include "rdkafka.h"
#include "cmdline.h"

#include <stdio.h>
#include <signal.h>
#include <string.h>

static volatile sig_atomic_t run = 1;

static void parse_option(int argc, char** argv, cmdline::parser& parser) {
    parser.add<std::string>("type", 't', "producer or consumer", true);
    parser.add<std::string>("broker", '\0', "kafka broker", false);
    parser.add<std::string>("topic", '\0', "kafka topic", false);
    parser.add<std::string>("groupid", '\0', "kafka groupid", false);
    parser.add<int>("count", '\0', "kafka producer data count", false);
    parser.parse_check(argc, argv);
}

static int is_printable(const char *buf, size_t size) {
    size_t i;

    for (i = 0; i < size; i++)
        if (!isprint((int)buf[i]))
            return 0;

    return 1;
}

static void stop(int sig) {
    run = 0;
    fclose(stdin); /* abort fgets() */
}

static void pd_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
            rd_kafka_err2str(rkmessage->err));
    else
        fprintf(stderr,
                "%% Message delivered (%zd bytes, "
                "partition %" PRId32 ")\n",
                rkmessage->len, rkmessage->partition);
}

static void producer(const cmdline::parser& parser) {
    int count = 1;
    std::string broker, topic;
    rd_kafka_t *rk;        /* Producer instance handle */
    rd_kafka_conf_t *conf; /* Temporary configuration object */
    char errstr[512];      /* librdkafka API error reporting buffer */
    char buf[512];         /* Message value temporary buffer */

    if (parser.exist("broker"))
        broker = parser.get<std::string>("broker");
    if (parser.exist("topic"))
        topic = parser.get<std::string>("topic");
    if (parser.exist("count"))
        count = parser.get<int>("count");

    if (broker.empty() || topic.empty()) {
        fprintf(stderr, "param error, broker[%s] topic[%s]", broker.c_str(), topic.c_str());
        return;
    }

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", broker.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        return;
    }

    rd_kafka_conf_set_dr_msg_cb(conf, pd_msg_cb);

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        return;
    }

    signal(SIGINT, stop);

    fprintf(stderr,
            "%% Type some text and hit enter to produce message\n"
            "%% Or just hit enter to only serve delivery reports\n"
            "%% Press Ctrl-C or Ctrl-D to exit\n");

    fgets(buf, sizeof(buf), stdin);
    while (run && count--) {
        size_t len = strlen(buf);
        rd_kafka_resp_err_t err;

        if (buf[len - 1] == '\n') /* Remove newline */
                buf[--len] = '\0';

        if (len == 0) {
                /* Empty line: only serve delivery reports */
                rd_kafka_poll(rk, 0);
                continue;
        }

retry:
        err = rd_kafka_producev(
            /* Producer handle */
            rk,
            /* Topic name */
            RD_KAFKA_V_TOPIC(topic.c_str()),
            /* Make a copy of the payload. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            /* Message value and length */
            RD_KAFKA_V_VALUE(buf, len),
            /* End sentinel */
            RD_KAFKA_V_END);

        if (err) {
            fprintf(stderr,
                    "%% Failed to produce to topic %s: %s\n", topic.c_str(),
                    rd_kafka_err2str(err));
            if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                rd_kafka_poll(rk, 1000);
                goto retry;
            }
        } else {
            fprintf(stderr,
                    "%% Enqueued message (%zd bytes) "
                    "for topic %s\n",
                    len, topic);
        }

        rd_kafka_poll(rk, 0 );
    }

    fprintf(stderr, "%% Flushing final messages..\n");
    rd_kafka_flush(rk, 10 * 1000);

    if (rd_kafka_outq_len(rk) > 0)
        fprintf(stderr, "%% %d message(s) were not delivered\n",
            rd_kafka_outq_len(rk));
}

static void consumer(const cmdline::parser& parser) {
    int i;
    std::string broker, groupid, topic;
    std::vector<std::string> topic_vec;
    rd_kafka_t *rk;          /* Consumer instance handle */
    rd_kafka_conf_t *conf;   /* Temporary configuration object */
    rd_kafka_resp_err_t err; /* librdkafka API error code */
    rd_kafka_topic_partition_list_t *subscription; /* Subscribed topics */
    char errstr[512];        /* librdkafka API error reporting buffer */

    if (parser.exist("broker"))
        broker = parser.get<std::string>("broker");
    if (parser.exist("topic"))
        topic = parser.get<std::string>("topic");
    if (parser.exist("groupid"))
        groupid = parser.get<std::string>("groupid");

    if (broker.empty() || topic.empty() || groupid.empty()) {
        fprintf(stderr, "param error, broker[%s] topic[%s] groupid[%s]", broker.c_str(), topic.c_str(), groupid.c_str());
        return;
    }

    std::string topic_temp = topic;
    while (true) {
        if (topic_temp.find(",") == std::string::npos) {
            topic_vec.push_back(topic_temp);
            break;
        }
        topic_vec.push_back(topic_temp.substr(0, topic_temp.find(",")));
        topic_temp = topic_temp.substr(topic_temp.find(",") + 1);
    }

    conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", broker.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return;
    }

    if (rd_kafka_conf_set(conf, "group.id", groupid.c_str(), errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return;
    }

    if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return;
    }

    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% failed to create new consumer: %s\n", errstr);
        return;
    }

    conf = NULL;
    rd_kafka_poll_set_consumer(rk);
    subscription = rd_kafka_topic_partition_list_new((int)topic_vec.size());
    for (i = 0; i < (int)topic_vec.size(); i++)
        rd_kafka_topic_partition_list_add(subscription, topic_vec[i].c_str(), RD_KAFKA_PARTITION_UA);

    err = rd_kafka_subscribe(rk, subscription);
    if (err) {
        fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n", subscription->cnt, rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(rk);
        return;
    }

    fprintf(stderr,
            "%% Subscribed to %d topic(s), "
            "waiting for rebalance and messages...\n",
            subscription->cnt);

    rd_kafka_topic_partition_list_destroy(subscription);

    signal(SIGINT, stop);

    while (run) {
        rd_kafka_message_t *rkm;

        rkm = rd_kafka_consumer_poll(rk, 100);
        if (!rkm)
            continue;

        if (rkm->err) {
            fprintf(stderr, "%% Consumer error: %s\n", rd_kafka_message_errstr(rkm));
            rd_kafka_message_destroy(rkm);
            continue;
        }

        /* Print message. */
        printf("Message on %s [%" PRId32 "] at offset %" PRId64 ":\n",
                rd_kafka_topic_name(rkm->rkt), rkm->partition,
                rkm->offset);

        /* Print the message key. */
        if (rkm->key && is_printable((const char*)rkm->key, rkm->key_len))
            printf(" Key: %.*s\n", (int)rkm->key_len,
                    (const char *)rkm->key);
        else if (rkm->key)
            printf(" Key: (%d bytes)\n", (int)rkm->key_len);

        /* Print the message value/payload. */
        if (rkm->payload && is_printable((const char*)rkm->payload, rkm->len))
            printf(" Value: %.*s\n", (int)rkm->len,
                    (const char *)rkm->payload);
        else if (rkm->payload)
            printf(" Value: (%d bytes) payload=%s\n", (int)rkm->len, (char*)rkm->payload);

        rd_kafka_message_destroy(rkm);
    }

    /* Close the consumer: commit final offsets and leave the group. */
    fprintf(stderr, "%% Closing consumer\n");
    rd_kafka_consumer_close(rk);

    /* Destroy the consumer */
    rd_kafka_destroy(rk);
}

int main(int argc, char** argv) {
    cmdline::parser parser;

    parse_option(argc, argv, parser);

    if (parser.exist("type") && 0 == parser.get<std::string>("type").compare("producer"))
        producer(parser);
    else if (parser.exist("type") && 0 == parser.get<std::string>("type").compare("consumer"))
        consumer(parser);

    return 0;
}