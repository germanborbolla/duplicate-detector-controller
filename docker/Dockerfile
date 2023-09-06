FROM 246946804217.dkr.ecr.us-west-2.amazonaws.com/sumologic/sumo-base:ubuntu22.jre11
LABEL maintainer="Panda \"panda@sumologic.com\""
WORKDIR /controller
COPY libs libs/
COPY controller.sh bin/
RUN chmod +x bin/controller.sh
ENV JAVA_OPTS="-Xms256m -Xmx1g"
ENTRYPOINT ["/controller/bin/controller.sh"]
