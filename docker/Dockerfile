FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/S4HanaConnector/master/docker/global.properties conf/rtdiconfig/S4HanaConnector/
ADD https://github.com/rtdi/S4HanaConnector/releases/latest/download/s4hanaconnector.war ./webapps/

EXPOSE 8080
EXPOSE 8443

CMD ["appcontainer.sh"]
