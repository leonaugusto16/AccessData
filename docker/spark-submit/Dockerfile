FROM centos:7

RUN yum install -y epel-release && yum update -y
RUN yum install -y java-1.8.0 && yum install -y R
COPY ./FILES/scala-2.12.3.rpm /tmp/
ADD ./FILES/spark-2.2.0-bin-hadoop2.7.tgz /usr/local/
RUN mv /usr/local/spark-2.2.0-bin-hadoop2.7/ /usr/local/spark/
RUN rpm -Uvh /tmp/scala-2.12.3.rpm
COPY ./FILES/entrypoint.sh /
COPY 


ENTRYPOINT ["sh","/entrypoint.sh"]
CMD ["/usr/local/spark/examples/src/main/python/pi.py"]
