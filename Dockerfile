FROM abs/python-oracle:latest

ADD requirements.txt /tmp/

RUN pip install -r /tmp/requirements.txt

ADD .ssh /tmp/.ssh
RUN cp -R /tmp/.ssh /root/.ssh
RUN chmod 700 /root/.ssh
RUN chmod 644 /root/.ssh/id_rsa.pub
RUN chmod 600 /root/.ssh/id_rsa

#COPY docker-entrypoint.sh /bin/docker-entrypoint.sh
#RUN chmod +x /bin/docker-entrypoint.sh
#ENTRYPOINT ["/bin/docker-entrypoint.sh"]