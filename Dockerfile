FROM abs/python-oracle:latest

ADD requirements.txt /tmp/

RUN pip install -r /tmp/requirements.txt

#COPY docker-entrypoint.sh /bin/docker-entrypoint.sh
#RUN chmod +x /bin/docker-entrypoint.sh
#ENTRYPOINT ["/bin/docker-entrypoint.sh"]