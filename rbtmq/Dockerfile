# adapted from:
# http://mpas.github.io/post/2015/06/11/20150611_docker-rabbitmq-default-users/

FROM rabbitmq:3.9-management

# Add script to create default users / vhosts
ADD init.sh /init.sh

# expose the two important queue ports
EXPOSE 15672
EXPOSE 5672

# Set correct executable permissions
RUN chmod +x /init.sh

# Define default command
CMD ["/init.sh", "/"]

# docker run --rm=true --name aragorn_rbtmq_image -p 5672:5672 -p 15672:15672 -it \
#--env QUEUE_ADMIN_PW="$QUEUE_ADMIN_PW" --env QUEUE_USER="$QUEUE_USER" --env QUEUE_PW="$QUEUE_PW" \
#--hostname aragorn_rbtmq rabbitmq:3.9-management