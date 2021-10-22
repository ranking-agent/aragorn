#!/bin/sh

# Create a default RabbitMQ setup
( 
sleep 30 ; \

# Create users
# rabbitmqctl add_user <username> <password>
rabbitmqctl add_user admin "$QUEUE_ADMIN_PW" ; \
rabbitmqctl add_user "$QUEUE_USER" "$QUEUE_PW" ; \

# Set user rights
# rabbitmqctl set_user_tags <username> <tag>
rabbitmqctl set_user_tags admin administrator ; \
rabbitmqctl set_user_tags "$QUEUE_USER" administrator ; \

for vhost in "$@"
do
# Create vhosts
# rabbitmqctl add_vhost <vhostname>
rabbitmqctl add_vhost $vhost ; \

# Set vhost permissions
# rabbitmqctl set_permissions -p <vhostname> <username> ".*" ".*" ".*"
rabbitmqctl set_permissions -p $vhost admin ".*" ".*" ".*" ; \
rabbitmqctl set_permissions -p $vhost "$QUEUE_USER" ".*" ".*" ".*" ; \
done
) &    
rabbitmq-server
