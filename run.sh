#!/bin/bash

./pre-install.sh
if [ ! -d /root/.ssh/ ]; then
    cp ssh_config ~/.ssh/config
    printf '\n\n\n\n\n\n' | ssh-keygen
fi

tox -e venv -- broker
