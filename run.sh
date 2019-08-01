#!/bin/bash

cp ssh_config ~/.ssh/config
printf '\n\n\n\n\n\n' | ssh-keygen

tox -e venv -- broker
