#!/bin/bash
docker build . -t bench && docker-compose rm -f database && docker-compose up database
