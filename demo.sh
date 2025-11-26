#!/bin/bash
echo "Starting Chaos Engineering Framework..."
docker-compose up -d
sleep 10
python src/chaos_framework.py
