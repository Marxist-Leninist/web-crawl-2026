#!/bin/bash
# Auto-restart wrapper for crawl_rust_v3
LOG=/workspace/crawl_rust.log
BINARY=/workspace/crawl_rust_v3

while true; do
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [wrapper] Starting Rust crawler..." >> "$LOG"
    "$BINARY" >> "$LOG" 2>&1
    EXIT_CODE=$?
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [wrapper] Crawler exited with code $EXIT_CODE" >> "$LOG"
    
    if [ $EXIT_CODE -eq 137 ]; then
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [wrapper] Killed by OOM/signal, waiting 60s..." >> "$LOG"
        sleep 60
    elif [ $EXIT_CODE -ne 0 ]; then
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [wrapper] Crash detected, restarting in 10s..." >> "$LOG"
        sleep 10
    else
        echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [wrapper] Clean exit, restarting in 5s..." >> "$LOG"
        sleep 5
    fi
done
