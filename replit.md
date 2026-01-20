# Discord WebSocket Relay Server

## Overview
A Python-based remote host server that monitors Discord channels for job postings and relays them in real-time to connected WebSocket clients (typically Lua game clients). The server runs continuously on Replit's infrastructure and can be deployed to production.

## Purpose
- Monitor multiple Discord channels for job posting embeds
- Extract job data (Job ID, money per second, pet name) from Discord messages
- Broadcast job information to all connected WebSocket clients
- Provide automatic retry and rate limit handling for Discord API

## Current State
✅ **RUNNING** - Server is live and operational

### Active Features
- WebSocket server on port 8080 for client connections
- Discord channel monitoring for 3 channels:
  - 1401775181025775738
  - 1420821538336411659
  - 1420821708121702411
- Automatic retry logic with exponential backoff
- Rate limit detection and handling
- Multi-client broadcast support
- Connection status tracking

## Recent Changes
**2025-10-12**: Initial deployment
- Installed Python 3.11 and dependencies (websockets, aiohttp)
- Configured DISCORD_TOKEN secret for API authentication
- Set up workflow for continuous server operation
- Configured VM deployment for always-on hosting
- Server successfully connecting to Discord and broadcasting job data

## Project Architecture

### Main Components
- `main.py`: Core server application
  - `handle_client()`: Manages WebSocket client connections
  - `send_to_clients()`: Broadcasts data to all connected clients
  - `monitor_discord_channel()`: Polls Discord API for new messages
  - `process_message()`: Extracts job data from Discord embeds
  - `get_discord_token()`: Retrieves Discord token from environment

### Data Flow
1. Server polls Discord channels every 2 seconds
2. New messages with embeds are processed
3. Job data (ID, money/sec, name) is extracted
4. Data is broadcast to all connected WebSocket clients via JSON

### Environment Variables
- `DISCORD_TOKEN`: Discord user token for API authentication

### Dependencies
- `websockets`: WebSocket server implementation
- `aiohttp`: Async HTTP client for Discord API requests
- `asyncio`: Asynchronous I/O framework

## Deployment
- **Type**: VM (always-on)
- **Port**: 8080 (WebSocket)
- **Command**: `python main.py`

## Connection Details
WebSocket clients can connect to:
- Development: `ws://0.0.0.0:8080`
- Production: Will be available after publishing

### Expected JSON Message Format
```json
{
  "jobid": "556744444C413344...",
  "money": "33700000.0",
  "name": "Los 67"
}
```

## User Preferences
- Server configured as console output (backend application)
- Discord token managed via Replit Secrets
- Multi-channel monitoring enabled
