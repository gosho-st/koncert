import asyncio
import websockets
import aiohttp
import json
import os
from datetime import datetime

connected = set()
channel_status = {}  # Track which channels are accessible

async def handle_client(websocket):
    """Handle incoming WebSocket connections from Lua clients"""
    client_address = websocket.remote_address
    print(f"✅ New client connected: {client_address}")
    connected.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected.remove(websocket)
        print(f"❌ Client disconnected: {client_address}")

async def send_to_clients(data):
    """Broadcast data to all connected WebSocket clients"""
    if not connected:
        return
    
    disconnected_clients = []
    for client in connected:
        try:
            await client.send(json.dumps(data))
        except (websockets.ConnectionClosed, ConnectionResetError):
            disconnected_clients.append(client)
    
    for client in disconnected_clients:
        connected.discard(client)

async def verify_channel_access(session, headers, channel_id):
    """Verify if we have access to a channel before monitoring it"""
    try:
        url = f"https://discord.com/api/v9/channels/{channel_id}"
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                channel_data = await response.json()
                print(f"✅ Channel {channel_id} is accessible: {channel_data.get('name', 'Unknown')}")
                return True
            elif response.status == 403:
                print(f"❌ No access to channel {channel_id} - You may not be in this server or lack permissions")
                return False
            elif response.status == 404:
                print(f"❌ Channel {channel_id} not found - Check if the ID is correct")
                return False
            else:
                print(f"⚠️ Channel {channel_id} verification returned status {response.status}")
                return False
    except Exception as e:
        print(f"⚠️ Error verifying channel {channel_id}: {e}")
        return False

async def monitor_discord_channel(token, channel_id):
    """Monitor a Discord channel with automatic retry and rate limit handling"""
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    retry_count = 0
    max_retries = 10
    base_delay = 2
    permanent_error = False
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # Verify access on first connection or after errors
                if channel_id not in channel_status or not channel_status[channel_id]:
                    has_access = await verify_channel_access(session, headers, channel_id)
                    channel_status[channel_id] = has_access
                    
                    if not has_access:
                        print(f"⏸️ Skipping channel {channel_id} - will retry in 5 minutes")
                        await asyncio.sleep(300)  # Wait 5 minutes before retrying
                        continue
                
                last_message_id = None
                
                while True:
                    try:
                        if last_message_id is None:
                            url = f"https://discord.com/api/v9/channels/{channel_id}/messages?limit=1"
                        else:
                            url = f"https://discord.com/api/v9/channels/{channel_id}/messages?after={last_message_id}&limit=10"
                        
                        async with session.get(url, headers=headers) as response:
                            if response.status == 200:
                                messages = await response.json()
                                
                                if last_message_id is None:
                                    last_message_id = messages[0]['id'] if messages else None
                                    print(f"✅ Connected to Discord channel {channel_id}. Last message: {last_message_id}")
                                else:
                                    for message in reversed(messages):
                                        await process_message(message)
                                        last_message_id = message['id']
                                
                                retry_count = 0
                                await asyncio.sleep(2)
                            
                            elif response.status == 429:
                                retry_after = response.headers.get('Retry-After')
                                if retry_after:
                                    try:
                                        wait_time = float(retry_after)
                                    except ValueError:
                                        wait_time = 60
                                    print(f"⚠️ Rate limited on channel {channel_id}. Waiting {wait_time}s...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    backoff_delay = min(base_delay * (2 ** retry_count), 300)
                                    print(f"⚠️ Rate limited on channel {channel_id}. Retrying in {backoff_delay}s...")
                                    await asyncio.sleep(backoff_delay)
                                    retry_count += 1
                            
                            elif response.status == 401:
                                print(f"❌ Authentication error (401) - Your DISCORD_TOKEN is invalid or expired")
                                print(f"   Please update your token in the Secrets tab")
                                await asyncio.sleep(3600)  # Wait 1 hour
                            
                            elif response.status == 403:
                                print(f"⚠️ Access denied (403) for channel {channel_id}")
                                print(f"   This channel will be rechecked in 5 minutes")
                                channel_status[channel_id] = False
                                await asyncio.sleep(300)  # Wait 5 minutes
                                break  # Exit inner loop to re-verify access
                            
                            elif response.status == 404:
                                print(f"❌ Channel {channel_id} not found (404)")
                                print(f"   This channel will be rechecked in 5 minutes")
                                channel_status[channel_id] = False
                                await asyncio.sleep(300)
                                break
                            
                            else:
                                backoff_delay = min(base_delay * (2 ** retry_count), 60)
                                print(f"⚠️ API error {response.status} for channel {channel_id}. Retrying in {backoff_delay}s...")
                                await asyncio.sleep(backoff_delay)
                                retry_count = min(retry_count + 1, max_retries)
                    
                    except aiohttp.ClientError as e:
                        backoff_delay = min(base_delay * (2 ** retry_count), 60)
                        print(f"⚠️ Network error for channel {channel_id}: {e}. Retrying in {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                        retry_count = min(retry_count + 1, max_retries)
                    
                    except Exception as e:
                        print(f"❌ Unexpected error in polling loop for channel {channel_id}: {e}")
                        await asyncio.sleep(5)
        
        except Exception as e:
            backoff_delay = min(base_delay * (2 ** retry_count), 300)
            print(f"❌ Session error for channel {channel_id}: {e}. Reconnecting in {backoff_delay}s...")
            await asyncio.sleep(backoff_delay)
            retry_count = min(retry_count + 1, max_retries)

async def process_message(message):
    """Extract job data from Discord embed messages"""
    if 'embeds' in message and message['embeds']:
        for embed in message['embeds']:
            if 'fields' in embed and embed['fields']:
                jobId, moneyPerSec, petName = None, 0, 'Unknown'
                
                for field in embed['fields']:
                    fval = field.get('value', '')
                    
                    if 'Job ID' in field.get('name', ''):
                        jobId = fval.replace('`', '')
                    
                    if 'Name' in field.get('name', ''):
                        petName = fval
                    
                    if '$' in fval and 'M/s' in fval:
                        dollar = fval.split('$')[1].split('M/s')[0]
                        if dollar:
                            moneyPerSec = float(dollar) * 1000000
                    elif '$' in fval and 'K/s' in fval:
                        k = fval.split('$')[1].split('K/s')[0]
                        if k:
                            moneyPerSec = float(k) * 1000
                
                if jobId and moneyPerSec > 0 and petName:
                    data = {"jobid": jobId, "money": str(moneyPerSec), "name": petName}
                    print(f"🚀 Broadcasting data: {data}")
                    await send_to_clients(data)

async def get_discord_token():
    """Get Discord token from environment variable"""
    token = os.environ.get('DISCORD_TOKEN')
    if not token:
        print("❌ ERROR: DISCORD_TOKEN environment variable not set!")
        print("   Please set your Discord user token in the Secrets tab")
        return None
    
    if not (token.startswith('mfa.') or token.startswith('MTk') or token.startswith('OD') or token.startswith('Nz')):
        print("⚠️  Warning: Token format might be incorrect")
        print("   Discord tokens usually start with: mfa., MTk, OD, Nz, etc.")
    
    return token

async def main():
    print("=" * 60)
    print("🚀 Discord WebSocket Relay Server - Remote Host")
    print("=" * 60)
    
    discord_token = await get_discord_token()
    if not discord_token:
        return
    
    # Liste des channels à monitorer
    channel_ids = [
        1401775181025775738,    # Serveur 1 - Channel 1
    ]
    
    monitor_tasks = []
    for channel_id in channel_ids:
        task = asyncio.create_task(monitor_discord_channel(discord_token, channel_id))
        monitor_tasks.append(task)
    
    async with websockets.serve(handle_client, "0.0.0.0", 8080):
        print(f"✅ WebSocket server running on ws://0.0.0.0:8080")
        print(f"🔄 Attempting to monitor {len(channel_ids)} Discord channels...")
        print(f"📋 Channels:")
        for i, channel_id in enumerate(channel_ids, 1):
            print(f"   {i}. {channel_id}")
        print("=" * 60)
        print(f"⏰ Server started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"👥 Waiting for client connections...")
        print("=" * 60)
        
        try:
            await asyncio.gather(*monitor_tasks)
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
