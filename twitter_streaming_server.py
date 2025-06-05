"""
Twitter Streaming Server - Enhanced with Live Monitoring
Simulates Twitter streaming with real-time server stats and lively updates
"""

import socket
import json
import time
import threading
import random
import signal
import sys
import os
from datetime import datetime, timedelta
from collections import defaultdict, deque
from pyspark.sql import SparkSession
from pyspark.sql.functions import length

class LiveTwitterStreamingServer:
    def __init__(self, host='localhost', port=9999, data_source=None):
        self.host = host
        self.port = port
        self.running = False
        self.server_socket = None
        self.tweet_data = []
        self.data_source = data_source or "chatgpt_tweets_batch_1749106385"
        
        # Enhanced client tracking
        self.clients = {}  # More detailed client info
        self.client_stats = defaultdict(dict)
        
        # Live server statistics
        self.server_stats = {
            'start_time': None,
            'total_tweets_sent': 0,
            'total_connections': 0,
            'active_connections': 0,
            'tweets_per_minute': deque(maxlen=60),
            'connection_history': deque(maxlen=100),
            'error_count': 0,
            'last_activity': None
        }
        
        # Real-time monitoring
        self.monitoring_thread = None
        self.status_display_active = False
        
        # Initialize Spark for data loading
        self.spark = SparkSession.builder \
            .appName("LiveTwitterStreamingServer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Setup signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        
    def signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        print("\n🛑 Shutting down server...")
        self.stop_server()
        sys.exit(0)
        
    def load_twitter_data(self):
        """Load Twitter data for streaming simulation"""
        print(f"📂 Loading Twitter data from: {self.data_source}")
        
        try:
            # Load existing Twitter data
            if os.path.exists(f"{self.data_source}"):
                df = self.spark.read.json(self.data_source)
                
                # Clean and prepare data
                cleaned_df = df.filter(
                    df.text.isNotNull() & 
                    (df.lang == "en") &
                    (length(df.text) > 20)
                ).select(
                    "id", "text", "created_at", "author_id", "public_metrics"
                )
                
                self.tweet_data = cleaned_df.collect()
                print(f"✅ Loaded {len(self.tweet_data)} tweets for streaming")
                
                # Show sample with more details
                print("\n📋 Sample tweets loaded:")
                for i, tweet in enumerate(self.tweet_data[:3]):
                    print(f"   {i+1}. ID: {tweet['id']} | {tweet['text'][:60]}...")
                    
                return True
                
            else:
                print(f"❌ Data source not found: {self.data_source}")
                return False
                
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            self.server_stats['error_count'] += 1
            return False
    
    def generate_streaming_tweet(self, client_address):
        """Generate a tweet for streaming with enhanced metadata"""
        if not self.tweet_data:
            return None
            
        # Pick random tweet
        tweet = random.choice(self.tweet_data)
        
        # Safely handle public_metrics conversion
        public_metrics = {}
        if tweet["public_metrics"]:
            try:
                if hasattr(tweet["public_metrics"], 'asDict'):
                    public_metrics = tweet["public_metrics"].asDict()
                elif isinstance(tweet["public_metrics"], dict):
                    public_metrics = tweet["public_metrics"]
                else:
                    public_metrics = dict(tweet["public_metrics"])
            except:
                public_metrics = {}
        
        # Enhanced streaming packet with more server info
        streaming_packet = {
            "id": tweet["id"],
            "text": tweet["text"],
            "original_created_at": tweet["created_at"],
            "author_id": tweet["author_id"],
            "public_metrics": public_metrics,
            
            # Enhanced streaming metadata
            "stream_timestamp": datetime.now().isoformat(),
            "stream_id": random.randint(10000, 99999),
            "server_info": {
                "server_host": self.host,
                "server_port": self.port,
                "active_clients": len(self.clients),
                "total_tweets_sent": self.server_stats['total_tweets_sent'],
                "server_uptime": self.get_uptime(),
                "client_ip": client_address[0]
            }
        }
        
        return json.dumps(streaming_packet, ensure_ascii=False)
    
    def get_uptime(self):
        """Get server uptime in seconds"""
        if self.server_stats['start_time']:
            return (datetime.now() - self.server_stats['start_time']).total_seconds()
        return 0
    
    def update_server_stats(self, client_address, action):
        """Update server statistics in real-time"""
        now = datetime.now()
        self.server_stats['last_activity'] = now
        
        if action == 'tweet_sent':
            self.server_stats['total_tweets_sent'] += 1
            self.server_stats['tweets_per_minute'].append(now)
            
            # Update client stats
            if client_address in self.client_stats:
                self.client_stats[client_address]['tweets_sent'] += 1
                self.client_stats[client_address]['last_tweet_time'] = now
                
        elif action == 'client_connected':
            self.server_stats['total_connections'] += 1
            self.server_stats['active_connections'] += 1
            self.server_stats['connection_history'].append({
                'time': now,
                'action': 'connect',
                'client': client_address
            })
            
        elif action == 'client_disconnected':
            self.server_stats['active_connections'] -= 1
            self.server_stats['connection_history'].append({
                'time': now,
                'action': 'disconnect',
                'client': client_address
            })
            
        elif action == 'error':
            self.server_stats['error_count'] += 1
    
    def get_tweets_per_minute(self):
        """Calculate current tweets per minute"""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        recent_tweets = [t for t in self.server_stats['tweets_per_minute'] if t > one_minute_ago]
        return len(recent_tweets)
    
    def handle_client(self, client_socket, client_address):
        """Handle individual client connection with enhanced tracking"""
        print(f"🎉 NEW CLIENT CONNECTED: {client_address[0]}:{client_address[1]}")
        
        # Enhanced client tracking
        self.clients[client_address] = {
            'socket': client_socket,
            'connected_at': datetime.now(),
            'status': 'active'
        }
        
        self.client_stats[client_address] = {
            'tweets_sent': 0,
            'connection_time': datetime.now(),
            'last_tweet_time': None,
            'status': 'streaming'
        }
        
        self.update_server_stats(client_address, 'client_connected')
        
        try:
            # Send enhanced welcome message
            welcome_msg = {
                "type": "welcome",
                "message": f"🎊 Welcome to Live Twitter Streaming Server!",
                "server_time": datetime.now().isoformat(),
                "total_tweets_available": len(self.tweet_data),
                "server_stats": {
                    "uptime": self.get_uptime(),
                    "total_connections": self.server_stats['total_connections'],
                    "active_clients": len(self.clients),
                    "tweets_per_minute": self.get_tweets_per_minute()
                },
                "client_id": f"{client_address[0]}:{client_address[1]}"
            }
            client_socket.send((json.dumps(welcome_msg) + '\n').encode('utf-8'))
            
            # Stream tweets with live updates
            tweet_count = 0
            max_tweets = 50  # Limit per client session
            
            while self.running and tweet_count < max_tweets:
                try:
                    # Generate tweet packet
                    tweet_packet = self.generate_streaming_tweet(client_address)
                    if tweet_packet:
                        client_socket.send((tweet_packet + '\n').encode('utf-8'))
                        tweet_count += 1
                        
                        # Update stats
                        self.update_server_stats(client_address, 'tweet_sent')
                        
                        # Live server feedback
                        tweets_per_min = self.get_tweets_per_minute()
                        print(f"📤 {client_address[0]} | Tweet #{tweet_count} | TPM: {tweets_per_min} | Total: {self.server_stats['total_tweets_sent']}")
                        
                        # Simulate realistic streaming interval
                        time.sleep(random.uniform(0.8, 1.5))  # 0.8-1.5 seconds between tweets
                        
                except ConnectionResetError:
                    print(f"🔌 CLIENT DISCONNECTED: {client_address}")
                    break
                except Exception as e:
                    print(f"❌ ERROR sending to {client_address}: {e}")
                    self.update_server_stats(client_address, 'error')
                    break
            
            # Send enhanced completion message
            completion_msg = {
                "type": "completion",
                "message": f"🎯 Streaming completed! Sent {tweet_count} tweets to {client_address[0]}",
                "total_sent": tweet_count,
                "server_time": datetime.now().isoformat(),
                "session_duration": (datetime.now() - self.client_stats[client_address]['connection_time']).total_seconds(),
                "server_stats": {
                    "total_tweets_sent": self.server_stats['total_tweets_sent'],
                    "active_clients": len(self.clients) - 1  # About to disconnect
                }
            }
            client_socket.send((json.dumps(completion_msg) + '\n').encode('utf-8'))
            
        except Exception as e:
            print(f"❌ ERROR handling client {client_address}: {e}")
            self.update_server_stats(client_address, 'error')
        finally:
            # Cleanup client
            if client_address in self.clients:
                del self.clients[client_address]
            if client_address in self.client_stats:
                session_duration = (datetime.now() - self.client_stats[client_address]['connection_time']).total_seconds()
                tweets_sent = self.client_stats[client_address]['tweets_sent']
                print(f"👋 {client_address[0]} SESSION ENDED | {tweets_sent} tweets | {session_duration:.1f}s")
                del self.client_stats[client_address]
                
            self.update_server_stats(client_address, 'client_disconnected')
            client_socket.close()
    
    def start_live_monitoring(self):
        """Start live server monitoring display"""
        def monitor():
            while self.running and self.status_display_active:
                try:
                    self.display_live_status()
                    time.sleep(5)  # Update every 5 seconds
                except Exception as e:
                    print(f"❌ Monitoring error: {e}")
                    time.sleep(5)
        
        self.status_display_active = True
        self.monitoring_thread = threading.Thread(target=monitor, daemon=True)
        self.monitoring_thread.start()
    
    def display_live_status(self):
        """Display live server status"""
        uptime = self.get_uptime()
        tweets_per_min = self.get_tweets_per_minute()
        
        # Clear screen effect (for better visual update)
        print("\n" + "🔥" * 80)
        print("📊 LIVE SERVER STATUS DASHBOARD")
        print("🔥" * 80)
        
        # Server info
        print(f"🚀 Server: {self.host}:{self.port} | ⏰ Uptime: {uptime:.0f}s | 🔴 LIVE")
        print(f"📈 Total Tweets: {self.server_stats['total_tweets_sent']:,} | ⚡ Rate: {tweets_per_min} tweets/min")
        print(f"👥 Connections: {self.server_stats['active_connections']} active | {self.server_stats['total_connections']} total")
        print(f"❌ Errors: {self.server_stats['error_count']} | 📦 Available: {len(self.tweet_data):,} tweets")
        
        # Active clients
        if self.clients:
            print(f"\n👥 ACTIVE CLIENTS ({len(self.clients)}):")
            for addr, client_info in self.clients.items():
                duration = (datetime.now() - client_info['connected_at']).total_seconds()
                tweets_sent = self.client_stats.get(addr, {}).get('tweets_sent', 0)
                print(f"   🔗 {addr[0]}:{addr[1]} | {duration:.0f}s | {tweets_sent} tweets")
        else:
            print(f"\n👥 No active clients - waiting for connections...")
        
        # Recent activity
        if self.server_stats['connection_history']:
            print(f"\n📝 RECENT ACTIVITY:")
            recent = list(self.server_stats['connection_history'])[-3:]
            for activity in recent:
                action_emoji = "🟢" if activity['action'] == 'connect' else "🔴"
                time_str = activity['time'].strftime("%H:%M:%S")
                print(f"   {action_emoji} {time_str} | {activity['client'][0]} {activity['action']}")
        
        # Performance metrics
        if self.server_stats['tweets_per_minute']:
            avg_rate = len(self.server_stats['tweets_per_minute']) / min(60, uptime) * 60 if uptime > 0 else 0
            print(f"\n📊 PERFORMANCE:")
            print(f"   📈 Current rate: {tweets_per_min} tweets/min")
            print(f"   📊 Average rate: {avg_rate:.1f} tweets/min")
            print(f"   🎯 Data remaining: {len(self.tweet_data):,} tweets available")
        
        print("🔥" * 80)
        
        # Live status indicator
        status_emoji = "🟢" if self.server_stats['active_connections'] > 0 else "🟡"
        status_text = "STREAMING" if self.server_stats['active_connections'] > 0 else "WAITING"
        print(f"{status_emoji} STATUS: {status_text} | Last activity: {datetime.now().strftime('%H:%M:%S')}")
    
    def start_server(self):
        """Start the enhanced streaming server"""
        # Load data first
        if not self.load_twitter_data():
            print("❌ Cannot start server without data")
            return
            
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)  # Allow up to 5 concurrent clients
            
            # Initialize server stats
            self.server_stats['start_time'] = datetime.now()
            self.running = True
            
            print("\n" + "🚀" * 80)
            print("🎊 LIVE TWITTER STREAMING SERVER STARTED!")
            print("🚀" * 80)
            print(f"📡 Listening on: {self.host}:{self.port}")
            print(f"📊 Tweets ready: {len(self.tweet_data):,}")
            print(f"👥 Max clients: 5 concurrent")
            print(f"🎬 Live monitoring: ENABLED")
            print(f"⏰ Started at: {self.server_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print("🚀" * 80)
            
            # Start live monitoring
            self.start_live_monitoring()
            
            print("🔴 LIVE | Waiting for client connections... (Press Ctrl+C to stop)\n")
            
            while self.running:
                try:
                    # Accept client connections
                    client_socket, client_address = self.server_socket.accept()
                    
                    # Handle each client in separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    client_thread.start()
                    
                except KeyboardInterrupt:
                    print("\n🛑 Server shutdown requested")
                    break
                except Exception as e:
                    if self.running:
                        print(f"❌ Server error: {e}")
                        self.update_server_stats(None, 'error')
                        
        except Exception as e:
            print(f"❌ Cannot start server: {e}")
        finally:
            self.stop_server()
    
    def stop_server(self):
        """Stop the streaming server with final stats"""
        print("\n🛑 Stopping Live Twitter Streaming Server...")
        self.running = False
        self.status_display_active = False
        
        # Final statistics
        uptime = self.get_uptime()
        print("\n" + "📊" * 80)
        print("📈 FINAL SERVER STATISTICS")
        print("📊" * 80)
        print(f"⏰ Total uptime: {uptime:.0f} seconds ({uptime/60:.1f} minutes)")
        print(f"📤 Total tweets sent: {self.server_stats['total_tweets_sent']:,}")
        print(f"👥 Total connections: {self.server_stats['total_connections']}")
        print(f"❌ Total errors: {self.server_stats['error_count']}")
        
        if uptime > 0:
            avg_rate = self.server_stats['total_tweets_sent'] / (uptime / 60)
            print(f"📊 Average rate: {avg_rate:.1f} tweets/minute")
        
        print("📊" * 80)
        
        if self.server_socket:
            self.server_socket.close()
            
        if hasattr(self, 'spark'):
            self.spark.stop()
            
        print("✅ Server stopped successfully")
        print("👋 Thanks for using Live Twitter Streaming Server!")

def main():
    """Main server function with enhanced startup"""
    print("🐦" * 80)
    print("🎊 LIVE TWITTER STREAMING SERVER")
    print("🐦" * 80)
    print("🎬 Enhanced with Real-time Monitoring & Live Updates!")
    print("📊 Features: Live stats, client tracking, performance metrics")
    print("🐦" * 80)
    
    # Create and start enhanced server
    server = LiveTwitterStreamingServer(
        host='localhost',
        port=9999,
        data_source="chatgpt_tweets_batch_1749106385"
    )
    
    try:
        server.start_server()
    except KeyboardInterrupt:
        print("\n🛑 Server interrupted by user")
    except Exception as e:
        print(f"❌ Server error: {e}")
    finally:
        print("\n🎭 Server session ended")

if __name__ == "__main__":
    main()