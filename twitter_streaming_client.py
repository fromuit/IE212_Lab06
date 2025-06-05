"""
Twitter Streaming Client with Real-time Sentiment Analysis
Connects to Twitter streaming server and performs live sentiment analysis
"""

import socket
import json
import time
import threading
import signal
import sys
from datetime import datetime
from collections import defaultdict, deque
import pandas as pd

# Sentiment analysis imports
try:
    from textblob import TextBlob
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    SENTIMENT_AVAILABLE = True
except ImportError:
    print("⚠️ Installing required packages...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "textblob", "vaderSentiment"])
    from textblob import TextBlob
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    SENTIMENT_AVAILABLE = True

class TwitterSentimentStreamingClient:
    def __init__(self, host='localhost', port=9999, save_results=True):
        self.host = host
        self.port = port
        self.save_results = save_results
        self.running = False
        self.client_socket = None
        
        # Initialize sentiment analyzers
        self.vader_analyzer = SentimentIntensityAnalyzer()
        
        # Statistics tracking
        self.stats = {
            'total_tweets': 0,
            'positive_tweets': 0,
            'negative_tweets': 0,
            'neutral_tweets': 0,
            'processing_times': deque(maxlen=100),
            'sentiment_history': deque(maxlen=50),
            'current_batch': 0,
            'batch_start_time': None
        }
        
        # Results storage
        self.results = []
        self.batch_results = []
        self.current_batch_tweets = []
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        
    def signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        print("\n🛑 Shutting down client...")
        self.stop_client()
        sys.exit(0)
    
    def analyze_sentiment_textblob(self, text):
        """Analyze sentiment using TextBlob"""
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # Classify sentiment
            if polarity > 0.1:
                sentiment = "positive"
            elif polarity < -0.1:
                sentiment = "negative"
            else:
                sentiment = "neutral"
                
            return {
                'polarity': float(polarity),
                'subjectivity': float(subjectivity),
                'sentiment': sentiment
            }
        except Exception as e:
            print(f"❌ TextBlob error: {e}")
            return {'polarity': 0.0, 'subjectivity': 0.0, 'sentiment': 'neutral'}
    
    def analyze_sentiment_vader(self, text):
        """Analyze sentiment using VADER"""
        try:
            scores = self.vader_analyzer.polarity_scores(text)
            compound = scores['compound']
            
            # Classify sentiment based on compound score
            if compound >= 0.05:
                sentiment = "positive"
            elif compound <= -0.05:
                sentiment = "negative"
            else:
                sentiment = "neutral"
                
            return {
                'compound': float(compound),
                'positive': float(scores['pos']),
                'negative': float(scores['neg']),
                'neutral': float(scores['neu']),
                'sentiment': sentiment
            }
        except Exception as e:
            print(f"❌ VADER error: {e}")
            return {'compound': 0.0, 'positive': 0.0, 'negative': 0.0, 'neutral': 1.0, 'sentiment': 'neutral'}
    
    def process_tweet(self, tweet_data):
        """Process a single tweet with sentiment analysis"""
        start_time = time.time()
        
        try:
            # Parse tweet data
            tweet = json.loads(tweet_data.strip())
            
            # Skip non-tweet messages (welcome, completion, etc.)
            if 'type' in tweet:
                if tweet['type'] == 'welcome':
                    print(f"🎉 {tweet['message']}")
                    print(f"📊 Server has {tweet.get('total_tweets_available', 'unknown')} tweets available")
                    return None
                elif tweet['type'] == 'completion':
                    print(f"✅ {tweet['message']}")
                    return None
                else:
                    return None
            
            # Extract text for analysis
            text = tweet.get('clean_text') or tweet.get('text', '')
            if not text:
                return None
            
            # Perform sentiment analysis
            textblob_result = self.analyze_sentiment_textblob(text)
            vader_result = self.analyze_sentiment_vader(text)
            
            # Create comprehensive result
            result = {
                'tweet_id': tweet.get('id'),
                'stream_id': tweet.get('stream_id'),
                'text': text,
                'original_text': tweet.get('text', ''),
                'timestamp': tweet.get('timestamp') or tweet.get('stream_timestamp'),
                'processed_at': datetime.now().isoformat(),
                
                # TextBlob results
                'textblob_polarity': textblob_result['polarity'],
                'textblob_subjectivity': textblob_result['subjectivity'],
                'textblob_sentiment': textblob_result['sentiment'],
                
                # VADER results
                'vader_compound': vader_result['compound'],
                'vader_positive': vader_result['positive'],
                'vader_negative': vader_result['negative'],
                'vader_neutral': vader_result['neutral'],
                'vader_sentiment': vader_result['sentiment'],
                
                # Processing metadata
                'processing_time': time.time() - start_time
            }
            
            # Update statistics
            self.update_stats(result)
            
            # Store result
            if self.save_results:
                self.results.append(result)
                self.batch_results.append(result)
                self.current_batch_tweets.append(result)
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error: {e}")
            return None
        except Exception as e:
            print(f"❌ Processing error: {e}")
            return None
    
    def update_stats(self, result):
        """Update running statistics"""
        self.stats['total_tweets'] += 1
        self.stats['processing_times'].append(result['processing_time'])
        
        # Count sentiments (using VADER as primary)
        sentiment = result['vader_sentiment']
        if sentiment == 'positive':
            self.stats['positive_tweets'] += 1
        elif sentiment == 'negative':
            self.stats['negative_tweets'] += 1
        else:
            self.stats['neutral_tweets'] += 1
        
        # Track sentiment history for trends
        self.stats['sentiment_history'].append({
            'timestamp': result['processed_at'],
            'sentiment': sentiment,
            'compound': result['vader_compound']
        })
    
    def display_result(self, result, tweet_num_in_batch):
        """Display real-time analysis result with batch context"""
        if not result:
            return
            
        print(f"\n📧 BATCH {self.stats['current_batch']} | Tweet {tweet_num_in_batch}/10 | Total: #{self.stats['total_tweets']}")
        print(f"🆔 Stream ID: {result['stream_id']} | ⏰ {result['processed_at']}")
        print(f"📝 {result['text'][:120]}{'...' if len(result['text']) > 120 else ''}")
        
        # Sentiment results with visual indicators
        sentiment_emoji = {
            'positive': '😊',
            'negative': '😞', 
            'neutral': '😐'
        }
        
        tb_emoji = sentiment_emoji.get(result['textblob_sentiment'], '🤔')
        vader_emoji = sentiment_emoji.get(result['vader_sentiment'], '🤔')
        
        print(f"🎭 TextBlob: {tb_emoji} {result['textblob_sentiment'].upper()} ({result['textblob_polarity']:+.3f})")
        print(f"🎭 VADER:    {vader_emoji} {result['vader_sentiment'].upper()} ({result['vader_compound']:+.3f})")
        print(f"⚡ Processing: {result['processing_time']*1000:.1f}ms")
        
        # Progress bar for batch
        progress = "█" * tweet_num_in_batch + "░" * (10 - tweet_num_in_batch)
        print(f"📊 Batch Progress: [{progress}] {tweet_num_in_batch}/10")
    
    def display_batch_summary(self):
        """Display summary for completed batch"""
        if not self.current_batch_tweets:
            return
            
        batch_size = len(self.current_batch_tweets)
        batch_positive = sum(1 for t in self.current_batch_tweets if t['vader_sentiment'] == 'positive')
        batch_negative = sum(1 for t in self.current_batch_tweets if t['vader_sentiment'] == 'negative')
        batch_neutral = batch_size - batch_positive - batch_negative
        
        avg_compound = sum(t['vader_compound'] for t in self.current_batch_tweets) / batch_size
        avg_processing = sum(t['processing_time'] for t in self.current_batch_tweets) / batch_size * 1000
        
        batch_duration = time.time() - self.stats['batch_start_time'] if self.stats['batch_start_time'] else 0
        
        print("\n" + "🔥" * 60)
        print(f"📊 BATCH {self.stats['current_batch']} COMPLETED!")
        print("🔥" * 60)
        print(f"📈 Tweets in batch: {batch_size}")
        print(f"😊 Positive: {batch_positive} ({batch_positive/batch_size*100:.1f}%)")
        print(f"😞 Negative: {batch_negative} ({batch_negative/batch_size*100:.1f}%)")
        print(f"😐 Neutral:  {batch_neutral} ({batch_neutral/batch_size*100:.1f}%)")
        print(f"📊 Avg sentiment: {avg_compound:+.3f}")
        print(f"⚡ Avg processing: {avg_processing:.1f}ms")
        print(f"⏱️ Batch duration: {batch_duration:.1f}s")
        
        # Batch trend
        if avg_compound > 0.1:
            print("📈 Batch trend: POSITIVE VIBES! 🎉")
        elif avg_compound < -0.1:
            print("📉 Batch trend: NEGATIVE MOOD 😔")
        else:
            print("📊 Batch trend: BALANCED SENTIMENT ⚖️")
    
    def display_overall_stats(self):
        """Display overall streaming statistics"""
        if self.stats['total_tweets'] == 0:
            return
            
        total = self.stats['total_tweets']
        pos_pct = (self.stats['positive_tweets'] / total) * 100
        neg_pct = (self.stats['negative_tweets'] / total) * 100
        neu_pct = (self.stats['neutral_tweets'] / total) * 100
        
        avg_processing_time = sum(self.stats['processing_times']) / len(self.stats['processing_times']) * 1000
        
        print(f"\n🌟 OVERALL STREAMING STATISTICS (Total: {total} tweets)")
        print("=" * 60)
        print(f"😊 Positive: {self.stats['positive_tweets']} ({pos_pct:.1f}%)")
        print(f"😞 Negative: {self.stats['negative_tweets']} ({neg_pct:.1f}%)")
        print(f"😐 Neutral:  {self.stats['neutral_tweets']} ({neu_pct:.1f}%)")
        print(f"⚡ Avg processing: {avg_processing_time:.2f}ms")
        print(f"📦 Batches completed: {self.stats['current_batch']}")
        
        # Show recent sentiment trend
        if len(self.stats['sentiment_history']) >= 5:
            recent_sentiments = [s['sentiment'] for s in list(self.stats['sentiment_history'])[-5:]]
            trend_emojis = {'positive': '😊', 'negative': '😞', 'neutral': '😐'}
            trend_display = ' → '.join([trend_emojis.get(s, '🤔') for s in recent_sentiments])
            print(f"📈 Recent trend: {trend_display}")
    
    def countdown_between_batches(self, seconds=5):
        """Show countdown between batches for dramatic effect"""
        print("\n" + "⏸️" * 60)
        print("🔄 PREPARING NEXT BATCH...")
        print("⏸️" * 60)
        
        for i in range(seconds, 0, -1):
            print(f"\r⏰ Next batch starting in {i} seconds... ", end="", flush=True)
            time.sleep(1)
        
        print(f"\r✅ Starting next batch now!                    ")
        print("🚀" * 60)
    
    def save_batch_results(self, batch_size=10):
        """Save batch results to file"""
        if len(self.batch_results) >= batch_size:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"streaming_sentiment_batch_{timestamp}.json"
            
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.batch_results, f, indent=2, ensure_ascii=False)
                
                print(f"💾 Saved batch of {len(self.batch_results)} results to {filename}")
                self.batch_results = []  # Clear batch
                
            except Exception as e:
                print(f"❌ Error saving batch: {e}")
    
    def connect_to_server(self):
        """Connect to the streaming server"""
        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.host, self.port))
            print(f"✅ Connected to streaming server at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"❌ Cannot connect to server: {e}")
            return False
    
    def start_streaming(self):
        """Start the streaming sentiment analysis with enhanced visualization"""
        print("🐦 TWITTER SENTIMENT STREAMING CLIENT")
        print("="*60)
        print("🎬 LIVE STREAMING MODE ACTIVATED!")
        print("="*60)
        
        if not self.connect_to_server():
            return
        
        self.running = True
        
        try:
            print("📡 Listening for tweets... (Press Ctrl+C to stop)")
            print("🎯 Processing tweets in batches of 10 with dramatic pauses!\n")
            
            buffer = ""
            tweet_count_in_batch = 0
            
            while self.running:
                try:
                    # Start new batch
                    if tweet_count_in_batch == 0:
                        self.stats['current_batch'] += 1
                        self.stats['batch_start_time'] = time.time()
                        self.current_batch_tweets = []
                        
                        print(f"\n🎬 STARTING BATCH {self.stats['current_batch']}")
                        print("🎯" * 60)
                    
                    # Receive data from server
                    data = self.client_socket.recv(4096).decode('utf-8')
                    
                    if not data:
                        print("🔌 Server disconnected")
                        break
                    
                    # Handle partial JSON messages
                    buffer += data
                    lines = buffer.split('\n')
                    buffer = lines[-1]  # Keep incomplete line in buffer
                    
                    # Process complete lines
                    for line in lines[:-1]:
                        if line.strip() and self.running:
                            result = self.process_tweet(line)
                            if result:
                                tweet_count_in_batch += 1
                                
                                # Display result with batch context
                                self.display_result(result, tweet_count_in_batch)
                                
                                # Brief pause between tweets for visibility
                                time.sleep(0.8)
                                
                                # Check if batch is complete
                                if tweet_count_in_batch >= 10:
                                    # Show batch summary
                                    self.display_batch_summary()
                                    
                                    # Display overall stats
                                    self.display_overall_stats()
                                    
                                    # Save batch results
                                    if self.save_results:
                                        self.save_batch_results(10)
                                    
                                    # Reset batch counter
                                    tweet_count_in_batch = 0
                                    
                                    # Dramatic pause between batches
                                    if self.running:
                                        self.countdown_between_batches(5)
                    
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"❌ Error receiving data: {e}")
                    break
                    
        except KeyboardInterrupt:
            print("\n🛑 Interrupted by user")
        finally:
            # Handle incomplete batch
            if tweet_count_in_batch > 0:
                print(f"\n📊 Incomplete batch had {tweet_count_in_batch} tweets")
                self.display_batch_summary()
            
            self.stop_client()
    
    def stop_client(self):
        """Stop the client and save final results"""
        print("\n🛑 Stopping streaming client...")
        self.running = False
        
        if self.client_socket:
            self.client_socket.close()
        
        # Save final results
        if self.save_results and self.results:
            self.save_final_results()
        
        # Display final statistics
        self.display_final_stats()
        
        print("✅ Client stopped successfully")
    
    def save_final_results(self):
        """Save all results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Save as JSON
            json_filename = f"sentiment_analysis_results_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            # Save as CSV using pandas
            if self.results:
                df = pd.DataFrame(self.results)
                csv_filename = f"sentiment_analysis_results_{timestamp}.csv"
                df.to_csv(csv_filename, index=False)
                
                print(f"💾 Final results saved:")
                print(f"   📄 JSON: {json_filename}")
                print(f"   📊 CSV:  {csv_filename}")
                print(f"   📝 Total records: {len(self.results)}")
            
        except Exception as e:
            print(f"❌ Error saving final results: {e}")
    
    def display_final_stats(self):
        """Display comprehensive final statistics"""
        if self.stats['total_tweets'] == 0:
            print("📊 No tweets were processed")
            return
        
        total = self.stats['total_tweets']
        print(f"\n🏆 FINAL STREAMING SUMMARY")
        print("="*60)
        print(f"🎬 Total batches processed: {self.stats['current_batch']}")
        print(f"📈 Total tweets processed: {total}")
        print(f"😊 Positive: {self.stats['positive_tweets']} ({self.stats['positive_tweets']/total*100:.1f}%)")
        print(f"😞 Negative: {self.stats['negative_tweets']} ({self.stats['negative_tweets']/total*100:.1f}%)")
        print(f"😐 Neutral:  {self.stats['neutral_tweets']} ({self.stats['neutral_tweets']/total*100:.1f}%)")
        
        if self.stats['processing_times']:
            avg_time = sum(self.stats['processing_times']) / len(self.stats['processing_times']) * 1000
            max_time = max(self.stats['processing_times']) * 1000
            min_time = min(self.stats['processing_times']) * 1000
            
            print(f"\n⚡ Processing Performance:")
            print(f"   Average: {avg_time:.2f}ms")
            print(f"   Range: {min_time:.2f}ms - {max_time:.2f}ms")
        
        # Sentiment trend analysis
        if len(self.stats['sentiment_history']) > 1:
            recent_compounds = [s['compound'] for s in self.stats['sentiment_history']]
            avg_sentiment = sum(recent_compounds) / len(recent_compounds)
            
            print(f"\n📈 Final Sentiment Analysis:")
            print(f"   Overall sentiment score: {avg_sentiment:.3f}")
            if avg_sentiment > 0.1:
                print("   🎉 Overall trend: POSITIVE STREAM! 😊")
            elif avg_sentiment < -0.1:
                print("   😔 Overall trend: NEGATIVE STREAM 😞")
            else:
                print("   ⚖️ Overall trend: BALANCED STREAM 😐")

def main():
    """Main function to run the streaming client"""
    print("🐦 TWITTER SENTIMENT STREAMING CLIENT")
    print("="*60)
    print("🎬 Enhanced with Dramatic Live Streaming Effects!")
    print("📺 Tweets processed in batches of 10 with countdowns")
    print("="*60)
    
    # Create and start client
    client = TwitterSentimentStreamingClient(
        host='localhost',
        port=9999,
        save_results=True
    )
    
    try:
        client.start_streaming()
    except Exception as e:
        print(f"❌ Client error: {e}")
    finally:
        print("\n👋 Thanks for watching the stream!")

if __name__ == "__main__":
    main()
