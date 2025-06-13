"""
Twitter Streaming Client with Real-time Sentiment Analysis using PySpark
Connects to Twitter streaming server and performs live sentiment analysis with Spark Streaming
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

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

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

# Global sentiment analyzer for Spark workers
global_vader_analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment_textblob_static(text):
    """Static function for TextBlob sentiment analysis"""
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
        return {'polarity': 0.0, 'subjectivity': 0.0, 'sentiment': 'neutral'}

def analyze_sentiment_vader_static(text):
    """Static function for VADER sentiment analysis"""
    try:
        scores = global_vader_analyzer.polarity_scores(text)
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
        return {'compound': 0.0, 'positive': 0.0, 'negative': 0.0, 'neutral': 1.0, 'sentiment': 'neutral'}

def process_tweet_static(tweet_data):
    """Static function to process a single tweet with sentiment analysis"""
    start_time = time.time()
    
    try:
        # Parse tweet data
        if isinstance(tweet_data, str):
            tweet = json.loads(tweet_data.strip())
        else:
            tweet = tweet_data
        
        # Skip non-tweet messages
        if 'type' in tweet:
            return None
        
        # Extract text for analysis
        text = tweet.get('clean_text') or tweet.get('text', '')
        if not text:
            return None
        
        # Perform sentiment analysis
        textblob_result = analyze_sentiment_textblob_static(text)
        vader_result = analyze_sentiment_vader_static(text)
        
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
        
        return result
        
    except Exception as e:
        return None

class SparkTwitterSentimentStreamingClient:
    def __init__(self, host='localhost', port=9999, save_results=True, spark_batch_duration=2):
        self.host = host
        self.port = port
        self.save_results = save_results
        self.running = False
        self.client_socket = None
        self.spark_batch_duration = spark_batch_duration
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("TwitterSentimentStreaming") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Initialize streaming context
        self.ssc = StreamingContext(self.spark.sparkContext, spark_batch_duration)
        self.ssc.checkpoint("./spark_checkpoint")
        
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
        print("\n🛑 Shutting down Spark streaming client...")
        self.stop_client()
        sys.exit(0)
    
    def process_batch_spark(self, rdd):
        """Process a batch of tweets using Spark RDD operations"""
        if rdd.isEmpty():
            return
            
        try:
            # Start new batch
            self.stats['current_batch'] += 1
            self.stats['batch_start_time'] = time.time()
            self.current_batch_tweets = []
            
            print(f"\n🚀 SPARK BATCH {self.stats['current_batch']} PROCESSING...")
            print("⚡" * 60)
            
            # Process tweets in parallel using Spark with static function
            processed_tweets = rdd.map(process_tweet_static).filter(lambda x: x is not None)
            
            # Collect results
            batch_results = processed_tweets.collect()
            
            if not batch_results:
                print("No valid tweets in this batch")
                return
            
            # Update stats and display results
            for i, result in enumerate(batch_results, 1):
                self.update_stats(result)
                self.current_batch_tweets.append(result)
                
                if self.save_results:
                    self.results.append(result)
                    self.batch_results.append(result)
                
                # Display result with batch context
                self.display_result(result, i)
                
            # Show batch summary
            self.display_batch_summary()
            
            # Display overall stats
            self.display_overall_stats()
            
            # Save batch results
            if self.save_results and len(self.batch_results) >= 10:
                self.save_batch_results(10)
            
            print("✅ Spark batch processing completed!")
            
        except Exception as e:
            print(f"❌ Spark batch processing error: {e}")
    
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
            
        print(f"\n📧 SPARK BATCH {self.stats['current_batch']} | Tweet {tweet_num_in_batch} | Total: #{self.stats['total_tweets']}")
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
        print(f"⚡ Spark Processing: {result['processing_time']*1000:.1f}ms")
    
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
        print(f"📊 SPARK BATCH {self.stats['current_batch']} COMPLETED!")
        print("🔥" * 60)
        print(f"📈 Tweets in batch: {batch_size}")
        print(f"😊 Positive: {batch_positive} ({batch_positive/batch_size*100:.1f}%)")
        print(f"😞 Negative: {batch_negative} ({batch_negative/batch_size*100:.1f}%)")
        print(f"😐 Neutral:  {batch_neutral} ({batch_neutral/batch_size*100:.1f}%)")
        print(f"📊 Avg sentiment: {avg_compound:+.3f}")
        print(f"⚡ Avg Spark processing: {avg_processing:.1f}ms")
        print(f"⏱️ Batch duration: {batch_duration:.1f}s")
        
        # Batch trend
        if avg_compound > 0.1:
            print("📈 Spark Batch trend: POSITIVE VIBES! 🎉")
        elif avg_compound < -0.1:
            print("📉 Spark Batch trend: NEGATIVE MOOD 😔")
        else:
            print("📊 Spark Batch trend: BALANCED SENTIMENT ⚖️")
    
    def display_overall_stats(self):
        """Display overall streaming statistics"""
        if self.stats['total_tweets'] == 0:
            return
            
        total = self.stats['total_tweets']
        pos_pct = (self.stats['positive_tweets'] / total) * 100
        neg_pct = (self.stats['negative_tweets'] / total) * 100
        neu_pct = (self.stats['neutral_tweets'] / total) * 100
        
        avg_processing_time = sum(self.stats['processing_times']) / len(self.stats['processing_times']) * 1000
        
        print(f"\n🌟 SPARK STREAMING STATISTICS (Total: {total} tweets)")
        print("=" * 60)
        print(f"😊 Positive: {self.stats['positive_tweets']} ({pos_pct:.1f}%)")
        print(f"😞 Negative: {self.stats['negative_tweets']} ({neg_pct:.1f}%)")
        print(f"😐 Neutral:  {self.stats['neutral_tweets']} ({neu_pct:.1f}%)")
        print(f"⚡ Avg Spark processing: {avg_processing_time:.2f}ms")
        print(f"📦 Spark batches completed: {self.stats['current_batch']}")
        
        # Show recent sentiment trend
        if len(self.stats['sentiment_history']) >= 5:
            recent_sentiments = [s['sentiment'] for s in list(self.stats['sentiment_history'])[-5:]]
            trend_emojis = {'positive': '😊', 'negative': '😞', 'neutral': '😐'}
            trend_display = ' → '.join([trend_emojis.get(s, '🤔') for s in recent_sentiments])
            print(f"📈 Recent Spark trend: {trend_display}")
    
    def save_batch_results(self, batch_size=10):
        """Save batch results to file using Spark DataFrame"""
        if len(self.batch_results) >= batch_size:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            try:
                # Create Spark DataFrame
                df = self.spark.createDataFrame(self.batch_results)
                
                # Save as Parquet (efficient for Spark)
                parquet_path = f"spark_streaming_sentiment_batch_{timestamp}.parquet"
                df.write.mode("overwrite").parquet(parquet_path)
                
                # Also save as JSON for compatibility
                json_filename = f"spark_streaming_sentiment_batch_{timestamp}.json"
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(self.batch_results, f, indent=2, ensure_ascii=False)
                
                print(f"💾 Saved Spark batch of {len(self.batch_results)} results:")
                print(f"   📊 Parquet: {parquet_path}")
                print(f"   📄 JSON: {json_filename}")
                
                self.batch_results = []  # Clear batch
                
            except Exception as e:
                print(f"❌ Error saving Spark batch: {e}")
    
    def start_streaming(self):
        """Start the Spark streaming sentiment analysis"""
        print("🚀 SPARK TWITTER SENTIMENT STREAMING CLIENT")
        print("="*60)
        print("⚡ SPARK STREAMING MODE ACTIVATED!")
        print(f"🔥 Batch Duration: {self.spark_batch_duration} seconds")
        print("="*60)
        
        try:
            # Create DStream from socket
            lines = self.ssc.socketTextStream(self.host, self.port)
            
            # Process each batch using Spark
            lines.foreachRDD(self.process_batch_spark)
            
            # Start the streaming context
            self.ssc.start()
            print(f"📡 Spark streaming started, listening on {self.host}:{self.port}")
            print("🎯 Processing tweets with Spark parallel processing!")
            print("⚡ Press Ctrl+C to stop\n")
            
            # Wait for termination
            self.ssc.awaitTermination()
            
        except Exception as e:
            print(f"❌ Spark streaming error: {e}")
        finally:
            self.stop_client()
    
    def stop_client(self):
        """Stop the Spark client and save final results"""
        print("\n🛑 Stopping Spark streaming client...")
        
        if hasattr(self, 'ssc') and self.ssc:
            self.ssc.stop(stopSparkContext=False, stopGraceFully=True)
        
        # Save final results
        if self.save_results and self.results:
            self.save_final_results_spark()
        
        # Display final statistics
        self.display_final_stats()
        
        # Stop Spark session
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()
        
        print("✅ Spark client stopped successfully")
    
    def save_final_results_spark(self):
        """Save all results to files using Spark"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            if self.results:
                # Create Spark DataFrame
                df = self.spark.createDataFrame(self.results)
                
                # Save as Parquet
                parquet_path = f"spark_sentiment_analysis_final_{timestamp}.parquet"
                df.write.mode("overwrite").parquet(parquet_path)
                
                # Save as CSV
                csv_path = f"spark_sentiment_analysis_final_{timestamp}.csv"
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
                
                # Save as JSON for compatibility
                json_filename = f"spark_sentiment_analysis_final_{timestamp}.json"
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(self.results, f, indent=2, ensure_ascii=False)
                
                print(f"💾 Final Spark results saved:")
                print(f"   📊 Parquet: {parquet_path}")
                print(f"   📈 CSV: {csv_path}")
                print(f"   📄 JSON: {json_filename}")
                print(f"   📝 Total records: {len(self.results)}")
            
        except Exception as e:
            print(f"❌ Error saving final Spark results: {e}")
    
    def display_final_stats(self):
        """Display comprehensive final statistics"""
        if self.stats['total_tweets'] == 0:
            print("📊 No tweets were processed by Spark")
            return
        
        total = self.stats['total_tweets']
        print(f"\n🏆 FINAL SPARK STREAMING SUMMARY")
        print("="*60)
        print(f"🚀 Total Spark batches processed: {self.stats['current_batch']}")
        print(f"📈 Total tweets processed: {total}")
        print(f"😊 Positive: {self.stats['positive_tweets']} ({self.stats['positive_tweets']/total*100:.1f}%)")
        print(f"😞 Negative: {self.stats['negative_tweets']} ({self.stats['negative_tweets']/total*100:.1f}%)")
        print(f"😐 Neutral:  {self.stats['neutral_tweets']} ({self.stats['neutral_tweets']/total*100:.1f}%)")
        
        if self.stats['processing_times']:
            avg_time = sum(self.stats['processing_times']) / len(self.stats['processing_times']) * 1000
            max_time = max(self.stats['processing_times']) * 1000
            min_time = min(self.stats['processing_times']) * 1000
            
            print(f"\n⚡ Spark Processing Performance:")
            print(f"   Average: {avg_time:.2f}ms")
            print(f"   Range: {min_time:.2f}ms - {max_time:.2f}ms")
        
        # Sentiment trend analysis
        if len(self.stats['sentiment_history']) > 1:
            recent_compounds = [s['compound'] for s in self.stats['sentiment_history']]
            avg_sentiment = sum(recent_compounds) / len(recent_compounds)
            
            print(f"\n📈 Final Spark Sentiment Analysis:")
            print(f"   Overall sentiment score: {avg_sentiment:.3f}")
            if avg_sentiment > 0.1:
                print("   🎉 Overall Spark trend: POSITIVE STREAM! 😊")
            elif avg_sentiment < -0.1:
                print("   😔 Overall Spark trend: NEGATIVE STREAM 😞")
            else:
                print("   ⚖️ Overall Spark trend: BALANCED STREAM 😐")

def main():
    """Main function to run the Spark streaming client"""
    print("🚀 SPARK TWITTER SENTIMENT STREAMING CLIENT")
    print("="*60)
    print("⚡ Enhanced with Apache Spark Distributed Processing!")
    print("🔥 Real-time sentiment analysis with parallel processing")
    print("="*60)
    
    # Create and start Spark client
    client = SparkTwitterSentimentStreamingClient(
        host='localhost',
        port=9999,
        save_results=True,
        spark_batch_duration=2  # Process every 2 seconds
    )
    
    try:
        client.start_streaming()
    except Exception as e:
        print(f"❌ Spark client error: {e}")
    finally:
        print("\n👋 Thanks for using Spark streaming!")

if __name__ == "__main__":
    main()