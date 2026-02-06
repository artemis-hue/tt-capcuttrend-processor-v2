#!/usr/bin/env python3
"""
TIKTOK AUTOMATION MAIN
Version: 5.4.0 - Added v3.5.0 Enhanced Analytics
Changes from 5.3.0:
  - Added v3.5.0 velocity predictions + competitor analysis
  - Generates 3 additional Enhanced Excel files per day
  - Velocity streak cache for variant allocation
  - Enhanced stats in Discord notification
"""

import os
import sys
import json

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from apify_fetcher import fetch_all_data
from daily_processor import process_data, load_yesterday_cache, save_today_cache, calculate_metrics
from discord_notify import send_discord_notification
from v35_enhancements import integrate_with_daily_processor, generate_daily_briefing
import pandas as pd


def run_v35_enhancements(us_data, uk_data, yesterday_us, yesterday_uk, output_dir, cache_dir):
    """
    Run v3.5.0 enhanced analytics: velocity predictions, competitor analysis,
    variant allocation, and stop rules.
    
    Returns dict of generated file paths, or empty dict on failure.
    """
    try:
        from v35_enhancements import integrate_with_daily_processor
    except ImportError as e:
        print(f"  WARNING: v3.5.0 enhancements not available: {e}")
        print("  Continuing with standard files only.")
        return {}
    
    print("\n[Step 3b] Running v3.5.0 Enhanced Analytics...")
    
    # Convert raw data to DataFrames with calculated metrics
    us_df = pd.DataFrame(us_data) if us_data else pd.DataFrame()
    uk_df = pd.DataFrame(uk_data) if uk_data else pd.DataFrame()
    
    if len(us_df) > 0:
        us_df = us_df.drop_duplicates(subset=['webVideoUrl'], keep='first')
        us_df = calculate_metrics(us_df)
        # Add author column needed by enhancements
        from daily_processor import get_author_name, detect_ai, calculate_status, calculate_build_now
        us_df['author'] = us_df.apply(get_author_name, axis=1)
        us_df['AI_CATEGORY'] = us_df.get('text', pd.Series([''])).apply(detect_ai)
        us_df = calculate_status(us_df, yesterday_us)
        us_df['BUILD_NOW'] = us_df.apply(calculate_build_now, axis=1)
        # Cross-market detection
        uk_urls = set(pd.DataFrame(uk_data)['webVideoUrl']) if uk_data else set()
        us_df['Market'] = us_df['webVideoUrl'].apply(
            lambda u: '🌐 BOTH' if u in uk_urls else '🇺🇸 US ONLY'
        )
        # Map status column name for enhancements
        if 'status' in us_df.columns and 'acceleration_status' not in us_df.columns:
            us_df['acceleration_status'] = us_df['status']
    
    if len(uk_df) > 0:
        uk_df = uk_df.drop_duplicates(subset=['webVideoUrl'], keep='first')
        uk_df = calculate_metrics(uk_df)
        from daily_processor import get_author_name, detect_ai, calculate_status, calculate_build_now
        uk_df['author'] = uk_df.apply(get_author_name, axis=1)
        uk_df['AI_CATEGORY'] = uk_df.get('text', pd.Series([''])).apply(detect_ai)
        uk_df = calculate_status(uk_df, yesterday_uk)
        uk_df['BUILD_NOW'] = uk_df.apply(calculate_build_now, axis=1)
        us_urls = set(pd.DataFrame(us_data)['webVideoUrl']) if us_data else set()
        uk_df['Market'] = uk_df['webVideoUrl'].apply(
            lambda u: '🌐 BOTH' if u in us_urls else '🇬🇧 UK ONLY'
        )
        if 'status' in uk_df.columns and 'acceleration_status' not in uk_df.columns:
            uk_df['acceleration_status'] = uk_df['status']
    
    # Convert yesterday's cache to DataFrames for velocity calculations
    yesterday_us_df = None
    yesterday_uk_df = None
    if yesterday_us:
        yesterday_us_df = pd.DataFrame(yesterday_us)
    if yesterday_uk:
        yesterday_uk_df = pd.DataFrame(yesterday_uk)
    
    try:
        enhanced_files = integrate_with_daily_processor(
            us_data=us_df,
            uk_data=uk_df,
            yesterday_us=yesterday_us_df,
            yesterday_uk=yesterday_uk_df,
            two_days_us=None,  # Not available in current cache system
            two_days_uk=None,
            output_dir=output_dir
        )
        
        if enhanced_files:
            print(f"  ✅ Generated {len(enhanced_files)} enhanced files:")
            for key, path in enhanced_files.items():
                print(f"    {key}: {os.path.basename(path)}")
        else:
            print("  ⚠️ No enhanced files generated (possibly empty data)")
        
        return enhanced_files
        
    except Exception as e:
        print(f"  ❌ v3.5.0 enhancement error: {e}")
        import traceback
        traceback.print_exc()
        print("  Continuing with standard files only.")
        return {}


def main():
    print("=" * 50)
    print("TikTok Daily Processor v5.4.0")
    print("  Standard processing: v3.3.0")
    print("  Enhanced analytics:  v3.5.0")
    print("=" * 50)
    
    # Directories
    output_dir = os.environ.get('OUTPUT_DIR', 'output')
    cache_dir = os.environ.get('CACHE_DIR', 'data')
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    
    print(f"\nOutput directory: {output_dir}")
    print(f"Cache directory: {cache_dir}")
    
    # Step 1: Fetch data from Apify
    print("\n[Step 1] Fetching data from Apify...")
    us_data, uk_data, us_music, uk_music = fetch_all_data()
    
    if not us_data and not uk_data:
        print("ERROR: No data fetched from Apify!")
        sys.exit(1)
    
    print(f"  US videos: {len(us_data) if us_data else 0}")
    print(f"  UK videos: {len(uk_data) if uk_data else 0}")
    print(f"  US music: {len(us_music) if us_music else 0}")
    print(f"  UK music: {len(uk_music) if uk_music else 0}")
    
    # Step 2: Load yesterday's cache
    print("\n[Step 2] Loading yesterday's cache...")
    yesterday_us, yesterday_uk = load_yesterday_cache(cache_dir)
    
    if yesterday_us and yesterday_uk:
        print(f"  Cache found! US: {len(yesterday_us)}, UK: {len(yesterday_uk)} records")
    else:
        print("  No cache found - all statuses will be NEW")
    
    # Step 3: Process data (standard v3.3.0 files)
    print("\n[Step 3] Processing data (standard files)...")
    stats = process_data(
        us_data, uk_data, 
        us_music, uk_music,
        yesterday_us, yesterday_uk,
        output_dir, cache_dir
    )
    
    # Step 3b: Run v3.5.0 enhancements (non-blocking)
    enhanced_files = run_v35_enhancements(
        us_data, uk_data,
        yesterday_us, yesterday_uk,
        output_dir, cache_dir
    )
    
    # Add enhancement info to stats for Discord
    if enhanced_files:
        stats['enhanced_files'] = len(enhanced_files)
    
    # Step 3c: Generate daily briefing and append to SUMMARY_REPORT
    print("\n[Step 3c] Generating daily briefing...")
    try:
        # Combine US + UK for full-picture briefing
        combined_df = pd.concat(
            [df for df in [
                pd.DataFrame(us_data) if us_data else pd.DataFrame(),
                pd.DataFrame(uk_data) if uk_data else pd.DataFrame()
            ] if len(df) > 0],
            ignore_index=True
        )
        
        if len(combined_df) > 0:
            combined_df = combined_df.drop_duplicates(subset=['webVideoUrl'], keep='first')
            combined_df = calculate_metrics(combined_df)
            from daily_processor import get_author_name, detect_ai, calculate_status, calculate_build_now
            combined_df['author'] = combined_df.apply(get_author_name, axis=1)
            combined_df['AI_CATEGORY'] = combined_df.get('text', pd.Series([''])).apply(detect_ai)
            
            # Cross-market detection
            us_urls = set(pd.DataFrame(us_data)['webVideoUrl']) if us_data else set()
            uk_urls = set(pd.DataFrame(uk_data)['webVideoUrl']) if uk_data else set()
            both_urls = us_urls & uk_urls
            combined_df['Market'] = combined_df['webVideoUrl'].apply(
                lambda u: '🌐 BOTH' if u in both_urls else '🇺🇸/🇬🇧 SINGLE'
            )
            
            # Status calculation
            combined_yesterday = None
            if yesterday_us and yesterday_uk:
                combined_yesterday = yesterday_us + yesterday_uk
            elif yesterday_us:
                combined_yesterday = yesterday_us
            elif yesterday_uk:
                combined_yesterday = yesterday_uk
            combined_df = calculate_status(combined_df, combined_yesterday)
            if 'status' in combined_df.columns:
                combined_df['acceleration_status'] = combined_df['status']
            
            # Yesterday as DataFrame for velocity
            yesterday_combined_df = None
            if combined_yesterday:
                yesterday_combined_df = pd.DataFrame(combined_yesterday)
            
            cache_dir_path = os.environ.get('CACHE_DIR', 'data')
            streak_cache = os.path.join(cache_dir_path, 'velocity_streak_cache.json')
            
            briefing_text = generate_daily_briefing(
                combined_df, yesterday_combined_df,
                output_dir, cache_path=streak_cache
            )
            
            # Append to SUMMARY_REPORT
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')
            summary_path = f"{output_dir}/SUMMARY_REPORT_{today}.txt"
            
            with open(summary_path, 'a') as f:
                f.write("\n\n")
                f.write(briefing_text)
            
            print("  ✅ Daily briefing appended to SUMMARY_REPORT")
        else:
            print("  ⚠️ No data available for briefing")
    except Exception as e:
        print(f"  ❌ Briefing generation error: {e}")
        import traceback
        traceback.print_exc()
        print("  Continuing without briefing.")
    
    # Step 4: Save today's cache for tomorrow
    print("\n[Step 4] Saving cache for tomorrow...")
    us_df = pd.DataFrame(us_data) if us_data else pd.DataFrame()
    uk_df = pd.DataFrame(uk_data) if uk_data else pd.DataFrame()
    
    if len(us_df) > 0:
        us_df = us_df.drop_duplicates(subset=['webVideoUrl'], keep='first')
        us_df = calculate_metrics(us_df)
    
    if len(uk_df) > 0:
        uk_df = uk_df.drop_duplicates(subset=['webVideoUrl'], keep='first')
        uk_df = calculate_metrics(uk_df)
    
    save_today_cache(us_df, uk_df, cache_dir)
    
    # Step 5: Send Discord notification
    print("\n[Step 5] Sending Discord notification...")
    send_discord_notification(stats)
    
    # Done
    print("\n" + "=" * 50)
    print("Processing complete!")
    print("=" * 50)
    
    # Print summary
    print(f"\nSummary:")
    print(f"  Your Posts: {stats.get('your_posts', 0)}")
    print(f"  Competitor Posts: {stats.get('competitor', 0)}")
    print(f"  🔥 URGENT: {stats.get('urgent', 0)}")
    print(f"  ⚡ HIGH: {stats.get('high', 0)}")
    print(f"  🟡 WATCH: {stats.get('watch', 0)}")
    print(f"  🚀 SPIKING: {stats.get('spiking', 0)}")
    if enhanced_files:
        print(f"  📊 Enhanced files: {len(enhanced_files)}")


if __name__ == '__main__':
    main()
