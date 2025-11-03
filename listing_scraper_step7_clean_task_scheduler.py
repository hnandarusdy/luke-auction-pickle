#!/usr/bin/env python3
"""
Task Scheduler Cleanup Script
Cleans up old Windows scheduled tasks that contain "online_pickles" and are scheduled for past dates
"""

import subprocess
import re
from datetime import datetime
import json
import xml.etree.ElementTree as ET
import os
import logging

class TaskSchedulerCleaner:
    """Clean up old Windows scheduled tasks"""
    
    def __init__(self):
        self.current_time = datetime.now()
        self.tasks_found = 0
        self.tasks_deleted = 0
        self.tasks_failed = 0
        self.deleted_tasks_log = []
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory if it doesn't exist
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Setup file handler with timestamp
        log_filename = f'task_scheduler_cleanup.log'
        log_filepath = os.path.join(log_dir, log_filename)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filepath, encoding='utf-8'),
                logging.StreamHandler()  # Also log to console
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.log_filepath = log_filepath
        
        # Log startup
        self.logger.info("=== Task Scheduler Cleanup Started ===")
        self.logger.info(f"Current time: {self.current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Log file: {log_filepath}")
        
    def log_deleted_task(self, task_name, task_info, schedule_time=None, reason="Past date"):
        """Log details of a deleted task"""
        deleted_task = {
            'deleted_at': self.current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'task_name': task_name,
            'original_status': task_info.get('Status', 'Unknown'),
            'next_run_time': task_info.get('Next Run Time', 'N/A'),
            'last_run_time': task_info.get('Last Run Time', 'N/A'),
            'schedule_time': schedule_time.strftime('%Y-%m-%d %H:%M:%S') if schedule_time else 'Unknown',
            'deletion_reason': reason,
            'task_path': task_info.get('TaskName', ''),
            'author': task_info.get('Author', 'Unknown'),
            'run_as_user': task_info.get('Run As User', 'Unknown')
        }
        
        self.deleted_tasks_log.append(deleted_task)
        
        # Log to file
        self.logger.info(f"DELETED TASK: {task_name}")
        self.logger.info(f"  - Status: {deleted_task['original_status']}")
        self.logger.info(f"  - Schedule: {deleted_task['schedule_time']}")
        self.logger.info(f"  - Reason: {deleted_task['deletion_reason']}")
        self.logger.info(f"  - Author: {deleted_task['author']}")
        self.logger.info(f"  - Run as: {deleted_task['run_as_user']}")
        
    def save_deleted_tasks_report(self):
        """Save a detailed report of all deleted tasks"""
        try:
            if not self.deleted_tasks_log:
                return
            
            # Create reports directory
            reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'cleanup_reports')
            os.makedirs(reports_dir, exist_ok=True)
            
            timestamp = self.current_time.strftime('%Y%m%d_%H%M%S')
            
            # Save JSON report
            json_filename = f'deleted_tasks_report_{timestamp}.json'
            json_filepath = os.path.join(reports_dir, json_filename)
            
            report_data = {
                'cleanup_session': {
                    'timestamp': self.current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'total_tasks_found': self.tasks_found,
                    'total_deleted': self.tasks_deleted,
                    'total_failed': self.tasks_failed,
                    'log_file': self.log_filepath
                },
                'deleted_tasks': self.deleted_tasks_log
            }
            
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            
            # Save CSV report for easy viewing
            csv_filename = f'deleted_tasks_report_{timestamp}.csv'
            csv_filepath = os.path.join(reports_dir, csv_filename)
            
            with open(csv_filepath, 'w', encoding='utf-8') as f:
                # Write header
                f.write('Deleted At,Task Name,Original Status,Schedule Time,Deletion Reason,Author,Run As User,Next Run Time,Last Run Time\n')
                
                # Write data
                for task in self.deleted_tasks_log:
                    f.write(f'"{task["deleted_at"]}",')
                    f.write(f'"{task["task_name"]}",')
                    f.write(f'"{task["original_status"]}",')
                    f.write(f'"{task["schedule_time"]}",')
                    f.write(f'"{task["deletion_reason"]}",')
                    f.write(f'"{task["author"]}",')
                    f.write(f'"{task["run_as_user"]}",')
                    f.write(f'"{task["next_run_time"]}",')
                    f.write(f'"{task["last_run_time"]}"\n')
            
            print(f"\n📄 Detailed reports saved:")
            print(f"   📊 JSON Report: {json_filepath}")
            print(f"   📋 CSV Report: {csv_filepath}")
            
            self.logger.info(f"Reports saved: JSON={json_filepath}, CSV={csv_filepath}")
            
        except Exception as e:
            print(f"❌ Error saving reports: {e}")
            self.logger.error(f"Error saving reports: {e}")
        
    def get_all_scheduled_tasks(self):
        """Get all scheduled tasks from Windows Task Scheduler"""
        try:
            print("🔍 Querying all scheduled tasks from Windows Task Scheduler...")
            
            # Get list of all tasks in CSV format
            cmd = ["schtasks", "/query", "/fo", "csv", "/v"]
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            
            if result.returncode != 0:
                print(f"❌ Error querying tasks: {result.stderr}")
                return []
            
            # Parse CSV output
            lines = result.stdout.strip().split('\n')
            if len(lines) < 2:
                print("⚠️ No tasks found")
                return []
            
            # Parse header and data
            header = [col.strip('"') for col in lines[0].split('","')]
            tasks = []
            
            for line in lines[1:]:
                # Handle CSV parsing with quotes
                columns = line.split('","')
                if len(columns) >= len(header):
                    # Clean up quotes
                    columns[0] = columns[0].lstrip('"')
                    columns[-1] = columns[-1].rstrip('"')
                    
                    task_dict = dict(zip(header, columns))
                    tasks.append(task_dict)
            
            print(f"✅ Found {len(tasks)} total scheduled tasks")
            return tasks
            
        except Exception as e:
            print(f"❌ Error getting scheduled tasks: {e}")
            return []
    
    def filter_online_pickles_tasks(self, all_tasks):
        """Filter tasks that contain 'online_pickles' in the task name"""
        try:
            online_pickles_tasks = []
            
            for task in all_tasks:
                task_name = task.get('TaskName', '').strip()
                
                # Remove leading path separators
                if task_name.startswith('\\'):
                    task_name = task_name[1:]
                
                if 'online_pickles' in task_name.lower():
                    online_pickles_tasks.append(task)
            
            print(f"🎯 Found {len(online_pickles_tasks)} tasks containing 'online_pickles'")
            return online_pickles_tasks
            
        except Exception as e:
            print(f"❌ Error filtering online_pickles tasks: {e}")
            return []
    
    def get_task_schedule_info(self, task_name):
        """Get detailed schedule information for a specific task"""
        try:
            # Clean task name
            if task_name.startswith('\\'):
                task_name = task_name[1:]
            
            # Get task details in XML format
            cmd = ["schtasks", "/query", "/tn", task_name, "/xml"]
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            
            if result.returncode != 0:
                print(f"   ⚠️ Could not get details for task '{task_name}': {result.stderr}")
                return None
            
            # Parse XML to get trigger information
            try:
                root = ET.fromstring(result.stdout)
                
                # Look for TimeTrigger elements
                triggers = root.findall('.//TimeTrigger')
                if not triggers:
                    # Also check for other trigger types
                    triggers = root.findall('.//Triggers/*')
                
                if triggers:
                    trigger = triggers[0]  # Use first trigger
                    
                    # Get start boundary (scheduled time)
                    start_boundary = trigger.find('StartBoundary')
                    if start_boundary is not None:
                        # Parse ISO format datetime
                        schedule_str = start_boundary.text
                        # Handle different datetime formats
                        try:
                            if 'T' in schedule_str:
                                # ISO format: 2025-11-04T14:45:00
                                schedule_time = datetime.fromisoformat(schedule_str.replace('Z', ''))
                            else:
                                # Date only format
                                schedule_time = datetime.strptime(schedule_str, '%Y-%m-%d')
                            
                            return schedule_time
                        except ValueError as ve:
                            print(f"   ⚠️ Could not parse schedule time '{schedule_str}': {ve}")
                            return None
                
                return None
                
            except ET.ParseError as pe:
                print(f"   ⚠️ Could not parse XML for task '{task_name}': {pe}")
                return None
            
        except Exception as e:
            print(f"   ❌ Error getting schedule info for '{task_name}': {e}")
            return None
    
    def is_task_in_past(self, task_name, task_info):
        """Check if a task is scheduled for a time in the past"""
        try:
            # First try to get schedule from the task info
            next_run_time = task_info.get('Next Run Time', '').strip()
            
            # If Next Run Time shows N/A or disabled, try to get from XML
            if next_run_time in ['N/A', 'Disabled', ''] or 'disabled' in next_run_time.lower():
                # Try to get actual schedule info from XML
                schedule_time = self.get_task_schedule_info(task_name)
                if schedule_time:
                    is_past = schedule_time < self.current_time
                    print(f"   📅 Scheduled for: {schedule_time.strftime('%Y-%m-%d %H:%M:%S')} (from XML)")
                    print(f"   {'🕐 IN PAST' if is_past else '🕑 IN FUTURE'}")
                    return is_past
                else:
                    # If we can't determine the schedule, assume it's past since it's disabled
                    print(f"   ⚠️ Could not determine schedule time, treating as past task")
                    return True
            
            # Try to parse Next Run Time
            try:
                # Handle different date formats
                if next_run_time and next_run_time != 'N/A':
                    # Common formats: "11/4/2025 2:45:00 PM", "04/11/2025 14:45:00"
                    try:
                        # Try parsing with different formats
                        formats = [
                            '%m/%d/%Y %I:%M:%S %p',  # 11/4/2025 2:45:00 PM
                            '%d/%m/%Y %H:%M:%S',     # 04/11/2025 14:45:00
                            '%Y-%m-%d %H:%M:%S',     # 2025-11-04 14:45:00
                        ]
                        
                        schedule_time = None
                        for fmt in formats:
                            try:
                                schedule_time = datetime.strptime(next_run_time, fmt)
                                break
                            except ValueError:
                                continue
                        
                        if schedule_time:
                            is_past = schedule_time < self.current_time
                            print(f"   📅 Next run: {schedule_time.strftime('%Y-%m-%d %H:%M:%S')}")
                            print(f"   {'🕐 IN PAST' if is_past else '🕑 IN FUTURE'}")
                            return is_past
                    except ValueError:
                        pass
            except:
                pass
            
            # Fallback: get schedule from XML
            schedule_time = self.get_task_schedule_info(task_name)
            if schedule_time:
                is_past = schedule_time < self.current_time
                print(f"   📅 Scheduled for: {schedule_time.strftime('%Y-%m-%d %H:%M:%S')} (from XML)")
                print(f"   {'🕐 IN PAST' if is_past else '🕑 IN FUTURE'}")
                return is_past
            
            # If all else fails, assume it's a past task since it's likely completed or disabled
            print(f"   ⚠️ Could not determine schedule time, treating as past task")
            return True
            
        except Exception as e:
            print(f"   ❌ Error checking if task is in past: {e}")
            return True  # Assume past if we can't determine
    
    def delete_task(self, task_name, task_info, schedule_time=None):
        """Delete a Windows scheduled task and log the deletion"""
        try:
            # Clean task name
            if task_name.startswith('\\'):
                task_name = task_name[1:]
            
            print(f"   🗑️ Deleting task: {task_name}")
            
            cmd = ["schtasks", "/delete", "/tn", task_name, "/f"]
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
            
            if result.returncode == 0:
                print(f"   ✅ Task deleted successfully")
                
                # Log the deleted task
                self.log_deleted_task(task_name, task_info, schedule_time, "Past date - automatic cleanup")
                
                return True
            else:
                print(f"   ❌ Failed to delete task: {result.stderr}")
                self.logger.error(f"Failed to delete task {task_name}: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error deleting task: {e}")
            self.logger.error(f"Error deleting task {task_name}: {e}")
            return False
    
    def clean_past_tasks(self):
        """Main function to clean up past online_pickles tasks"""
        print("🧹 Starting Task Scheduler Cleanup...")
        print("=" * 60)
        print(f"🕐 Current time: {self.current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("🎯 Looking for tasks containing 'online_pickles' that are in the past...")
        print()
        
        # Get all scheduled tasks
        all_tasks = self.get_all_scheduled_tasks()
        if not all_tasks:
            print("❌ No tasks found or error occurred")
            return
        
        # Filter for online_pickles tasks
        online_pickles_tasks = self.filter_online_pickles_tasks(all_tasks)
        if not online_pickles_tasks:
            print("✅ No 'online_pickles' tasks found - nothing to clean")
            return
        
        self.tasks_found = len(online_pickles_tasks)
        
        print("🔍 Analyzing tasks for cleanup...")
        print("=" * 60)
        
        # Check each task and delete if it's in the past
        for i, task in enumerate(online_pickles_tasks, 1):
            task_name = task.get('TaskName', '').strip()
            status = task.get('Status', '').strip()
            
            print(f"\n📋 Task {i}/{len(online_pickles_tasks)}: {task_name}")
            print(f"   📊 Status: {status}")
            
            # Get schedule time for logging
            schedule_time = None
            
            # Check if task is in the past
            if self.is_task_in_past(task_name, task):
                # Get schedule time for detailed logging
                schedule_time = self.get_task_schedule_info(task_name)
                
                # Task is in the past, delete it
                if self.delete_task(task_name, task, schedule_time):
                    self.tasks_deleted += 1
                else:
                    self.tasks_failed += 1
            else:
                print(f"   ⏭️ Task is for future, keeping...")
        
        # Save detailed reports
        self.save_deleted_tasks_report()
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 CLEANUP SUMMARY")
        print("=" * 60)
        print(f"🔍 Tasks found with 'online_pickles': {self.tasks_found}")
        print(f"✅ Tasks deleted (past): {self.tasks_deleted}")
        print(f"❌ Tasks failed to delete: {self.tasks_failed}")
        print(f"⏭️ Tasks kept (future): {self.tasks_found - self.tasks_deleted - self.tasks_failed}")
        
        if self.tasks_deleted > 0:
            print(f"\n🎉 Successfully cleaned up {self.tasks_deleted} old task(s)!")
            print(f"📝 Detailed logs saved to: {self.log_filepath}")
        else:
            print(f"\n✨ No cleanup needed - all tasks are for future dates")
        
        # Close logging
        self.logger.info("=== Task Scheduler Cleanup Completed ===")
        self.logger.info(f"Summary: Found={self.tasks_found}, Deleted={self.tasks_deleted}, Failed={self.tasks_failed}")
        
        # Close all logging handlers
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)

def main():
    """Main function"""
    try:
        cleaner = TaskSchedulerCleaner()
        cleaner.clean_past_tasks()
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    main()
