#!/usr/bin/env python3
"""
Basic Task Scheduler for Windows
Creates a scheduled task to run test.py every 5 minutes starting at 7:30 AM
"""

import subprocess
import sys
import os
from datetime import datetime, timedelta

def create_scheduled_task():
    """
    Create a Windows scheduled task using schtasks command
    """
    try:
        # Task configuration
        task_name = "test_1"
        script_path = r"C:\workZ\airtasker\Luke-Python\luke-pickles\test.py"
        python_path = sys.executable  # Get current Python interpreter path
        
        # Check if script exists
        if not os.path.exists(script_path):
            print(f"❌ Error: Script not found at {script_path}")
            return False
        
        print(f"🔧 Creating scheduled task: {task_name}")
        print(f"📄 Script to run: {script_path}")
        print(f"🐍 Python interpreter: {python_path}")
        print(f"⏰ Schedule: Every 5 minutes starting at 7:30 AM")
        
        # First, delete the task if it already exists
        try:
            delete_cmd = f'schtasks /delete /tn "{task_name}" /f'
            subprocess.run(delete_cmd, shell=True, capture_output=True, text=True)
            print(f"🗑️  Removed existing task (if any)")
        except:
            pass
        
        # Create the scheduled task
        # Note: Using /sc minute /mo 5 for every 5 minutes
        # /st 07:30 sets start time to 7:30 AM
        create_cmd = f'''schtasks /create /tn "{task_name}" /tr "\\"{python_path}\\" \\"{script_path}\\"" /sc minute /mo 5 /st 07:30'''
        
        print(f"\n📝 Running command:")
        print(f"   {create_cmd}")
        
        result = subprocess.run(create_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"\n✅ Task '{task_name}' created successfully!")
            print(f"📋 Task details:")
            print(f"   • Name: {task_name}")
            print(f"   • Frequency: Every 5 minutes")
            print(f"   • Start time: 7:30 AM daily")
            print(f"   • Command: {python_path} {script_path}")
            
            # Query the task to confirm it was created
            query_cmd = f'schtasks /query /tn "{task_name}"'
            query_result = subprocess.run(query_cmd, shell=True, capture_output=True, text=True)
            
            if query_result.returncode == 0:
                print(f"\n📊 Task verification successful:")
                print(query_result.stdout)
            
            return True
        else:
            print(f"\n❌ Failed to create task!")
            print(f"Error: {result.stderr}")
            print(f"Output: {result.stdout}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating scheduled task: {e}")
        return False

def list_scheduled_tasks():
    """
    List existing scheduled tasks
    """
    try:
        print(f"\n📋 Listing all scheduled tasks...")
        list_cmd = 'schtasks /query /fo table'
        result = subprocess.run(list_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            print(f"\n📋 Custom scheduled tasks:")
            found_tasks = False
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('TaskName') and not line.startswith('====='):
                    # Skip Microsoft system tasks
                    if not ('\\Microsoft\\' in line or line.startswith('Task')):
                        parts = line.split()
                        if parts:
                            task_name = parts[0]
                            print(f"   • {task_name}")
                            found_tasks = True
            
            if not found_tasks:
                print(f"   ⚠️  No custom tasks found")
            
            return True
        else:
            print(f"❌ Failed to list tasks: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error listing tasks: {e}")
        return False

def delete_scheduled_task():
    """
    Delete a scheduled task
    """
    try:
        print(f"\n📋 Available tasks:")
        # First list all tasks to help user choose
        list_cmd = 'schtasks /query /fo table'
        result = subprocess.run(list_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            found_tasks = False
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('TaskName') and not line.startswith('====='):
                    # Skip Microsoft system tasks
                    if not ('\\Microsoft\\' in line or line.startswith('Task')):
                        parts = line.split()
                        if parts:
                            task_name = parts[0]
                            print(f"   • {task_name}")
                            found_tasks = True
            
            if not found_tasks:
                print(f"   ⚠️  No custom tasks found")
                return False
        else:
            print(f"⚠️  Could not list tasks, proceeding anyway...")
        
        # Prompt user for task name
        task_name = input(f"\n🎯 Enter the task name to delete: ").strip()
        
        if not task_name:
            print(f"❌ No task name provided")
            return False
        
        # Confirm deletion
        confirm = input(f"⚠️  Are you sure you want to delete task '{task_name}'? (y/N): ").strip().lower()
        if confirm != 'y' and confirm != 'yes':
            print(f"❌ Deletion cancelled")
            return False
        
        print(f"🗑️  Deleting scheduled task: {task_name}")
        
        delete_cmd = f'schtasks /delete /tn "{task_name}" /f'
        result = subprocess.run(delete_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Task '{task_name}' deleted successfully!")
            return True
        else:
            print(f"❌ Failed to delete task: {result.stderr}")
            print(f"💡 Make sure the task name is correct and exists")
            return False
            
    except Exception as e:
        print(f"❌ Error deleting task: {e}")
        return False

def run_task_now():
    """
    Run a scheduled task immediately for testing
    """
    try:
        print(f"\n📋 Available tasks:")
        # First list all tasks to help user choose
        list_cmd = 'schtasks /query /fo list'
        result = subprocess.run(list_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            task_names = []
            for line in lines:
                if line.startswith('TaskName:'):
                    task_name = line.replace('TaskName:', '').strip()
                    if task_name and not task_name.startswith('\\Microsoft\\'):
                        task_names.append(task_name)
                        print(f"   • {task_name}")
            
            if not task_names:
                print(f"   ⚠️  No custom tasks found")
                return False
        else:
            print(f"⚠️  Could not list tasks, proceeding anyway...")
        
        # Prompt user for task name
        task_name = input(f"\n🎯 Enter the task name to run: ").strip()
        
        if not task_name:
            print(f"❌ No task name provided")
            return False
        
        print(f"▶️  Running task '{task_name}' immediately...")
        
        run_cmd = f'schtasks /run /tn "{task_name}"'
        result = subprocess.run(run_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Task '{task_name}' started successfully!")
            print(f"📝 Check the log file for output")
            return True
        else:
            print(f"❌ Failed to run task: {result.stderr}")
            print(f"💡 Make sure the task name is correct and exists")
            return False
            
    except Exception as e:
        print(f"❌ Error running task: {e}")
        return False

def main():
    """
    Main function - provides menu for task management
    """
    print("🕐 Windows Task Scheduler Manager")
    print("=" * 40)
    
    while True:
        print(f"\n📋 Options:")
        print(f"1. Create scheduled task")
        print(f"2. List scheduled tasks")
        print(f"3. Run task now (for testing)")
        print(f"4. Delete scheduled task")
        print(f"5. Exit")
        
        choice = input(f"\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            success = create_scheduled_task()
            if success:
                print(f"\n💡 Next steps:")
                print(f"   • The task will start running at 7:30 AM every day")
                print(f"   • It will repeat every 5 minutes")
                print(f"   • Check the log file: task_execution_log.txt")
                print(f"   • Use option 3 to test run immediately")
        
        elif choice == '2':
            list_scheduled_tasks()
        
        elif choice == '3':
            run_task_now()
        
        elif choice == '4':
            delete_scheduled_task()
        
        elif choice == '5':
            print(f"👋 Goodbye!")
            break
        
        else:
            print(f"❌ Invalid choice. Please enter 1-5.")

if __name__ == "__main__":
    # Check if running as administrator (recommended for schtasks)
    try:
        import ctypes
        is_admin = ctypes.windll.shell32.IsUserAnAdmin()
        if not is_admin:
            print(f"⚠️  Warning: Not running as administrator")
            print(f"   Some task creation operations may fail")
            print(f"   Consider running as administrator for full functionality")
    except:
        pass
    
    main()