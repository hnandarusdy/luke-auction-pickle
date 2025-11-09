#!/usr/bin/env python3
"""
Simple Task Scheduler
Creates a Windows scheduled task that:
1. Task name: task_1
2. Starts 1 hour from now
3. Working directory: same as this Python file
4. Action: run test.py
"""

import subprocess
import sys
import os
from datetime import datetime, timedelta

def create_task():
    """Create the scheduled task"""
    try:
        # Task configuration
        task_name = "task_1"
        
        # Get current directory (same as this Python file)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        test_script = os.path.join(script_dir, "test.py")
        python_exe = sys.executable
        
        # Calculate start time (1 hour from now)
        start_time = datetime.now() + timedelta(hours=1)
        start_time_str = start_time.strftime("%H:%M")
        start_date_str = start_time.strftime("%d/%m/%Y")
        
        print(f"🔧 Creating scheduled task: {task_name}")
        print(f"📅 Start date: {start_date_str}")
        print(f"⏰ Start time: {start_time_str}")
        print(f"📁 Working directory: {script_dir}")
        print(f"🐍 Python executable: {python_exe}")
        print(f"📄 Script to run: test.py")
        
        # Check if test.py exists
        if not os.path.exists(test_script):
            print(f"❌ Error: test.py not found at {test_script}")
            return False
        
        # Delete existing task if it exists
        try:
            delete_cmd = ["schtasks", "/delete", "/tn", task_name, "/f"]
            subprocess.run(delete_cmd, capture_output=True, text=True)
            print(f"🗑️ Removed existing task (if any)")
        except:
            pass
        
        # Create the task command that changes directory and runs the script
        task_command = f'cmd /c "cd /d "{script_dir}" & "{python_exe}" test.py"'
        
        # Build schtasks command
        create_cmd = [
            "schtasks", "/create",
            "/tn", task_name,
            "/tr", task_command,
            "/sc", "once",
            "/st", start_time_str,
            "/sd", start_date_str,
            "/f"
        ]
        
        print(f"\n📝 Executing command:")
        print(f"   {' '.join(create_cmd)}")
        
        # Execute the command
        result = subprocess.run(create_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"\n✅ Task '{task_name}' created successfully!")
            print(f"📋 Task will run at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"📁 Working directory: {script_dir}")
            print(f"🎯 Action: Run test.py")
            return True
        else:
            print(f"\n❌ Failed to create task!")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def main():
    """Main function"""
    print("🕐 Simple Task Scheduler")
    print("=" * 30)
    
    success = create_task()
    
    if success:
        print(f"\n💡 Task created! It will run automatically in 1 hour.")
        print(f"📊 You can check it in Windows Task Scheduler or run:")
        print(f"   schtasks /query /tn task_1")
    else:
        print(f"\n❌ Task creation failed!")

if __name__ == "__main__":
    main()
