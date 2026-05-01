#!/usr/bin/env python3
"""
WhatsApp Error Notification System
A reusable class for sending error notifications via WhatsApp when scripts fail
"""

import requests
import traceback
import os
import sys
from datetime import datetime
from functools import wraps
import socket


class WhatsAppNotifier:
    """
    A reusable class for sending error notifications via WhatsApp
    """
    
    def __init__(self, api_key=None, group_id=None):
        """
        Initialize the WhatsApp notifier
        
        Args:
            api_key (str): Wassenger API key
            group_id (str): WhatsApp group ID to send messages to
        """
        self.api_key = api_key or '4710ef4ccb46bc51f11340bc0fe0dd2d47afae9c9f2a888696164d75e05def0d61a988fcd4477c82'
        self.group_id = group_id or '120363402378382288@g.us'
        self.url = "https://api.wassenger.com/v1/messages"
        self.pc_name = socket.gethostname()
        
    def send_message(self, message):
        """
        Send a message to WhatsApp group
        
        Args:
            message (str): The message to send
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            params = {
                "group": self.group_id,
                "message": message
            }
            
            headers = {
                "Content-Type": "application/json",
                "Token": self.api_key
            }
            
            response = requests.post(self.url, json=params, headers=headers, timeout=30)
            
            if response.status_code in [200, 201]:  # 200 = OK, 201 = Created/Queued
                print(f"📱 WhatsApp notification sent successfully (Status: {response.status_code})")
                return True
            else:
                print(f"❌ Failed to send WhatsApp notification: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Exception sending WhatsApp notification: {e}")
            return False
    
    def send_error_notification(self, script_name, error, traceback_str=None):
        """
        Send an error notification with details
        
        Args:
            script_name (str): Name of the script that failed
            error (Exception): The exception that occurred
            traceback_str (str): Full traceback string (optional)
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"""🚨 SCRIPT ERROR ALERT 🚨

📅 Time: {timestamp}
💻 PC: {self.pc_name}
📄 Script: {script_name}
❌ Error: {str(error)}
🔍 Error Type: {type(error).__name__}

"""
        
        if traceback_str:
            # Truncate traceback if too long (WhatsApp has message limits)
            if len(traceback_str) > 1000:
                traceback_str = traceback_str[-1000:] + "\n... (truncated)"
            message += f"📋 Traceback:\n{traceback_str}"
        
        message += f"\n⚠️ Please check the script and logs for more details."
        
        return self.send_message(message)
    
    def send_success_notification(self, script_name, details=None):
        """
        Send a success notification
        
        Args:
            script_name (str): Name of the script that completed successfully
            details (str): Optional details about the success
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message = f"""✅ SCRIPT SUCCESS

📅 Time: {timestamp}
💻 PC: {self.pc_name}
📄 Script: {script_name}
🎉 Status: Completed Successfully
"""
        
        if details:
            message += f"\n📊 Details: {details}"
        
        return self.send_message(message)


def with_error_notification(notifier=None, script_name=None, send_success=False):
    """
    Decorator to automatically send WhatsApp notifications on errors
    
    Args:
        notifier (WhatsAppNotifier): The notifier instance (if None, creates default)
        script_name (str): Name of the script (if None, uses filename)
        send_success (bool): Whether to send success notifications too
    
    Usage:
        @with_error_notification()
        def main():
            # Your script code here
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get script name
            if script_name:
                current_script = script_name
            else:
                current_script = os.path.basename(sys.argv[0])
            
            # Get or create notifier
            current_notifier = notifier or WhatsAppNotifier()
            
            try:
                print(f"🚀 Starting {current_script}...")
                result = func(*args, **kwargs)
                
                if send_success:
                    current_notifier.send_success_notification(current_script)
                
                print(f"✅ {current_script} completed successfully")
                return result
                
            except Exception as e:
                print(f"❌ Error in {current_script}: {e}")
                
                # Get full traceback
                traceback_str = traceback.format_exc()
                print(f"Full traceback:\n{traceback_str}")
                
                # Send error notification
                current_notifier.send_error_notification(current_script, e, traceback_str)
                
                # Re-raise the exception so the script still fails
                raise
        
        return wrapper
    return decorator


# Example usage functions
def test_notification():
    """Test function to verify WhatsApp notifications work"""
    notifier = WhatsAppNotifier()
    
    # Test success message
    print("Testing success notification...")
    notifier.send_success_notification("test_script.py", "All tests passed!")
    
    # Test error message
    print("Testing error notification...")
    try:
        raise ValueError("This is a test error")
    except Exception as e:
        notifier.send_error_notification("test_script.py", e, traceback.format_exc())


def test_decorator():
    """Test the decorator functionality"""
    @with_error_notification(send_success=True)
    def sample_script():
        print("Doing some work...")
        # Simulate an error (uncomment to test)
        # raise RuntimeError("Something went wrong!")
        return "Success!"
    
    return sample_script()


if __name__ == "__main__":
    print("🧪 Testing WhatsApp Notifier...")
    
    # Test basic notifications
    test_notification()
    
    # Test decorator
    test_decorator()
    
    print("✅ All tests completed!")