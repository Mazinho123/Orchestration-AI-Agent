"""
Example client for testing the Agentic AI System.
Demonstrates task submission and SSE streaming.
"""
import requests
import json
import sys
import time


def submit_task(instruction: str, context: dict = None) -> str:
    """
    Submit a task to the API.
    
    Args:
        instruction: User instruction
        context: Optional context dictionary
        
    Returns:
        Task ID
    """
    url = "http://localhost:8000/task"
    payload = {
        "instruction": instruction,
        "context": context or {}
    }
    
    print(f"\n📤 Submitting task...")
    print(f"Instruction: {instruction}")
    
    response = requests.post(url, json=payload)
    response.raise_for_status()
    
    data = response.json()
    task_id = data["task_id"]
    
    print(f"✅ Task submitted: {task_id}")
    print(f"Stream URL: {data['stream_url']}")
    
    return task_id


def stream_progress(task_id: str):
    """
    Stream task progress using SSE.
    
    Args:
        task_id: Task ID to stream
    """
    url = f"http://localhost:8000/stream/{task_id}"
    
    print(f"\n📡 Streaming progress...\n")
    print("-" * 60)
    
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            
            for line in response.iter_lines():
                if not line:
                    continue
                
                line = line.decode('utf-8')
                
                # Parse SSE format
                if line.startswith('event:'):
                    event_type = line.split(':', 1)[1].strip()
                    
                elif line.startswith('data:'):
                    data = line.split(':', 1)[1].strip()
                    
                    try:
                        data_obj = json.loads(data)
                        
                        if event_type == 'connected':
                            print(f"🔌 Connected to stream")
                            
                        elif event_type == 'progress':
                            status = data_obj.get('status', 'unknown')
                            message = data_obj.get('message', '')
                            progress = data_obj.get('progress_percent', 0)
                            
                            emoji = {
                                'pending': '⏳',
                                'planning': '🧠',
                                'retrieving': '🔍',
                                'analyzing': '🔬',
                                'writing': '✍️',
                                'completed': '✅',
                                'failed': '❌'
                            }.get(status, '📋')
                            
                            if progress:
                                print(f"{emoji} [{progress}%] {status.upper()}: {message}")
                            else:
                                print(f"{emoji} {status.upper()}: {message}")
                        
                        elif event_type == 'chunk':
                            chunk = data_obj.get('chunk', '')
                            print(chunk, end='', flush=True)
                        
                        elif event_type == 'complete':
                            status = data_obj.get('status', 'completed')
                            if status == 'completed':
                                print(f"\n\n✅ Task completed successfully!")
                            else:
                                print(f"\n\n❌ Task failed: {data_obj.get('error', 'Unknown error')}")
                            
                            print("-" * 60)
                            return
                            
                    except json.JSONDecodeError:
                        # Plain text data (like chunks)
                        if event_type == 'chunk':
                            print(data, end='', flush=True)
                
                elif line.startswith(':'):
                    # Keepalive comment
                    pass
                    
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error streaming: {e}")
        sys.exit(1)


def get_status(task_id: str):
    """
    Get task status.
    
    Args:
        task_id: Task ID
    """
    url = f"http://localhost:8000/status/{task_id}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"\n📊 Task Status:")
        print(f"  ID: {data.get('task_id', 'N/A')}")
        print(f"  Status: {data.get('status', 'N/A')}")
        print(f"  Progress: {data.get('completed_steps', 0)}/{data.get('total_steps', 0)} steps")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error getting status: {e}")


def main():
    """Main function."""
    print("=" * 60)
    print("  Agentic AI System - Example Client")
    print("=" * 60)
    
    # Example tasks
    examples = [
        "Explain how asynchronous programming works in distributed systems and provide examples",
        "Analyze the benefits and trade-offs of using Redis as a message queue",
        "Compare multi-agent architectures with monolithic approaches in AI systems",
    ]
    
    print("\nAvailable example tasks:")
    for i, task in enumerate(examples, 1):
        print(f"{i}. {task}")
    
    print(f"{len(examples) + 1}. Custom instruction")
    
    try:
        choice = input(f"\nSelect a task (1-{len(examples) + 1}): ").strip()
        
        if choice.isdigit() and 1 <= int(choice) <= len(examples):
            instruction = examples[int(choice) - 1]
        elif choice.isdigit() and int(choice) == len(examples) + 1:
            instruction = input("Enter your instruction: ").strip()
        else:
            print("Invalid choice")
            return
        
        if not instruction:
            print("Instruction cannot be empty")
            return
        
        # Submit task
        task_id = submit_task(instruction)
        
        # Wait a moment for processing to start
        time.sleep(0.5)
        
        # Stream progress
        stream_progress(task_id)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
