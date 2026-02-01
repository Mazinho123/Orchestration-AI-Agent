"""
Simple test to verify system works
"""
import asyncio
import httpx
import json

async def test_api():
    """Test the API endpoints."""
    
    client = httpx.AsyncClient(timeout=10)
    
    try:
        # Test root endpoint
        print("Testing GET /...")
        response = await client.get("http://localhost:8001/")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Test health check
        print("\nTesting GET /health...")
        response = await client.get("http://localhost:8001/health")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Test task submission
        print("\nTesting POST /task...")
        task_data = {
            "instruction": "Explain how multi-agent systems work",
            "context": {}
        }
        response = await client.post("http://localhost:8001/task", json=task_data)
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        if response.status_code == 200:
            task_id = response.json()["task_id"]
            print(f"\nTask created: {task_id}")
            
            # Get status
            print(f"\nTesting GET /status/{task_id}...")
            response = await client.get(f"http://localhost:8001/status/{task_id}")
            print(f"Status: {response.status_code}")
            print(f"Response: {json.dumps(response.json(), indent=2)}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.aclose()

if __name__ == "__main__":
    asyncio.run(test_api())
