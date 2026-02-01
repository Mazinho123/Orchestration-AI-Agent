"""
Demo script to test the Agentic AI System
"""
import asyncio
import httpx
import sys

async def run_demo():
    base_url = "http://localhost:8001"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Health check
        print("=" * 60)
        print("🏥 HEALTH CHECK")
        print("=" * 60)
        try:
            resp = await client.get(f"{base_url}/health")
            print(f"Status: {resp.status_code}")
            print(f"Response: {resp.json()}")
        except Exception as e:
            print(f"❌ Server not running: {e}")
            print("\n⚠️  Please start the server first with: python main.py")
            return
        
        # 2. Submit a task
        print("\n" + "=" * 60)
        print("📝 SUBMITTING TASK")
        print("=" * 60)
        task_data = {
            "instruction": "Explain how multi-agent AI systems work and list their key benefits",
            "context": {"domain": "artificial intelligence"}
        }
        print(f"Task: {task_data['instruction']}")
        
        resp = await client.post(f"{base_url}/task", json=task_data)
        result = resp.json()
        print(f"\nResponse: {result}")
        
        task_id = result.get("task_id")
        if not task_id:
            print("❌ No task_id returned")
            return
        
        # 3. Poll for status
        print("\n" + "=" * 60)
        print("⏳ MONITORING PROGRESS")
        print("=" * 60)
        
        for i in range(20):  # Poll for up to 20 seconds
            await asyncio.sleep(1)
            resp = await client.get(f"{base_url}/status/{task_id}")
            status = resp.json()
            print(f"  [{i+1}s] Status: {status}")
            
            if status.get("status") in ["completed", "failed"]:
                break
        
        # 4. Get final result
        print("\n" + "=" * 60)
        print("✅ FINAL RESULT")
        print("=" * 60)
        resp = await client.get(f"{base_url}/status/{task_id}")
        final = resp.json()
        print(f"Task ID: {task_id}")
        print(f"Status: {final.get('status')}")
        if final.get('output'):
            print(f"\nOutput:\n{final.get('output')}")
        
        print("\n" + "=" * 60)
        print("🎉 DEMO COMPLETE!")
        print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_demo())
