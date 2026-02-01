"""Test different tasks with the Agentic AI System"""
import httpx
import asyncio

async def submit_task(instruction: str):
    async with httpx.AsyncClient(timeout=60) as client:
        print("=" * 60)
        print(f"📝 TASK: {instruction}")
        print("=" * 60)
        
        resp = await client.post('http://localhost:8001/task', json={'instruction': instruction})
        result = resp.json()
        task_id = result['task_id']
        print(f"Task ID: {task_id}")
        
        print("\n⏳ Progress:")
        final_status = None
        for i in range(30):
            await asyncio.sleep(1)
            resp = await client.get(f'http://localhost:8001/status/{task_id}')
            status = resp.json()
            print(f"  [{i+1:2d}s] {status['status']:12s} | Steps: {status['completed_steps']}/{status['total_steps']}")
            final_status = status
            if status['status'] in ['completed', 'failed']:
                break
        
        # Try to get the output
        print("\n" + "=" * 60)
        print("📄 OUTPUT:")
        print("=" * 60)
        if final_status and final_status.get('output'):
            print(final_status['output'])
        else:
            # Try to fetch from Redis directly
            try:
                import redis
                r = redis.Redis(decode_responses=True)
                # Look for result keys
                keys = r.keys(f"result:{task_id}:*")
                for key in keys:
                    data = r.get(key)
                    if data:
                        import json
                        result_data = json.loads(data)
                        if result_data.get('content'):
                            print(result_data['content'])
                            break
                else:
                    print("(Output not yet available or task still processing)")
            except:
                print("(Could not retrieve output)")
        
        print("\n✅ Done!")
        return status

async def main():
    await submit_task("Write a Python function to calculate fibonacci numbers")

if __name__ == "__main__":
    asyncio.run(main())
