"""Watch task flow through agents in real-time"""
import httpx
import asyncio
import redis
import json

async def watch_task(instruction: str):
    async with httpx.AsyncClient(timeout=60) as client:
        # Submit task
        resp = await client.post('http://localhost:8001/task', json={'instruction': instruction})
        task_id = resp.json()['task_id']
        print(f'📝 Task submitted: {task_id}')
        print(f'   Instruction: "{instruction}"')
        print()
        print('🔄 Watching agent flow:')
        print('=' * 60)
        
        last_status = None
        for i in range(40):
            await asyncio.sleep(0.5)
            resp = await client.get(f'http://localhost:8001/status/{task_id}')
            status = resp.json()
            
            # Only print when status changes
            current = f"{status['status']} ({status['completed_steps']}/{status['total_steps']})"
            if current != last_status:
                agent = {
                    'planning': '🧠 PLANNER AGENT    - Breaking down task into steps',
                    'retrieving': '🔍 RETRIEVER AGENT  - Fetching relevant context', 
                    'analyzing': '📊 ANALYZER AGENT   - Processing data',
                    'writing': '✍️  WRITER AGENT     - Generating output',
                    'completed': '✅ COMPLETED',
                    'failed': '❌ FAILED'
                }.get(status['status'], status['status'])
                
                steps_info = f"Steps: {status['completed_steps']}/{status['total_steps']}" if status['total_steps'] > 0 else "Creating plan..."
                print(f'  [{i*0.5:5.1f}s] {agent}')
                print(f'          {steps_info}')
                print()
                last_status = current
                
            if status['status'] in ['completed', 'failed']:
                break
        
        print('=' * 60)
        print()
        
        # Get output from Redis
        r = redis.Redis(decode_responses=True)
        for k in r.keys(f'result:{task_id}:*'):
            data = json.loads(r.get(k))
            if data.get('content'):
                print('📄 GENERATED OUTPUT:')
                print('-' * 60)
                print(data['content'])
                print('-' * 60)
                break
        else:
            print('(Output still processing...)')

if __name__ == "__main__":
    import sys
    task = sys.argv[1] if len(sys.argv) > 1 else "Write a Python function to calculate factorial"
    asyncio.run(watch_task(task))
