import asyncio
import os
import sys
import json
from decimal import Decimal

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.api.endpoints.dashboard_golden import verify_golden_dataset

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

async def test_endpoint():
    print("Executing verify_golden_dataset from API logic...")
    results = await verify_golden_dataset()
    
    print(json.dumps(results, indent=2, cls=DecimalEncoder))
    
    # Simple Assertions
    assert "test_1" in results
    assert results["test_1"]["status"] == "PASS"
    assert results["test_6"]["status"] == "PASS"
    assert results["test_9"]["status"] == "PASS"
    
    print("\n✅ API Logic verified successfully.")

if __name__ == "__main__":
    asyncio.run(test_endpoint())
