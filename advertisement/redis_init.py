# redis_init.py
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

# 광고 예산 초기화
initial_budgets = {
    "ad_101": 1000,
    "ad_102": 200
}

for ad_id, budget in initial_budgets.items():
    r.set(f"ad_budget:{ad_id}", budget)
    print(f"Set ad_budget:{ad_id} = {budget}원")
