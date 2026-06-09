import sys
sys.path.insert(0, ".")
import server as s

tests = [
    # Should search (current / factual / business - the cases user wants)
    ("what is the weather in Columbus Ohio", True),
    ("phone number for Joe's Pizza in Columbus", True),
    ("stock price of AAPL right now", True),
    ("who won the Yankees game today", True),
    ("latest news on the election", True),
    ("is the hardware store on Main open today", True),
    ("how much does the 2025 model cost", True),
    ("what is the address for the DMV near me", True),
    ("score of the game last night", True),

    # Should NOT search (historical or pure conversational)
    ("where did Abraham Lincoln live", False),
    ("who was the 16th president", False),
    ("how are you doing today", False),
    ("what is the capital of France", False),
    ("tell me about the history of Rome", False),
    ("when did World War II end", False),
]

print("Testing needs_web_lookup heuristic:\n")
all_pass = True
for q, expected in tests:
    result = s.needs_web_lookup(q, [])
    status = "PASS" if result == expected else "FAIL"
    if status == "FAIL":
        all_pass = False
    print(f"{status}: {q[:60]:<60}  expected={expected} got={result}")

print("\nOverall:", "ALL TESTS PASS" if all_pass else "SOME TESTS FAILED")
