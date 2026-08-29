import re

with open(r'C:\Users\user\.gemini\antigravity-ide\brain\d822fbb3-c1ff-45ed-936c-bf31a8034959\.system_generated\steps\690\content.md', 'r', encoding='utf-8') as f:
    text = f.read()

endpoints = re.findall(r'/api/v[123]/[a-zA-Z0-9/\-]+', text)
unique_endpoints = list(set(endpoints))
print("Endpoints found:", unique_endpoints)

leverage_mentions = re.findall(r'.{0,50}leverage.{0,50}', text, re.IGNORECASE)
for m in set(leverage_mentions[:20]):
    print(m)
