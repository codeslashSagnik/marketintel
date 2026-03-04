from bs4 import BeautifulSoup
import os

html_path = r"e:\cv projects\real_time-market-intelligence\data\debug\bb_dom_dump.html"
if not os.path.exists(html_path):
    print(f"File not found: {html_path}")
    exit(1)

with open(html_path, "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

print("--- 1. Location Button Analysis ---")
# Verified class from previous run: dawgLh
btns = soup.find_all("button", class_="dawgLh")
for b in btns:
    print(f"Found Location Button: {b.get('id')}, text='{b.text.strip()}'")

print("\n--- 2. All Input Fields ---")
for inp in soup.find_all("input"):
    print(f"Input: placeholder='{inp.get('placeholder')}', id='{inp.get('id')}', class='{inp.get('class')}', type='{inp.get('type')}'")

print("\n--- 3. Potential Location Containers ---")
# Look for text like "Select City" or "Enter Area"
for text in ["City", "Area", "Pincode", "Location"]:
    matches = soup.find_all(string=lambda t: t and text.lower() in t.lower())
    for m in matches[:10]:
        print(f"Text Match '{text}': {m.strip()} | Parent: {m.parent.name} | Grandparent: {m.parent.parent.name if m.parent.parent else 'N/A'}")
