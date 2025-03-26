import json
import difflib
import os

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "歌手名称表.json"), 'r', encoding='utf-8') as file:
    all_names: list[str] = json.load(file)

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "歌手别名一对一.json"), 'r', encoding='utf-8') as file:
    aliases: dict[str, str] = json.load(file)

def find_closest(target: str, candidates: list[str], n=1, cutoff=0.6) -> str | None:
    matches = difflib.get_close_matches(target, candidates, n=n, cutoff=cutoff)
    if matches:
        return matches[0]
    else:
        return None

def find_original_name(name: str) -> str:
    alias = find_closest(name, all_names)
    if alias:
        if alias in aliases:
            return aliases[alias]
        else:
            return alias
    else:
        return name
    
if __name__ == "__main__":
    print(find_original_name("夢ノ結唱ROSE"))