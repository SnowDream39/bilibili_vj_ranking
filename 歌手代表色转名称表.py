import json
from itertools import chain

with open("歌手代表色.json", 'r', encoding='utf-8') as file:
    color_file: list = json.load(file)

aliases = list(map(lambda x: x[1:], color_file))
aliases.sort(key=lambda x: len(x), reverse=True)

with open("歌手别名表.json", 'w', encoding='utf-8') as file:
    json.dump(aliases, file, indent=4, ensure_ascii=False)


all_names = list(chain.from_iterable(map(lambda x: x[1:], color_file)))

with open("歌手名称表.json", 'w', encoding='utf-8') as file:
    json.dump(all_names, file, indent=4, ensure_ascii=False)


one_to_one = {}
for alias in aliases:
    alias_dict = {}
    for name in alias[1:]:
        alias_dict[name] = alias[0]
    one_to_one.update(alias_dict)


with open("歌手别名一对一.json", 'w', encoding='utf-8') as file:
    json.dump(one_to_one, file, indent=4, ensure_ascii=False)
