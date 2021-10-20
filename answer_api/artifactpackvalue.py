import requests
import json


def check_prices(heroes, items):
    base_url = "https://steamcommunity.com/market/search/render/"
    params = {'search_descriptions': 0, 'sort_column': 'default', 'sort_dir': 'desc', 'appid': 583950, 'norender': 1,
              'count': 500, 'start': 0}
    proxies = {
        'http': 'http://proxy.swmed.edu:3128',
        'https': 'https://proxy.swmed.edu:3128',
    }
    cards = []
    while params['start'] < 237:
        response = requests.get(base_url, params, proxies=proxies)
        results = response.json()['results']
        cards += results
        params['start'] += 100

    rare_heroes = 0
    rare_items = 0
    rare_other = 0
    rare_hero_value = 0.0
    rare_item_value = 0.0
    rare_other_value = 0.0
    uncommon_heroes = 0
    uncommon_items = 0
    uncommon_other = 0
    uncommon_hero_value = 0.0
    uncommon_item_value = 0.0
    uncommon_other_value = 0.0
    print(cards[0])
    for item in cards:
        name = item['name']
        if item['asset_description']['type'] == 'Rare Card':
            if name in heroes:
                if name == 'Axe':
                    print(item)
                rare_hero_value += item['sell_price']
                rare_heroes += 1
            elif name in items:
                rare_item_value += item['sell_price']
                rare_items += 1
            else:
                rare_other_value += item['sell_price']
                rare_other += 1

        if item['asset_description']['type'] == 'Uncommon Card':
            if name in heroes:
                uncommon_hero_value += item['sell_price']
                uncommon_heroes += 1
            elif name in items:
                uncommon_item_value += item['sell_price']
                uncommon_items += 1
            else:
                uncommon_other += 1
                uncommon_other_value += item['sell_price']

    print("Rare Heroes:", rare_heroes, "Total Value:", rare_hero_value)
    print("Uncommon Heroes:", uncommon_heroes, "Total Value:", uncommon_hero_value)
    print("Rare Items:", rare_items, "Total Value:", rare_item_value)
    print("Uncommon Items:", uncommon_items, "Uncommon Item Value: ", uncommon_item_value)
    print("Other Rares:", rare_other, "Toal Value:", rare_other_value)
    print("Other Uncommons:", uncommon_other, "Total Value:", uncommon_other_value)
    uncommon_value = uncommon_hero_value + uncommon_item_value + uncommon_other_value
    uncommon_totals = uncommon_heroes + uncommon_items + uncommon_other
    pack_value = 0.0
    rare_value_per_pack = 0.0
    uncommon_pack_value = 0.0
    common_pack_value = 0.0
    rare_value_per_pack += (rare_hero_value / rare_heroes) * 0.0975
    rare_value_per_pack += (rare_item_value / rare_items) * 0.195
    rare_value_per_pack += (rare_other_value / rare_other) * 0.8775
    uncommon_pack_value += (uncommon_value / uncommon_totals) * 3.23
    common_pack_value += 7.6 * 5
    pack_value = rare_value_per_pack + uncommon_pack_value + common_pack_value
    print("Rare value per pack:", rare_value_per_pack / 100)
    print("Uncommon value per pack:", uncommon_pack_value / 100)
    print("Expected Pack Value:", pack_value / 100)
    print("Expected value after losing 15% to valve:", (pack_value * 0.85) / 100)


with open("cardset.json") as cardset_file:
    base_set = json.loads(cardset_file.read())

cards = base_set['card_set']['card_list']
print(cards[0].keys())
print(cards[0]['card_name']['english'])
card_types = set()
for card in cards:
    card_types.add(card['card_type'])
print(card_types)
heroes = []
items = []
for card in cards:
    if (card['card_type'] == 'Hero'):
        heroes.append(card['card_name']['english'])
    if (card['card_type'] == 'Item'):
        items.append(card['card_name']['english'])
print(len(heroes))
print(heroes)
print(len(items))
print(items)
check_prices(heroes, items)
