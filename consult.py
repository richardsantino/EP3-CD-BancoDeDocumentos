import os
from pymongo import MongoClient

#pip install "pymongo[srv]"

#https://crivelaro.notion.site/Agregando-informa-es-de-Pok-mons-cdfd27c240e5494d9eca07081cecad7d

## CONSULTAS E AGREGAÇÕES

client = MongoClient(os.environ['MONGO_URL'])
db = client.cluster

## ... consultas ...

#PokedexColor = Green
#match = {
#  "PokedexColor": "Green"
#}

# Ghost in Types
#match = {
#  "Types": {"$in": ["Ghost"]}
#}

# Types = Dragon and Ghost
# Size(m) > 1
# Weight(kg) < 20
#match = {
#  "Types": ["Dragon", "Ghost"],
#  "Size(m)": {"$gt": 1},
#  "Weight(kg)": {"$lt": 20}
#}

# Damages.Dark >= 4
# Damages.Ghost >= 3.5
#match = {
#  "Damages.Dark": {"$gte": 4},
#  "Damages.Ghost": {"$gte": 3.5}
#}

# PokedexColor = Black
# Types = Normal or Damages.Dark = 2
#match = {
#  "PokedexColor": "Black",
#  "$or": [{"Types": ["Normal"]}, {"Damages.Dark": 2}]
#}

#for pokemon in db.pokemons.find(match):
#  print(dict(pokemon), "\n")

## ... agregações ...

# - IMC médio ( Peso / Altura * Altura ) por tipo de Pokémon

# "divide" os pokemons por types
"""
unwind = {"$unwind": "$Types"}

group = { "$group": {
  "_id": "$Types",
  "IMC (avg)": {"$avg": {"$divide": ["$Weight(kg)", {"$multiply": ["$Size(m)", "$Size(m)"]}]}}
}}

pipeline = [unwind, group]
"""

# - Cor(Pokedex?) mais comum por tipo de Pokémon
"""
unwind = {"$unwind": "$Types"}

group = {
  "$group": {
    "_id": {
      "type": "$Types",
      "Color": "$PokedexColor"
    },
    "Count": {"$sum": 1}
  }
}

notNull = {"$match":
{"_id.Color": {"$exists": True}}
}

sort = {"$sort": {"_id": -1}}

group2 = {
  "$group": {
    "_id": "$_id.type",
    "ColorCount": {"$push": {"Color": "$_id.Color", "Count": "$Count"}}
    
  }
}

proj = {"$project": 
{"max": {
    "$filter": {
    "input": "$ColorCount",
    "as": "color",
    "cond": {"$eq": ["$$color.Count", {"$max": "$ColorCount.Count"}]}}
  }}
}

pipeline = [unwind, group, notNull, sort, group2, proj]
"""
# - Dano médio de cada tipo por tipo. (Exemplo. Fogo recebe na média 2.5 de gelo)
""""
unwind = {"$unwind": "$Types"}

group = {"$group": {
  "_id": "$Types",
  "(avg)NormalDamage": {"$avg": "$Damages.Normal"},
  "(avg)FireDamage": {"$avg": "$Damages.Fire"},
  "(avg)WaterDamage": {"$avg": "$Damages.Water"},
  "(avg)ElectricDamage": {"$avg": "$Damages.Electric"},
  "(avg)GrassDamage": {"$avg": "$Damages.Grass"},
  "(avg)IceDamage": {"$avg": "$Damages.Ice"},
  "(avg)FightingDamage": {"$avg": "$Damages.Fighting"},
  "(avg)PoisonDamage": {"$avg": "$Damages.Poison"},
  "(avg)GroundDamage": {"$avg": "$Damages.Ground"},
  "(avg)FlyingDamage": {"$avg": "$Damages.Flying"},
  "(avg)PsychcDamage": {"$avg": "$Damages.Psychc"},
  "(avg)BugDamage": {"$avg": "$Damages.Bug"},
  "(avg)RockDamage": {"$avg": "$Damages.Rock"},
  "(avg)GhostDamage": {"$avg": "$Damages.Ghost"},
  "(avg)DragonDamage": {"$avg": "$Damages.Dragon"},
  "(avg)DarkDamage": {"$avg": "$Damages.Dark"},
  "(avg)SteelDamage": {"$avg": "$Damages.Steel"},
  "(avg)FairyDamage": {"$avg": "$Damages.Fairy"},
}}

pipeline = [unwind, group]
"""

# - Quais Pokémon no mínimo dobram seu Peso na evolução seguinte (atenção a modelagem da evolução no documento)
"""
unwind = {"$unwind": "$Evolutions"}

group = {
  "$group": {
    "_id": "$Number",
    "Weight": {"$sum": "$Weight(kg)"},
    "FirstEvolution": {"$first": "$Evolutions"}
  }
}

lookup = {
  "$lookup": {
    "from": "pokemons",
    "localField": "FirstEvolution",
    "foreignField": "Number",
    "as": "Evolution"
  }
}

unwind2 = {"$unwind": "$Evolution"}

match = {
  "$match": {
    "$expr": {"$gte": ["$Evolution.Weight(kg)", {"$multiply": ["$Weight", 2]}]}
  }
}

pipeline = [unwind, group, lookup, unwind2, match]
"""

# - Quais Pokémon mudam de tipo na sua evolução (Exemplo: 1 - Água , 2 - Água e Gelo)
"""
unwind = {"$unwind": "$Evolutions"}

group = {
  "$group": {
    "_id": "$Number",
    "Type": {"$addToSet": "$Types"},
    "FirstEvolution": {"$first": "$Evolutions"}
  }
}

lookup = {
  "$lookup": {
    "from": "pokemons",
    "localField": "FirstEvolution",
    "foreignField": "Number",
    "as": "Evolution"
  }
}

unwind2 = {"$unwind": "$Evolution"}
unwind3 = {"$unwind": "$Type"}

add = {
  "$addFields": {
    "aEq": {"$eq": ["$Evolution.Types", "$Type"]}
  }
}

match = {
  "$match": {
    "aEq": False
  }
}

pipeline = [unwind, group, lookup, unwind2, unwind3, add, match]
"""

#for pokemon in db.pokemons.aggregate(pipeline):
#  print(pokemon, "\n")
