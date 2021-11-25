import os, csv
from pymongo import MongoClient

#pip install "pymongo[srv]"

#https://crivelaro.notion.site/Agregando-informa-es-de-Pok-mons-cdfd27c240e5494d9eca07081cecad7d

## INSERÇÃO

client = MongoClient(os.environ['MONGO_URL'])
db = client.cluster

n_pokemons = 0
with open('pokemonData.csv', newline='') as data:
  #lê o nome das 'colunas'
  reader = csv.DictReader(data)
  headers = reader.fieldnames
  #print(headers, "\n")
  #input()

  #lê cada pokemon e insere no banco
  reader = csv.reader(data)
  for pokemon in reader:
    pokemon_dict = {} #informações do pokemon ficam aqui
    
    ## adiciona ao documento:

    pokemon_dict[headers[0]] = pokemon[0] #number   
    pokemon_dict[headers[1]] = pokemon[1] #name
    
    if pokemon[2]: #se tiver
      pokemon_dict[headers[2]] = float(pokemon[2]) #size(m)

    if pokemon[3]: #se tiver
      pokemon_dict[headers[3]] = float(pokemon[3]) #weight(lb)

    if pokemon[4]: #se tiver
      pokemon_dict[headers[4]] = float(pokemon[4]) #weight(kg)

    if pokemon[5]: #se tiver
      pokemon_dict[headers[5]] = pokemon[5].replace(" ","") #pokedexColor

    tipos = [] #lista de tipos
    if pokemon[6]: #se tiver
      tipos.append(pokemon[6]) #Type1
    if pokemon[7]: #se tiver
      tipos.append(pokemon[7]) #Type2
    
    if tipos: #se a lista conter item
      pokemon_dict["Types"] = tipos

    if pokemon[8] and pokemon[8] != "[]": #se tiver
      #... tratamento ...
      pokemon[8] = pokemon[8].replace("[","")
      pokemon[8] = pokemon[8].replace("]","")
      pokemon[8] = pokemon[8].replace(" ","")

      evolucoes = pokemon[8].split(',') #lista de evolucoes
      pokemon_dict["Evolutions"] = evolucoes

    pokemon_dict["Damages"] = {} #dict dos danos
    for i in range(9,27): #percorre todos os danos
      if pokemon[i]: #se tiver
        pokemon_dict["Damages"][headers[i].replace("Damage","")] = float(pokemon[i].replace("*","")) 
  
    db.pokemons.insert_one(pokemon_dict).inserted_id
    print(pokemon_dict[headers[1]], "Added.")
    n_pokemons += 1
    #break

print("\na total of", n_pokemons, "added.")