const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const esClient = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'

async function run() {

  try{
    await esClient.indices.delete({
      index : heroesIndexName
    });
  } catch (error) {
    console.log("Index can't be deleted. Error : " + error.message);
  }

  await esClient.indices.create({ index: heroesIndexName });

  await esClient.indices.putMapping({
    index: heroesIndexName,
    body: {
      properties: {
        "suggest" : {
          type: 'completion'
        }
      }
    }
  });

  let heroes = [];
  // Read CSV file
  fs.createReadStream("all-heroes.csv")
    .pipe(
      csv({
        separator: ","
      })
    )
    .on("data", data => {
      heroes.push({
        "id": data.id,
        "name": data.name,
        "description": data.description,
        "imageUrl": data.imageUrl,
        "backgroundImageUrl": data.backgroundImageUrl,
        "externalLink": data.externalLink,
        "secretIdentities": data.secretIdentities,
        "birthPlace": data.birthPlace,
        "occupation": data.occupation,
        "aliases": data.aliases,
        "alignment": data.alignment,
        "firstAppearance": data.firstAppearance,
        "yearAppearance": data.yearAppearance,
        "universe": data.universe,
        "gender": data.gender,
        "race": data.race,
        "type": data.type,
        "height": data.height,
        "weight": data.weight,
        "eyeColor": data.eyeColor,
        "hairColor": data.hairColor,
        "teams": data.teams,
        "powers": data.powers,
        "partners": data.partners,
        "intelligence": data.intelligence,
        "strength": data.strength,
        "speed": data.speed,
        "durability": data.durability,
        "power": data.power,
        "combat": data.combat,
        "creators": data.creators,
        "suggest": [
          { "input": data.name, "weight": 10 },
          { "input": data.aliases, "weight": 5 },
          { "input": data.secretIdentities, "weight": 5 }
        ]
      });
    })
    .on("end", async () => {
      while(heroes.length) {
        try {
          let response = await esClient.bulk(createBulkInsertQuery(heroes.splice(0,20000)));
          console.log(`Inserted ${response.body.items.length} heroes`);
        } catch (error) {
          console.trace(error)
        }
      }
    });
}


run().catch(console.error);

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(heroes) {
  const body = heroes.reduce((acc, hero) => {
    acc.push({ index: { _index: heroesIndexName, _type: '_doc', _id: hero.id } })
    acc.push(hero)
    return acc
  }, []);

  return { body };
}


