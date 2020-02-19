var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

const insertHeroes = (db, callback) => {
    const collection = db.collection(collectionName);

    const heroes = [];
    fs.createReadStream('all-heroes.csv')
        .pipe(csv())
        // Pour chaque ligne on créé un document JSON pour le héros correspondant
        .on('data', data => {
            heroes.push({
                id : data.id,
                name : data.name,
                imageUrl : data.imageUrl,
                backgroundImageUrl : data.backgroundImageUrl,
                externalLink : data.externalLink,
                description : data.description,
                teams : data.teams.split(','),
                powers : data.powers.split(','),
                partners : data.partners.split(','),
                creators : data.creators.split(','),
                appearance : {
                    gender : data.gender,
                    type : data.type,
                    race : data["race"],
                    height : data.height,
                    weight : data.weight,
                    eyeColor : data.eyeColor,
                    hairColor : data.hairColor
                },
                identity : {
                    secretIdentities : data.secretIdentities.split(','),
                    birthPlace : data.birthPlace,
                    occupation : data.occupation,
                    aliases : data.aliases.split(','),
                    alignment : data.alignment,
                    firstAppearance : data.firstAppearance,
                    yearAppearance : data.yearAppearance,
                    universe : data.universe
                },
                skills : {
                    intelligence : data.intelligence,
                    strength : data.strength,
                    speed : data.speed,
                    durability : data.durability,
                    combat : data.combat,
                    power : data.power
                }
            });
        })
        // A la fin on créé l'ensemble des acteurs dans MongoDB
        .on('end', () => {
            collection.insertMany(heroes, (err, result) => {
                callback(result);
            });
        });
}

MongoClient.connect(mongoUrl, (err, client) => {
    if (err) {
        console.error(err);
        throw err;
    }
    const db = client.db(dbName);
    insertHeroes(db, result => {
        console.log(`${result.insertedCount} heroes inserted`);
        client.close();
    });
});
