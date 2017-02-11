// Trier les quartiers en fonction du nombre d'incidents dans les parcs
// Manhattan 59, Brooklyn 52, Bronx 42, Queens 29, Staten Island 3, Brooklyn/Queens 2
use park_crimes
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$total'}}},{$sort :{total:1}}])

// Mais qu'en est-il si on distingue le type d'accident ? 
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$murder'}}},{$sort :{total:1}}])
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$rape'}}},{$sort :{total:1}}])
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$robbery'}}},{$sort :{total:1}}]) // Brooklyn, Manhattan et le Bronx présentent beaucoup de vol
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$felony_assault'}}},{$sort :{total:1}}])
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$burglary'}}},{$sort :{total:1}}])
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$grand_larceny'}}},{$sort :{total:1}}])
db.park_crimes.aggregate([{$group:{_id:'$borough',total:{$sum:'$grand_larceny_of_motor_vehicle'}}},{$sort :{total:1}}])

// Mais pas trop d'information
// Trier les quartiers en fonction du nombre de monuments dans les parcs
// Manhattan 946, Brooklyn 349, Queens 249, Bronx 222, Staten Island 96, inconnu 7
use park_monuments
db.park_monuments.aggregate([{$group:{_id:'$borough',total:{$sum:1}}},{$sort :{total:-1}}])

// Manhattan et Brooklyn sont des quartiers bien plus anciens et donc avec bien plus d'histoire. Malheureusement, ce sont aussi les quartiers avec le plus d'incidents...
// On peut s'intéresser à d'autres critères pour chercher le meilleur quartier, comme par exemple : les marchés de producteurs !
use farmers_market
db.farmers_market.mapReduce(function(){emit(this.borough, 1);},function(key,values){return Array.sum(values)},{out:{inline:1}})

// Ainsi, on peut voir que Brooklyn (48) est en tête avec le plus grand nombre de marchés, suivi de Manhattan (39) puis du Bronx (32).
// Qu'en est-il des infrastructures culturelles ? 
use cultural
db.cultural.aggregate([{$group:{_id:{borough:'$borough',discipline:'$discipline'},total:{$sum:1}}},{$sort :{total:-1}}])

// Manhattan se détache très largement du lot ici avec 277 théatres, 206 espaces de musique, 136 espaces de danse... Tandis que Brooklyn a une grande partie de complexes multi-activités (64). 
// Nottons aussi les très nombreux musées (58) et beaucoup d'activités liées aux films (57) de Manhattan.
// Pour finir, on va compter le nombre d'hopitaux par quartiers, parce que oui, ça compte !

use hospital_health
db.hospital_health.aggregate([{$group:{_id:'$borough',total:{$sum:1}}},{$sort :{total:-1}}])

// Ainsi, on peut voir que Brooklyn a le plus d'hopitaux (26), suivi de Manhattan (24).
// Finalement, je pense que le meilleur endroit où vivre, c'est Brooklyn (si on aime les quartiers avec des choses à voir). On pourra ainsi accéder à de nombreuses activités, monuments, marchés... Autrement dit de quoi s'occuper ! Le deuxième choix évident est Manhattan,  il faudrait par exemple comparer les prix pour savoir lequel est le plus intéressant.  

// Regardons les loyers moyens à Brooklyn et Manhattan
use development
db.development.aggregate([{$match:{$or:[{borough:'MANHATTAN'},{borough:'BROOKLYN'}]}},{$group:{_id:'$borough',moyenne:{$avg:'$avg_rent'}}},{$sort :{total:-1}}])

// Brooklyn est légèrement plus cher que Manhattan ($483.593861386139 en moyenne contre $482.902788461538). 
// Et la population totale par rapport aux données que l'on a
db.development.aggregate([{$match:{$or:[{borough:'MANHATTAN'},{borough:'BROOKLYN'}]}},{$group:{_id:'$borough',total:{$sum:'$total_pop'}}},{$sort :{total:-1}}])

// Brooklyn est un peu plus peuplée que Manhattan (sur les données utilisées) :  130364 habitants contre 116327 habitants.
// Mais y a t-il plus d'appartements à Brooklyn ou à Manhattan ?
db.development.aggregate([{$match:{$or:[{borough:'MANHATTAN'},{borough:'BROOKLYN'}]}},{$group:{_id:'$borough',total:{$sum:'$total_number_apartments'}}},{$sort :{total:-1}}])

// Le résultat est celui auquel on pouvait s'attendre : Brooklyn propose plus d'appartements que Manhattan (51745 contre 50303).
// Pour conclure, je préfère aller vivre à Manhattan, qui propose plus d'activités et des loyers un tout petit moins plus chers.

// Regardons donc où vivre à Manhattan, en se basant sur les évènements disponibles dans la ville
use events
db.events.aggregate([{$match:{borough:'Manhattan'}},{$group:{_id:{address:'$address'},total:{$sum:1}}},{$sort :{total:-1}}])

// Je pense donc que j'irais vivre à Manhattan, dans une des rues le plus vivantes retournées par la requête précedente. 

