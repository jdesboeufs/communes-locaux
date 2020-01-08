#!/usr/bin/env node
const {Transform} = require('stream')
const {join} = require('path')
const {chain, keyBy} = require('lodash')
const getStream = require('get-stream')
const {outputJson} = require('fs-extra')
const {parse} = require('@etalab/majic')
const communes = require('@etalab/decoupage-administratif/data/communes.json')
  .filter(c => ['arrondissement-municipal', 'commune-actuelle'].includes(c.type))

const communesIndex = keyBy(communes, 'code')

const ACCEPTED_CATEGORIES_LOCAUX = [
  'maison',
  'appartement',
  'commerce-sans-boutique',
  'divers',
  'commerce-boutique',
  'port-de-plaisance',
  'site-industriel',
  'gare'
]

function groupByCommune() {
  const context = {}

  return new Transform({
    transform(row, enc, done) {
      if (row.codeCommune !== context.currentCommune) {
        if (context.currentCommune) {
          this.push({
            codeCommune: context.currentCommune,
            locaux: context.locaux
          })
        }

        context.currentCommune = row.codeCommune
        context.locaux = []
      }

      context.locaux.push(row)

      done()
    },

    flush(done) {
      this.push({
        codeCommune: context.currentCommune,
        locaux: context.locaux
      })

      done()
    },

    objectMode: true
  })
}

async function main() {
  const communesLocaux = await getStream.array(
    process.stdin
      .pipe(parse({profile: 'simple'}))
      .pipe(groupByCommune())
      .pipe(new Transform({
        transform({codeCommune, locaux}, enc, done) {
          const commune = communesIndex[codeCommune]

          if (!commune) {
            return done()
          }

          const acceptedLocaux = locaux.filter(l => ACCEPTED_CATEGORIES_LOCAUX.includes(l.categorieLocal))
          const nbAdressesUniques = chain(acceptedLocaux).map(l => `${l.codeVoie}-${l.numero}`).uniq().value().length

          const communeLocaux = {
            codeCommune,
            nomCommune: commune.nom,
            population: commune.population,
            nbLocaux: acceptedLocaux.length,
            nbAdressesUniques
          }

          if (commune.population) {
            communeLocaux.adressesRatio = Math.round(nbAdressesUniques / commune.population * 1000)
          }

          done(null, communeLocaux)
        },
        objectMode: true
      }))
  )

  const filePath = join(__dirname, 'dist', 'communes-locaux.json')

  await outputJson(filePath, communesLocaux)
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
