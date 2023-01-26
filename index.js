const csvParser = require('csv-parser')
const fs = require('fs')
const mongoose = require('mongoose')
const _ = require('lodash')
const { Parser } = require('@json2csv/plainjs')
const json2csv = new Parser()
const moment = require('moment')
const unitsRentedData = require('./units_rented.json')

mongoose.set('debug', true)

const RealPropertyLegal = mongoose.model('RealPropertyLegal', new mongoose.Schema({
  'STREET NUMBER': String,
  'STREET NAME': String,
  'DOCUMENT ID': Number,
  'BLOCK': Number,
  'LOT': Number,
  'UNIT': String
}))

const RealPropertyParty = mongoose.model('RealPropertyParty', new mongoose.Schema({
  'DOCUMENT ID': Number,
  'RECORD TYPE': String,
  'PARTY TYPE': Number,
  'NAME': String
}))

const RealPropertyMaster = mongoose.model('RealPropertyMaster', new mongoose.Schema({
  'DOCUMENT ID': Number,
  'RECORDED / FILED': Date,
  'DOC': {
    ' AMOUNT': Number
  }
}))

async function makeModel (fileName) {
  const modelName = _.chain(fileName).camelCase().upperFirst().value()
  let model = null
  fs.createReadStream(`data/${fileName}.csv`)
    .pipe(csvParser({
      mapHeaders: ({ header }) => _.snakeCase(header)
    }))
    .on('headers', (data) => {
      const schema = {}
      _.map(data, (f) => {
        schema[f] = String
      })

      console.log(schema)

      model = mongoose.model(modelName, new mongoose.Schema(schema))
    })
    .on('data', (data) => {
      model.create(data)
    })
}

const getUnitType = function (floor, unit) {
  if (floor >= 15 && floor <= 16) {
    if ('AEFL'.includes(unit)) {
      return '2/2'
    } else if ('BCD'.includes(unit)) {
      return '1/1'
    } else if ('G'.includes(unit)) {
      return '0/1'
    }
  } else if (floor >= 17 && floor <= 38) {
    if ('AEFL'.includes(unit)) {
      return '2/2'
    } else if ('BCDHJK'.includes(unit)) {
      return '1/1'
    } else if ('G'.includes(unit)) {
      return '0/1'
    }
  } else if (floor >= 41 && floor <= 68) {
    if ('ABCH'.includes(unit)) {
      return '2/2'
    } else if ('GJF'.includes(unit)) {
      return '1/1'
    } else if ('E'.includes(unit)) {
      return '3/2'
    } else if ('D'.includes(unit)) {
      return '3/3'
    }
  }
}

async function main () {
  await mongoose.connect('mongodb://127.0.0.1:27017/acris')

  const legals = await RealPropertyLegal.find({
    BLOCK: 149,
    LOT: 1102
  })

  const parties = await RealPropertyParty.find({
    'DOCUMENT ID': {
      $in: _.map(legals, 'DOCUMENT ID')
    }
  })

  const masters = await RealPropertyMaster.find({
    'DOCUMENT ID': {
      $in: _.map(legals, 'DOCUMENT ID')
    }
  })

  const legalsByUnitId = _.groupBy(legals, 'UNIT')
  const partiesByDocId = _.groupBy(parties, 'DOCUMENT ID')
  const mastersByDocId = _.groupBy(masters, 'DOCUMENT ID')

  const units = _.chain(legals)
    .map('UNIT')
    .uniq()
    .sortBy()
    .value()

  // console.log(mastersByDocId)

  const data = _.map(units, (unit) => {
    const docIds = _.map(legalsByUnitId[unit], 'DOCUMENT ID')

    const masters = _.chain(docIds)
      .map((docId) => {
        return mastersByDocId[docId]
      })
      .flatten()
      .value()

    const parties = _.chain(docIds)
      .map((docId) => {
        return partiesByDocId[docId]
      })
      .flatten()
      .value()

    const amount = _.chain(masters)
      .map('DOC')
      .map(' AMOUNT')
      .compact()
      .first()
      .value()

    let date = _.chain(masters)
      .map('RECORDED / FILED')
      .sort()
      .first()
      .value()

    date = moment(date).format('MM/DD/YY')

    const buyers = _.chain(parties)
      .reject({
        NAME: '138 WILLOUGHBY LLC'
      })
      .map('NAME')
      .uniq()
      .value()

    const isLLC = _.chain(buyers)
      .map(b => b.includes('LLC') || b.includes('LTD') || b.includes('TRUST'))
      .some()
      .value()

    const floor = unit.slice(0, 2)

    const line = unit.substr(-1)

    const unitType = getUnitType(floor, line) || ''
    const [beds, baths] = unitType.split('/')

    const rented = _.includes(unitsRentedData, unit)

    return {
      unit,
      floor,
      line,
      beds,
      baths,
      date,
      amount: Math.round(amount / 10000) / 100,
      isLLC,
      rented,
      buyers
    }
  })

  console.log(data)
  console.log({
    unitsSold: data.length,
    llcs: _.filter(data, { isLLC: true }).length
  })


  const csvData = await json2csv.parse(data)
  await fs.promises.writeFile('report.csv', csvData)

  // console.log(parties)

  // console.log(legals)
}

main()
