import irregularPlurals_ from './lang/plurals/irregular-plurals-en.json' with {type: 'json'};

const irregularPlurals = new Map(Object.entries(irregularPlurals_));

export default irregularPlurals;
